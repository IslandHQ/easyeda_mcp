#!/usr/bin/env node
/**
 * Bridge daemon: a single persistent process that serves MCP over Streamable
 * HTTP (/mcp) AND owns the WebSocket bridge to the EasyEDA Pro extension.
 *
 * Multiple agents (Zed, Hermes, etc.) connect to the same HTTP endpoint
 * and share a single EasyEDA Pro extension connection. No port conflicts,
 * no single-agent bottleneck.
 *
 * Architecture:
 *   EasyEDA extension  <--WS upgrade on same HTTP server-->  BridgeDaemon  <--HTTP(/mcp)-->  Agents
 *
 * The HTTP server listens on a single port. WebSocket upgrade requests
 * from the EasyEDA extension are handled by the WS server attached to the
 * same HTTP server. MCP requests come in as regular POST /mcp.
 *
 * Stateless MCP mode: each HTTP request gets a fresh transport + McpServer
 * pair. The bridge state (extension connection, pending calls) is shared
 * across all requests, so tools work regardless of which agent calls them.
 */
import { createServer, IncomingMessage, ServerResponse } from "node:http";
import { randomUUID } from "node:crypto";
import { WebSocketServer, WebSocket } from "ws";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { registerEasyEdaTools } from "./mcp/registerTools.js";
import {
  type ClientToServerMessage,
  createDisconnectedStatus,
  evaluateProtocolCompatibility,
  type EditorStatus,
  parseClientMessage
} from "./protocol/messages.js";
import { BridgeUnavailableError } from "./bridge/errors.js";

const MCP_SERVER_NAME = "easyeda-pro-mcp";
const MCP_SERVER_VERSION = "0.1.0";

async function main(): Promise<void> {
  const host = process.env.EASYEDA_MCP_WS_HOST ?? "127.0.0.1";
  const port = Number(process.env.EASYEDA_MCP_WS_PORT ?? 8765);

  // -- Shared bridge state --
  let extensionSocket: WebSocket | undefined;
  let status: EditorStatus = createDisconnectedStatus();
  const pending = new Map<string, {
    requestId: string;
    resolve: (value: unknown) => void;
    reject: (reason: unknown) => void;
    timer: ReturnType<typeof setTimeout>;
  }>();

  function rejectAll(error: Error): void {
    for (const [, p] of pending) {
      clearTimeout(p.timer);
      p.reject(error);
      pending.delete(p.requestId);
    }
  }

  function handleMessage(message: ClientToServerMessage): void {
    if (message.kind === "hello") {
      const compat = evaluateProtocolCompatibility(message.protocolVersion);
      status = {
        connected: true,
        connectionState: compat.compatible ? "connected" : "blocked",
        extensionVersion: message.version,
        protocolVersion: message.protocolVersion,
        compatibility: compat,
        capabilities: message.capabilities,
        ...message.status,
        message: compat.compatible ? message.status?.message : compat.reason,
        updatedAt: new Date().toISOString()
      };
      return;
    }
    if (message.kind === "status") {
      const reported = message.status.protocolVersion ?? status.protocolVersion;
      const compat = evaluateProtocolCompatibility(reported);
      status = {
        ...status,
        ...message.status,
        connected: true,
        connectionState: compat.compatible ? "connected" : "blocked",
        compatibility: compat,
        message: compat.compatible ? message.status.message ?? status.message : compat.reason,
        updatedAt: new Date().toISOString()
      };
      return;
    }
    if (message.kind === "result") {
      const p = pending.get(message.requestId);
      if (!p) return;
      clearTimeout(p.timer);
      pending.delete(message.requestId);
      p.resolve(message.result);
      return;
    }
    if (message.kind === "error") {
      const p = pending.get(message.requestId);
      if (!p) return;
      clearTimeout(p.timer);
      pending.delete(message.requestId);
      p.reject(new Error(message.error.message));
    }
  }

  // Bridge-like object: shared across all MCP requests
  const bridgeLike = {
    get endpoint(): string { return `ws://${host}:${port}`; },
    getStatus(): EditorStatus { return status; },
    async call(method: string, params?: unknown, timeoutMs = 10_000): Promise<unknown> {
      if (!extensionSocket || extensionSocket.readyState !== WebSocket.OPEN) {
        throw new BridgeUnavailableError();
      }
      const requestId = randomUUID();
      const msg = { kind: "call", requestId, method, params, timeoutMs };
      const response = new Promise<unknown>((resolve, reject) => {
        const timer = setTimeout(() => {
          pending.delete(requestId);
          reject(new Error(`Timed out waiting ${timeoutMs}ms for EasyEDA Pro extension method "${method}".`));
        }, timeoutMs);
        pending.set(requestId, { requestId, resolve, reject, timer });
      });
      extensionSocket.send(JSON.stringify(msg));
      return response;
    }
  };

  // Factory: create a fresh McpServer + transport per HTTP request (stateless)
  function createTransportAndServer(): { transport: StreamableHTTPServerTransport; server: McpServer } {
    const transport = new StreamableHTTPServerTransport({
      sessionIdGenerator: undefined,
    });
    const server = new McpServer({
      name: MCP_SERVER_NAME,
      version: MCP_SERVER_VERSION
    });
    registerEasyEdaTools(server, bridgeLike as never);
    return { transport, server };
  }

  // -- HTTP server --
  const httpServer = createServer(async (req: IncomingMessage, res: ServerResponse) => {
    // MCP endpoint — handles initialize, tools/list, tools/call, notifications
    if (req.method === "POST" && req.url === "/mcp") {
      const chunks: Buffer[] = [];
      for await (const chunk of req) chunks.push(chunk as Buffer);
      const body = JSON.parse(Buffer.concat(chunks).toString("utf-8"));

      // Skip notifications (no id) — just accept them
      if (body.id === undefined || body.id === null) {
        res.writeHead(202);
        res.end();
        return;
      }

      const { transport, server } = createTransportAndServer();

      try {
        await server.connect(transport);
        await transport.handleRequest(req, res, body);
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        console.error(`[easyeda-mcp-daemon] MCP error: ${message}`);
        if (!res.headersSent) {
          res.writeHead(500, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ error: "internal_error", message }));
        }
      } finally {
        await server.close();
      }
      return;
    }

    // Discovery
    if (req.method === "GET" && req.url === "/mcp") {
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({
        name: MCP_SERVER_NAME,
        version: MCP_SERVER_VERSION,
        transport: "streamable-http"
      }));
      return;
    }

    // Health
    if (req.method === "GET" && (req.url === "/" || req.url === "/health")) {
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({
        status: "ok",
        bridgeConnected: status.connected,
        bridgeEndpoint: `ws://${host}:${port}`,
        mcpEndpoint: `http://${host}:${port}/mcp`
      }));
      return;
    }

    res.writeHead(404, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ error: "not_found" }));
  });

  // -- WebSocket server (same HTTP server, upgrade path) --
  const wss = new WebSocketServer({ noServer: true });

  httpServer.on("upgrade", (req: IncomingMessage, socket: import("node:net").Socket, head: Buffer) => {
    wss.handleUpgrade(req, socket, head, (ws: WebSocket) => {
      if (extensionSocket && extensionSocket.readyState === WebSocket.OPEN) {
        extensionSocket.close(1012, "A newer extension connection replaced this one.");
      }
      extensionSocket = ws;
      console.error("[easyeda-mcp-daemon] EasyEDA Pro extension connected");

      ws.on("message", (data) => {
        try {
          handleMessage(parseClientMessage(data.toString()));
        } catch (error) {
          console.warn(`[easyeda-mcp-daemon] Invalid bridge message: ${String(error)}`);
        }
      });

      ws.on("close", () => {
        if (extensionSocket === ws) {
          extensionSocket = undefined;
          status = createDisconnectedStatus("EasyEDA Pro extension disconnected.");
          rejectAll(new BridgeUnavailableError(status.message));
        }
        console.error("[easyeda-mcp-daemon] EasyEDA Pro extension disconnected");
      });
    });
  });

  // -- Start --
  httpServer.listen(port, host, () => {
    console.error(`[easyeda-mcp-daemon] Listening at http://${host}:${port}`);
    console.error(`[easyeda-mcp-daemon] MCP endpoint:  http://${host}:${port}/mcp`);
    console.error(`[easyeda-mcp-daemon] WS bridge:     ws://${host}:${port} (extension connects here)`);
  });

  // Graceful shutdown
  const shutdown = (): void => {
    console.error("[easyeda-mcp-daemon] Shutting down...");
    wss.close();
    httpServer.close(() => process.exit(0));
  };
  process.once("SIGINT", shutdown);
  process.once("SIGTERM", shutdown);
}

main().catch((error) => {
  console.error(`[easyeda-mcp-daemon] Fatal error: ${error instanceof Error ? error.stack ?? error.message : String(error)}`);
  process.exit(1);
});
