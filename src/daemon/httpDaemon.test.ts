import { request as httpRequest } from "node:http";
import { afterEach, describe, expect, it } from "vitest";
import { Client, StreamableHTTPClientTransport } from "@modelcontextprotocol/client";
import WebSocket from "ws";
import { parseDaemonCliArgs } from "../daemon.js";
import { PROTOCOL_VERSION } from "../protocol/messages.js";
import { EXTENSION_VERSION, MCP_SERVER_NAME, MCP_SERVER_VERSION } from "../version.js";
import {
  DEFAULT_DAEMON_HOST,
  DEFAULT_DAEMON_MCP_PATH,
  DEFAULT_DAEMON_PORT,
  DEFAULT_MAX_BODY_BYTES,
  resolveDaemonConfig,
  startBridgeDaemon,
  type BridgeDaemonHandle,
  type StartBridgeDaemonOptions
} from "./httpDaemon.js";

const silentLogger = {
  error: () => undefined,
  warn: () => undefined,
  info: () => undefined
};

const activeClients: Client[] = [];
const activeSockets: WebSocket[] = [];
const activeDaemons: BridgeDaemonHandle[] = [];

afterEach(async () => {
  await Promise.allSettled(activeClients.splice(0).map((client) => client.close()));
  for (const socket of activeSockets.splice(0)) {
    if (socket.readyState === WebSocket.OPEN || socket.readyState === WebSocket.CONNECTING) {
      socket.close();
    }
  }
  await Promise.allSettled(activeDaemons.splice(0).map((daemon) => daemon.stop()));
});

describe("Streamable HTTP bridge daemon", () => {
  it("serves health metadata on GET /health and GET / with dynamic port endpoints", async () => {
    const daemon = await startTestDaemon();

    expect(daemon.port).toBeGreaterThan(0);
    expect(daemon.mcpEndpoint).toBe(`http://127.0.0.1:${daemon.port}/mcp`);
    expect(daemon.bridgeEndpoint).toBe(`ws://127.0.0.1:${daemon.port}`);

    for (const path of ["/health", "/"]) {
      const response = await fetch(`http://127.0.0.1:${daemon.port}${path}`);
      expect(response.status).toBe(200);
      expect(response.headers.get("content-type")).toContain("application/json");
      const body = await response.json();
      expect(body).toEqual({
        status: "ok",
        name: MCP_SERVER_NAME,
        version: MCP_SERVER_VERSION,
        extensionVersion: EXTENSION_VERSION,
        transport: "streamable-http",
        bridgeConnected: false,
        connectionState: "disconnected",
        bridgeEndpoint: daemon.bridgeEndpoint,
        mcpEndpoint: daemon.mcpEndpoint
      });
    }
  });

  it("lists all 76 tools and runs status/doctor tools while extension is disconnected", async () => {
    const daemon = await startTestDaemon();
    const client = await connectMcpClient(daemon.mcpEndpoint, "unconnected-client");

    const listed = await client.listTools();
    expect(listed.tools).toHaveLength(76);

    const liveStatus = await client.callTool({ name: "easyeda_live_status", arguments: {} });
    expect(liveStatus.isError).not.toBe(true);
    expect(liveStatus.structuredContent).toMatchObject({
      bridgeEndpoint: daemon.bridgeEndpoint,
      status: {
        connected: false,
        connectionState: "disconnected"
      }
    });

    const doctor = await client.callTool({ name: "easyeda_doctor", arguments: {} });
    expect(doctor.isError).not.toBe(true);
    expect(doctor.structuredContent).toMatchObject({
      doctor: {
        server: {
          name: MCP_SERVER_NAME,
          version: MCP_SERVER_VERSION,
          protocolVersion: PROTOCOL_VERSION
        },
        bridge: {
          endpoint: daemon.bridgeEndpoint
        },
        extension: {
          connected: false,
          connectionState: "disconnected"
        }
      }
    });
  });

  it("serves concurrent tool calls from two separate MCP clients over one shared extension connection", async () => {
    const daemon = await startTestDaemon();
    const extension = await connectMockExtension(daemon.bridgeEndpoint);

    let callCounter = 0;
    extension.on("message", (raw) => {
      const message = JSON.parse(raw.toString()) as { kind: string; requestId: string; method: string };
      if (message.kind !== "call") {
        return;
      }
      callCounter += 1;
      const seq = callCounter;
      setTimeout(() => {
        extension.send(
          JSON.stringify({
            kind: "result",
            requestId: message.requestId,
            result: {
              method: message.method,
              requestId: message.requestId,
              seq,
              documentName: "SharedBoard.epro"
            }
          })
        );
      }, 15);
    });

    const healthResponse = await fetch(`http://127.0.0.1:${daemon.port}/health`);
    const health = (await healthResponse.json()) as { bridgeConnected: boolean; connectionState: string };
    expect(health.bridgeConnected).toBe(true);
    expect(health.connectionState).toBe("connected");

    const [clientA, clientB] = await Promise.all([
      connectMcpClient(daemon.mcpEndpoint, "mcp-client-a"),
      connectMcpClient(daemon.mcpEndpoint, "mcp-client-b")
    ]);

    const [resultA, resultB] = await Promise.all([
      clientA.callTool({ name: "easyeda_get_context", arguments: {} }),
      clientB.callTool({ name: "easyeda_get_context", arguments: {} })
    ]);

    expect(resultA.isError).not.toBe(true);
    expect(resultB.isError).not.toBe(true);
    expect(resultA.structuredContent).toMatchObject({
      result: {
        method: "getContext",
        documentName: "SharedBoard.epro"
      }
    });
    expect(resultB.structuredContent).toMatchObject({
      result: {
        method: "getContext",
        documentName: "SharedBoard.epro"
      }
    });

    const idA = (resultA.structuredContent as { result: { requestId: string } }).result.requestId;
    const idB = (resultB.structuredContent as { result: { requestId: string } }).result.requestId;
    expect(idA).not.toBe(idB);
    expect(callCounter).toBe(2);
  });

  it("propagates structured BridgeRpcError responses from the extension to MCP tool results", async () => {
    const daemon = await startTestDaemon();
    const extension = await connectMockExtension(daemon.bridgeEndpoint);

    extension.on("message", (raw) => {
      const message = JSON.parse(raw.toString()) as { kind: string; requestId: string };
      if (message.kind !== "call") {
        return;
      }
      extension.send(
        JSON.stringify({
          kind: "error",
          requestId: message.requestId,
          error: {
            code: "api_unavailable",
            message: "missing EasyEDA API",
            details: { feature: "pcb" }
          }
        })
      );
    });

    const client = await connectMcpClient(daemon.mcpEndpoint, "error-propagation-client");
    const result = await client.callTool({ name: "easyeda_get_context", arguments: {} });

    expect(result.isError).toBe(true);
    expect(result.structuredContent).toMatchObject({
      error: "api_unavailable",
      message: "missing EasyEDA API",
      retryable: false,
      details: { feature: "pcb" }
    });
    expect(result.content).toEqual([
      {
        type: "text",
        text: expect.stringContaining("api_unavailable")
      }
    ]);
    expect(result.content).toEqual([
      {
        type: "text",
        text: expect.stringContaining("missing EasyEDA API")
      }
    ]);
  });

  it("reflects extension disconnect in /health and returns clean disconnected errors on subsequent calls", async () => {
    const daemon = await startTestDaemon();
    const extension = await connectMockExtension(daemon.bridgeEndpoint);

    const beforeClose = (await (await fetch(`http://127.0.0.1:${daemon.port}/health`)).json()) as {
      bridgeConnected: boolean;
    };
    expect(beforeClose.bridgeConnected).toBe(true);

    const closed = new Promise<void>((resolve) => {
      extension.once("close", () => resolve());
    });
    extension.close();
    await closed;
    await waitForCondition(() => !daemon.bridge.getStatus().connected);

    const afterClose = (await (await fetch(`http://127.0.0.1:${daemon.port}/health`)).json()) as {
      bridgeConnected: boolean;
      connectionState: string;
    };
    expect(afterClose.bridgeConnected).toBe(false);
    expect(afterClose.connectionState).toBe("disconnected");

    const client = await connectMcpClient(daemon.mcpEndpoint, "post-disconnect-client");
    const result = await client.callTool({ name: "easyeda_get_context", arguments: {} });
    expect(result.isError).toBe(true);
    expect(result.structuredContent).toMatchObject({
      error: "bridge_unavailable",
      retryable: true
    });
  });

  it("supports custom mcpPath, returns 404 on unknown paths, 405 on unsupported methods, and 413 on oversized bodies", async () => {
    const daemon = await startTestDaemon({
      mcpPath: "custom/mcp/",
      maxBodyBytes: 128
    });

    expect(daemon.mcpPath).toBe("/custom/mcp");
    expect(daemon.mcpEndpoint).toBe(`http://127.0.0.1:${daemon.port}/custom/mcp`);

    // Default /mcp is not mounted when custom mcpPath is configured
    const defaultPathRes = await fetch(`http://127.0.0.1:${daemon.port}/mcp`, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: "{}"
    });
    expect(defaultPathRes.status).toBe(404);
    expect(await defaultPathRes.json()).toEqual({ error: "not_found" });

    // Unknown path returns 404
    const unknownRes = await fetch(`http://127.0.0.1:${daemon.port}/does-not-exist`);
    expect(unknownRes.status).toBe(404);
    expect(await unknownRes.json()).toEqual({ error: "not_found" });

    // Unsupported HTTP method on MCP endpoint returns 405
    const putRes = await fetch(daemon.mcpEndpoint, {
      method: "PUT",
      headers: { "content-type": "application/json" },
      body: "{}"
    });
    expect(putRes.status).toBe(405);
    expect(await putRes.json()).toMatchObject({ error: "method_not_allowed" });

    // Oversized body returns 413 (declared Content-Length)
    const oversizedBody = JSON.stringify({ data: "x".repeat(256) });
    const tooLargeRes = await fetch(daemon.mcpEndpoint, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        accept: "application/json, text/event-stream"
      },
      body: oversizedBody
    });
    expect(tooLargeRes.status).toBe(413);
    expect(await tooLargeRes.json()).toMatchObject({
      error: "payload_too_large",
      maxBodyBytes: 128
    });

    // Oversized chunked body (no Content-Length header) also returns 413
    const chunkedResult = await new Promise<{ statusCode?: number; body: string }>((resolve, reject) => {
      const req = httpRequest(
        daemon.mcpEndpoint,
        {
          method: "POST",
          headers: {
            "content-type": "application/json",
            "transfer-encoding": "chunked"
          }
        },
        (res) => {
          const chunks: Buffer[] = [];
          res.on("data", (chunk: Buffer) => chunks.push(chunk));
          res.on("end", () => resolve({ statusCode: res.statusCode, body: Buffer.concat(chunks).toString("utf8") }));
        }
      );
      req.on("error", reject);
      req.write("a".repeat(80));
      req.end("b".repeat(80));
    });
    expect(chunkedResult.statusCode).toBe(413);
    expect(JSON.parse(chunkedResult.body)).toMatchObject({
      error: "payload_too_large",
      maxBodyBytes: 128
    });

    // DELETE on stateless MCP endpoint returns 200
    const deleteRes = await fetch(daemon.mcpEndpoint, { method: "DELETE" });
    expect(deleteRes.status).toBe(200);

    // Real MCP client works against the custom mcpPath (within maxBodyBytes -> start a normal-limit daemon)
    const customNormalDaemon = await startTestDaemon({ mcpPath: "/api/easyeda/" });
    const client = await connectMcpClient(customNormalDaemon.mcpEndpoint, "custom-path-client");
    const tools = await client.listTools();
    expect(tools.tools).toHaveLength(76);

    // stop() is idempotent
    await expect(daemon.stop()).resolves.toBeUndefined();
    await expect(daemon.stop()).resolves.toBeUndefined();
  });
});

describe("resolveDaemonConfig", () => {
  it("applies options > env > defaults precedence and falls back on blank env vars", () => {
    expect(resolveDaemonConfig({}, {})).toEqual({
      host: DEFAULT_DAEMON_HOST,
      port: DEFAULT_DAEMON_PORT,
      mcpPath: DEFAULT_DAEMON_MCP_PATH,
      maxBodyBytes: DEFAULT_MAX_BODY_BYTES
    });

    expect(
      resolveDaemonConfig(
        {},
        {
          EASYEDA_MCP_WS_HOST: "   ",
          EASYEDA_MCP_WS_PORT: "  ",
          EASYEDA_MCP_HTTP_PATH: "   "
        }
      )
    ).toEqual({
      host: DEFAULT_DAEMON_HOST,
      port: DEFAULT_DAEMON_PORT,
      mcpPath: DEFAULT_DAEMON_MCP_PATH,
      maxBodyBytes: DEFAULT_MAX_BODY_BYTES
    });

    expect(
      resolveDaemonConfig(
        {},
        {
          EASYEDA_MCP_WS_HOST: "0.0.0.0",
          EASYEDA_MCP_WS_PORT: "9001",
          EASYEDA_MCP_HTTP_PATH: "bridge/mcp/"
        }
      )
    ).toEqual({
      host: "0.0.0.0",
      port: 9001,
      mcpPath: "/bridge/mcp",
      maxBodyBytes: DEFAULT_MAX_BODY_BYTES
    });

    expect(
      resolveDaemonConfig(
        {
          host: "::1",
          port: 0,
          mcpPath: "/override/",
          maxBodyBytes: 2048
        },
        {
          EASYEDA_MCP_WS_HOST: "0.0.0.0",
          EASYEDA_MCP_WS_PORT: "9001",
          EASYEDA_MCP_HTTP_PATH: "/env"
        }
      )
    ).toEqual({
      host: "::1",
      port: 0,
      mcpPath: "/override",
      maxBodyBytes: 2048
    });
  });

  it("rejects invalid ports, reserved paths, and invalid maxBodyBytes", () => {
    expect(() => resolveDaemonConfig({ port: -1 }, {})).toThrow(/port/i);
    expect(() => resolveDaemonConfig({ port: 65536 }, {})).toThrow(/port/i);
    expect(() => resolveDaemonConfig({ port: 8765.5 }, {})).toThrow(/port/i);
    expect(() => resolveDaemonConfig({ port: Number.NaN }, {})).toThrow(/port/i);
    expect(() => resolveDaemonConfig({}, { EASYEDA_MCP_WS_PORT: "not-a-port" })).toThrow(/EASYEDA_MCP_WS_PORT/);
    expect(() => resolveDaemonConfig({}, { EASYEDA_MCP_WS_PORT: "70000" })).toThrow(/EASYEDA_MCP_WS_PORT/);

    expect(() => resolveDaemonConfig({ mcpPath: "/" }, {})).toThrow(/reserved/i);
    expect(() => resolveDaemonConfig({ mcpPath: "///" }, {})).toThrow(/reserved/i);
    expect(() => resolveDaemonConfig({ mcpPath: "/health" }, {})).toThrow(/reserved/i);
    expect(() => resolveDaemonConfig({ mcpPath: "/health/" }, {})).toThrow(/reserved/i);
    expect(() => resolveDaemonConfig({ mcpPath: "health" }, {})).toThrow(/reserved/i);
    expect(() => resolveDaemonConfig({ mcpPath: "" }, {})).toThrow(/path/i);

    expect(() => resolveDaemonConfig({ maxBodyBytes: 0 }, {})).toThrow(/maxBodyBytes/i);
    expect(() => resolveDaemonConfig({ maxBodyBytes: -10 }, {})).toThrow(/maxBodyBytes/i);
  });
});

describe("parseDaemonCliArgs", () => {
  it("parses space-separated and equals-style flags as well as help/version", () => {
    expect(parseDaemonCliArgs([])).toEqual({ help: false, version: false });

    expect(parseDaemonCliArgs(["--host", "0.0.0.0", "--port", "9090", "--path", "/custom"])).toEqual({
      help: false,
      version: false,
      host: "0.0.0.0",
      port: 9090,
      mcpPath: "/custom"
    });

    expect(parseDaemonCliArgs(["--host=127.0.0.1", "--port=0", "--path=/mcp-v2"])).toEqual({
      help: false,
      version: false,
      host: "127.0.0.1",
      port: 0,
      mcpPath: "/mcp-v2"
    });

    expect(parseDaemonCliArgs(["-h"])).toMatchObject({ help: true, version: false });
    expect(parseDaemonCliArgs(["--help"])).toMatchObject({ help: true, version: false });
    expect(parseDaemonCliArgs(["-v"])).toMatchObject({ help: false, version: true });
    expect(parseDaemonCliArgs(["--version"])).toMatchObject({ help: false, version: true });
  });

  it("throws clear errors on unknown options, missing values, and invalid ports", () => {
    expect(() => parseDaemonCliArgs(["--unknown"])).toThrow(/Unknown option/);
    expect(() => parseDaemonCliArgs(["positional"])).toThrow(/Unknown option/);
    expect(() => parseDaemonCliArgs(["--host"])).toThrow(/Missing value for --host/);
    expect(() => parseDaemonCliArgs(["--host="])).toThrow(/Missing value for --host/);
    expect(() => parseDaemonCliArgs(["--host", "--port", "8765"])).toThrow(/Missing value for --host/);
    expect(() => parseDaemonCliArgs(["--port"])).toThrow(/Missing value for --port/);
    expect(() => parseDaemonCliArgs(["--port=abc"])).toThrow(/Invalid value for --port/);
    expect(() => parseDaemonCliArgs(["--port", "99999"])).toThrow(/Invalid value for --port/);
    expect(() => parseDaemonCliArgs(["--path"])).toThrow(/Missing value for --path/);
    expect(() => parseDaemonCliArgs(["--path="])).toThrow(/Missing value for --path/);
  });
});

async function startTestDaemon(overrides: StartBridgeDaemonOptions = {}): Promise<BridgeDaemonHandle> {
  const daemon = await startBridgeDaemon({
    host: "127.0.0.1",
    port: 0,
    env: {},
    logger: silentLogger,
    ...overrides
  });
  activeDaemons.push(daemon);
  return daemon;
}

async function connectMcpClient(endpoint: string, clientName: string): Promise<Client> {
  const client = new Client({ name: clientName, version: "0.0.0" });
  const transport = new StreamableHTTPClientTransport(new URL(endpoint));
  await client.connect(transport);
  activeClients.push(client);
  return client;
}

async function connectMockExtension(endpoint: string): Promise<WebSocket> {
  const socket = new WebSocket(endpoint);
  activeSockets.push(socket);

  await new Promise<void>((resolve, reject) => {
    socket.once("open", resolve);
    socket.once("error", reject);
  });

  socket.send(
    JSON.stringify({
      kind: "hello",
      client: "easyeda-pro-extension",
      version: EXTENSION_VERSION,
      protocolVersion: PROTOCOL_VERSION,
      capabilities: {
        websocket: true,
        pcbDocument: true,
        schDocument: true
      },
      status: {
        projectName: "TestProject",
        documentName: "SharedBoard.epro",
        activeDocumentType: "pcb"
      }
    })
  );

  await wait(25);
  return socket;
}

async function wait(ms: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitForCondition(predicate: () => boolean, timeoutMs = 1_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) {
      return;
    }
    await wait(10);
  }
  expect(predicate()).toBe(true);
}
