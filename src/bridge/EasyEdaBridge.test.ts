import { createServer, type Server as HttpServer } from "node:http";
import { afterEach, describe, expect, it } from "vitest";
import WebSocket from "ws";
import { EasyEdaBridge } from "./EasyEdaBridge.js";
import { BridgeProtocolCompatibilityError, BridgeRpcError, BridgeUnavailableError } from "./errors.js";

const bridges: EasyEdaBridge[] = [];
const httpServers: HttpServer[] = [];

afterEach(async () => {
  await Promise.all(bridges.splice(0).map((bridge) => bridge.stop()));
  await Promise.all(
    httpServers.splice(0).map(
      (server) =>
        new Promise<void>((resolve) => {
          if (!server.listening) {
            resolve();
            return;
          }
          server.closeAllConnections?.();
          server.close(() => resolve());
        })
    )
  );
});

describe("EasyEdaBridge", () => {
  it("reports disconnected status before an extension connects", async () => {
    const bridge = await startBridge();

    expect(bridge.getStatus().connected).toBe(false);
    await expect(bridge.call("getContext")).rejects.toBeInstanceOf(BridgeUnavailableError);
  });

  it("round-trips RPC calls through a WebSocket extension client", async () => {
    const bridge = await startBridge();
    const client = await connectClient(bridge.endpoint);
    client.on("message", (data) => {
      const message = JSON.parse(data.toString());
      client.send(
        JSON.stringify({
          kind: "result",
          requestId: message.requestId,
          result: {
            method: message.method,
            params: message.params
          }
        })
      );
    });

    const result = await bridge.call("findComponent", { query: "U1" });

    expect(result).toEqual({
      method: "findComponent",
      params: { query: "U1" }
    });
    client.close();
  });

  it("updates status from hello messages", async () => {
    const bridge = await startBridge();
    const client = await connectClient(bridge.endpoint);
    client.send(
      JSON.stringify({
        kind: "hello",
        client: "easyeda-pro-extension",
        version: "0.1.0",
        protocolVersion: "0.1.0",
        capabilities: {
          websocket: true,
          pcbDocument: true
        },
        status: {
          documentName: "main pcb"
        }
      })
    );

    await wait(20);

    expect(bridge.getStatus()).toMatchObject({
      connected: true,
      extensionVersion: "0.1.0",
      documentName: "main pcb"
    });
    client.close();
  });

  it("blocks calls when the extension protocol is incompatible", async () => {
    const bridge = await startBridge();
    const client = await connectClient(bridge.endpoint);
    client.send(
      JSON.stringify({
        kind: "hello",
        client: "easyeda-pro-extension",
        version: "0.1.0",
        protocolVersion: "9.9.9",
        capabilities: {
          websocket: true
        }
      })
    );

    await wait(20);

    await expect(bridge.call("getContext")).rejects.toBeInstanceOf(BridgeProtocolCompatibilityError);
    client.close();
  });

  it("turns remote errors into BridgeRpcError", async () => {
    const bridge = await startBridge();
    const client = await connectClient(bridge.endpoint);
    client.on("message", (data) => {
      const message = JSON.parse(data.toString());
      client.send(
        JSON.stringify({
          kind: "error",
          requestId: message.requestId,
          error: {
            code: "api_unavailable",
            message: "missing EasyEDA API"
          }
        })
      );
    });

    await expect(bridge.call("zoomBoard")).rejects.toBeInstanceOf(BridgeRpcError);
    client.close();
  });

  it("allows a newer extension connection to replace the old one", async () => {
    const bridge = await startBridge();
    const oldClient = await connectClient(bridge.endpoint);
    const newClient = await connectClient(bridge.endpoint);
    newClient.on("message", (data) => {
      const message = JSON.parse(data.toString());
      newClient.send(
        JSON.stringify({
          kind: "result",
          requestId: message.requestId,
          result: "new-client"
        })
      );
    });

    await wait(20);

    await expect(bridge.call("getContext")).resolves.toBe("new-client");
    oldClient.close();
    newClient.close();
  });

  it("rejects pending calls when a newer extension connection replaces the old one", async () => {
    const bridge = await startBridge();
    const oldClient = await connectClient(bridge.endpoint);
    const pendingCall = bridge.call("getContext");
    pendingCall.catch(() => undefined);

    await wait(20);
    const newClient = await connectClient(bridge.endpoint);
    newClient.on("message", (data) => {
      const message = JSON.parse(data.toString());
      newClient.send(
        JSON.stringify({
          kind: "result",
          requestId: message.requestId,
          result: "new-client"
        })
      );
    });

    await expect(pendingCall).rejects.toBeInstanceOf(BridgeUnavailableError);
    oldClient.close();
    newClient.close();
  });

  it("falls back to default host and port when env vars are blank strings", () => {
    const prevHost = process.env.EASYEDA_MCP_WS_HOST;
    const prevPort = process.env.EASYEDA_MCP_WS_PORT;
    try {
      process.env.EASYEDA_MCP_WS_HOST = "   ";
      process.env.EASYEDA_MCP_WS_PORT = "";
      const bridge = new EasyEdaBridge();
      expect(bridge.endpoint).toBe("ws://127.0.0.1:8765");
    } finally {
      if (prevHost === undefined) delete process.env.EASYEDA_MCP_WS_HOST;
      else process.env.EASYEDA_MCP_WS_HOST = prevHost;
      if (prevPort === undefined) delete process.env.EASYEDA_MCP_WS_PORT;
      else process.env.EASYEDA_MCP_WS_PORT = prevPort;
    }
  });

  it("falls back to default port when EASYEDA_MCP_WS_PORT is invalid and honors valid values", () => {
    const prevPort = process.env.EASYEDA_MCP_WS_PORT;
    try {
      for (const invalid of ["not-a-number", "-1", "65536", "8765.5", "Infinity"]) {
        process.env.EASYEDA_MCP_WS_PORT = invalid;
        const bridge = new EasyEdaBridge({ host: "127.0.0.1" });
        expect(bridge.endpoint).toBe("ws://127.0.0.1:8765");
      }

      process.env.EASYEDA_MCP_WS_PORT = "9001";
      expect(new EasyEdaBridge({ host: "127.0.0.1" }).endpoint).toBe("ws://127.0.0.1:9001");

      process.env.EASYEDA_MCP_WS_PORT = "0";
      expect(new EasyEdaBridge({ host: "127.0.0.1" }).endpoint).toBe("ws://127.0.0.1:0");
    } finally {
      if (prevPort === undefined) delete process.env.EASYEDA_MCP_WS_PORT;
      else process.env.EASYEDA_MCP_WS_PORT = prevPort;
    }
  });

  it("formats IPv6 literal hosts cleanly in endpoint", () => {
    const ipv6Bridge = new EasyEdaBridge({ host: "::1", port: 8765 });
    expect(ipv6Bridge.endpoint).toBe("ws://[::1]:8765");

    const bracketedBridge = new EasyEdaBridge({ host: "[::1]", port: 8765 });
    expect(bracketedBridge.endpoint).toBe("ws://[::1]:8765");
  });

  it("attaches to an external http.Server, resolves dynamic port, handles hello, RPC calls, and remote errors", async () => {
    const httpServer = createTrackedHttpServer((_req, res) => {
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ ok: true }));
    });
    const bridge = createSilentBridge({ host: "127.0.0.1", port: 0 });

    bridge.attachToHttpServer(httpServer);
    // Endpoint falls back cleanly before listen() without throwing noServer errors
    expect(bridge.endpoint).toBe("ws://127.0.0.1:0");

    const boundPort = await listenHttpServer(httpServer);
    expect(bridge.endpoint).toBe(`ws://127.0.0.1:${boundPort}`);

    const client = await connectClient(bridge.endpoint);
    try {
      client.send(
        JSON.stringify({
          kind: "hello",
          client: "easyeda-pro-extension",
          version: "0.5.1",
          protocolVersion: "0.1.0",
          capabilities: {
            websocket: true,
            pcbDocument: true
          },
          status: {
            documentName: "attached-pcb"
          }
        })
      );

      await wait(20);

      expect(bridge.getStatus()).toMatchObject({
        connected: true,
        connectionState: "connected",
        extensionVersion: "0.5.1",
        documentName: "attached-pcb"
      });

      client.on("message", (data) => {
        const message = JSON.parse(data.toString());
        if (message.method === "failingMethod") {
          client.send(
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
          return;
        }
        client.send(
          JSON.stringify({
            kind: "result",
            requestId: message.requestId,
            result: {
              method: message.method,
              params: message.params
            }
          })
        );
      });

      const result = await bridge.call("getContext", { includeSelection: true });
      expect(result).toEqual({
        method: "getContext",
        params: { includeSelection: true }
      });

      await expect(bridge.call("failingMethod")).rejects.toSatisfy(
        (err: unknown) =>
          err instanceof BridgeRpcError &&
          err.code === "api_unavailable" &&
          err.message === "missing EasyEDA API" &&
          JSON.stringify(err.details) === JSON.stringify({ feature: "pcb" })
      );
    } finally {
      client.close();
    }
  });

  it("removes the upgrade listener on stop() without closing the external http.Server", async () => {
    const httpServer = createTrackedHttpServer((_req, res) => {
      res.writeHead(200, { "content-type": "text/plain" });
      res.end("still-alive");
    });
    const boundPort = await listenHttpServer(httpServer);
    const bridge = createSilentBridge({ host: "127.0.0.1", port: 0 });

    expect(httpServer.listenerCount("upgrade")).toBe(0);
    bridge.attachToHttpServer(httpServer);
    expect(httpServer.listenerCount("upgrade")).toBe(1);

    const client = await connectClient(bridge.endpoint);
    const pendingCall = bridge.call("getContext");
    pendingCall.catch(() => undefined);

    await bridge.stop();

    await expect(pendingCall).rejects.toBeInstanceOf(BridgeUnavailableError);
    expect(httpServer.listenerCount("upgrade")).toBe(0);
    expect(httpServer.listening).toBe(true);
    expect(bridge.getStatus().connected).toBe(false);

    const response = await fetch(`http://127.0.0.1:${boundPort}/`);
    expect(response.status).toBe(200);
    expect(await response.text()).toBe("still-alive");

    client.close();
  });

  it("is idempotent when attachToHttpServer is called twice with the same server and throws if already active", async () => {
    const serverA = createTrackedHttpServer();
    const serverB = createTrackedHttpServer();

    const attachedBridge = createSilentBridge({ host: "127.0.0.1", port: 0 });
    attachedBridge.attachToHttpServer(serverA);
    attachedBridge.attachToHttpServer(serverA);
    expect(serverA.listenerCount("upgrade")).toBe(1);

    expect(() => attachedBridge.attachToHttpServer(serverB)).toThrow(
      "EasyEdaBridge is already started or attached to an HTTP server."
    );

    const startedBridge = await startBridge();
    expect(() => startedBridge.attachToHttpServer(serverB)).toThrow(
      "EasyEdaBridge is already started or attached to an HTTP server."
    );
  });
});

function createSilentBridge(options: { host?: string; port?: number } = {}): EasyEdaBridge {
  const bridge = new EasyEdaBridge({
    ...options,
    logger: {
      error: () => undefined,
      warn: () => undefined,
      info: () => undefined
    }
  });
  bridges.push(bridge);
  return bridge;
}

function createTrackedHttpServer(requestListener?: Parameters<typeof createServer>[0]): HttpServer {
  const server = requestListener ? createServer(requestListener) : createServer();
  httpServers.push(server);
  return server;
}

async function listenHttpServer(server: HttpServer): Promise<number> {
  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      server.off("error", reject);
      resolve();
    });
  });
  const address = server.address();
  if (!address || typeof address === "string") {
    throw new Error("Expected TCP address from HTTP server.");
  }
  return address.port;
}

async function startBridge(): Promise<EasyEdaBridge> {
  const bridge = createSilentBridge({ port: 0 });
  await bridge.start();
  return bridge;
}

async function connectClient(endpoint: string): Promise<WebSocket> {
  const client = new WebSocket(endpoint);
  await new Promise<void>((resolve, reject) => {
    client.once("open", resolve);
    client.once("error", reject);
  });
  return client;
}

async function wait(ms: number): Promise<void> {
  await new Promise((resolve) => setTimeout(resolve, ms));
}
