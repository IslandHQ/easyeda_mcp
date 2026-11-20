import { createServer, type IncomingMessage, type Server as HttpServer, type ServerResponse } from "node:http";
import { Readable } from "node:stream";
import type { ReadableStream as NodeWebReadableStream } from "node:stream/web";
import { WebStandardStreamableHTTPServerTransport } from "@modelcontextprotocol/server";
import { EasyEdaBridge } from "../bridge/EasyEdaBridge.js";
import { createMcpServer } from "../mcp/server.js";
import { EXTENSION_VERSION, MCP_SERVER_NAME, MCP_SERVER_VERSION } from "../version.js";

export const DEFAULT_DAEMON_HOST = "127.0.0.1";
export const DEFAULT_DAEMON_PORT = 8765;
export const DEFAULT_DAEMON_MCP_PATH = "/mcp";
export const DEFAULT_MAX_BODY_BYTES = 10 * 1024 * 1024;

export type DaemonLogger = Pick<Console, "error" | "warn" | "info">;

export type DaemonConfigOptions = {
  host?: string;
  port?: number;
  mcpPath?: string;
  maxBodyBytes?: number;
};

export type ResolvedDaemonConfig = {
  host: string;
  port: number;
  mcpPath: string;
  maxBodyBytes: number;
};

export type StartBridgeDaemonOptions = DaemonConfigOptions & {
  env?: NodeJS.ProcessEnv;
  logger?: DaemonLogger;
};

export type BridgeDaemonHandle = {
  host: string;
  port: number;
  mcpPath: string;
  mcpEndpoint: string;
  bridgeEndpoint: string;
  bridge: EasyEdaBridge;
  stop: () => Promise<void>;
};

class PayloadTooLargeError extends Error {
  readonly maxBodyBytes: number;

  constructor(maxBodyBytes: number) {
    super(`Request body exceeds maximum allowed size of ${maxBodyBytes} bytes.`);
    this.name = "PayloadTooLargeError";
    this.maxBodyBytes = maxBodyBytes;
  }
}

export function resolveDaemonConfig(
  options: DaemonConfigOptions = {},
  env: NodeJS.ProcessEnv = process.env
): ResolvedDaemonConfig {
  const host = options.host?.trim() || env.EASYEDA_MCP_WS_HOST?.trim() || DEFAULT_DAEMON_HOST;
  const port = resolvePort(options.port, env.EASYEDA_MCP_WS_PORT);
  const mcpPath = resolveMcpPath(options.mcpPath, env.EASYEDA_MCP_HTTP_PATH);
  const maxBodyBytes = resolveMaxBodyBytes(options.maxBodyBytes);

  return {
    host,
    port,
    mcpPath,
    maxBodyBytes
  };
}

export async function startBridgeDaemon(options: StartBridgeDaemonOptions = {}): Promise<BridgeDaemonHandle> {
  const config = resolveDaemonConfig(options, options.env ?? process.env);
  const logger = options.logger ?? console;

  const bridge = new EasyEdaBridge({
    host: config.host,
    port: config.port,
    logger
  });

  let boundPort = config.port;
  let baseUrl = formatHttpBaseUrl(config.host, boundPort);
  let mcpEndpoint = `${baseUrl}${config.mcpPath}`;
  const activeCleanups = new Set<() => void>();

  const registerActiveCleanup = (cleanup: () => void): (() => void) => {
    activeCleanups.add(cleanup);
    return () => {
      activeCleanups.delete(cleanup);
    };
  };

  const httpServer = createServer((req, res) => {
    void handleHttpRequest({
      req,
      res,
      config,
      bridge,
      logger,
      baseUrl,
      mcpEndpoint,
      registerActiveCleanup
    }).catch((error) => {
      logger.error(
        `[easyeda-mcp-daemon] Unhandled request error: ${error instanceof Error ? (error.stack ?? error.message) : String(error)}`
      );
      if (!res.headersSent && !res.destroyed && !res.writableEnded) {
        sendJson(res, 500, {
          error: "internal_server_error",
          message: "Internal MCP HTTP daemon error."
        });
      } else if (!res.destroyed && !res.writableEnded) {
        res.destroy();
      }
    });
  });

  bridge.attachToHttpServer(httpServer);

  try {
    await listenHttpServer(httpServer, config.host, config.port);
  } catch (error) {
    await bridge.stop().catch(() => undefined);
    throw error;
  }

  const address = httpServer.address();
  boundPort = typeof address === "object" && address ? address.port : config.port;
  baseUrl = formatHttpBaseUrl(config.host, boundPort);
  mcpEndpoint = `${baseUrl}${config.mcpPath}`;

  logger.info(`[easyeda-mcp-daemon] Streamable HTTP MCP endpoint: ${mcpEndpoint}`);
  logger.info(`[easyeda-mcp-daemon] Health endpoint: ${baseUrl}/health`);
  logger.info(`[easyeda-mcp-daemon] WebSocket bridge endpoint: ${bridge.endpoint}`);

  let stopPromise: Promise<void> | undefined;

  const stop = (): Promise<void> => {
    if (!stopPromise) {
      stopPromise = (async () => {
        for (const cleanup of Array.from(activeCleanups)) {
          try {
            cleanup();
          } catch {
            // Ignore per-request teardown errors during shutdown.
          }
        }
        activeCleanups.clear();

        let bridgeStopError: unknown;
        try {
          await bridge.stop();
        } catch (error) {
          bridgeStopError = error;
        }

        await closeHttpServer(httpServer);

        if (bridgeStopError) {
          throw bridgeStopError;
        }
      })();
    }

    return stopPromise;
  };

  return {
    host: config.host,
    port: boundPort,
    mcpPath: config.mcpPath,
    mcpEndpoint,
    bridgeEndpoint: bridge.endpoint,
    bridge,
    stop
  };
}

type RequestContext = {
  req: IncomingMessage;
  res: ServerResponse;
  config: ResolvedDaemonConfig;
  bridge: EasyEdaBridge;
  logger: DaemonLogger;
  baseUrl: string;
  mcpEndpoint: string;
  registerActiveCleanup: (cleanup: () => void) => () => void;
};

async function handleHttpRequest(ctx: RequestContext): Promise<void> {
  const { req, res, config, bridge, baseUrl, mcpEndpoint } = ctx;
  const method = (req.method ?? "GET").toUpperCase();

  let parsedUrl: URL;
  try {
    parsedUrl = buildRequestUrl(req, baseUrl);
  } catch {
    sendJson(res, 400, { error: "bad_request", message: "Invalid request URL." });
    return;
  }

  const pathname = stripTrailingSlash(parsedUrl.pathname);

  if (pathname === config.mcpPath) {
    await handleMcpHttpRequest(ctx, method, parsedUrl);
    return;
  }

  if (method === "GET" && (pathname === "/" || pathname === "/health")) {
    const editorStatus = bridge.getStatus();
    sendJson(res, 200, {
      status: "ok",
      name: MCP_SERVER_NAME,
      version: MCP_SERVER_VERSION,
      extensionVersion: EXTENSION_VERSION,
      transport: "streamable-http",
      bridgeConnected: editorStatus.connected,
      connectionState: editorStatus.connectionState,
      bridgeEndpoint: bridge.endpoint,
      mcpEndpoint
    });
    return;
  }

  sendJson(res, 404, { error: "not_found" });
}

async function handleMcpHttpRequest(ctx: RequestContext, method: string, parsedUrl: URL): Promise<void> {
  const { req, res, config, bridge, logger, registerActiveCleanup } = ctx;

  if (method !== "POST" && method !== "GET" && method !== "DELETE") {
    sendJson(
      res,
      405,
      {
        error: "method_not_allowed",
        message: `Method ${method} is not allowed on ${config.mcpPath}.`
      },
      { allow: "GET, POST, DELETE" }
    );
    return;
  }

  let bodyBuffer: Buffer | undefined;
  if (method !== "GET") {
    try {
      bodyBuffer = await readRequestBody(req, config.maxBodyBytes);
    } catch (error) {
      if (error instanceof PayloadTooLargeError) {
        sendJson(
          res,
          413,
          {
            error: "payload_too_large",
            message: error.message,
            maxBodyBytes: error.maxBodyBytes
          },
          { connection: "close" }
        );
        return;
      }
      throw error;
    }
  }

  const abortController = new AbortController();
  const unregisterRequestAbort = registerActiveCleanup(() => {
    abortController.abort();
  });
  const onResCloseBeforeEnd = () => {
    if (!res.writableEnded) {
      abortController.abort();
    }
  };
  res.once("close", onResCloseBeforeEnd);

  const webHeaders = buildWebHeaders(req.headers);
  const requestInit: RequestInit = {
    method,
    headers: webHeaders,
    signal: abortController.signal
  };

  if (bodyBuffer && bodyBuffer.length > 0) {
    requestInit.body = new Uint8Array(bodyBuffer);
  }

  const webReq = new Request(parsedUrl.toString(), requestInit);
  const transport = new WebStandardStreamableHTTPServerTransport({
    sessionIdGenerator: undefined,
    enableJsonResponse: true,
    maxRequestBodySize: config.maxBodyBytes
  });
  const server = createMcpServer(bridge);

  try {
    await server.connect(transport);
    const webRes = await transport.handleRequest(webReq);
    await writeWebResponseToNode(webRes, res, registerActiveCleanup);
  } catch (error) {
    if (!abortController.signal.aborted) {
      logger.error(
        `[easyeda-mcp-daemon] MCP request failed: ${error instanceof Error ? (error.stack ?? error.message) : String(error)}`
      );
      if (!res.headersSent && !res.destroyed && !res.writableEnded) {
        sendJson(res, 500, {
          error: "internal_server_error",
          message: "Failed to handle MCP request."
        });
      }
    }
  } finally {
    unregisterRequestAbort();
    res.off("close", onResCloseBeforeEnd);
    await Promise.allSettled([transport.close(), server.close()]);
  }
}

function resolvePort(explicitPort: number | undefined, envPortRaw: string | undefined): number {
  if (explicitPort !== undefined) {
    if (
      typeof explicitPort !== "number" ||
      !Number.isInteger(explicitPort) ||
      explicitPort < 0 ||
      explicitPort > 65535
    ) {
      throw new Error(`Invalid daemon port "${String(explicitPort)}". Expected an integer between 0 and 65535.`);
    }
    return explicitPort;
  }

  const trimmed = envPortRaw?.trim();
  if (!trimmed) {
    return DEFAULT_DAEMON_PORT;
  }

  if (!/^\d+$/.test(trimmed)) {
    throw new Error(`Invalid EASYEDA_MCP_WS_PORT "${envPortRaw}". Expected an integer between 0 and 65535.`);
  }

  const parsed = Number(trimmed);
  if (!Number.isInteger(parsed) || parsed < 0 || parsed > 65535) {
    throw new Error(`Invalid EASYEDA_MCP_WS_PORT "${envPortRaw}". Expected an integer between 0 and 65535.`);
  }

  return parsed;
}

function resolveMcpPath(explicitPath: string | undefined, envPathRaw: string | undefined): string {
  const candidate = explicitPath !== undefined ? explicitPath.trim() : envPathRaw?.trim() || DEFAULT_DAEMON_MCP_PATH;

  if (!candidate) {
    throw new Error('Invalid MCP HTTP path "". Paths "/", "/health", and empty paths are not allowed.');
  }

  if (candidate.includes("?") || candidate.includes("#") || /\s/.test(candidate)) {
    throw new Error(`Invalid MCP HTTP path "${candidate}". Path must not contain whitespace, query, or fragment.`);
  }

  const withLeadingSlash = candidate.startsWith("/") ? candidate : `/${candidate}`;
  const normalized = stripTrailingSlash(withLeadingSlash);

  if (normalized === "/" || normalized === "/health") {
    throw new Error(`Invalid MCP HTTP path "${normalized}". Paths "/" and "/health" are reserved.`);
  }

  return normalized;
}

function resolveMaxBodyBytes(explicitMaxBodyBytes: number | undefined): number {
  if (explicitMaxBodyBytes === undefined) {
    return DEFAULT_MAX_BODY_BYTES;
  }

  if (
    typeof explicitMaxBodyBytes !== "number" ||
    !Number.isInteger(explicitMaxBodyBytes) ||
    explicitMaxBodyBytes <= 0
  ) {
    throw new Error(`Invalid maxBodyBytes "${String(explicitMaxBodyBytes)}". Expected a positive integer.`);
  }

  return explicitMaxBodyBytes;
}

function formatHostForUrl(host: string): string {
  if (host.includes(":") && !host.startsWith("[")) {
    return `[${host}]`;
  }
  return host;
}

function formatHttpBaseUrl(host: string, port: number): string {
  return `http://${formatHostForUrl(host)}:${port}`;
}

function stripTrailingSlash(pathname: string): string {
  let end = pathname.length;
  while (end > 1 && pathname.charCodeAt(end - 1) === 47) {
    end -= 1;
  }
  return pathname.slice(0, end);
}

function buildRequestUrl(req: IncomingMessage, fallbackBaseUrl: string): URL {
  const hostHeader = req.headers.host?.trim();
  if (hostHeader) {
    try {
      return new URL(req.url ?? "/", `http://${hostHeader}`);
    } catch {
      // Fall back to server base URL if Host header cannot be parsed.
    }
  }
  return new URL(req.url ?? "/", fallbackBaseUrl);
}

function buildWebHeaders(nodeHeaders: IncomingMessage["headers"]): Headers {
  const headers = new Headers();
  for (const [key, value] of Object.entries(nodeHeaders)) {
    if (value === undefined) {
      continue;
    }
    if (Array.isArray(value)) {
      for (const item of value) {
        headers.append(key, item);
      }
    } else {
      headers.set(key, value);
    }
  }
  return headers;
}

async function readRequestBody(req: IncomingMessage, maxBodyBytes: number): Promise<Buffer> {
  const contentLengthHeader = req.headers["content-length"];
  if (typeof contentLengthHeader === "string") {
    const declaredLength = Number(contentLengthHeader);
    if (Number.isFinite(declaredLength) && declaredLength > maxBodyBytes) {
      req.on("error", () => undefined);
      req.resume();
      throw new PayloadTooLargeError(maxBodyBytes);
    }
  }

  const chunks: Buffer[] = [];
  let totalBytes = 0;

  return await new Promise<Buffer>((resolve, reject) => {
    let settled = false;

    const cleanup = () => {
      req.off("data", onData);
      req.off("end", onEnd);
      req.off("error", onError);
      req.off("aborted", onAborted);
    };

    const onData = (chunk: Buffer | string) => {
      if (settled) {
        return;
      }
      const bufferChunk = typeof chunk === "string" ? Buffer.from(chunk) : chunk;
      totalBytes += bufferChunk.length;
      if (totalBytes > maxBodyBytes) {
        settled = true;
        cleanup();
        req.on("error", () => undefined);
        req.resume();
        reject(new PayloadTooLargeError(maxBodyBytes));
        return;
      }
      chunks.push(bufferChunk);
    };

    const onEnd = () => {
      if (settled) {
        return;
      }
      settled = true;
      cleanup();
      resolve(Buffer.concat(chunks, totalBytes));
    };

    const onError = (error: Error) => {
      if (settled) {
        return;
      }
      settled = true;
      cleanup();
      reject(error);
    };

    const onAborted = () => {
      if (settled) {
        return;
      }
      settled = true;
      cleanup();
      reject(new Error("Request aborted by client."));
    };

    req.on("data", onData);
    req.once("end", onEnd);
    req.once("error", onError);
    req.once("aborted", onAborted);
  });
}

function writeWebResponseHeaders(webRes: Response, res: ServerResponse): void {
  const headersWithSetCookie = webRes.headers as Headers & { getSetCookie?: () => string[] };
  const setCookies = typeof headersWithSetCookie.getSetCookie === "function" ? headersWithSetCookie.getSetCookie() : [];

  webRes.headers.forEach((value, key) => {
    if (key.toLowerCase() === "set-cookie") {
      return;
    }
    res.setHeader(key, value);
  });

  if (setCookies.length > 0) {
    res.setHeader("set-cookie", setCookies);
  } else {
    const fallbackSetCookie = webRes.headers.get("set-cookie");
    if (fallbackSetCookie !== null) {
      res.setHeader("set-cookie", fallbackSetCookie);
    }
  }

  res.statusCode = webRes.status;
  if (webRes.statusText) {
    res.statusMessage = webRes.statusText;
  }
}

async function writeWebResponseToNode(
  webRes: Response,
  res: ServerResponse,
  registerActiveCleanup: (cleanup: () => void) => () => void
): Promise<void> {
  if (res.destroyed || res.writableEnded) {
    await webRes.body?.cancel().catch(() => undefined);
    return;
  }

  writeWebResponseHeaders(webRes, res);

  if (!webRes.body) {
    res.end();
    return;
  }

  const contentType = webRes.headers.get("content-type")?.toLowerCase() ?? "";
  if (contentType.includes("text/event-stream")) {
    res.flushHeaders();
  }

  const nodeStream = Readable.fromWeb(webRes.body as unknown as NodeWebReadableStream<Uint8Array>);

  await new Promise<void>((resolve) => {
    let settled = false;

    const finish = () => {
      if (settled) {
        return;
      }
      settled = true;
      unregisterCleanup();
      res.off("finish", onFinish);
      res.off("close", onClose);
      resolve();
    };

    const abortStream = () => {
      if (!nodeStream.destroyed) {
        nodeStream.destroy();
      }
      if (!res.writableEnded && !res.destroyed) {
        res.end();
      }
      finish();
    };

    const unregisterCleanup = registerActiveCleanup(abortStream);

    const onFinish = () => {
      finish();
    };

    const onClose = () => {
      if (!nodeStream.destroyed) {
        nodeStream.destroy();
      }
      finish();
    };

    const onStreamError = () => {
      if (!res.writableEnded && !res.destroyed) {
        res.destroy();
      }
      finish();
    };

    const onResError = () => {
      if (!nodeStream.destroyed) {
        nodeStream.destroy();
      }
      finish();
    };

    nodeStream.on("error", onStreamError);
    res.on("error", onResError);
    res.once("finish", onFinish);
    res.once("close", onClose);

    nodeStream.pipe(res);
  });
}

function sendJson(
  res: ServerResponse,
  statusCode: number,
  payload: Record<string, unknown>,
  extraHeaders: Record<string, string> = {}
): void {
  if (res.destroyed || res.writableEnded) {
    return;
  }

  const body = JSON.stringify(payload);
  res.writeHead(statusCode, {
    "content-type": "application/json; charset=utf-8",
    "content-length": Buffer.byteLength(body),
    ...extraHeaders
  });
  res.end(body);
}

async function listenHttpServer(httpServer: HttpServer, host: string, port: number): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    const onListening = () => {
      httpServer.off("error", onError);
      resolve();
    };

    const onError = (error: Error) => {
      httpServer.off("listening", onListening);
      reject(error);
    };

    httpServer.once("listening", onListening);
    httpServer.once("error", onError);
    httpServer.listen(port, host);
  });
}

async function closeHttpServer(httpServer: HttpServer): Promise<void> {
  if (!httpServer.listening) {
    return;
  }

  await new Promise<void>((resolve, reject) => {
    httpServer.close((error) => {
      if (error && (error as NodeJS.ErrnoException).code !== "ERR_SERVER_NOT_RUNNING") {
        reject(error);
        return;
      }
      resolve();
    });

    httpServer.closeIdleConnections?.();
    httpServer.closeAllConnections?.();
  });
}
