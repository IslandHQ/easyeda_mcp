#!/usr/bin/env node
import { realpathSync } from "node:fs";
import path from "node:path";
import process from "node:process";
import { pathToFileURL } from "node:url";
import {
  DEFAULT_DAEMON_HOST,
  DEFAULT_DAEMON_MCP_PATH,
  DEFAULT_DAEMON_PORT,
  startBridgeDaemon,
  type BridgeDaemonHandle
} from "./daemon/httpDaemon.js";
import { EXTENSION_VERSION, MCP_SERVER_NAME, MCP_SERVER_VERSION } from "./version.js";

export type ParsedDaemonCliArgs = {
  help: boolean;
  version: boolean;
  host?: string;
  port?: number;
  mcpPath?: string;
};

export function parseDaemonCliArgs(argv: string[]): ParsedDaemonCliArgs {
  const result: ParsedDaemonCliArgs = {
    help: false,
    version: false
  };

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];

    if (arg === "--help" || arg === "-h") {
      result.help = true;
      continue;
    }

    if (arg === "--version" || arg === "-v") {
      result.version = true;
      continue;
    }

    if (arg === "--host" || arg.startsWith("--host=")) {
      const value = readOptionValue("--host", arg, () => {
        i += 1;
        return argv[i];
      });
      result.host = value;
      continue;
    }

    if (arg === "--port" || arg.startsWith("--port=")) {
      const rawPort = readOptionValue("--port", arg, () => {
        i += 1;
        return argv[i];
      });
      if (!/^\d+$/.test(rawPort)) {
        throw new Error(`Invalid value for --port: "${rawPort}". Expected an integer between 0 and 65535.`);
      }
      const parsedPort = Number(rawPort);
      if (!Number.isInteger(parsedPort) || parsedPort < 0 || parsedPort > 65535) {
        throw new Error(`Invalid value for --port: "${rawPort}". Expected an integer between 0 and 65535.`);
      }
      result.port = parsedPort;
      continue;
    }

    if (arg === "--path" || arg.startsWith("--path=")) {
      const value = readOptionValue("--path", arg, () => {
        i += 1;
        return argv[i];
      });
      result.mcpPath = value;
      continue;
    }

    throw new Error(`Unknown option or argument: "${arg}". Run with --help for usage.`);
  }

  return result;
}

function readOptionValue(flag: string, currentArg: string, consumeNext: () => string | undefined): string {
  const inlinePrefix = `${flag}=`;
  if (currentArg.startsWith(inlinePrefix)) {
    const raw = currentArg.slice(inlinePrefix.length).trim();
    if (!raw) {
      throw new Error(`Missing value for ${flag}.`);
    }
    return raw;
  }

  const next = consumeNext();
  const trimmed = next?.trim();
  if (!trimmed || trimmed.startsWith("-")) {
    throw new Error(`Missing value for ${flag}.`);
  }
  return trimmed;
}

export function renderDaemonHelp(): string {
  return [
    `EasyEDA Pro MCP Streamable HTTP Daemon v${MCP_SERVER_VERSION} (extension v${EXTENSION_VERSION})`,
    "",
    "Usage:",
    "  easyeda-pro-mcp-daemon [options]",
    "",
    "Options:",
    `  --host <host>   Bind host (default: EASYEDA_MCP_WS_HOST or ${DEFAULT_DAEMON_HOST})`,
    `  --port <port>   Bind port (default: EASYEDA_MCP_WS_PORT or ${DEFAULT_DAEMON_PORT})`,
    `  --path <path>   Streamable HTTP MCP path (default: EASYEDA_MCP_HTTP_PATH or ${DEFAULT_DAEMON_MCP_PATH})`,
    "  -v, --version   Print server and bundled extension versions",
    "  -h, --help      Show this help message"
  ].join("\n");
}

function installDaemonShutdownHandlers(handle: BridgeDaemonHandle): void {
  let shutdownPromise: Promise<void> | undefined;

  const shutdown = (signal: NodeJS.Signals): void => {
    if (shutdownPromise) {
      return;
    }
    process.off("SIGINT", onSigInt);
    process.off("SIGTERM", onSigTerm);
    console.error(`[easyeda-mcp-daemon] Received ${signal}, shutting down...`);
    shutdownPromise = handle
      .stop()
      .catch((error) => {
        console.error(
          `[easyeda-mcp-daemon] Error during shutdown: ${error instanceof Error ? error.message : String(error)}`
        );
        process.exitCode = 1;
      })
      .finally(() => {
        process.exit(process.exitCode ?? 0);
      });
  };

  const onSigInt = () => shutdown("SIGINT");
  const onSigTerm = () => shutdown("SIGTERM");

  process.once("SIGINT", onSigInt);
  process.once("SIGTERM", onSigTerm);
}

async function main(): Promise<void> {
  const args = parseDaemonCliArgs(process.argv.slice(2));

  if (args.help) {
    console.log(renderDaemonHelp());
    return;
  }

  if (args.version) {
    console.log(`${MCP_SERVER_NAME} v${MCP_SERVER_VERSION} (extension v${EXTENSION_VERSION})`);
    return;
  }

  const handle = await startBridgeDaemon({
    host: args.host,
    port: args.port,
    mcpPath: args.mcpPath
  });

  installDaemonShutdownHandlers(handle);
}

function isDirectExecution(): boolean {
  const entry = process.argv[1];
  if (!entry) {
    return false;
  }

  let resolvedEntry = path.resolve(entry);
  try {
    resolvedEntry = realpathSync(resolvedEntry);
  } catch {
    // Keep path.resolve result when realpath is unavailable.
  }

  const entryUrl = pathToFileURL(resolvedEntry).href;
  if (process.platform === "win32") {
    return entryUrl.toLowerCase() === import.meta.url.toLowerCase();
  }
  return entryUrl === import.meta.url;
}

if (isDirectExecution()) {
  main().catch((error) => {
    console.error(`[easyeda-mcp-daemon] Fatal error: ${error instanceof Error ? error.message : String(error)}`);
    process.exit(1);
  });
}
