@echo off
REM ============================================================
REM  EasyEDA Pro MCP Bridge Daemon
REM  Starts the bridge daemon on http://127.0.0.1:8765/mcp
REM  Keep this window open while using EasyEDA MCP tools.
REM  Press Ctrl+C to stop.
REM ============================================================

cd /d C:\dev\easyeda_mcp

echo Starting EasyEDA Pro MCP Bridge Daemon...
echo   MCP endpoint:  http://127.0.0.1:8765/mcp
echo   WS bridge:     ws://127.0.0.1:8765
echo.

node dist\daemon.js
