@echo off
setlocal

cd /d "%~dp0"

where node >nul 2>&1
if errorlevel 1 (
  echo [ERROR] Node.js was not found in PATH.
  echo         Install Node.js ^>= 20 or add it to your PATH, then try again.
  exit /b 1
)

if not exist "dist\daemon.js" (
  echo [ERROR] Built daemon entry point not found: dist\daemon.js
  echo         Run "npm run build" first, then re-run this script.
  exit /b 1
)

set "HOST=%EASYEDA_MCP_WS_HOST%"
if not defined HOST set "HOST=127.0.0.1"
if "%HOST: =%"=="" set "HOST=127.0.0.1"

set "PORT=%EASYEDA_MCP_WS_PORT%"
if not defined PORT set "PORT=8765"
if "%PORT: =%"=="" set "PORT=8765"

set "MCP_PATH=%EASYEDA_MCP_HTTP_PATH%"
if not defined MCP_PATH set "MCP_PATH=/mcp"
if "%MCP_PATH: =%"=="" set "MCP_PATH=/mcp"
if not "%MCP_PATH:~0,1%"=="/" set "MCP_PATH=/%MCP_PATH%"

if not "%HOST::=%"=="%HOST%" if not "%HOST:~0,1%"=="[" set "HOST=[%HOST%]"

echo ============================================================
echo  EasyEDA Pro MCP Daemon (Streamable HTTP + WebSocket Bridge)
echo ============================================================
echo  MCP Endpoint : http://%HOST%:%PORT%%MCP_PATH%
echo  Health Check : http://%HOST%:%PORT%/health
echo  WS Bridge    : ws://%HOST%:%PORT%
echo  Note         : CLI flags (e.g. --host, --port, --path) override environment variables.
echo ============================================================
echo.

node dist\daemon.js %*
set "EXIT_CODE=%ERRORLEVEL%"
endlocal & exit /b %EXIT_CODE%
