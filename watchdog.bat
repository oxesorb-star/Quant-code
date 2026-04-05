@echo off
setlocal EnableExtensions EnableDelayedExpansion
title Quant Trade Watchdog

set "ROOT=E:\Python\Quant Trade"
set "PYTHON_EXE=%ROOT%\.venv\Scripts\python.exe"
set "MAIN_SCRIPT=%ROOT%\Monitor Stable.py"
set "ENV_FILE=%ROOT%\.env"

if not exist "%PYTHON_EXE%" (
    echo [%date% %time%] Missing Python runtime: %PYTHON_EXE%
    pause
    exit /b 1
)

if not exist "%MAIN_SCRIPT%" (
    echo [%date% %time%] Missing script: %MAIN_SCRIPT%
    pause
    exit /b 1
)

if not exist "%ENV_FILE%" (
    echo [%date% %time%] Missing env file: %ENV_FILE%
    echo Create it from .env.example before starting watchdog.
    pause
    exit /b 1
)

cd /d "%ROOT%"

:loop
call :load_env
if errorlevel 1 (
    echo [%date% %time%] Invalid environment configuration. Watchdog stopped.
    pause
    exit /b 1
)

echo [%date% %time%] Starting Monitor Stable...
"%PYTHON_EXE%" "%MAIN_SCRIPT%"
set "EXIT_CODE=%ERRORLEVEL%"
echo [%date% %time%] Process exited with code !EXIT_CODE!, restarting in 5 seconds...
timeout /t 5 /nobreak >nul
goto loop

:load_env
for %%V in (
    SCT_KEY
    TCB_ENV_ID
    TCB_FUNCTION_URL
    TCB_PUBLISHABLE_KEY
    SYNC_TOKEN
    FLASK_SECRET_KEY
    WEB_ADMIN_USER
    WEB_ADMIN_PASSWORD
    OLLAMA_API
    MODEL_R1
    MODEL_GEMMA
    OPENAI_API_KEY
    ANTHROPIC_API_KEY
    DEEPSEEK_API_KEY
) do set "%%V="

rem Clear broken system proxy variables before starting the app
for %%V in (
    HTTP_PROXY
    HTTPS_PROXY
    ALL_PROXY
    http_proxy
    https_proxy
    all_proxy
) do set "%%V="
set "NO_PROXY=localhost,127.0.0.1,::1"
set "no_proxy=%NO_PROXY%"

for /f "usebackq eol=# tokens=1* delims==" %%A in ("%ENV_FILE%") do (
    if not "%%~A"=="" (
        set "ENV_KEY=%%~A"
        set "ENV_VALUE=%%~B"
        if defined ENV_VALUE (
            if "!ENV_VALUE:~0,1!"=="^"" if "!ENV_VALUE:~-1!"=="^"" set "ENV_VALUE=!ENV_VALUE:~1,-1!"
            set "!ENV_KEY!=!ENV_VALUE!"
        ) else (
            set "!ENV_KEY!="
        )
    )
)

call :require_env SCT_KEY || exit /b 1
call :require_env TCB_ENV_ID || exit /b 1
call :require_env TCB_FUNCTION_URL || exit /b 1
call :require_env TCB_PUBLISHABLE_KEY || exit /b 1
call :require_env SYNC_TOKEN || exit /b 1
call :require_env FLASK_SECRET_KEY || exit /b 1
call :require_env WEB_ADMIN_USER || exit /b 1
call :require_env WEB_ADMIN_PASSWORD || exit /b 1

if not defined OLLAMA_API set "OLLAMA_API=http://127.0.0.1:11434/api/generate"
if not defined MODEL_R1 set "MODEL_R1=deepseek-r1:8b"
if not defined MODEL_GEMMA set "MODEL_GEMMA=gemma3:4b"
exit /b 0

:require_env
if not defined %~1 (
    echo [%date% %time%] Missing required environment variable in .env: %~1
    exit /b 1
)
exit /b 0

