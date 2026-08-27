@echo off
setlocal EnableExtensions

cd /d "%~dp0"

set "APP_NAME=Rust AI Bridge"
set "EXE_PATH=%~dp0target\release\rust-ai-bridge.exe"

if /I "%~1"=="help" goto :help
if /I "%~1"=="--help" goto :help
if /I "%~1"=="-h" goto :help
if /I "%~1"=="build" goto :build_only
if /I "%~1"=="test" goto :test

if not exist "%EXE_PATH%" (
    echo [INFO] Release EXE not found. Building now...
    call :check_cargo || exit /b 1
    cargo build --release
    if errorlevel 1 (
        echo [ERROR] Build failed.
        exit /b 1
    )
)

echo [INFO] Starting %APP_NAME%...
start "" "%EXE_PATH%"
if errorlevel 1 (
    echo [ERROR] Failed to start: "%EXE_PATH%"
    exit /b 1
)
exit /b 0

:build_only
call :check_cargo || exit /b 1
echo [INFO] Building release executable...
cargo build --release
exit /b %errorlevel%

:test
call :check_cargo || exit /b 1
echo [INFO] Running format check, Clippy and tests...
cargo fmt --all -- --check
if errorlevel 1 exit /b 1
cargo clippy --all-targets -- -D warnings
if errorlevel 1 exit /b 1
cargo test --all-targets
exit /b %errorlevel%

:check_cargo
where cargo >nul 2>nul
if errorlevel 1 (
    echo [ERROR] Rust Cargo was not found in PATH.
    echo         Install Rust from https://rustup.rs/ and reopen CMD.
    exit /b 1
)
exit /b 0

:help
echo.
echo %APP_NAME% command launcher
echo.
echo Usage:
echo   run.cmd          Start the application; build it first if needed
echo   run.cmd build    Build target\release\rust-ai-bridge.exe
echo   run.cmd test     Run format check, Clippy and all tests
echo   run.cmd help     Show this help
echo.
exit /b 0
