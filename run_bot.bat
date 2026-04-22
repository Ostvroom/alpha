@echo off
TITLE Velcor3 Bot
CLS

echo ================================================================================
echo                         VELCOR3 BOT LAUNCHER
echo ================================================================================
echo.

:: ── STEP 1: Python check ─────────────────────────────────────────────────────
python --version >nul 2>&1
IF %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Python is not installed or not in your PATH.
    echo.
    echo Opening Python download page...
    start https://www.python.org/downloads/
    echo After installing Python, re-run this file.
    echo.
    pause
    exit /b 1
)

echo [1/4] Python found:
python --version
echo.

:: ── STEP 2: Install / upgrade dependencies ───────────────────────────────────
echo [2/4] Installing/Updating dependencies (twikit, discord.py, openai, etc.)...
python -m pip install -r requirements.txt -q
IF %ERRORLEVEL% NEQ 0 (
    echo.
    echo [ERROR] Failed to install dependencies.
    echo Check your internet connection or requirements.txt, then try again.
    echo.
    pause
    exit /b 1
)
echo     Done.
echo.

:: ── STEP 3: Apply twikit reduce() patch (safe, idempotent) ───────────────────
echo [3/4] Applying twikit compatibility patch...
python patch_twikit.py
IF %ERRORLEVEL% NEQ 0 (
    echo     [WARN] Patch step failed - bot may still work. Continuing...
)
echo.

:: ── STEP 4: Launch bot ───────────────────────────────────────────────────────
echo [4/4] Starting Velcor3...
echo ================================================================================
echo.

:: UTF-8 fixes UnicodeEncodeError for emoji on Windows console
set PYTHONUTF8=1
set PYTHONIOENCODING=utf-8

python main.py

:: ── Bot stopped ──────────────────────────────────────────────────────────────
echo.
echo ================================================================================
echo [STOPPED] The bot has exited.
echo If it crashed, read the error above before closing this window.
pause
