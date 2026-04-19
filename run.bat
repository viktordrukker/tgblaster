@echo off
REM One-click run script for Windows.
REM Creates venv if missing, installs deps, starts Streamlit.

SETLOCAL
cd /d "%~dp0"

IF NOT EXIST ".venv\Scripts\python.exe" (
    echo [1/3] Creating virtual environment...
    python -m venv .venv || goto :error
)

echo [2/3] Installing dependencies...
.venv\Scripts\python.exe -m pip install --quiet --upgrade pip
.venv\Scripts\python.exe -m pip install --quiet -r requirements.txt || goto :error

echo [3/3] Starting Meetup TG Blaster...
echo.
echo Open http://localhost:8501 in your browser.
echo Press Ctrl+C to stop.
echo.
.venv\Scripts\python.exe -m streamlit run app.py

ENDLOCAL
exit /b 0

:error
echo.
echo ERROR: setup failed. See messages above.
pause
exit /b 1
