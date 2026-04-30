@echo off
cd /d "%~dp0"

echo.
echo ========================================
echo   Beijing Housing Data Scraper
echo ========================================
echo.

echo Checking Python...
python --version
if errorlevel 1 (
    echo.
    echo ERROR: Python not found. Please install Python 3.x first.
    echo.
    pause
    exit /b 1
)

echo.
echo Running scraper...
echo.

python house_stat.py

echo.
echo ========================================
echo Done! Press any key to exit...
echo ========================================
pause
