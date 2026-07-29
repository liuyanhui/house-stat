@echo off
cd /d "%~dp0"

echo.
echo ========================================
echo   Beijing Housing Data Scraper
echo ========================================
echo.

echo Checking Python...
py -3.14 --version
if errorlevel 1 (
    echo.
    echo ERROR: Python 3.14 not found via "py -3.14".
    echo Install Python 3.14 or adjust the launcher in run.bat.
    echo.
    pause
    exit /b 1
)

echo.
echo Running scraper...
echo.

rem Python 3.14 has native cp314 wheels; clear PYTHONPATH so the global
rem Python313 site-packages does not shadow them (cp313 numpy breaks 3.14).
set "PYTHONPATH="
py -3.14 main.py

echo.
echo ========================================
echo Done! Press any key to exit...
echo ========================================
pause
