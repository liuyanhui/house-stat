@echo off
cd /d "%~dp0"

echo.
echo ========================================
echo   Beijing Resale Trend Report
echo ========================================
echo.

echo Generating trend report (Markdown + PNG)...
echo.

py -3.13 script/analyze.py --report
if errorlevel 1 (
    echo.
    echo ERROR: report generation failed.
    pause
    exit /b 1
)

echo.
echo Report: report\trend_report.md
echo Charts: report\*.png
echo.
echo ========================================
echo Done! Press any key to exit...
echo ========================================
pause
