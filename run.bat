@echo off
echo Current path is : %~dp0
cd /d "%~dp0"
python house_stat.py
pause