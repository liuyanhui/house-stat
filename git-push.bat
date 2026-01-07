@echo off
REM GitHub 推送批处理文件
REM 使用方法：
REM 1. 先修改下面的 YOUR_USERNAME 为你的GitHub用户名
REM 2. 双击运行此文件

set YOUR_USERNAME=your-username-here

echo ========================================
echo   Git 推送到 GitHub
echo ========================================
echo.

REM 检查是否已设置远程仓库
git remote -v >nul 2>&1
if errorlevel 1 (
    echo [1/3] 添加远程仓库...
    git remote add origin git@github.com:%YOUR_USERNAME%/house-stat.git
    if errorlevel 1 (
        echo 错误：添加远程仓库失败
        echo 请确保：
        echo 1. 已在GitHub上创建仓库 house-stat
        echo 2. 已正确配置SSH key
        echo 3. 已将此文件中的 YOUR_USERNAME 改为你的GitHub用户名
        pause
        exit /b 1
    )
    echo 远程仓库已添加
) else (
    echo [1/3] 远程仓库已存在
)

echo.
echo [2/3] 推送代码到 GitHub...
git push -u origin master
if errorlevel 1 (
    echo.
    echo 错误：推送失败
    echo 请检查：
    echo 1. 网络连接是否正常
    echo 2. SSH key 是否正确配置
    echo 3. GitHub仓库是否已创建
    pause
    exit /b 1
)

echo.
echo [3/3] 推送成功！
echo.
echo 你可以访问以下地址查看你的代码：
echo https://github.com/%YOUR_USERNAME%/house-stat
echo.
pause
