@echo off
chcp 65001 > null
cd /d %~dp0
set start_time=%time%
start /k python -u main.py
set end_time=%time%
echo 開始時間：%start_time%
echo 結束時間：%end_time%

if %ERRORLEVEL% == 0 (
    echo 執行成功！
) else (
    echo 執行失敗，錯誤代碼：%ERRORLEVEL%
)
pause
