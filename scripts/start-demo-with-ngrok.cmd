@echo off
setlocal
powershell -ExecutionPolicy Bypass -File "%~dp0start-demo.ps1" -Profile mvp -StartNgrok
endlocal
