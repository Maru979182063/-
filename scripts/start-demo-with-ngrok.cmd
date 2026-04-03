@echo off
setlocal
powershell -ExecutionPolicy Bypass -File "%~dp0start-demo.ps1" -PromptPort 8011 -PassagePort 8001 -StartNgrok
endlocal
