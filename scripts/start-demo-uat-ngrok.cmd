@echo off
setlocal
powershell -ExecutionPolicy Bypass -File "%~dp0start-demo.ps1" -PromptPort 8011 -PassagePort 8001 -PromptEnvFile "C:\Users\Maru\Documents\agent\.env.demo" -PassageEnvFile "C:\Users\Maru\Documents\agent\passage_service\.env" -StartNgrok
endlocal
