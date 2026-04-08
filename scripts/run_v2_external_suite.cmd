@echo off
setlocal

where py >nul 2>nul
if %errorlevel%==0 (
  py -3 "%~dp0run_v2_external_suite.py" %*
) else (
  python "%~dp0run_v2_external_suite.py" %*
)

exit /b %errorlevel%
