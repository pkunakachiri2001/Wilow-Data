@echo off
echo Stopping all Vibration Monitoring services...

echo.
echo 1. Stopping ngrok...
taskkill /f /im ngrok.exe 2>nul
if %errorlevel% equ 0 (
    echo - ngrok stopped successfully.
) else (
    echo - ngrok was not running.
)

echo.
echo 2. Stopping Python processes (server.py and rpi_client.py)...
powershell -Command "Get-CimInstance Win32_Process -Filter \"Name = 'python.exe' AND (CommandLine LIKE '%server.py%' OR CommandLine LIKE '%rpi_client.py%')\" | Invoke-CimMethod -MethodName Terminate" 2>nul
echo - Python services stopped.

echo.
echo All services have been stopped successfully!
pause
