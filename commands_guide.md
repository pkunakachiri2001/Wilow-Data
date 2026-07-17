# Commands Guide - Vibration Monitoring System

This document outlines the commands to start, stop, and manage the different components of the vibration monitoring pipeline on Windows.

---

## 1. Dashboard Server (`server.py`)

Runs the Flask & SocketIO dashboard that displays statistics and visualizations.

* **Start command**:
  ```powershell
  python server.py
  ```
* **Stop command**:
  - In the running terminal window: Press `Ctrl + C`
  - From another command window (force stop):
    ```powershell
    powershell -Command "Get-CimInstance Win32_Process -Filter \"Name = 'python.exe' AND CommandLine LIKE '%server.py%'\" | Invoke-CimMethod -MethodName Terminate"
    ```

---

## 2. Raspberry Pi Client (`rpi_client.py`)

Runs the HTTP listener that receives JSON payloads from the Teensy sender, writes local CSV files, and forwards them to the remote dashboard server.

* **Start command**:
  ```powershell
  python rpi_client.py
  ```
* **Stop command**:
  - In the running terminal window: Press `Ctrl + C`
  - From another command window (force stop):
    ```powershell
    powershell -Command "Get-CimInstance Win32_Process -Filter \"Name = 'python.exe' AND CommandLine LIKE '%rpi_client.py%'\" | Invoke-CimMethod -MethodName Terminate"
    ```

---

## 3. ngrok Tunnel (`ngrok.exe`)

Exposes your dashboard server (running on local port 5000) to the internet so that the Raspberry Pi client (or other remote clients) can send data to `REMOTE_SERVER_URL`.

* **Start command**:
  ```powershell
  .\ngrok.exe http 5000
  ```
  *(If you have a custom reserved domain, use: `.\ngrok.exe http --url=YOUR_NGROK_ID.ngrok-free.app 5000`)*

* **Stop command**:
  - In the running terminal window: Press `Ctrl + C`
  - From another command window (force stop):
    ```powershell
    taskkill /IM ngrok.exe /F
    ```

---

## Quick-Start and Quick-Stop Batch Files

To make managing these tasks easier, we have created the following batch scripts in the project directory:

1. **[start_server.bat](file:///c:/Users/DELL/OneDrive/Desktop/server/server2/start_server.bat)**: Launches the dashboard server in a new window.
2. **[start_client.bat](file:///c:/Users/DELL/OneDrive/Desktop/server/server2/start_client.bat)**: Launches the RPi client HTTP listener in a new window.
3. **[start_ngrok.bat](file:///c:/Users/DELL/OneDrive/Desktop/server/server2/start_ngrok.bat)**: Launches ngrok in a new window.
4. **[stop_all.bat](file:///c:/Users/DELL/OneDrive/Desktop/server/server2/stop_all.bat)**: Instantly finds and terminates all running server, client, and ngrok processes cleanly.
