# Deployment Guide

This guide deploys the lightweight Streamlit dashboard. It does not deploy or execute the PySpark/Delta notebooks.

## Local Windows

From PowerShell:

```powershell
Set-Location "C:\path\to\upi-sentinel-lakehouse"
powershell -NoProfile -ExecutionPolicy Bypass -File .\run_upi_sentinel.ps1
```

The launcher installs `requirements.txt`, chooses a free port, opens the local browser, and prints the same-Wi-Fi mobile URL. Stop it with `Ctrl+C`.

## Local macOS/Linux

```bash
cd /path/to/upi-sentinel-lakehouse
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python -m streamlit run app.py --server.address 0.0.0.0 --server.port 8501
```

Open `http://localhost:8501` on the laptop. For a phone on the same Wi-Fi, open `http://<laptop-ip>:8501`.

## Streamlit Community Cloud

Streamlit Community Cloud is the recommended free host for this public GitHub portfolio application.

1. Push the current `main` branch to GitHub.
2. Open [share.streamlit.io](https://share.streamlit.io/).
3. Sign in with GitHub and choose **Create app**.
4. Repository: `nidhi2025-arch/upi-sentinel-lakehouse`.
5. Branch: `main`.
6. Entrypoint: `app.py`.
7. In Advanced settings, select Python 3.12 if available.
8. Click **Deploy**.
9. Read the build logs and open the generated `streamlit.app` URL.
10. Test all four sections on desktop and mobile.

The root `requirements.txt` is intentionally lightweight so the hosted app does not install Java, PySpark, or Delta Lake. Community Cloud updates the app when the selected GitHub branch changes. Free hosting may sleep when idle and is subject to platform resource limits.

## Render fallback

Render documents free Python web services, but they spin down after 15 minutes without traffic and restart with a cold start. Use this only if Community Cloud is unavailable.

1. Create a free Render Web Service from the public GitHub repository.
2. Build command: `pip install -r requirements.txt`.
3. Start command: `streamlit run app.py --server.address 0.0.0.0 --server.port $PORT`.
4. Choose the Free plan and deploy.
5. Open the generated `onrender.com` URL and inspect logs.

## Hosting security

No secrets or email authentication are required. Do not add credentials to the repository. The application contains Synthetic Data only and uses local files committed to GitHub. A public hosted app is intentionally a portfolio demonstration, not a banking system.
