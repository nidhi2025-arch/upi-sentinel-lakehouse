$ErrorActionPreference = "Stop"

$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $projectRoot

if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
    throw "Python 3.10+ is required. Install Python from https://www.python.org/downloads/ and enable Add Python to PATH."
}

if (-not (Test-Path ".venv\Scripts\python.exe")) {
    python -m venv .venv
}

$venvPython = Join-Path $projectRoot ".venv\Scripts\python.exe"
& $venvPython -m pip install --disable-pip-version-check --quiet -r requirements.txt

$port = 8501
while (Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue) {
    $port++
}

$localIp = Get-NetIPAddress -AddressFamily IPv4 -ErrorAction SilentlyContinue |
    Where-Object { $_.IPAddress -notlike "127.*" -and $_.PrefixOrigin -ne "WellKnown" } |
    Select-Object -First 1 -ExpandProperty IPAddress

Write-Host "UPI Sentinel Lakehouse is starting..." -ForegroundColor Cyan
Write-Host "Laptop URL: http://localhost:$port" -ForegroundColor Green
if ($localIp) {
    Write-Host "Mobile URL on the same Wi-Fi: http://${localIp}:$port" -ForegroundColor Yellow
}
Write-Host "Synthetic Data only. Press Ctrl+C to stop." -ForegroundColor DarkYellow

Start-Process "http://localhost:$port"
& $venvPython -m streamlit run app.py --server.address 0.0.0.0 --server.port $port
