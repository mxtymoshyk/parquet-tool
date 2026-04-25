#Requires -Version 5.1
$ErrorActionPreference = 'Stop'

$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

$Venv = Join-Path $Root '.venv'
$Py = if ($env:PYTHON) { $env:PYTHON } else { 'python' }

if (-not (Test-Path $Venv)) {
    & $Py -m venv $Venv
}

$Activate = Join-Path $Venv 'Scripts\Activate.ps1'
. $Activate

& python -c "import parquet_tool" 2>$null
if ($LASTEXITCODE -ne 0) {
    & python -m pip install --upgrade pip
    & pip install -e .
}

& parquet-tool @args
exit $LASTEXITCODE
