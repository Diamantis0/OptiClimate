$ErrorActionPreference = "Stop"
python -m pip install -e . | Out-Host
pytest -q
