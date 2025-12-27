@echo off
python -m pip install -e .
pytest -q tests/core
