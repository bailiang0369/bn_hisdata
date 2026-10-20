# Empty root conftest: ensures pytest inserts the repo root into sys.path so
# tests can `import binance_vision.*` regardless of the invocation style
# (`pytest` vs `python -m pytest`).
