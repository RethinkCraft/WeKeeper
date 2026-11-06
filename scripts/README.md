# Operational and development tools

Top-level scripts are deployment/maintenance entrypoints: pinned gateway preparation, protocol bot compatibility launcher, archive bridge, session diagnostics, OAuth callback relay and release-content verification.

`diagnostics/` contains explicit protocol fixtures, business oracles and latency probes. `benchmarks/` contains simulated-transport runtime benchmarks and pinned observer/calibration layers. These are development tools, excluded from the Python wheel. Their default previews do not open production state; any real-model mode must be explicitly requested and uses a separate protected copy. They do not establish phone delivery.

The protocol corpus data is synthetic. Pinned development helper hashes are updated as a complete chain when reorganized. Product code has a single protocol bot runtime and memory MCP host; a benchmark flag describes only its selected instrumentation, not an alternative supported product transport.
