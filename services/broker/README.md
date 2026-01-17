# broker service

Executable entrypoint for running a Felix broker node.

Responsibilities:
- Bootstraps the broker runtime
- Hosts the data plane for pub/sub and cache
- Runs under Tokio with structured logging
