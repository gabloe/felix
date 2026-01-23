# Felix Dockerized Test Harness

A Docker-based test harness for testing Felix pub/sub at scale with multiple publishers and subscribers.

## Overview

This test harness allows you to deploy and test Felix pub/sub with:
- A single broker instance
- Configurable number of publisher clients
- Configurable number of subscriber clients
- Configurable message rates, payload sizes, and test durations

## Quick Start

### Prerequisites

- Docker (20.10 or later)
- Docker Compose (1.29 or later)

### Run a Basic Test

From the `test-harness` directory:

```bash
# Run with default settings (2 publishers, 2 subscribers)
./run-test.sh

# Run with custom configuration
./run-test.sh --publishers 5 --subscribers 10 --rate 200 --duration 120
```

### Using Docker Compose Directly

```bash
# Start the test harness
docker-compose up -d

# Scale publishers and subscribers
docker-compose up -d --scale publisher=5 --scale subscriber=10

# View logs
docker-compose logs -f

# Stop and clean up
docker-compose down -v
```

## Architecture

```
┌─────────────────────────────────────────────┐
│                                             │
│  Docker Network (felix-net)                │
│                                             │
│  ┌──────────┐                               │
│  │  Broker  │ ← QUIC (UDP 5000)            │
│  │          │ ← HTTP Metrics (8080)        │
│  └────┬─────┘                               │
│       │                                     │
│       ├─────► ┌────────────┐               │
│       │       │ Publisher 1│               │
│       │       └────────────┘               │
│       │                                     │
│       ├─────► ┌────────────┐               │
│       │       │ Publisher 2│               │
│       │       └────────────┘               │
│       │                                     │
│       ├─────► ┌────────────┐               │
│       │       │ Publisher N│               │
│       │       └────────────┘               │
│       │                                     │
│       ├─────► ┌──────────────┐             │
│       │       │ Subscriber 1 │             │
│       │       └──────────────┘             │
│       │                                     │
│       ├─────► ┌──────────────┐             │
│       │       │ Subscriber 2 │             │
│       │       └──────────────┘             │
│       │                                     │
│       └─────► ┌──────────────┐             │
│               │ Subscriber M │             │
│               └──────────────┘             │
│                                             │
└─────────────────────────────────────────────┘
```

## Components

### Broker

The Felix broker service that handles pub/sub message routing:
- Built from the main Felix Dockerfile
- Exposes QUIC on UDP port 5000
- Exposes Prometheus metrics on HTTP port 8080
- Uses self-signed certificates for testing

### Publishers

Publisher clients that publish messages to the broker:
- Configurable message rate (messages per second)
- Configurable payload size
- Configurable total message count
- Each instance gets a unique ID for logging

### Subscribers

Subscriber clients that receive messages from the broker:
- Subscribe to the test stream
- Log received message counts and rates
- Configurable timeout for graceful shutdown

### Initialization

An init container that waits for the broker to be ready before starting publishers and subscribers.

## Configuration

### Test Harness Script Options

The `run-test.sh` script supports the following options:

| Option | Default | Description |
|--------|---------|-------------|
| `--publishers N` | 2 | Number of publisher instances |
| `--subscribers N` | 2 | Number of subscriber instances |
| `--rate N` | 100 | Messages per second per publisher |
| `--payload-size N` | 1024 | Message payload size in bytes |
| `--duration N` | 60 | Test duration in seconds |
| `--build` | - | Force rebuild of Docker images |
| `--down` | - | Stop and clean up the test harness |
| `--logs` | - | Show logs from all services |
| `--help` | - | Show help message |

### Environment Variables

You can also configure the broker via environment variables in `docker-compose.yml`:

- `FELIX_QUIC_BIND`: Broker QUIC bind address (default: 0.0.0.0:5000)
- `FELIX_BROKER_METRICS_BIND`: Metrics HTTP bind address (default: 0.0.0.0:8080)
- `FELIX_DISABLE_TIMINGS`: Disable timing collection (default: 1)
- `RUST_LOG`: Logging level (default: info)

## Use Cases

### Scale Testing

Test how the system performs with different numbers of publishers and subscribers:

```bash
# Light load
./run-test.sh --publishers 2 --subscribers 5 --rate 50

# Medium load
./run-test.sh --publishers 5 --subscribers 10 --rate 200

# Heavy load
./run-test.sh --publishers 10 --subscribers 20 --rate 500
```

### Throughput Testing

Test maximum throughput with large payloads and high rates:

```bash
./run-test.sh --publishers 5 --subscribers 5 --rate 1000 --payload-size 4096
```

### Fanout Testing

Test high fanout scenarios (many subscribers per publisher):

```bash
./run-test.sh --publishers 1 --subscribers 50 --rate 100
```

### Latency Testing

Test with low rates and small payloads to measure latency:

```bash
./run-test.sh --publishers 1 --subscribers 1 --rate 10 --payload-size 128
```

## Monitoring

### View Logs

```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f broker
docker-compose logs -f publisher
docker-compose logs -f subscriber

# Last N lines
docker-compose logs --tail=100 publisher
```

### Check Metrics

The broker exposes Prometheus metrics on port 8080:

```bash
curl http://localhost:8080/metrics
```

### Container Stats

```bash
# View resource usage
docker stats

# View specific containers
docker stats felix-broker
```

## Advanced Usage

### Custom Publisher/Subscriber Configuration

Edit `docker-compose.yml` to customize publisher and subscriber behavior:

```yaml
services:
  publisher:
    command:
      - publisher
      - --broker=broker:5000
      - --tenant=custom-tenant
      - --stream=custom-stream
      - --payload-size=2048
      - --rate=500
```

### Multiple Streams

Run different publishers/subscribers on different streams by creating additional service definitions in `docker-compose.yml`:

```yaml
services:
  publisher-stream1:
    # ... config for stream1
  
  publisher-stream2:
    # ... config for stream2
  
  subscriber-stream1:
    # ... config for stream1
  
  subscriber-stream2:
    # ... config for stream2
```

### Long-Running Tests

For long-running tests, use `docker-compose up -d` and monitor with logs:

```bash
# Start in detached mode
docker-compose up -d --scale publisher=10 --scale subscriber=20

# Monitor progress
docker-compose logs -f --tail=100 publisher subscriber

# Stop when done
docker-compose down
```

## Troubleshooting

### Broker Not Starting

Check broker logs:
```bash
docker-compose logs broker
```

Common issues:
- Port conflicts (check if ports 5000 or 8080 are in use)
- Resource constraints (check `docker stats`)

### Publishers/Subscribers Can't Connect

Ensure the init container completed successfully:
```bash
docker-compose logs init
```

Check network connectivity:
```bash
docker-compose exec publisher ping broker
```

### High Error Rates

Check publisher logs for specific errors:
```bash
docker-compose logs publisher | grep -i error
```

Common causes:
- Broker overload (reduce rate or add more broker resources)
- Network issues (check Docker network configuration)
- Resource constraints (increase Docker resource limits)

### Rebuilding Images

If you make changes to the code, rebuild the images:

```bash
./run-test.sh --build
# Or
docker-compose build --no-cache
```

## Performance Tips

1. **Increase Docker Resources**: Allocate more CPU and memory to Docker
2. **Tune Buffer Sizes**: Adjust FELIX_EVENT_* environment variables
3. **Disable Timing**: Set `FELIX_DISABLE_TIMINGS=1` for lower overhead
4. **Use Binary Mode**: Configure Felix to use binary encoding
5. **Batch Messages**: Adjust batch sizes for better throughput

## Cleanup

Remove all containers, networks, and volumes:

```bash
./run-test.sh --down
# Or
docker-compose down -v
```

## Development

### Building Locally

To build and test locally without Docker:

```bash
cd test-harness
cargo build --release
./target/release/publisher --help
./target/release/subscriber --help
./target/release/init-broker --help
```

### Adding New Test Clients

1. Create a new binary in `test-harness/src/`
2. Add it to `Cargo.toml` as a `[[bin]]` entry
3. Update `test-harness/Dockerfile` to copy the binary
4. Add a service definition in `docker-compose.yml`

## License

Apache 2.0 - See LICENSE file in the repository root
