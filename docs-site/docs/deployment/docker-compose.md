# Docker Compose Deployment

This guide demonstrates how to deploy Felix using Docker Compose for local development, testing, and small-scale production scenarios.

## Overview

Docker Compose provides an easy way to run Felix with multiple components:

- **Felix broker**: Main data plane service
- **Prometheus** (optional): Metrics collection
- **OpenTelemetry Collector** (optional): Distributed tracing
- **Control plane** (future): Metadata and coordination

!!! info "Compose vs Kubernetes"
    Use Docker Compose for local development and testing. For production deployments with high availability, see the [Kubernetes guide](kubernetes.md).

## Prerequisites

- **Docker**: 20.10 or later
- **Docker Compose**: v2.0 or later (or `docker compose` plugin)
- **4GB RAM minimum**: Recommended 8GB for comfortable operation
- **Git**: To clone the repository

Install Docker Compose:

```bash
# Check if already installed
docker compose version

# If not, install Docker Desktop (includes Compose)
# Or install standalone: https://docs.docker.com/compose/install/
```

## Quick Start

### Basic Broker Deployment

Create a minimal `docker-compose.yml`:

```yaml
version: '3.8'

services:
  felix-broker:
    image: felix/broker:latest
    build:
      context: .
      dockerfile: docker/broker.Dockerfile
      args:
        PROFILE: release
    ports:
      - "5000:5000/udp"  # QUIC data plane
      - "8080:8080"      # Metrics HTTP
    environment:
      - FELIX_QUIC_BIND=0.0.0.0:5000
      - FELIX_BROKER_METRICS_BIND=0.0.0.0:8080
      - RUST_LOG=info
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "wget", "-qO-", "http://localhost:8080/healthz"]
      interval: 10s
      timeout: 2s
      retries: 3
      start_period: 10s
```

**Start the broker:**

```bash
docker compose up -d
```

**Check status:**

```bash
docker compose ps
docker compose logs -f felix-broker
```

**Test connectivity:**

```bash
curl http://localhost:8080/healthz
```

### Full Stack with Observability

Complete setup with monitoring:

**`docker-compose.yml`:**

```yaml
version: '3.8'

services:
  felix-broker:
    image: felix/broker:latest
    build:
      context: .
      dockerfile: docker/broker.Dockerfile
      args:
        PROFILE: release
        CARGO_FEATURES: "--features telemetry"
    ports:
      - "5000:5000/udp"
      - "8080:8080"
    environment:
      - FELIX_QUIC_BIND=0.0.0.0:5000
      - FELIX_BROKER_METRICS_BIND=0.0.0.0:8080
      - FELIX_EVENT_BATCH_MAX_EVENTS=64
      - FELIX_EVENT_BATCH_MAX_DELAY_US=250
      - FELIX_CACHE_CONN_POOL=8
      - FELIX_CACHE_STREAMS_PER_CONN=4
      - RUST_LOG=info
    volumes:
      - felix-data:/data
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "wget", "-qO-", "http://localhost:8080/healthz"]
      interval: 10s
      timeout: 2s
      retries: 3
      start_period: 10s
    networks:
      - felix-net

  prometheus:
    image: prom/prometheus:latest
    ports:
      - "9090:9090"
    volumes:
      - ./docker/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro
      - prometheus-data:/prometheus
    command:
      - '--config.file=/etc/prometheus/prometheus.yml'
      - '--storage.tsdb.path=/prometheus'
      - '--web.console.libraries=/usr/share/prometheus/console_libraries'
      - '--web.console.templates=/usr/share/prometheus/consoles'
    restart: unless-stopped
    networks:
      - felix-net
    depends_on:
      - felix-broker

  otel-collector:
    image: otel/opentelemetry-collector:latest
    ports:
      - "4317:4317"   # OTLP gRPC
      - "4318:4318"   # OTLP HTTP
      - "8888:8888"   # Prometheus metrics
    volumes:
      - ./docker/otel-collector/config.yml:/etc/otel-collector-config.yml:ro
    command: ["--config=/etc/otel-collector-config.yml"]
    restart: unless-stopped
    networks:
      - felix-net

volumes:
  felix-data:
    driver: local
  prometheus-data:
    driver: local

networks:
  felix-net:
    driver: bridge
```

**Prometheus configuration** (`docker/prometheus/prometheus.yml`):

```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: 'felix-broker'
    static_configs:
      - targets: ['felix-broker:8080']
        labels:
          service: 'felix'
          component: 'broker'

  - job_name: 'otel-collector'
    static_configs:
      - targets: ['otel-collector:8888']
```

**Start the full stack:**

```bash
docker compose up -d

# View logs
docker compose logs -f

# Check services
docker compose ps

# Access Prometheus UI
open http://localhost:9090
```

## Configuration Options

### Environment Variables

Pass configuration via environment variables in `docker-compose.yml`:

```yaml
services:
  felix-broker:
    environment:
      # Network
      - FELIX_QUIC_BIND=0.0.0.0:5000
      - FELIX_BROKER_METRICS_BIND=0.0.0.0:8080
      
      # Control plane
      - FELIX_CONTROLPLANE_URL=http://controlplane:8443
      - FELIX_CONTROLPLANE_SYNC_INTERVAL_MS=2000
      
      # Publishing
      - FELIX_ACK_ON_COMMIT=false
      - FELIX_MAX_FRAME_BYTES=16777216
      - FELIX_PUBLISH_QUEUE_WAIT_MS=2000
      
      # Event batching
      - FELIX_EVENT_BATCH_MAX_EVENTS=64
      - FELIX_EVENT_BATCH_MAX_BYTES=262144
      - FELIX_EVENT_BATCH_MAX_DELAY_US=250
      - FELIX_FANOUT_BATCH=64
      
      # Cache
      - FELIX_CACHE_CONN_POOL=8
      - FELIX_CACHE_STREAMS_PER_CONN=4
      - FELIX_CACHE_CONN_RECV_WINDOW=268435456
      - FELIX_CACHE_STREAM_RECV_WINDOW=67108864
      
      # Performance
      - FELIX_DISABLE_TIMINGS=false
      - FELIX_BINARY_SINGLE_EVENT=false
      
      # Logging
      - RUST_LOG=info
```

### Config File Mount

Use a YAML config file instead:

**`config/broker.yml`:**

```yaml
quic_bind: "0.0.0.0:5000"
metrics_bind: "0.0.0.0:8080"
event_batch_max_events: 64
event_batch_max_delay_us: 250
cache_conn_recv_window: 268435456
```

**Mount in Compose:**

```yaml
services:
  felix-broker:
    volumes:
      - ./config/broker.yml:/etc/felix/broker.yml:ro
    environment:
      - FELIX_BROKER_CONFIG=/etc/felix/broker.yml
```

## Multi-Broker Setup

Deploy multiple broker instances for testing clustering behavior:

```yaml
version: '3.8'

services:
  felix-broker-1:
    image: felix/broker:latest
    build:
      context: .
      dockerfile: docker/broker.Dockerfile
    ports:
      - "5001:5000/udp"
      - "8081:8080"
    environment:
      - FELIX_QUIC_BIND=0.0.0.0:5000
      - FELIX_BROKER_METRICS_BIND=0.0.0.0:8080
      - RUST_LOG=info
    hostname: broker-1
    networks:
      - felix-net

  felix-broker-2:
    image: felix/broker:latest
    ports:
      - "5002:5000/udp"
      - "8082:8080"
    environment:
      - FELIX_QUIC_BIND=0.0.0.0:5000
      - FELIX_BROKER_METRICS_BIND=0.0.0.0:8080
      - RUST_LOG=info
    hostname: broker-2
    networks:
      - felix-net

  felix-broker-3:
    image: felix/broker:latest
    ports:
      - "5003:5000/udp"
      - "8083:8080"
    environment:
      - FELIX_QUIC_BIND=0.0.0.0:5000
      - FELIX_BROKER_METRICS_BIND=0.0.0.0:8080
      - RUST_LOG=info
    hostname: broker-3
    networks:
      - felix-net

networks:
  felix-net:
    driver: bridge
```

**Access each broker:**

```bash
# Broker 1
curl http://localhost:8081/healthz

# Broker 2
curl http://localhost:8082/healthz

# Broker 3
curl http://localhost:8083/healthz
```

## Building Images

### Building Locally

Build the broker image from source:

```bash
# Build with default settings
docker compose build

# Build with specific profile
docker compose build --build-arg PROFILE=release

# Build with telemetry enabled
docker compose build --build-arg CARGO_FEATURES="--features telemetry"

# Build with custom RUSTFLAGS
docker compose build --build-arg RUSTFLAGS="-C target-cpu=native"
```

### Using Pre-built Images

When official images are available:

```yaml
services:
  felix-broker:
    image: ghcr.io/gabloe/felix-broker:latest
    # Or specific version
    # image: ghcr.io/gabloe/felix-broker:v0.1.0
```

## Persistence and Volumes

### Data Persistence

Store broker data on persistent volumes:

```yaml
services:
  felix-broker:
    volumes:
      - felix-data:/data
      - felix-logs:/var/log/felix

volumes:
  felix-data:
    driver: local
    driver_opts:
      type: none
      o: bind
      device: /path/to/host/data
  
  felix-logs:
    driver: local
```

### Backup Strategy

```bash
# Backup volume data
docker run --rm -v felix-data:/data -v $(pwd):/backup \
  alpine tar czf /backup/felix-data-backup.tar.gz -C /data .

# Restore from backup
docker run --rm -v felix-data:/data -v $(pwd):/backup \
  alpine tar xzf /backup/felix-data-backup.tar.gz -C /data
```

## Networking

### Bridge Network (Default)

Services communicate via internal network:

```yaml
networks:
  felix-net:
    driver: bridge
    ipam:
      config:
        - subnet: 172.28.0.0/16
```

### Host Network

Use host networking for better performance:

```yaml
services:
  felix-broker:
    network_mode: host
    environment:
      - FELIX_QUIC_BIND=0.0.0.0:5000
```

!!! warning "Host Networking Limitations"
    Host networking doesn't work on Docker Desktop for Mac/Windows. Use bridge networking or run on Linux.

## Resource Limits

Constrain resource usage:

```yaml
services:
  felix-broker:
    deploy:
      resources:
        limits:
          cpus: '4'
          memory: 4G
        reservations:
          cpus: '2'
          memory: 2G
    ulimits:
      nofile:
        soft: 65536
        hard: 65536
```

## Health Checks

Configure health checks for automatic restart:

```yaml
services:
  felix-broker:
    healthcheck:
      test: ["CMD", "wget", "-qO-", "http://localhost:8080/healthz"]
      interval: 10s
      timeout: 2s
      retries: 3
      start_period: 10s
```

## Common Operations

### Starting Services

```bash
# Start all services
docker compose up -d

# Start specific service
docker compose up -d felix-broker

# Start with rebuild
docker compose up -d --build
```

### Stopping Services

```bash
# Stop all services
docker compose stop

# Stop specific service
docker compose stop felix-broker

# Stop and remove containers
docker compose down

# Stop and remove volumes
docker compose down -v
```

### Viewing Logs

```bash
# All services
docker compose logs -f

# Specific service
docker compose logs -f felix-broker

# Last 100 lines
docker compose logs --tail=100 felix-broker
```

### Scaling Services

```bash
# Run 3 broker instances
docker compose up -d --scale felix-broker=3

# Note: You'll need to configure dynamic ports
```

### Executing Commands

```bash
# Shell into container
docker compose exec felix-broker /bin/sh

# Run one-off command
docker compose exec felix-broker ls -la /data
```

## Development Workflow

### Live Reloading Setup

For development with live code updates:

```yaml
services:
  felix-broker:
    build:
      context: .
      dockerfile: docker/broker.Dockerfile
      target: builder  # Stop at build stage
    volumes:
      - .:/src
      - cargo-cache:/usr/local/cargo/registry
    command: cargo watch -x 'run --release -p broker'
    
volumes:
  cargo-cache:
```

### Running Tests in Docker

```bash
# Run tests
docker compose run --rm felix-broker cargo test --workspace

# Run specific test
docker compose run --rm felix-broker cargo test test_name

# Run with output
docker compose run --rm felix-broker cargo test -- --nocapture
```

## Monitoring and Debugging

### Prometheus Queries

Access Prometheus UI at `http://localhost:9090`:

```promql
# Request rate
rate(felix_broker_requests_total[1m])

# Error rate
rate(felix_broker_errors_total[1m])

# Latency p99
histogram_quantile(0.99, rate(felix_broker_request_duration_seconds_bucket[5m]))
```

### Container Metrics

```bash
# Container stats
docker compose stats

# Inspect container
docker compose inspect felix-broker

# View container processes
docker compose top felix-broker
```

## Troubleshooting

### Container Won't Start

```bash
# Check logs
docker compose logs felix-broker

# Check exit code
docker compose ps felix-broker

# Run interactively
docker compose run --rm felix-broker /bin/sh
```

### Port Conflicts

**Error:** `port is already allocated`

**Solution:**

```yaml
ports:
  - "5001:5000/udp"  # Change host port
  - "8081:8080"
```

### Build Failures

```bash
# Clean build cache
docker compose build --no-cache

# Remove old images
docker image prune -a

# Check Dockerfile
docker compose config
```

### Connection Issues

```bash
# Check network
docker network inspect felix_felix-net

# Test connectivity between services
docker compose exec felix-broker ping prometheus

# Check DNS resolution
docker compose exec felix-broker nslookup felix-broker
```

### Performance Issues

```bash
# Check resource usage
docker compose stats

# Increase resources in docker-compose.yml
deploy:
  resources:
    limits:
      memory: 8G

# Use host networking
network_mode: host
```

## Next Steps

- **Production deployment**: [Kubernetes Guide](kubernetes.md)
- **Performance tuning**: [Performance Guide](../features/performance.md)
- **Full configuration reference**: [Configuration Reference](../reference/configuration.md)
- **Monitoring setup**: [Observability Guide](../features/observability.md)
