#!/bin/bash
# Test harness runner script for Felix pub/sub at scale

set -e

# Default values
PUBLISHERS=2
SUBSCRIBERS=2
PUBLISHER_RATE=100
PAYLOAD_SIZE=1024
DURATION=60

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --publishers)
      PUBLISHERS="$2"
      shift 2
      ;;
    --subscribers)
      SUBSCRIBERS="$2"
      shift 2
      ;;
    --rate)
      PUBLISHER_RATE="$2"
      shift 2
      ;;
    --payload-size)
      PAYLOAD_SIZE="$2"
      shift 2
      ;;
    --duration)
      DURATION="$2"
      shift 2
      ;;
    --build)
      BUILD_IMAGES="yes"
      shift
      ;;
    --down)
      docker-compose down -v
      echo "Test harness stopped and cleaned up"
      exit 0
      ;;
    --logs)
      docker-compose logs -f
      exit 0
      ;;
    --help)
      echo "Usage: $0 [OPTIONS]"
      echo ""
      echo "Options:"
      echo "  --publishers N       Number of publisher instances (default: 2)"
      echo "  --subscribers N      Number of subscriber instances (default: 2)"
      echo "  --rate N            Messages per second per publisher (default: 100)"
      echo "  --payload-size N    Message payload size in bytes (default: 1024)"
      echo "  --duration N        Test duration in seconds (default: 60)"
      echo "  --build             Force rebuild of Docker images"
      echo "  --down              Stop and clean up the test harness"
      echo "  --logs              Show logs from all services"
      echo "  --help              Show this help message"
      echo ""
      echo "Examples:"
      echo "  $0 --publishers 5 --subscribers 10 --rate 200"
      echo "  $0 --build --publishers 3 --subscribers 5"
      echo "  $0 --down"
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      echo "Use --help for usage information"
      exit 1
      ;;
  esac
done

echo "=========================================="
echo "Felix Pub/Sub Test Harness"
echo "=========================================="
echo "Configuration:"
echo "  Publishers:   $PUBLISHERS"
echo "  Subscribers:  $SUBSCRIBERS"
echo "  Rate:         $PUBLISHER_RATE msg/s per publisher"
echo "  Payload Size: $PAYLOAD_SIZE bytes"
echo "  Duration:     $DURATION seconds"
echo "=========================================="

# Build images if requested
if [ "$BUILD_IMAGES" = "yes" ]; then
  echo "Building Docker images..."
  docker-compose build
fi

# Create docker-compose override with custom settings
cat > docker-compose.override.yml <<EOF
version: '3.8'

services:
  publisher:
    command:
      - publisher
      - --broker=broker:5000
      - --tenant=test-tenant
      - --namespace=default
      - --stream=test-stream
      - --payload-size=${PAYLOAD_SIZE}
      - --rate=${PUBLISHER_RATE}
      - --count=0
    deploy:
      replicas: ${PUBLISHERS}

  subscriber:
    deploy:
      replicas: ${SUBSCRIBERS}
EOF

echo "Starting test harness..."
docker-compose up -d --scale publisher=${PUBLISHERS} --scale subscriber=${SUBSCRIBERS}

echo ""
echo "Test harness is running!"
echo ""
echo "Monitor logs:    docker-compose logs -f"
echo "Check metrics:   curl http://localhost:8080/metrics"
echo "Stop test:       docker-compose down"
echo ""
echo "Waiting for ${DURATION} seconds..."

# Wait for the specified duration
sleep ${DURATION}

echo ""
echo "=========================================="
echo "Test completed after ${DURATION} seconds"
echo "=========================================="
echo ""
echo "Collecting final logs..."
docker-compose logs --tail=50 publisher
docker-compose logs --tail=50 subscriber

echo ""
echo "To stop the test harness, run:"
echo "  docker-compose down"
