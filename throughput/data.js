window.BENCHMARK_DATA = {
  "lastUpdate": 1786242966010,
  "repoUrl": "https://github.com/gabloe/felix",
  "entries": {
    "Benchmark": [
      {
        "commit": {
          "author": {
            "email": "gabrielloewen@outlook.com",
            "name": "Gabriel Loewen",
            "username": "gabloe"
          },
          "committer": {
            "email": "noreply@github.com",
            "name": "GitHub",
            "username": "web-flow"
          },
          "distinct": true,
          "id": "b1113abe4c68ab6f2c47d0d012907244e17059db",
          "message": "fix(pubsub): prevent subscription ID collisions and fanout delivery loss (#148)\n\n* fix(pubsub): prevent subscription ID collisions and fanout delivery loss\n\n• use broker-assigned subscription IDs\n• atomically create per-connection writers\n• apply lossless backpressure to binary benchmark runs\n• align subscriber idle timeouts and improve warmup errors\n\n* fix(perf): update performance scripts and configurations for accuracy",
          "timestamp": "2026-08-08T19:32:34-07:00",
          "tree_id": "2eb15bbbee558651abf2a3cb7f5092dbd99949ea",
          "url": "https://github.com/gabloe/felix/commit/b1113abe4c68ab6f2c47d0d012907244e17059db"
        },
        "date": 1786242965346,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "balanced/P1_hash fanout=1 batch=1 payload=256B - throughput (msg/s)",
            "value": 6688.22,
            "range": "24.24",
            "unit": "msg/s",
            "extra": "n=5, mean=6688.83"
          },
          {
            "name": "balanced/P1_hash fanout=1 batch=1 payload=256B - delivered throughput (msg/s)",
            "value": 6688.22,
            "range": "24.24",
            "unit": "msg/s",
            "extra": "n=5, mean=6688.83"
          },
          {
            "name": "balanced/P8_hash fanout=1 batch=64 payload=1024B - throughput (msg/s)",
            "value": 173776.64,
            "range": "14560.49",
            "unit": "msg/s",
            "extra": "n=5, mean=173524.20"
          },
          {
            "name": "balanced/P8_hash fanout=1 batch=64 payload=1024B - delivered throughput (msg/s)",
            "value": 173776.64,
            "range": "14560.49",
            "unit": "msg/s",
            "extra": "n=5, mean=173524.20"
          },
          {
            "name": "balanced/P1_hash fanout=10 batch=1 payload=256B - throughput (msg/s)",
            "value": 4696.98,
            "range": "327.06",
            "unit": "msg/s",
            "extra": "n=5, mean=4806.13"
          },
          {
            "name": "balanced/P1_hash fanout=10 batch=1 payload=256B - delivered throughput (msg/s)",
            "value": 46969.83,
            "range": "3270.59",
            "unit": "msg/s",
            "extra": "n=5, mean=48061.32"
          },
          {
            "name": "balanced/P8_hash fanout=10 batch=64 payload=1024B - throughput (msg/s)",
            "value": 51786.28,
            "range": "733.91",
            "unit": "msg/s",
            "extra": "n=5, mean=51678.50"
          },
          {
            "name": "balanced/P8_hash fanout=10 batch=64 payload=1024B - delivered throughput (msg/s)",
            "value": 517862.81,
            "range": "7339.11",
            "unit": "msg/s",
            "extra": "n=5, mean=516784.98"
          }
        ]
      }
    ]
  }
}