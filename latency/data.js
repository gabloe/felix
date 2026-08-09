window.BENCHMARK_DATA = {
  "lastUpdate": 1786242963922,
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
        "date": 1786242962303,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "balanced/P1_hash fanout=1 batch=1 payload=256B - p50 (us)",
            "value": 155,
            "range": "0.45",
            "unit": "us",
            "extra": "n=5, mean=155.20"
          },
          {
            "name": "balanced/P1_hash fanout=1 batch=1 payload=256B - p99 (us)",
            "value": 209,
            "range": "2.61",
            "unit": "us",
            "extra": "n=5, mean=208.40"
          },
          {
            "name": "balanced/P1_hash fanout=1 batch=1 payload=256B - p999 (us)",
            "value": 271,
            "range": "11.25",
            "unit": "us",
            "extra": "n=5, mean=267.00"
          },
          {
            "name": "balanced/P8_hash fanout=1 batch=64 payload=1024B - p50 (us)",
            "value": 6602,
            "range": "2758.15",
            "unit": "us",
            "extra": "n=5, mean=7860.20"
          },
          {
            "name": "balanced/P8_hash fanout=1 batch=64 payload=1024B - p99 (us)",
            "value": 12091,
            "range": "4246.79",
            "unit": "us",
            "extra": "n=5, mean=12351.40"
          },
          {
            "name": "balanced/P8_hash fanout=1 batch=64 payload=1024B - p999 (us)",
            "value": 12111,
            "range": "4244.65",
            "unit": "us",
            "extra": "n=5, mean=12367.80"
          },
          {
            "name": "balanced/P1_hash fanout=10 batch=1 payload=256B - p50 (us)",
            "value": 205,
            "range": "10.33",
            "unit": "us",
            "extra": "n=5, mean=207.20"
          },
          {
            "name": "balanced/P1_hash fanout=10 batch=1 payload=256B - p99 (us)",
            "value": 603,
            "range": "121.23",
            "unit": "us",
            "extra": "n=5, mean=644.40"
          },
          {
            "name": "balanced/P1_hash fanout=10 batch=1 payload=256B - p999 (us)",
            "value": 1836,
            "range": "499.28",
            "unit": "us",
            "extra": "n=5, mean=1614.40"
          },
          {
            "name": "balanced/P8_hash fanout=10 batch=64 payload=1024B - p50 (us)",
            "value": 34568,
            "range": "4026.78",
            "unit": "us",
            "extra": "n=5, mean=36383.60"
          },
          {
            "name": "balanced/P8_hash fanout=10 batch=64 payload=1024B - p99 (us)",
            "value": 64824,
            "range": "3477.09",
            "unit": "us",
            "extra": "n=5, mean=66734.00"
          },
          {
            "name": "balanced/P8_hash fanout=10 batch=64 payload=1024B - p999 (us)",
            "value": 65521,
            "range": "3342.16",
            "unit": "us",
            "extra": "n=5, mean=66955.00"
          }
        ]
      }
    ]
  }
}