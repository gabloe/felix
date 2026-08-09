window.BENCHMARK_DATA = {
  "lastUpdate": 1786244319913,
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
      },
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
          "id": "8731761dd9580cfab2d37888976c0ce3507519cd",
          "message": "feat(docs): add historical benchmark dashboards to documentation (#149)",
          "timestamp": "2026-08-08T19:50:58-07:00",
          "tree_id": "0202858300f4337faf66c29402ce2c9f111de9e0",
          "url": "https://github.com/gabloe/felix/commit/8731761dd9580cfab2d37888976c0ce3507519cd"
        },
        "date": 1786243927072,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "balanced/P1_hash fanout=1 batch=1 payload=256B - p50 (us)",
            "value": 190,
            "range": "2.07",
            "unit": "us",
            "extra": "n=5, mean=190.40"
          },
          {
            "name": "balanced/P1_hash fanout=1 batch=1 payload=256B - p99 (us)",
            "value": 250,
            "range": "14.24",
            "unit": "us",
            "extra": "n=5, mean=255.20"
          },
          {
            "name": "balanced/P1_hash fanout=1 batch=1 payload=256B - p999 (us)",
            "value": 316,
            "range": "99.56",
            "unit": "us",
            "extra": "n=5, mean=353.20"
          },
          {
            "name": "balanced/P8_hash fanout=1 batch=64 payload=1024B - p50 (us)",
            "value": 5813,
            "range": "811.18",
            "unit": "us",
            "extra": "n=5, mean=5454.80"
          },
          {
            "name": "balanced/P8_hash fanout=1 batch=64 payload=1024B - p99 (us)",
            "value": 9533,
            "range": "1756.45",
            "unit": "us",
            "extra": "n=5, mean=10245.80"
          },
          {
            "name": "balanced/P8_hash fanout=1 batch=64 payload=1024B - p999 (us)",
            "value": 9553,
            "range": "1756.11",
            "unit": "us",
            "extra": "n=5, mean=10263.80"
          },
          {
            "name": "balanced/P1_hash fanout=10 batch=1 payload=256B - p50 (us)",
            "value": 238,
            "range": "8.20",
            "unit": "us",
            "extra": "n=5, mean=242.60"
          },
          {
            "name": "balanced/P1_hash fanout=10 batch=1 payload=256B - p99 (us)",
            "value": 641,
            "range": "249.60",
            "unit": "us",
            "extra": "n=5, mean=790.40"
          },
          {
            "name": "balanced/P1_hash fanout=10 batch=1 payload=256B - p999 (us)",
            "value": 2228,
            "range": "750.95",
            "unit": "us",
            "extra": "n=5, mean=1874.60"
          },
          {
            "name": "balanced/P8_hash fanout=10 batch=64 payload=1024B - p50 (us)",
            "value": 35742,
            "range": "7501.44",
            "unit": "us",
            "extra": "n=5, mean=33591.80"
          },
          {
            "name": "balanced/P8_hash fanout=10 batch=64 payload=1024B - p99 (us)",
            "value": 62172,
            "range": "12065.06",
            "unit": "us",
            "extra": "n=5, mean=65790.00"
          },
          {
            "name": "balanced/P8_hash fanout=10 batch=64 payload=1024B - p999 (us)",
            "value": 62192,
            "range": "12077.79",
            "unit": "us",
            "extra": "n=5, mean=65825.20"
          }
        ]
      },
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
          "id": "724bcfd3c5e61fa27ff6b3c8e8d8a113185e4ec2",
          "message": "fix(pages): update benchmark dashboards to use local Chart.js instead of CDN (#150)",
          "timestamp": "2026-08-08T19:57:26-07:00",
          "tree_id": "f043862276d5373cc277954f65a39d01cd4587cb",
          "url": "https://github.com/gabloe/felix/commit/724bcfd3c5e61fa27ff6b3c8e8d8a113185e4ec2"
        },
        "date": 1786244318161,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "balanced/P1_hash fanout=1 batch=1 payload=256B - p50 (us)",
            "value": 191,
            "range": "1.92",
            "unit": "us",
            "extra": "n=5, mean=190.80"
          },
          {
            "name": "balanced/P1_hash fanout=1 batch=1 payload=256B - p99 (us)",
            "value": 259,
            "range": "7.54",
            "unit": "us",
            "extra": "n=5, mean=260.40"
          },
          {
            "name": "balanced/P1_hash fanout=1 batch=1 payload=256B - p999 (us)",
            "value": 417,
            "range": "64.59",
            "unit": "us",
            "extra": "n=5, mean=407.00"
          },
          {
            "name": "balanced/P8_hash fanout=1 batch=64 payload=1024B - p50 (us)",
            "value": 8223,
            "range": "4118.90",
            "unit": "us",
            "extra": "n=5, mean=8063.00"
          },
          {
            "name": "balanced/P8_hash fanout=1 batch=64 payload=1024B - p99 (us)",
            "value": 11702,
            "range": "6285.69",
            "unit": "us",
            "extra": "n=5, mean=12751.60"
          },
          {
            "name": "balanced/P8_hash fanout=1 batch=64 payload=1024B - p999 (us)",
            "value": 11713,
            "range": "6282.93",
            "unit": "us",
            "extra": "n=5, mean=12765.40"
          },
          {
            "name": "balanced/P1_hash fanout=10 batch=1 payload=256B - p50 (us)",
            "value": 241,
            "range": "3.24",
            "unit": "us",
            "extra": "n=5, mean=239.00"
          },
          {
            "name": "balanced/P1_hash fanout=10 batch=1 payload=256B - p99 (us)",
            "value": 597,
            "range": "31.49",
            "unit": "us",
            "extra": "n=5, mean=600.80"
          },
          {
            "name": "balanced/P1_hash fanout=10 batch=1 payload=256B - p999 (us)",
            "value": 929,
            "range": "158.74",
            "unit": "us",
            "extra": "n=5, mean=972.80"
          },
          {
            "name": "balanced/P8_hash fanout=10 batch=64 payload=1024B - p50 (us)",
            "value": 41956,
            "range": "5827.36",
            "unit": "us",
            "extra": "n=5, mean=43853.40"
          },
          {
            "name": "balanced/P8_hash fanout=10 batch=64 payload=1024B - p99 (us)",
            "value": 82055,
            "range": "2701.09",
            "unit": "us",
            "extra": "n=5, mean=81856.40"
          },
          {
            "name": "balanced/P8_hash fanout=10 batch=64 payload=1024B - p999 (us)",
            "value": 82072,
            "range": "2700.09",
            "unit": "us",
            "extra": "n=5, mean=81883.60"
          }
        ]
      }
    ]
  }
}