window.BENCHMARK_DATA = {
  "lastUpdate": 1786244474477,
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
        "date": 1786243929535,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "balanced/P1_hash fanout=1 batch=1 payload=256B - throughput (msg/s)",
            "value": 5404.19,
            "range": "44.25",
            "unit": "msg/s",
            "extra": "n=5, mean=5381.99"
          },
          {
            "name": "balanced/P1_hash fanout=1 batch=1 payload=256B - delivered throughput (msg/s)",
            "value": 5404.19,
            "range": "44.25",
            "unit": "msg/s",
            "extra": "n=5, mean=5381.99"
          },
          {
            "name": "balanced/P8_hash fanout=1 batch=64 payload=1024B - throughput (msg/s)",
            "value": 162927.32,
            "range": "4559.16",
            "unit": "msg/s",
            "extra": "n=5, mean=162873.42"
          },
          {
            "name": "balanced/P8_hash fanout=1 batch=64 payload=1024B - delivered throughput (msg/s)",
            "value": 162927.32,
            "range": "4559.16",
            "unit": "msg/s",
            "extra": "n=5, mean=162873.42"
          },
          {
            "name": "balanced/P1_hash fanout=10 batch=1 payload=256B - throughput (msg/s)",
            "value": 4239.67,
            "range": "239.53",
            "unit": "msg/s",
            "extra": "n=5, mean=4118.28"
          },
          {
            "name": "balanced/P1_hash fanout=10 batch=1 payload=256B - delivered throughput (msg/s)",
            "value": 42396.71,
            "range": "2395.34",
            "unit": "msg/s",
            "extra": "n=5, mean=41182.81"
          },
          {
            "name": "balanced/P8_hash fanout=10 batch=64 payload=1024B - throughput (msg/s)",
            "value": 43667.12,
            "range": "2560.95",
            "unit": "msg/s",
            "extra": "n=5, mean=45324.27"
          },
          {
            "name": "balanced/P8_hash fanout=10 batch=64 payload=1024B - delivered throughput (msg/s)",
            "value": 436671.22,
            "range": "25609.49",
            "unit": "msg/s",
            "extra": "n=5, mean=453242.73"
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
        "date": 1786244321677,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "balanced/P1_hash fanout=1 batch=1 payload=256B - throughput (msg/s)",
            "value": 5301.87,
            "range": "55.69",
            "unit": "msg/s",
            "extra": "n=5, mean=5287.74"
          },
          {
            "name": "balanced/P1_hash fanout=1 batch=1 payload=256B - delivered throughput (msg/s)",
            "value": 5301.87,
            "range": "55.69",
            "unit": "msg/s",
            "extra": "n=5, mean=5287.74"
          },
          {
            "name": "balanced/P8_hash fanout=1 batch=64 payload=1024B - throughput (msg/s)",
            "value": 155048.84,
            "range": "13083.80",
            "unit": "msg/s",
            "extra": "n=5, mean=153267.88"
          },
          {
            "name": "balanced/P8_hash fanout=1 batch=64 payload=1024B - delivered throughput (msg/s)",
            "value": 155048.84,
            "range": "13083.80",
            "unit": "msg/s",
            "extra": "n=5, mean=153267.88"
          },
          {
            "name": "balanced/P1_hash fanout=10 batch=1 payload=256B - throughput (msg/s)",
            "value": 4244.79,
            "range": "80.06",
            "unit": "msg/s",
            "extra": "n=5, mean=4279.60"
          },
          {
            "name": "balanced/P1_hash fanout=10 batch=1 payload=256B - delivered throughput (msg/s)",
            "value": 42447.93,
            "range": "800.58",
            "unit": "msg/s",
            "extra": "n=5, mean=42795.97"
          },
          {
            "name": "balanced/P8_hash fanout=10 batch=64 payload=1024B - throughput (msg/s)",
            "value": 47180.38,
            "range": "1252.29",
            "unit": "msg/s",
            "extra": "n=5, mean=47078.63"
          },
          {
            "name": "balanced/P8_hash fanout=10 batch=64 payload=1024B - delivered throughput (msg/s)",
            "value": 471803.75,
            "range": "12522.86",
            "unit": "msg/s",
            "extra": "n=5, mean=470786.24"
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
        "date": 1786244473714,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "balanced/P1_hash fanout=1 batch=1 payload=256B - throughput (msg/s)",
            "value": 5380.78,
            "range": "55.76",
            "unit": "msg/s",
            "extra": "n=5, mean=5416.81"
          },
          {
            "name": "balanced/P1_hash fanout=1 batch=1 payload=256B - delivered throughput (msg/s)",
            "value": 5380.78,
            "range": "55.76",
            "unit": "msg/s",
            "extra": "n=5, mean=5416.81"
          },
          {
            "name": "balanced/P8_hash fanout=1 batch=64 payload=1024B - throughput (msg/s)",
            "value": 166499.14,
            "range": "8238.14",
            "unit": "msg/s",
            "extra": "n=5, mean=171880.83"
          },
          {
            "name": "balanced/P8_hash fanout=1 batch=64 payload=1024B - delivered throughput (msg/s)",
            "value": 166499.14,
            "range": "8238.14",
            "unit": "msg/s",
            "extra": "n=5, mean=171880.83"
          },
          {
            "name": "balanced/P1_hash fanout=10 batch=1 payload=256B - throughput (msg/s)",
            "value": 4355.84,
            "range": "79.17",
            "unit": "msg/s",
            "extra": "n=5, mean=4347.60"
          },
          {
            "name": "balanced/P1_hash fanout=10 batch=1 payload=256B - delivered throughput (msg/s)",
            "value": 43558.36,
            "range": "791.71",
            "unit": "msg/s",
            "extra": "n=5, mean=43476.00"
          },
          {
            "name": "balanced/P8_hash fanout=10 batch=64 payload=1024B - throughput (msg/s)",
            "value": 48483.56,
            "range": "600.05",
            "unit": "msg/s",
            "extra": "n=5, mean=48465.17"
          },
          {
            "name": "balanced/P8_hash fanout=10 batch=64 payload=1024B - delivered throughput (msg/s)",
            "value": 484835.58,
            "range": "6000.49",
            "unit": "msg/s",
            "extra": "n=5, mean=484651.73"
          }
        ]
      }
    ]
  }
}