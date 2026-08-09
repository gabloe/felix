window.BENCHMARK_DATA = {
  "lastUpdate": 1786245493824,
  "repoUrl": "https://github.com/gabloe/felix",
  "entries": {
    "Felix throughput - batch=64, GitHub-hosted runner": [
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
          "id": "029364a1bdfe04c8c4f0df39bec8ff574a643dea",
          "message": "fix(benchmarks): update benchmark data paths and enhance dashboard documentation (#151)",
          "timestamp": "2026-08-08T20:17:07-07:00",
          "tree_id": "fada4e5a008088ce0265f9738898fa07b4cd2e35",
          "url": "https://github.com/gabloe/felix/commit/029364a1bdfe04c8c4f0df39bec8ff574a643dea"
        },
        "date": 1786245493107,
        "tool": "customBiggerIsBetter",
        "benches": [
          {
            "name": "balanced/P8_hash fanout=1 batch=64 payload=1024B - throughput (msg/s)",
            "value": 222282.68,
            "range": "19158.80",
            "unit": "msg/s",
            "extra": "n=5, mean=230169.89"
          },
          {
            "name": "balanced/P8_hash fanout=1 batch=64 payload=1024B - delivered throughput (msg/s)",
            "value": 222282.68,
            "range": "19158.80",
            "unit": "msg/s",
            "extra": "n=5, mean=230169.89"
          },
          {
            "name": "balanced/P8_hash fanout=10 batch=64 payload=1024B - throughput (msg/s)",
            "value": 64720.38,
            "range": "14187.97",
            "unit": "msg/s",
            "extra": "n=5, mean=59037.55"
          },
          {
            "name": "balanced/P8_hash fanout=10 batch=64 payload=1024B - delivered throughput (msg/s)",
            "value": 647203.84,
            "range": "141879.71",
            "unit": "msg/s",
            "extra": "n=5, mean=590375.50"
          }
        ]
      }
    ]
  }
}