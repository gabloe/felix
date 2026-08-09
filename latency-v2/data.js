window.BENCHMARK_DATA = {
  "lastUpdate": 1786245734282,
  "repoUrl": "https://github.com/gabloe/felix",
  "entries": {
    "Felix latency - batch=1, GitHub-hosted runner": [
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
        "date": 1786245489880,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "balanced/P1_hash fanout=1 batch=1 payload=256B - p50 (us)",
            "value": 119,
            "range": "0.55",
            "unit": "us",
            "extra": "n=5, mean=118.60"
          },
          {
            "name": "balanced/P1_hash fanout=1 batch=1 payload=256B - p99 (us)",
            "value": 156,
            "range": "1.87",
            "unit": "us",
            "extra": "n=5, mean=156.00"
          },
          {
            "name": "balanced/P1_hash fanout=1 batch=1 payload=256B - p999 (us)",
            "value": 197,
            "range": "14.06",
            "unit": "us",
            "extra": "n=5, mean=195.80"
          },
          {
            "name": "balanced/P1_hash fanout=10 batch=1 payload=256B - p50 (us)",
            "value": 157,
            "range": "1.00",
            "unit": "us",
            "extra": "n=5, mean=157.00"
          },
          {
            "name": "balanced/P1_hash fanout=10 batch=1 payload=256B - p99 (us)",
            "value": 461,
            "range": "78.52",
            "unit": "us",
            "extra": "n=5, mean=481.40"
          },
          {
            "name": "balanced/P1_hash fanout=10 batch=1 payload=256B - p999 (us)",
            "value": 1693,
            "range": "814.21",
            "unit": "us",
            "extra": "n=5, mean=1788.80"
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
          "id": "029364a1bdfe04c8c4f0df39bec8ff574a643dea",
          "message": "fix(benchmarks): update benchmark data paths and enhance dashboard documentation (#151)",
          "timestamp": "2026-08-08T20:17:07-07:00",
          "tree_id": "fada4e5a008088ce0265f9738898fa07b4cd2e35",
          "url": "https://github.com/gabloe/felix/commit/029364a1bdfe04c8c4f0df39bec8ff574a643dea"
        },
        "date": 1786245732778,
        "tool": "customSmallerIsBetter",
        "benches": [
          {
            "name": "balanced/P1_hash fanout=1 batch=1 payload=256B - p50 (us)",
            "value": 191,
            "range": "1.73",
            "unit": "us",
            "extra": "n=5, mean=190.00"
          },
          {
            "name": "balanced/P1_hash fanout=1 batch=1 payload=256B - p99 (us)",
            "value": 251,
            "range": "5.15",
            "unit": "us",
            "extra": "n=5, mean=251.00"
          },
          {
            "name": "balanced/P1_hash fanout=1 batch=1 payload=256B - p999 (us)",
            "value": 297,
            "range": "15.66",
            "unit": "us",
            "extra": "n=5, mean=293.20"
          },
          {
            "name": "balanced/P1_hash fanout=10 batch=1 payload=256B - p50 (us)",
            "value": 240,
            "range": "10.41",
            "unit": "us",
            "extra": "n=5, mean=243.60"
          },
          {
            "name": "balanced/P1_hash fanout=10 batch=1 payload=256B - p99 (us)",
            "value": 593,
            "range": "523.10",
            "unit": "us",
            "extra": "n=5, mean=829.20"
          },
          {
            "name": "balanced/P1_hash fanout=10 batch=1 payload=256B - p999 (us)",
            "value": 893,
            "range": "954.58",
            "unit": "us",
            "extra": "n=5, mean=1304.80"
          }
        ]
      }
    ]
  }
}