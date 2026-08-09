/*
 * Enhances github-action-benchmark's generated Chart.js 2.9.2 dashboards.
 * The Pages workflow injects this after Chart.js and before the generated page
 * constructs its charts.
 */
(function () {
  'use strict';

  if (typeof Chart === 'undefined' || !Chart.plugins || !Chart.plugins.register) {
    return;
  }

  var charts = [];
  var storagePrefix = 'felixBenchmark.';
  var state = {
    rollingWindow: readNumber('rollingWindow', 10),
    zeroAxis: readBoolean('zeroAxis', false),
    showNoise: readBoolean('showNoise', true),
    showThreshold: readBoolean('showThreshold', true),
    showConfig: readBoolean('showConfig', true),
  };
  var series = null;

  function readNumber(key, fallback) {
    var value = Number(window.localStorage.getItem(storagePrefix + key));
    return isFinite(value) && value > 0 ? value : fallback;
  }

  function readBoolean(key, fallback) {
    var value = window.localStorage.getItem(storagePrefix + key);
    return value === null ? fallback : value === 'true';
  }

  function saveState() {
    Object.keys(state).forEach(function (key) {
      window.localStorage.setItem(storagePrefix + key, String(state[key]));
    });
  }

  function median(values) {
    var sorted = values.slice().sort(function (a, b) {
      return a - b;
    });
    var mid = Math.floor(sorted.length / 2);
    return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
  }

  function formatValue(value) {
    var abs = Math.abs(value);
    if (abs >= 999500) {
      return (value / 1000000).toFixed(2) + 'M';
    }
    if (abs >= 1000) {
      return (value / 1000).toFixed(abs >= 100000 ? 0 : 1) + 'k';
    }
    if (abs >= 100) {
      return value.toFixed(1);
    }
    return value.toFixed(2);
  }

  function formatPercent(value) {
    if (!isFinite(value)) {
      return 'n/a';
    }
    return (value >= 0 ? '+' : '') + value.toFixed(1) + '%';
  }

  function parseExtra(extra) {
    var fields = {};
    String(extra || '')
      .split('\n')
      .forEach(function (line) {
        var colon = line.indexOf(':');
        if (colon > 0) {
          fields[line.slice(0, colon).trim().toLowerCase()] = line
            .slice(colon + 1)
            .trim();
        }
      });
    return fields;
  }

  function buildSeriesIndex() {
    var index = {};
    var data = window.BENCHMARK_DATA;
    if (!data || !data.entries) {
      return index;
    }
    Object.keys(data.entries).forEach(function (setName) {
      data.entries[setName].forEach(function (entry) {
        (entry.benches || []).forEach(function (bench) {
          if (!index[bench.name]) {
            index[bench.name] = [];
          }
          index[bench.name].push({
            bench: bench,
            commit: entry.commit,
            date: entry.date,
            tool: entry.tool,
            fields: parseExtra(bench.extra),
          });
        });
      });
    });
    return index;
  }

  function seriesIndex() {
    if (series === null) {
      series = buildSeriesIndex();
    }
    return series;
  }

  function chartSeries(chart) {
    var datasets = (chart.data && chart.data.datasets) || [];
    var index = seriesIndex();
    return datasets.length ? index[datasets[0].label] || [] : [];
  }

  function valuesFor(chart) {
    var datasets = (chart.data && chart.data.datasets) || [];
    return datasets.length
      ? (datasets[0].data || []).filter(function (value) {
          return typeof value === 'number' && isFinite(value);
        })
      : [];
  }

  function directionFor(chart) {
    var metadata = chartSeries(chart);
    return metadata.length && metadata[0].tool === 'customSmallerIsBetter'
      ? 'smaller'
      : 'bigger';
  }

  function priorWindow(values) {
    if (values.length < 2) {
      return [];
    }
    var end = values.length - 1;
    var start = Math.max(0, end - state.rollingWindow);
    return values.slice(start, end);
  }

  function findValueScale(chart) {
    var scales = chart.scales || {};
    var keys = Object.keys(scales);
    for (var i = 0; i < keys.length; i++) {
      var scale = scales[keys[i]];
      if (scale && typeof scale.isHorizontal === 'function' && !scale.isHorizontal()) {
        return scale;
      }
    }
    return null;
  }

  function findPoints(chart) {
    var meta = chart.getDatasetMeta(0);
    return meta && meta.data ? meta.data : [];
  }

  function pointXY(point) {
    return point && (point._view || point._model);
  }

  function applyAxisMode(chart) {
    var yAxes =
      chart.options &&
      chart.options.scales &&
      chart.options.scales.yAxes;
    if (yAxes && yAxes.length) {
      yAxes[0].ticks = yAxes[0].ticks || {};
      yAxes[0].ticks.beginAtZero = state.zeroAxis;
    }
  }

  function refreshCharts() {
    saveState();
    charts.forEach(function (chart) {
      applyAxisMode(chart);
      updateSummary(chart);
      chart.update();
    });
  }

  function addCheckbox(parent, label, key) {
    var wrapper = document.createElement('label');
    wrapper.className = 'felix-control-check';
    var input = document.createElement('input');
    input.type = 'checkbox';
    input.checked = state[key];
    input.onchange = function () {
      state[key] = input.checked;
      refreshCharts();
    };
    wrapper.appendChild(input);
    wrapper.appendChild(document.createTextNode(' ' + label));
    parent.appendChild(wrapper);
  }

  function ensureControls() {
    if (document.getElementById('felix-benchmark-controls')) {
      return;
    }
    var controls = document.createElement('section');
    controls.id = 'felix-benchmark-controls';

    var axisButton = document.createElement('button');
    axisButton.type = 'button';
    function updateAxisButton() {
      axisButton.textContent = state.zeroAxis ? 'Axis: zero-based' : 'Axis: trend-focused';
    }
    updateAxisButton();
    axisButton.onclick = function () {
      state.zeroAxis = !state.zeroAxis;
      updateAxisButton();
      refreshCharts();
    };
    controls.appendChild(axisButton);

    var rollingLabel = document.createElement('label');
    rollingLabel.textContent = 'Prior median window: ';
    var select = document.createElement('select');
    [5, 10, 20, 50].forEach(function (value) {
      var option = document.createElement('option');
      option.value = String(value);
      option.textContent = String(value) + ' commits';
      option.selected = state.rollingWindow === value;
      select.appendChild(option);
    });
    select.onchange = function () {
      state.rollingWindow = Number(select.value);
      refreshCharts();
    };
    rollingLabel.appendChild(select);
    controls.appendChild(rollingLabel);

    addCheckbox(controls, 'noise band', 'showNoise');
    addCheckbox(controls, 'alert threshold', 'showThreshold');
    addCheckbox(controls, 'config markers', 'showConfig');

    var header = document.getElementById('header');
    if (header && header.parentNode) {
      header.parentNode.insertBefore(controls, header.nextSibling);
    }
  }

  function ensureStyles() {
    if (document.getElementById('felix-benchmark-styles')) {
      return;
    }
    var style = document.createElement('style');
    style.id = 'felix-benchmark-styles';
    style.textContent =
      '#felix-benchmark-controls{position:sticky;top:0;z-index:10;display:flex;gap:12px;align-items:center;flex-wrap:wrap;padding:10px 12px;margin:8px 0 16px;border:1px solid #d0d7de;border-radius:8px;background:rgba(255,255,255,.96);box-shadow:0 1px 3px rgba(0,0,0,.08)}' +
      '#felix-benchmark-controls button,#felix-benchmark-controls select{font:inherit;padding:5px 9px;border:1px solid #8c959f;border-radius:6px;background:#f6f8fa;color:#24292f}' +
      '.felix-control-check{white-space:nowrap}' +
      '.felix-chart-card{box-sizing:border-box;width:min(100%,1000px);padding:14px;margin:10px;border:1px solid #d8dee4;border-radius:10px;background:#fff;box-shadow:0 1px 2px rgba(0,0,0,.06)}' +
      '.felix-chart-title{margin:0 0 7px;font-size:1.05rem;line-height:1.35;color:#24292f}' +
      '.felix-chart-summary{display:flex;gap:8px;flex-wrap:wrap;margin:0 0 10px;font-size:.78rem;color:#57606a}' +
      '.felix-stat{padding:3px 7px;border-radius:999px;background:#f6f8fa;border:1px solid #d8dee4}' +
      '.felix-stat.good{color:#1a7f37;background:#dafbe1;border-color:#aceebb}' +
      '.felix-stat.bad{color:#cf222e;background:#ffebe9;border-color:#ffcecb}' +
      '.felix-stat.neutral{color:#8250df;background:#fbefff;border-color:#eac8ff}' +
      '.felix-chart-card .benchmark-chart{max-width:100%;width:100%!important}' +
      '@media(prefers-color-scheme:dark){#felix-benchmark-controls,.felix-chart-card{background:#161b22;color:#c9d1d9;border-color:#30363d}.felix-chart-title{color:#f0f6fc}.felix-chart-summary{color:#8b949e}.felix-stat{background:#21262d;border-color:#30363d}#felix-benchmark-controls button,#felix-benchmark-controls select{background:#21262d;color:#c9d1d9;border-color:#484f58}}';
    document.head.appendChild(style);
  }

  function humanTitle(raw) {
    var match = String(raw).match(
      /^([^/]+)\/([^ ]+) fanout=(\d+) batch=(\d+) payload=(\d+)B - (.+)$/
    );
    if (!match) {
      return raw;
    }
    var preset = match[2];
    var fanout = match[3];
    var payload = Number(match[5]);
    var metric = match[6]
      .replace('delivered throughput (msg/s)', 'Delivered aggregate throughput')
      .replace('throughput (msg/s)', 'Publisher throughput')
      .replace('(us)', 'latency');
    var payloadLabel = payload >= 1024 && payload % 1024 === 0
      ? payload / 1024 + ' KiB'
      : payload + ' B';
    return metric + ' · fanout ' + fanout + ' · ' + payloadLabel + ' · ' + preset;
  }

  function stat(parent, label, value, className) {
    var item = document.createElement('span');
    item.className = 'felix-stat ' + (className || '');
    item.textContent = label + ': ' + value;
    parent.appendChild(item);
  }

  function updateSummary(chart) {
    if (!chart.$felixSummary) {
      return;
    }
    var summary = chart.$felixSummary;
    while (summary.firstChild) {
      summary.removeChild(summary.firstChild);
    }
    var values = valuesFor(chart);
    if (!values.length) {
      return;
    }
    var latest = values[values.length - 1];
    var prior = priorWindow(values);
    var baseline = prior.length ? median(prior) : null;
    var direction = directionFor(chart);
    var delta = baseline ? ((latest - baseline) / baseline) * 100 : null;
    var good = delta === null
      ? 'neutral'
      : (direction === 'smaller' ? delta <= 0 : delta >= 0)
        ? 'good'
        : 'bad';
    var metadata = chartSeries(chart);
    var latestMeta = metadata[metadata.length - 1];
    var cv = latestMeta && latestMeta.fields.cv ? latestMeta.fields.cv : 'unknown';

    stat(summary, 'latest', formatValue(latest), good);
    stat(summary, 'vs prior median', formatPercent(delta), good);
    stat(summary, 'prior median', baseline === null ? 'n/a' : formatValue(baseline));
    stat(
      summary,
      'best',
      formatValue(direction === 'smaller' ? Math.min.apply(null, values) : Math.max.apply(null, values)),
      'good'
    );
    stat(
      summary,
      'worst',
      formatValue(direction === 'smaller' ? Math.max.apply(null, values) : Math.min.apply(null, values)),
      'bad'
    );
    stat(summary, 'latest CV', cv);
  }

  function wrapChart(chart) {
    var canvas = chart.canvas;
    if (!canvas || canvas.$felixWrapped || !canvas.parentNode) {
      return;
    }
    canvas.$felixWrapped = true;
    var parent = canvas.parentNode;
    var card = document.createElement('section');
    card.className = 'felix-chart-card';
    var title = document.createElement('h2');
    title.className = 'felix-chart-title';
    title.textContent = humanTitle(chart.data.datasets[0].label);
    var summary = document.createElement('div');
    summary.className = 'felix-chart-summary';
    parent.insertBefore(card, canvas);
    card.appendChild(title);
    card.appendChild(summary);
    card.appendChild(canvas);
    chart.$felixSummary = summary;
    updateSummary(chart);
    window.setTimeout(function () {
      chart.resize();
    }, 0);
  }

  function drawLine(chart, scale, value, color, dash, label, alignLeft) {
    var area = chart.chartArea;
    var ctx = chart.ctx;
    var y = scale.getPixelForValue(value);
    if (!isFinite(y) || y < area.top || y > area.bottom) {
      return;
    }
    ctx.save();
    ctx.beginPath();
    ctx.setLineDash(dash || []);
    ctx.lineWidth = 1.4;
    ctx.strokeStyle = color;
    ctx.moveTo(area.left, y);
    ctx.lineTo(area.right, y);
    ctx.stroke();
    ctx.setLineDash([]);
    ctx.font = '11px -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif';
    ctx.fillStyle = color;
    ctx.textAlign = alignLeft ? 'left' : 'right';
    ctx.textBaseline = y - 4 > area.top + 10 ? 'bottom' : 'top';
    ctx.fillText(
      label + ': ' + formatValue(value),
      alignLeft ? area.left + 4 : area.right - 4,
      y - 4 > area.top + 10 ? y - 3 : y + 3
    );
    ctx.restore();
  }

  function drawNoiseBand(chart, scale) {
    if (!state.showNoise) {
      return;
    }
    var metadata = chartSeries(chart);
    var points = findPoints(chart);
    var values = valuesFor(chart);
    if (!metadata.length || metadata.length !== points.length || values.length !== points.length) {
      return;
    }
    var upper = [];
    var lower = [];
    for (var i = 0; i < points.length; i++) {
      var model = pointXY(points[i]);
      var range = Number(metadata[i].bench.range);
      if (!model || !isFinite(range)) {
        continue;
      }
      upper.push({ x: model.x, y: scale.getPixelForValue(values[i] + range) });
      lower.push({ x: model.x, y: scale.getPixelForValue(Math.max(0, values[i] - range)) });
    }
    if (upper.length < 2) {
      return;
    }
    var ctx = chart.ctx;
    ctx.save();
    ctx.beginPath();
    ctx.moveTo(upper[0].x, upper[0].y);
    upper.slice(1).forEach(function (point) {
      ctx.lineTo(point.x, point.y);
    });
    lower
      .slice()
      .reverse()
      .forEach(function (point) {
        ctx.lineTo(point.x, point.y);
      });
    ctx.closePath();
    ctx.fillStyle = 'rgba(31,111,235,.10)';
    ctx.fill();
    ctx.strokeStyle = 'rgba(31,111,235,.35)';
    ctx.lineWidth = 1;
    for (var j = 0; j < upper.length; j++) {
      ctx.beginPath();
      ctx.moveTo(upper[j].x, upper[j].y);
      ctx.lineTo(lower[j].x, lower[j].y);
      ctx.stroke();
    }
    ctx.restore();
  }

  function drawConfigMarkers(chart) {
    if (!state.showConfig) {
      return;
    }
    var metadata = chartSeries(chart);
    var points = findPoints(chart);
    var area = chart.chartArea;
    var ctx = chart.ctx;
    for (var i = 1; i < metadata.length && i < points.length; i++) {
      var previous = metadata[i - 1].fields.config;
      var current = metadata[i].fields.config;
      if (!previous || !current || previous === 'mixed' || current === 'mixed' || previous === current) {
        continue;
      }
      var model = pointXY(points[i]);
      if (!model) {
        continue;
      }
      ctx.save();
      ctx.beginPath();
      ctx.setLineDash([2, 3]);
      ctx.strokeStyle = 'rgba(130,80,223,.75)';
      ctx.moveTo(model.x, area.top);
      ctx.lineTo(model.x, area.bottom);
      ctx.stroke();
      ctx.setLineDash([]);
      ctx.fillStyle = 'rgba(130,80,223,.95)';
      ctx.font = '10px -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif';
      ctx.fillText('config', model.x + 3, area.top + 10);
      ctx.restore();
    }
  }

  function drawLatest(chart, scale) {
    var values = valuesFor(chart);
    var points = findPoints(chart);
    if (!values.length || !points.length) {
      return;
    }
    var latest = values[values.length - 1];
    var prior = priorWindow(values);
    var baseline = prior.length ? median(prior) : null;
    var delta = baseline ? ((latest - baseline) / baseline) * 100 : null;
    var direction = directionFor(chart);
    var good = delta === null || (direction === 'smaller' ? delta <= 0 : delta >= 0);
    var model = pointXY(points[points.length - 1]);
    var area = chart.chartArea;
    if (!model) {
      return;
    }
    var color = delta === null ? '#8250df' : good ? '#1a7f37' : '#cf222e';
    var ctx = chart.ctx;
    ctx.save();
    ctx.beginPath();
    ctx.fillStyle = '#fff';
    ctx.strokeStyle = color;
    ctx.lineWidth = 3;
    ctx.arc(model.x, model.y, 6, 0, Math.PI * 2);
    ctx.fill();
    ctx.stroke();
    ctx.font = 'bold 11px -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif';
    ctx.fillStyle = color;
    ctx.textAlign = 'right';
    ctx.textBaseline = model.y - 12 > area.top ? 'bottom' : 'top';
    ctx.fillText(
      'latest ' + formatValue(latest) + (delta === null ? '' : ' (' + formatPercent(delta) + ')'),
      Math.min(area.right - 3, model.x),
      model.y - 12 > area.top ? model.y - 9 : model.y + 9
    );
    ctx.restore();
  }

  Chart.plugins.register({
    id: 'felixBenchmarkEnhancements',

    beforeInit: function (chart) {
      ensureStyles();
      ensureControls();
      applyAxisMode(chart);
      chart.options.legend = chart.options.legend || {};
      chart.options.legend.display = false;
      charts.push(chart);
    },

    afterInit: function (chart) {
      wrapChart(chart);
    },

    beforeDatasetsDraw: function (chart) {
      var scale = findValueScale(chart);
      if (!scale) {
        return;
      }
      drawNoiseBand(chart, scale);
      drawConfigMarkers(chart);
    },

    afterDatasetsDraw: function (chart) {
      var values = valuesFor(chart);
      var scale = findValueScale(chart);
      if (!scale || values.length < 1) {
        return;
      }
      if (values.length > 1) {
        drawLine(
          chart,
          scale,
          median(values),
          'rgba(87,96,106,.9)',
          [6, 4],
          'all-history median',
          false
        );
      }
      var prior = priorWindow(values);
      if (prior.length) {
        var rolling = median(prior);
        drawLine(
          chart,
          scale,
          rolling,
          'rgba(31,111,235,.95)',
          [3, 3],
          'prior ' + prior.length + ' median',
          true
        );
        if (state.showThreshold) {
          // Keep these in sync with perf-publish.yml. github-action-benchmark
          // compares the latest point with the immediately preceding point.
          var previous = values[values.length - 2];
          var threshold =
            directionFor(chart) === 'smaller' ? previous * 1.15 : previous * 0.87;
          drawLine(
            chart,
            scale,
            threshold,
            'rgba(191,135,0,.95)',
            [10, 4],
            'alert threshold vs previous',
            false
          );
        }
      }
      drawLatest(chart, scale);
    },
  });
})();
