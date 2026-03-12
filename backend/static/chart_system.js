(function () {
  "use strict";

  function toNumber(value) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : 0;
  }

  function normalizeLabels(labels) {
    if (!Array.isArray(labels)) return [];
    return labels.map((label) => String(label ?? ""));
  }

  function normalizeSeries(labels, series) {
    const safeLabels = normalizeLabels(labels);
    const safeSeries = Array.isArray(series) ? series : [];
    return safeLabels.map((_, index) => toNumber(safeSeries[index]));
  }

  function normalizeDistribution(distribution) {
    const source = distribution && typeof distribution === "object" ? distribution : {};
    return [
      Math.max(0, toNumber(source.present)),
      Math.max(0, toNumber(source.absent)),
      Math.max(0, toNumber(source.late)),
    ];
  }

  function resolveThemeTokens(theme) {
    const isDark = String(theme || "").toLowerCase() === "dark";
    return {
      textColor: isDark ? "#cbd5e1" : "#334155",
      gridColor: isDark ? "rgba(71,85,105,0.45)" : "#e2e8f0",
      tooltipBackground: isDark ? "#020617" : "#0f172a",
      tooltipText: "#f8fafc",
      tooltipBorder: isDark ? "#334155" : "#1e293b",
      donutBorder: isDark ? "#0f172a" : "#ffffff",
    };
  }

  function applyGlobalChartDefaults(tokens) {
    if (!window.Chart) return;
    Chart.defaults.color = tokens.textColor;
    Chart.defaults.borderColor = tokens.gridColor;
    Chart.defaults.font.family = "ui-sans-serif, system-ui, -apple-system, Segoe UI, sans-serif";
  }

  function createVerticalGradient(canvas, fromColor, toColor) {
    if (!canvas) return fromColor;
    const context = canvas.getContext("2d");
    if (!context) return fromColor;
    const gradientHeight = canvas.clientHeight || 260;
    const gradient = context.createLinearGradient(0, 0, 0, gradientHeight);
    gradient.addColorStop(0, fromColor);
    gradient.addColorStop(1, toColor);
    return gradient;
  }

  function buildTooltipOptions(tokens, showColorBoxes) {
    return {
      backgroundColor: tokens.tooltipBackground,
      titleColor: tokens.tooltipText,
      bodyColor: tokens.tooltipText,
      borderColor: tokens.tooltipBorder,
      borderWidth: 1,
      padding: 10,
      displayColors: Boolean(showColorBoxes),
    };
  }

  function buildAxisOptions(tokens) {
    return {
      x: {
        grid: { color: tokens.gridColor },
        ticks: {
          color: tokens.textColor,
          maxRotation: 0,
          autoSkip: true,
        },
      },
      y: {
        beginAtZero: true,
        grid: { color: tokens.gridColor },
        ticks: {
          color: tokens.textColor,
          precision: 0,
        },
      },
    };
  }

  function applyAxisTheme(chart, tokens) {
    if (!chart || !chart.options) return;
    const scales = chart.options.scales || {};
    if (scales.x) {
      scales.x.grid = scales.x.grid || {};
      scales.x.grid.color = tokens.gridColor;
      scales.x.ticks = scales.x.ticks || {};
      scales.x.ticks.color = tokens.textColor;
    }
    if (scales.y) {
      scales.y.grid = scales.y.grid || {};
      scales.y.grid.color = tokens.gridColor;
      scales.y.ticks = scales.y.ticks || {};
      scales.y.ticks.color = tokens.textColor;
    }
    chart.options.plugins = chart.options.plugins || {};
    chart.options.plugins.tooltip = {
      ...(chart.options.plugins.tooltip || {}),
      ...buildTooltipOptions(tokens, false),
    };
    chart.update("none");
  }

  function applyDonutTheme(chart, tokens) {
    if (!chart || !chart.options) return;
    chart.options.plugins = chart.options.plugins || {};
    chart.options.plugins.legend = chart.options.plugins.legend || {};
    chart.options.plugins.legend.labels = chart.options.plugins.legend.labels || {};
    chart.options.plugins.legend.labels.color = tokens.textColor;
    chart.options.plugins.tooltip = {
      ...(chart.options.plugins.tooltip || {}),
      ...buildTooltipOptions(tokens, true),
    };
    const dataset = chart.data?.datasets?.[0];
    if (dataset) {
      dataset.borderColor = tokens.donutBorder;
    }
    chart.update("none");
  }

  function initCoreCharts(config) {
    if (!window.Chart) return null;
    const options = config && typeof config === "object" ? config : {};
    const gateCanvas = options.gateCanvas;
    const smsCanvas = options.smsCanvas;
    const attendanceCanvas = options.attendanceCanvas;

    if (!gateCanvas || !smsCanvas || !attendanceCanvas) {
      return null;
    }

    const labels = normalizeLabels(options.labels);
    const gateSeries = normalizeSeries(labels, options.gateSeries);
    const smsSeries = normalizeSeries(labels, options.smsSeries);
    const attendanceSeries = normalizeDistribution(options.attendanceDistribution);
    const initialTheme = options.initialTheme || "light";
    const initialTokens = resolveThemeTokens(initialTheme);

    applyGlobalChartDefaults(initialTokens);

    const gateAreaGradient = createVerticalGradient(
      gateCanvas,
      "rgba(5, 150, 105, 0.28)",
      "rgba(5, 150, 105, 0.03)"
    );
    const smsBarGradient = createVerticalGradient(smsCanvas, "#2563eb", "#60a5fa");

    const gateChart = new Chart(gateCanvas, {
      type: "line",
      data: {
        labels,
        datasets: [
          {
            label: "Gate Entries",
            data: gateSeries,
            borderColor: "#059669",
            backgroundColor: gateAreaGradient,
            fill: true,
            tension: 0.35,
            borderWidth: 2.25,
            pointRadius: 2.5,
            pointHoverRadius: 4,
            pointBackgroundColor: "#047857",
            pointBorderColor: "#ffffff",
            pointBorderWidth: 1.5,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
          tooltip: buildTooltipOptions(initialTokens, false),
        },
        scales: buildAxisOptions(initialTokens),
      },
    });

    const smsChart = new Chart(smsCanvas, {
      type: "bar",
      data: {
        labels,
        datasets: [
          {
            label: "SMS Sent",
            data: smsSeries,
            backgroundColor: smsBarGradient,
            borderRadius: 8,
            maxBarThickness: 28,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
          tooltip: buildTooltipOptions(initialTokens, false),
        },
        scales: buildAxisOptions(initialTokens),
      },
    });

    const attendanceChart = new Chart(attendanceCanvas, {
      type: "doughnut",
      data: {
        labels: ["Present", "Absent", "Late"],
        datasets: [
          {
            data: attendanceSeries,
            backgroundColor: ["#16a34a", "#ef4444", "#f59e0b"],
            borderColor: initialTokens.donutBorder,
            borderWidth: 2,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        cutout: "58%",
        plugins: {
          legend: {
            position: "bottom",
            labels: {
              color: initialTokens.textColor,
              boxWidth: 12,
              boxHeight: 12,
              usePointStyle: true,
              pointStyle: "circle",
            },
          },
          tooltip: buildTooltipOptions(initialTokens, true),
        },
      },
    });

    function applyTheme(theme) {
      const tokens = resolveThemeTokens(theme);
      applyGlobalChartDefaults(tokens);
      applyAxisTheme(gateChart, tokens);
      applyAxisTheme(smsChart, tokens);
      applyDonutTheme(attendanceChart, tokens);
    }

    return {
      gateChart,
      smsChart,
      attendanceChart,
      applyTheme,
    };
  }

  function normalizeAskValues(payload) {
    const source = payload && typeof payload === "object" ? payload : {};
    const labels = normalizeLabels(source.labels);
    const rawValues = Array.isArray(source.values) ? source.values : [];
    const values = labels.length
      ? labels.map((_, index) => toNumber(rawValues[index]))
      : rawValues.map((value) => toNumber(value));
    return { labels, values };
  }

  function createAiAskChart(canvas, chartPayload, theme) {
    if (!window.Chart || !canvas) return null;
    const payload = chartPayload && typeof chartPayload === "object" ? chartPayload : {};
    const normalized = normalizeAskValues(payload);
    if (!normalized.labels.length || !normalized.values.length) return null;

    const type = String(payload.type || "bar").toLowerCase() === "line" ? "line" : "bar";
    const tokens = resolveThemeTokens(theme || "light");
    applyGlobalChartDefaults(tokens);

    const chart = new Chart(canvas, {
      type,
      data: {
        labels: normalized.labels,
        datasets: [
          {
            label: payload.label || "Value",
            data: normalized.values,
            backgroundColor: type === "line" ? "rgba(5,150,105,0.16)" : "#2563eb",
            borderColor: "#059669",
            tension: 0.3,
            fill: type === "line",
            borderRadius: type === "bar" ? 6 : 0,
            maxBarThickness: type === "bar" ? 30 : undefined,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
          tooltip: buildTooltipOptions(tokens, false),
        },
        scales: buildAxisOptions(tokens),
      },
    });

    return chart;
  }

  const previous = window.AppCharts && typeof window.AppCharts === "object" ? window.AppCharts : {};
  window.AppCharts = Object.freeze({
    ...previous,
    initCoreCharts,
    createAiAskChart,
  });
})();
