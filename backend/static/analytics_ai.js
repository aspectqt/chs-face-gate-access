(function () {
  const moduleRoot = document.getElementById('aiAnalyticsModule');
  if (!moduleRoot) return;

  const ui = {
    range: document.getElementById('aiRangeFilter'),
    grade: document.getElementById('aiGradeFilter'),
    section: document.getElementById('aiSectionFilter'),
    refreshBtn: document.getElementById('aiRefreshBtn'),
    globalError: document.getElementById('aiGlobalError'),

    insightsMeta: document.getElementById('aiInsightsMeta'),
    insightsList: document.getElementById('aiInsightsList'),

    riskMeta: document.getElementById('aiRiskMeta'),
    riskBody: document.getElementById('aiRiskTableBody'),

    changesMode: document.getElementById('aiChangesMode'),
    changesStart: document.getElementById('aiChangesStart'),
    changesEnd: document.getElementById('aiChangesEnd'),
    compareBtn: document.getElementById('aiCompareBtn'),
    changesSummary: document.getElementById('aiChangesSummary'),
    changesExplain: document.getElementById('aiChangesExplain'),

    askForm: document.getElementById('aiAskForm'),
    askInput: document.getElementById('aiAskInput'),
    askStatus: document.getElementById('aiAskStatus'),
    askTableWrap: document.getElementById('aiAskTableWrap'),
    askHead: document.getElementById('aiAskHead'),
    askBody: document.getElementById('aiAskBody'),
    askChartWrap: document.getElementById('aiAskChartWrap'),
    askChartCanvas: document.getElementById('aiAskChart'),

    actionsList: document.getElementById('aiActionsList'),

    clearDrilldownBtn: document.getElementById('aiClearDrilldown'),
    drilldownTitle: document.getElementById('aiDrilldownTitle'),
    drilldownEmpty: document.getElementById('aiDrilldownEmpty'),
    drilldownTable: document.getElementById('aiDrilldownTable'),
    drilldownHead: document.getElementById('aiDrilldownHead'),
    drilldownBody: document.getElementById('aiDrilldownBody'),
  };

  const defaultRange = moduleRoot.dataset.defaultRange || '7d';
  const defaultChangeMode = moduleRoot.dataset.defaultChangeMode || 'today_vs_yesterday';
  const defaultStart = moduleRoot.dataset.defaultStart || '';
  const defaultEnd = moduleRoot.dataset.defaultEnd || '';

  const state = {
    range: defaultRange,
    grade: '',
    section: '',
    changeMode: defaultChangeMode,
    changeStart: defaultStart,
    changeEnd: defaultEnd,
    askChart: null,
  };

  ui.range.value = state.range;
  ui.changesMode.value = state.changeMode;
  ui.changesStart.value = state.changeStart;
  ui.changesEnd.value = state.changeEnd;

  function debounce(fn, waitMs) {
    let timeout;
    return function (...args) {
      clearTimeout(timeout);
      timeout = setTimeout(() => fn.apply(this, args), waitMs);
    };
  }

  function showError(message) {
    if (!message) {
      ui.globalError.classList.add('hidden');
      ui.globalError.textContent = '';
      return;
    }
    ui.globalError.textContent = message;
    ui.globalError.classList.remove('hidden');
  }

  async function fetchJson(url, options) {
    const res = await fetch(url, options);
    const data = await res.json().catch(() => ({}));
    if (!res.ok || data.status !== 'ok') {
      throw new Error(data.message || 'Request failed');
    }
    return data;
  }

  function createSeverityBadge(level) {
    const safe = String(level || 'info').toLowerCase();
    const classes = {
      info: 'bg-sky-100 text-sky-700 border-sky-200',
      warn: 'bg-amber-100 text-amber-700 border-amber-200',
      high: 'bg-rose-100 text-rose-700 border-rose-200',
    };
    return `<span class="inline-flex items-center rounded-full border px-2 py-0.5 text-[10px] font-semibold uppercase ${classes[safe] || classes.info}">${safe}</span>`;
  }

  function createRiskBadge(score) {
    const value = Number(score || 0);
    if (value >= 70) return '<span class="inline-flex rounded-full bg-rose-100 px-2 py-0.5 text-[11px] font-semibold text-rose-700">High</span>';
    if (value >= 45) return '<span class="inline-flex rounded-full bg-amber-100 px-2 py-0.5 text-[11px] font-semibold text-amber-700">Medium</span>';
    return '<span class="inline-flex rounded-full bg-sky-100 px-2 py-0.5 text-[11px] font-semibold text-sky-700">Low</span>';
  }

  function renderSimpleTable(headEl, bodyEl, columns, rows) {
    headEl.innerHTML = '';
    bodyEl.innerHTML = '';

    const trHead = document.createElement('tr');
    columns.forEach((column) => {
      const th = document.createElement('th');
      th.className = 'px-2.5 py-2 text-left text-[11px] font-semibold uppercase tracking-wide text-slate-600';
      th.textContent = column;
      trHead.appendChild(th);
    });
    headEl.appendChild(trHead);

    rows.forEach((row) => {
      const tr = document.createElement('tr');
      columns.forEach((column) => {
        const td = document.createElement('td');
        td.className = 'px-2.5 py-2 text-slate-700';
        td.textContent = String(row[column] ?? '');
        tr.appendChild(td);
      });
      bodyEl.appendChild(tr);
    });
  }

  function setDrilldown(payload) {
    if (!payload || !Array.isArray(payload.columns) || !Array.isArray(payload.rows)) {
      ui.drilldownTitle.textContent = 'No drilldown selected';
      ui.drilldownEmpty.classList.remove('hidden');
      ui.drilldownTable.classList.add('hidden');
      ui.drilldownHead.innerHTML = '';
      ui.drilldownBody.innerHTML = '';
      return;
    }

    ui.drilldownTitle.textContent = payload.title || 'Supporting Data';
    ui.drilldownEmpty.classList.add('hidden');
    ui.drilldownTable.classList.remove('hidden');
    renderSimpleTable(ui.drilldownHead, ui.drilldownBody, payload.columns, payload.rows);
  }

  function applyPrefill(prefill) {
    if (!prefill || typeof prefill !== 'object') return;
    const nextGrade = (prefill.grade || '').trim();
    const nextSection = (prefill.section || '').trim();

    if (nextGrade) {
      state.grade = nextGrade;
      ui.grade.value = nextGrade;
    }
    if (nextSection) {
      state.section = nextSection;
      ui.section.value = nextSection;
    }
  }

  function renderInsights(payload) {
    const insights = Array.isArray(payload.insights) ? payload.insights : [];
    ui.insightsMeta.textContent = insights.length ? `${insights.length} signal(s)` : 'No signals';

    if (!insights.length) {
      ui.insightsList.innerHTML = '<p class="text-sm text-slate-500">No insights available.</p>';
      return;
    }

    ui.insightsList.innerHTML = '';
    insights.forEach((item) => {
      const row = document.createElement('div');
      row.className = 'rounded-xl border border-slate-200 bg-white p-3 flex items-start justify-between gap-3';
      row.innerHTML = `
        <div class="min-w-0">
          <div class="flex items-center gap-2">
            ${createSeverityBadge(item.severity)}
            <p class="text-sm font-semibold text-slate-800 truncate">${item.title || 'Insight'}</p>
          </div>
          <p class="text-xs text-slate-600 mt-1">${item.explanation || ''}</p>
        </div>
        <button type="button" class="ai-view-insight shrink-0 rounded-lg border border-slate-300 px-2.5 py-1 text-xs font-semibold text-slate-700 hover:bg-slate-100">${item.view_label || 'View'}</button>
      `;

      const btn = row.querySelector('.ai-view-insight');
      btn.addEventListener('click', async () => {
        applyPrefill(item.prefill);
        setDrilldown(item.drilldown);
        await loadAll();
      });
      ui.insightsList.appendChild(row);
    });
  }

  function renderRisk(payload) {
    const rows = Array.isArray(payload.rows) ? payload.rows : [];
    ui.riskMeta.textContent = rows.length ? `Top ${rows.length} students` : 'No at-risk students';

    if (!rows.length) {
      ui.riskBody.innerHTML = '<tr><td colspan="6" class="px-3 py-5 text-center text-slate-500">No risk predictions for the selected filters.</td></tr>';
      return;
    }

    ui.riskBody.innerHTML = rows.map((row) => `
      <tr class="hover:bg-slate-50">
        <td class="px-3 py-2 font-medium text-slate-700">${row.student_id || ''}</td>
        <td class="px-3 py-2 text-slate-700">${row.name || ''}</td>
        <td class="px-3 py-2 text-slate-700">${row.grade || ''}</td>
        <td class="px-3 py-2 text-slate-700">${row.section || ''}</td>
        <td class="px-3 py-2">
          <div class="flex items-center gap-2">
            <span class="font-semibold text-slate-800">${row.risk_score || 0}</span>
            ${createRiskBadge(row.risk_score)}
          </div>
        </td>
        <td class="px-3 py-2 text-xs text-slate-600">${Array.isArray(row.reasons) ? row.reasons.join(' | ') : ''}</td>
      </tr>
    `).join('');
  }

  function renderChanges(payload) {
    const metrics = payload.metrics || {};
    const attendance = metrics.attendance || {};
    const late = metrics.late || {};
    const smsFailed = metrics.sms_failed || {};

    ui.changesSummary.innerHTML = `
      <div class="rounded-xl border border-slate-200 bg-white px-3 py-2 text-xs">
        <p class="text-slate-500">Attendance</p>
        <p class="font-semibold text-slate-800">${attendance.current ?? 0} <span class="text-slate-500">vs</span> ${attendance.previous ?? 0}</p>
        <p class="${(attendance.delta || 0) < 0 ? 'text-rose-600' : 'text-emerald-700'}">${attendance.delta ?? 0} (${attendance.delta_pct ?? 0}%)</p>
      </div>
      <div class="rounded-xl border border-slate-200 bg-white px-3 py-2 text-xs">
        <p class="text-slate-500">Late</p>
        <p class="font-semibold text-slate-800">${late.current ?? 0} <span class="text-slate-500">vs</span> ${late.previous ?? 0}</p>
        <p class="${(late.delta || 0) > 0 ? 'text-rose-600' : 'text-emerald-700'}">${late.delta ?? 0} (${late.delta_pct ?? 0}%)</p>
      </div>
      <div class="rounded-xl border border-slate-200 bg-white px-3 py-2 text-xs">
        <p class="text-slate-500">SMS Failed</p>
        <p class="font-semibold text-slate-800">${smsFailed.current ?? 0} <span class="text-slate-500">vs</span> ${smsFailed.previous ?? 0}</p>
        <p class="${(smsFailed.delta || 0) > 0 ? 'text-rose-600' : 'text-emerald-700'}">${smsFailed.delta ?? 0} (${smsFailed.delta_pct ?? 0}%)</p>
      </div>
    `;

    const lines = Array.isArray(payload.explanations) ? payload.explanations : [];
    ui.changesExplain.innerHTML = lines.length
      ? lines.map((line) => `<p>- ${line}</p>`).join('')
      : '<p>No explanation available.</p>';
  }

  function renderActions(payload) {
    const actions = Array.isArray(payload.actions) ? payload.actions : [];
    if (!actions.length) {
      ui.actionsList.innerHTML = '<p class="text-sm text-slate-500">No actions recommended for this range.</p>';
      return;
    }

    ui.actionsList.innerHTML = '';
    actions.forEach((action) => {
      const row = document.createElement('div');
      row.className = 'rounded-xl border border-slate-200 bg-slate-50/80 p-3 flex items-start justify-between gap-3';
      row.innerHTML = `
        <div class="min-w-0">
          <div class="flex items-center gap-2">
            ${createSeverityBadge(action.severity)}
            <p class="text-sm font-semibold text-slate-800 truncate">${action.title || 'Action'}</p>
          </div>
          <p class="text-xs text-slate-600 mt-1">${action.description || ''}</p>
          <p class="text-[11px] text-slate-500 mt-1">Count: ${action.count ?? 0}</p>
        </div>
        <button type="button" class="ai-view-action rounded-lg border border-slate-300 px-2.5 py-1 text-xs font-semibold text-slate-700 hover:bg-slate-100">${action.button_label || 'View'}</button>
      `;
      row.querySelector('.ai-view-action').addEventListener('click', async () => {
        applyPrefill(action.prefill);
        setDrilldown(action.drilldown);
        await loadAll();
      });
      ui.actionsList.appendChild(row);
    });
  }

  function resetAskResult() {
    ui.askStatus.textContent = '';
    ui.askTableWrap.classList.add('hidden');
    ui.askHead.innerHTML = '';
    ui.askBody.innerHTML = '';
    ui.askChartWrap.classList.add('hidden');
    if (state.askChart) {
      state.askChart.destroy();
      state.askChart = null;
    }
  }

  function renderAskResult(payload) {
    const rows = Array.isArray(payload.rows) ? payload.rows : [];
    const columns = Array.isArray(payload.columns) ? payload.columns : [];
    ui.askStatus.textContent = payload.title || 'Query Result';

    if (rows.length && columns.length) {
      ui.askTableWrap.classList.remove('hidden');
      renderSimpleTable(ui.askHead, ui.askBody, columns, rows);
    } else {
      ui.askTableWrap.classList.add('hidden');
    }

    const chart = payload.chart || null;
    if (!chart || !window.Chart || !Array.isArray(chart.labels) || !Array.isArray(chart.values)) {
      ui.askChartWrap.classList.add('hidden');
      if (state.askChart) {
        state.askChart.destroy();
        state.askChart = null;
      }
      return;
    }

    ui.askChartWrap.classList.remove('hidden');
    if (state.askChart) state.askChart.destroy();
    state.askChart = new Chart(ui.askChartCanvas, {
      type: chart.type || 'bar',
      data: {
        labels: chart.labels,
        datasets: [{
          label: chart.label || 'Value',
          data: chart.values,
          backgroundColor: chart.type === 'line' ? 'rgba(5,150,105,0.16)' : '#2563eb',
          borderColor: '#059669',
          tension: 0.3,
          fill: chart.type === 'line',
          borderRadius: chart.type === 'bar' ? 6 : 0,
        }],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: { legend: { display: false } },
        scales: {
          x: { grid: { color: '#e2e8f0' } },
          y: { beginAtZero: true, grid: { color: '#e2e8f0' } },
        },
      },
    });
  }

  function buildQueryString(params) {
    const query = new URLSearchParams();
    Object.entries(params).forEach(([key, value]) => {
      const text = String(value ?? '').trim();
      if (text) query.set(key, text);
    });
    return query.toString();
  }

  async function loadInsights() {
    const qs = buildQueryString({ range: state.range, grade: state.grade, section: state.section });
    const payload = await fetchJson(`/api/analytics/ai/insights?${qs}`);
    renderInsights(payload);
  }

  async function loadRisk() {
    const qs = buildQueryString({ target: 'next_school_day', limit: 20, grade: state.grade, section: state.section });
    const payload = await fetchJson(`/api/analytics/ai/risk?${qs}`);
    renderRisk(payload);
  }

  async function loadChanges() {
    const params = {
      mode: state.changeMode,
      start: state.changeStart,
      end: state.changeEnd,
      grade: state.grade,
      section: state.section,
    };
    const payload = await fetchJson(`/api/analytics/ai/changes?${buildQueryString(params)}`);
    renderChanges(payload);
  }

  async function loadActions() {
    const qs = buildQueryString({ range: state.range, grade: state.grade, section: state.section });
    const payload = await fetchJson(`/api/analytics/ai/actions?${qs}`);
    renderActions(payload);
  }

  async function loadAll() {
    showError('');
    try {
      await Promise.all([loadInsights(), loadRisk(), loadChanges(), loadActions()]);
    } catch (error) {
      showError(error.message || 'Failed to load AI analytics');
    }
  }

  const syncSectionDebounced = debounce(() => {
    state.section = ui.section.value.trim();
    loadAll();
  }, 300);

  ui.range.addEventListener('change', () => {
    state.range = ui.range.value;
    loadAll();
  });

  ui.grade.addEventListener('change', () => {
    state.grade = ui.grade.value;
    loadAll();
  });

  ui.section.addEventListener('input', syncSectionDebounced);

  ui.refreshBtn.addEventListener('click', () => {
    state.section = ui.section.value.trim();
    state.grade = ui.grade.value;
    state.range = ui.range.value;
    loadAll();
  });

  ui.compareBtn.addEventListener('click', () => {
    state.changeMode = ui.changesMode.value;
    state.changeStart = ui.changesStart.value;
    state.changeEnd = ui.changesEnd.value;
    loadChanges().catch((error) => showError(error.message || 'Failed to compare changes'));
  });

  function syncChangeDateInputs() {
    const isCustom = ui.changesMode.value === 'custom_range';
    ui.changesStart.disabled = !isCustom;
    ui.changesEnd.disabled = !isCustom;
    ui.changesStart.classList.toggle('opacity-60', !isCustom);
    ui.changesEnd.classList.toggle('opacity-60', !isCustom);
  }

  ui.changesMode.addEventListener('change', syncChangeDateInputs);
  syncChangeDateInputs();

  ui.askForm.addEventListener('submit', async (event) => {
    event.preventDefault();
    const query = ui.askInput.value.trim();
    if (!query) {
      ui.askStatus.textContent = 'Type a question first.';
      return;
    }

    resetAskResult();
    ui.askStatus.textContent = 'Processing...';
    try {
      const payload = await fetchJson('/api/analytics/ai/nlq', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          query,
          grade: state.grade,
          section: state.section,
        }),
      });
      renderAskResult(payload);
    } catch (error) {
      ui.askStatus.textContent = error.message || 'Query failed';
    }
  });

  ui.clearDrilldownBtn.addEventListener('click', () => setDrilldown(null));

  loadAll();
})();
