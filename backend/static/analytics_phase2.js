(function () {
  const module = document.getElementById("phase2OpsModule");
  if (!module) return;

  const ui = {
    reloadBtn: document.getElementById("phase2ReloadBtn"),
    error: document.getElementById("phase2OpsError"),

    reportForm: document.getElementById("scheduledReportForm"),
    reportName: document.getElementById("reportNameInput"),
    reportFrequency: document.getElementById("reportFrequencyInput"),
    reportSendTime: document.getElementById("reportSendTimeInput"),
    reportRecipients: document.getElementById("reportRecipientsInput"),
    reportGrade: document.getElementById("reportGradeInput"),
    reportSection: document.getElementById("reportSectionInput"),
    reportEnabled: document.getElementById("reportEnabledInput"),
    reportsList: document.getElementById("scheduledReportsList"),
    reportRunsList: document.getElementById("reportRunsList"),

    ruleForm: document.getElementById("anomalyRuleForm"),
    ruleName: document.getElementById("ruleNameInput"),
    ruleMetric: document.getElementById("ruleMetricInput"),
    ruleOperator: document.getElementById("ruleOperatorInput"),
    ruleThreshold: document.getElementById("ruleThresholdInput"),
    ruleWindow: document.getElementById("ruleWindowInput"),
    ruleSeverity: document.getElementById("ruleSeverityInput"),
    ruleCooldown: document.getElementById("ruleCooldownInput"),
    ruleNotifyEmails: document.getElementById("ruleNotifyEmailsInput"),
    ruleEnabled: document.getElementById("ruleEnabledInput"),
    rulesList: document.getElementById("anomalyRulesList"),
    eventsList: document.getElementById("anomalyEventsList"),
    evaluateAllBtn: document.getElementById("anomalyEvaluateAllBtn"),
  };

  function showError(message) {
    const text = String(message || "").trim();
    if (!text) {
      ui.error.classList.add("hidden");
      ui.error.textContent = "";
      return;
    }
    ui.error.classList.remove("hidden");
    ui.error.textContent = text;
  }

  function escapeHtml(value) {
    return String(value ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  async function fetchJson(url, options) {
    const res = await fetch(url, options);
    const payload = await res.json().catch(() => ({}));
    if (!res.ok || payload.status !== "ok") {
      throw new Error(payload.message || "Request failed");
    }
    return payload;
  }

  function renderReports(reports) {
    if (!Array.isArray(reports) || !reports.length) {
      ui.reportsList.innerHTML = '<p class="text-sm text-slate-500">No scheduled reports configured.</p>';
      return;
    }

    ui.reportsList.innerHTML = reports.map((report) => `
      <div class="rounded-xl border border-slate-200 bg-slate-50 px-3 py-2">
        <div class="flex items-start justify-between gap-2">
          <div class="min-w-0">
            <p class="text-sm font-semibold text-slate-800 truncate">${escapeHtml(report.name || "")}</p>
            <p class="text-xs text-slate-500 mt-0.5">${escapeHtml((report.frequency || "").toUpperCase())} at ${escapeHtml(report.send_time || "07:00")}</p>
            <p class="text-xs text-slate-500 mt-0.5 truncate">Recipients: ${escapeHtml((report.recipients || []).join(", "))}</p>
            <p class="text-xs text-slate-500 mt-0.5">Next Run: ${escapeHtml(report.next_run_at || "-")}</p>
          </div>
          <span class="inline-flex rounded-full px-2 py-0.5 text-[10px] font-semibold ${report.enabled ? "bg-emerald-100 text-emerald-700" : "bg-slate-200 text-slate-700"}">${report.enabled ? "Enabled" : "Disabled"}</span>
        </div>
        <div class="mt-2 flex flex-wrap gap-2">
          <button type="button" data-report-action="run" data-id="${escapeHtml(report._id || "")}" class="rounded-lg border border-slate-300 px-2.5 py-1 text-xs font-semibold text-slate-700 hover:bg-slate-100">Run Now</button>
          <button type="button" data-report-action="toggle" data-id="${escapeHtml(report._id || "")}" data-enabled="${report.enabled ? "1" : "0"}" class="rounded-lg border border-blue-200 px-2.5 py-1 text-xs font-semibold text-blue-700 hover:bg-blue-50">${report.enabled ? "Disable" : "Enable"}</button>
          <button type="button" data-report-action="delete" data-id="${escapeHtml(report._id || "")}" class="rounded-lg border border-rose-200 px-2.5 py-1 text-xs font-semibold text-rose-700 hover:bg-rose-50">Delete</button>
        </div>
      </div>
    `).join("");
  }

  function renderReportRuns(runs) {
    if (!Array.isArray(runs) || !runs.length) {
      ui.reportRunsList.innerHTML = '<p class="text-sm text-slate-500">No report runs yet.</p>';
      return;
    }
    ui.reportRunsList.innerHTML = runs.map((run) => `
      <div class="rounded-lg border border-slate-200 bg-slate-50 px-3 py-2">
        <p class="text-sm font-semibold text-slate-800 truncate">${escapeHtml(run.report_name || "Report")}</p>
        <p class="text-xs text-slate-500 mt-0.5">Status: <span class="${run.status === "success" ? "text-emerald-700" : "text-rose-700"}">${escapeHtml(run.status || "")}</span> | Trigger: ${escapeHtml(run.trigger || "")}</p>
        <p class="text-xs text-slate-500 mt-0.5">${escapeHtml(run.started_at || "")}</p>
      </div>
    `).join("");
  }

  function renderRules(rules) {
    if (!Array.isArray(rules) || !rules.length) {
      ui.rulesList.innerHTML = '<p class="text-sm text-slate-500">No anomaly rules configured.</p>';
      return;
    }

    ui.rulesList.innerHTML = rules.map((rule) => `
      <div class="rounded-xl border border-slate-200 bg-slate-50 px-3 py-2">
        <div class="flex items-start justify-between gap-2">
          <div class="min-w-0">
            <p class="text-sm font-semibold text-slate-800 truncate">${escapeHtml(rule.name || "")}</p>
            <p class="text-xs text-slate-500 mt-0.5">${escapeHtml(rule.metric || "")} ${escapeHtml(rule.operator || "")} ${escapeHtml(rule.threshold || "")} | Window ${escapeHtml(rule.window_days || 1)} day(s)</p>
            <p class="text-xs text-slate-500 mt-0.5">Last Value: ${escapeHtml(rule.last_value || 0)} | Last Result: ${escapeHtml(rule.last_result || "-")}</p>
          </div>
          <span class="inline-flex rounded-full px-2 py-0.5 text-[10px] font-semibold ${rule.enabled ? "bg-emerald-100 text-emerald-700" : "bg-slate-200 text-slate-700"}">${rule.enabled ? "Enabled" : "Disabled"}</span>
        </div>
        <div class="mt-2 flex flex-wrap gap-2">
          <button type="button" data-rule-action="evaluate" data-id="${escapeHtml(rule._id || "")}" class="rounded-lg border border-slate-300 px-2.5 py-1 text-xs font-semibold text-slate-700 hover:bg-slate-100">Evaluate</button>
          <button type="button" data-rule-action="toggle" data-id="${escapeHtml(rule._id || "")}" data-enabled="${rule.enabled ? "1" : "0"}" class="rounded-lg border border-blue-200 px-2.5 py-1 text-xs font-semibold text-blue-700 hover:bg-blue-50">${rule.enabled ? "Disable" : "Enable"}</button>
          <button type="button" data-rule-action="delete" data-id="${escapeHtml(rule._id || "")}" class="rounded-lg border border-rose-200 px-2.5 py-1 text-xs font-semibold text-rose-700 hover:bg-rose-50">Delete</button>
        </div>
      </div>
    `).join("");
  }

  function renderEvents(events) {
    if (!Array.isArray(events) || !events.length) {
      ui.eventsList.innerHTML = '<p class="text-sm text-slate-500">No anomaly events yet.</p>';
      return;
    }
    ui.eventsList.innerHTML = events.map((event) => `
      <div class="rounded-lg border border-slate-200 bg-slate-50 px-3 py-2">
        <p class="text-sm font-semibold text-slate-800 truncate">${escapeHtml(event.rule_name || "Rule")}</p>
        <p class="text-xs text-slate-500 mt-0.5">${escapeHtml(event.metric || "")}: ${escapeHtml(event.value || 0)} (${escapeHtml(event.operator || "")} ${escapeHtml(event.threshold || 0)})</p>
        <p class="text-xs text-slate-500 mt-0.5">${escapeHtml(event.triggered_at || "")}</p>
      </div>
    `).join("");
  }

  async function loadOpsData() {
    showError("");
    try {
      const [reportsPayload, rulesPayload] = await Promise.all([
        fetchJson("/api/analytics/scheduled-reports"),
        fetchJson("/api/analytics/anomaly-rules"),
      ]);
      renderReports(reportsPayload.reports || []);
      renderReportRuns(reportsPayload.runs || []);
      renderRules(rulesPayload.rules || []);
      renderEvents(rulesPayload.events || []);
    } catch (error) {
      showError(error.message || "Failed to load Phase 2 operations data.");
    }
  }

  async function submitCreateReport(event) {
    event.preventDefault();
    showError("");
    try {
      await fetchJson("/api/analytics/scheduled-reports", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name: ui.reportName.value.trim(),
          frequency: ui.reportFrequency.value,
          send_time: ui.reportSendTime.value || "07:00",
          recipients: ui.reportRecipients.value.split(",").map((item) => item.trim()).filter(Boolean),
          enabled: ui.reportEnabled.checked,
          filters: {
            grade: ui.reportGrade.value || "",
            section: ui.reportSection.value.trim(),
          },
        }),
      });
      ui.reportForm.reset();
      ui.reportEnabled.checked = true;
      ui.reportSendTime.value = "07:00";
      await loadOpsData();
    } catch (error) {
      showError(error.message || "Failed to create scheduled report.");
    }
  }

  async function submitCreateRule(event) {
    event.preventDefault();
    showError("");
    try {
      await fetchJson("/api/analytics/anomaly-rules", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          name: ui.ruleName.value.trim(),
          metric: ui.ruleMetric.value,
          operator: ui.ruleOperator.value,
          threshold: Number(ui.ruleThreshold.value || 0),
          window_days: Number(ui.ruleWindow.value || 1),
          severity: ui.ruleSeverity.value,
          cooldown_minutes: Number(ui.ruleCooldown.value || 60),
          enabled: ui.ruleEnabled.checked,
          notify_emails: ui.ruleNotifyEmails.value.split(",").map((item) => item.trim()).filter(Boolean),
        }),
      });
      ui.ruleForm.reset();
      ui.ruleEnabled.checked = true;
      ui.ruleThreshold.value = "5";
      ui.ruleWindow.value = "1";
      ui.ruleCooldown.value = "60";
      ui.ruleSeverity.value = "warn";
      await loadOpsData();
    } catch (error) {
      showError(error.message || "Failed to create anomaly rule.");
    }
  }

  async function handleReportAction(event) {
    const button = event.target.closest("[data-report-action]");
    if (!button) return;
    showError("");
    const action = button.dataset.reportAction || "";
    const reportId = button.dataset.id || "";
    if (!reportId) return;

    try {
      if (action === "run") {
        await fetchJson(`/api/analytics/scheduled-reports/${encodeURIComponent(reportId)}/run-now`, { method: "POST" });
      } else if (action === "toggle") {
        const enabled = button.dataset.enabled === "1";
        await fetchJson(`/api/analytics/scheduled-reports/${encodeURIComponent(reportId)}`, {
          method: "PUT",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ enabled: !enabled }),
        });
      } else if (action === "delete") {
        if (!window.confirm("Delete this scheduled report?")) return;
        await fetchJson(`/api/analytics/scheduled-reports/${encodeURIComponent(reportId)}`, { method: "DELETE" });
      }
      await loadOpsData();
    } catch (error) {
      showError(error.message || "Scheduled report action failed.");
    }
  }

  async function handleRuleAction(event) {
    const button = event.target.closest("[data-rule-action]");
    if (!button) return;
    showError("");
    const action = button.dataset.ruleAction || "";
    const ruleId = button.dataset.id || "";
    if (!ruleId) return;

    try {
      if (action === "evaluate") {
        await fetchJson(`/api/analytics/anomaly-rules/${encodeURIComponent(ruleId)}/evaluate`, { method: "POST" });
      } else if (action === "toggle") {
        const enabled = button.dataset.enabled === "1";
        await fetchJson(`/api/analytics/anomaly-rules/${encodeURIComponent(ruleId)}`, {
          method: "PUT",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ enabled: !enabled }),
        });
      } else if (action === "delete") {
        if (!window.confirm("Delete this anomaly rule?")) return;
        await fetchJson(`/api/analytics/anomaly-rules/${encodeURIComponent(ruleId)}`, { method: "DELETE" });
      }
      await loadOpsData();
    } catch (error) {
      showError(error.message || "Anomaly rule action failed.");
    }
  }

  ui.reportForm?.addEventListener("submit", submitCreateReport);
  ui.ruleForm?.addEventListener("submit", submitCreateRule);
  ui.reportsList?.addEventListener("click", handleReportAction);
  ui.rulesList?.addEventListener("click", handleRuleAction);
  ui.reloadBtn?.addEventListener("click", loadOpsData);
  ui.evaluateAllBtn?.addEventListener("click", async () => {
    showError("");
    try {
      await fetchJson("/api/analytics/anomaly-rules/evaluate", { method: "POST" });
      await loadOpsData();
    } catch (error) {
      showError(error.message || "Failed to evaluate anomaly rules.");
    }
  });

  loadOpsData();
})();
