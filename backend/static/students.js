(() => {
  const steps = [
    { key: "front", label: "Front", instruction: "Look Straight at the Camera" },
    { key: "left", label: "Left", instruction: "Turn your head slightly to the LEFT" },
    { key: "right", label: "Right", instruction: "Turn your head slightly to the RIGHT" },
    { key: "up", label: "Slight Up", instruction: "Look slightly UP" },
    { key: "down", label: "Slight Down", instruction: "Look slightly DOWN" },
  ];

  const state = {
    students: [],
    sectionsByGrade: {},
    filters: { q: "", grade: "", section: "" },
    pagination: { page: 1, limit: 10, total: 0, pages: 1 },
    activeModal: null,
    lastFocus: null,
    deleteTarget: { id: "", label: "" },
    face: {
      studentId: "",
      mode: "register",
      captures: [],
      stream: null,
      mesh: null,
      lastResults: null,
      rafId: null,
      processing: false,
      alignFrames: 0,
      cooldownUntil: 0,
    },
    requests: {
      students: null,
      sections: null,
      stats: null,
      studentsQueuedReload: false,
      sectionsQueuedReload: false,
      statsQueuedReload: false,
    },
    realtime: {
      stream: null,
      reconnectTimer: null,
      flushTimer: null,
      initialized: false,
      snapshot: {
        revision: 0,
        students: 0,
        sections: 0,
      },
      pending: {
        students: false,
        sections: false,
      },
    },
    sectionStats: {
      grade: "",
      section: "",
      total: 0,
      male: 0,
      female: 0,
      loading: false,
      note: "Live counts from MongoDB.",
      updatedAt: "",
    },
  };

  let sectionStatsRequestToken = 0;
  let centeredSuccessExitTimer = null;
  let centeredSuccessHideTimer = null;

  const refs = {
    toast: document.getElementById("toast"),
    statTotalStudents: document.getElementById("statTotalStudents"),
    statActiveStudents: document.getElementById("statActiveStudents"),
    statInactiveStudents: document.getElementById("statInactiveStudents"),
    statAddedToday: document.getElementById("statAddedToday"),
    searchInput: document.getElementById("searchInput"),
    gradeFilter: document.getElementById("gradeFilter"),
    clearSectionBtn: document.getElementById("clearSectionBtn"),
    newSectionGrade: document.getElementById("newSectionGrade"),
    newSectionName: document.getElementById("newSectionName"),
    addSectionBtn: document.getElementById("addSectionBtn"),
    sectionsPanel: document.getElementById("sectionsPanel"),
    sectionStatsTitle: document.getElementById("sectionStatsTitle"),
    sectionStatsSubtitle: document.getElementById("sectionStatsSubtitle"),
    sectionStatsTotal: document.getElementById("sectionStatsTotal"),
    sectionStatsMale: document.getElementById("sectionStatsMale"),
    sectionStatsFemale: document.getElementById("sectionStatsFemale"),
    sectionStatsNote: document.getElementById("sectionStatsNote"),
    sectionStatsUpdated: document.getElementById("sectionStatsUpdated"),
    studentsTableBody: document.getElementById("studentsTableBody"),
    paginationSummary: document.getElementById("paginationSummary"),
    paginationControls: document.getElementById("paginationControls"),
    openAddBtn: document.getElementById("openAddBtn"),
    addForm: document.getElementById("addForm"),
    addSectionSelect: document.getElementById("addSectionSelect"),
    addSectionValue: document.getElementById("addSectionValue"),
    addGradeLevelValue: document.getElementById("addGradeLevelValue"),
    addGradeLevelDisplay: document.getElementById("addGradeLevelDisplay"),
    addImportForm: document.getElementById("addImportForm"),
    addImportFile: document.getElementById("addImportFile"),
    addImportSubmitBtn: document.getElementById("addImportSubmitBtn"),
    addImportSummary: document.getElementById("addImportSummary"),
    editForm: document.getElementById("editForm"),
    deleteStudentLabel: document.getElementById("deleteStudentLabel"),
    confirmDeleteBtn: document.getElementById("confirmDeleteBtn"),
    faceTitle: document.getElementById("faceTitle"),
    faceSubtitle: document.getElementById("faceSubtitle"),
    faceVideo: document.getElementById("faceVideo"),
    faceOverlay: document.getElementById("faceOverlay"),
    faceCaptureCanvas: document.getElementById("faceCaptureCanvas"),
    guideText: document.getElementById("guideText"),
    captureProgressText: document.getElementById("captureProgressText"),
    faceStatus: document.getElementById("faceStatus"),
    stepTags: document.getElementById("stepTags"),
    captureGrid: document.getElementById("captureGrid"),
    resetCaptureBtn: document.getElementById("resetCaptureBtn"),
    submitFaceBtn: document.getElementById("submitFaceBtn"),
    faceUpdateSuccessOverlay: document.getElementById("faceUpdateSuccessOverlay"),
    faceUpdateSuccessBadge: document.getElementById("faceUpdateSuccessBadge"),
    faceUpdateSuccessText: document.getElementById("faceUpdateSuccessText"),
    centerSuccessOverlay: document.getElementById("centerSuccessOverlay"),
    centerSuccessPulse: document.getElementById("centerSuccessPulse"),
    centerSuccessBadge: document.getElementById("centerSuccessBadge"),
    centerSuccessText: document.getElementById("centerSuccessText"),
  };

  if (!refs.studentsTableBody || !refs.searchInput) {
    return;
  }

  const debounce = (fn, delay = 320) => {
    let timer = null;
    return (...args) => {
      clearTimeout(timer);
      timer = setTimeout(() => fn(...args), delay);
    };
  };

  const esc = (value) => String(value || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");

  const gradeKey = (raw) => {
    const text = String(raw || "").trim();
    const match = text.match(/\d+/);
    return match ? match[0] : text;
  };

  const gradeLabel = (key) => /^\d+$/.test(String(key)) ? `Grade ${key}` : String(key || "");
  const PH_CONTACT_PREFIX = "+63";
  const SECTION_ASSIGNMENT_DELIMITER = "||";

  const buildSectionAssignmentValue = (grade, section) => `${String(grade || "")}${SECTION_ASSIGNMENT_DELIMITER}${String(section || "")}`;

  const parseSectionAssignmentValue = (value) => {
    const raw = String(value || "");
    const [grade = "", ...sectionParts] = raw.split(SECTION_ASSIGNMENT_DELIMITER);
    return {
      gradeKey: String(grade || "").trim(),
      section: String(sectionParts.join(SECTION_ASSIGNMENT_DELIMITER) || "").trim(),
    };
  };

  const flattenSectionAssignments = () => {
    const items = [];
    Object.entries(state.sectionsByGrade || {}).forEach(([grade, sections]) => {
      (Array.isArray(sections) ? sections : []).forEach((section) => {
        const sectionText = String(section || "").trim();
        const gradeText = String(grade || "").trim();
        if (!gradeText || !sectionText) return;
        items.push({
          gradeKey: gradeText,
          section: sectionText,
          gradeLabel: gradeLabel(gradeText),
        });
      });
    });

    items.sort((a, b) => {
      const gradeSort = gradeKey(a.gradeKey).localeCompare(gradeKey(b.gradeKey), undefined, { numeric: true });
      if (gradeSort !== 0) return gradeSort;
      return a.section.localeCompare(b.section, undefined, { sensitivity: "base" });
    });
    return items;
  };

  const syncAddSectionAssignment = () => {
    if (!refs.addSectionSelect) return;
    const { gradeKey: selectedGrade, section: selectedSection } = parseSectionAssignmentValue(refs.addSectionSelect.value);
    const resolvedGrade = selectedGrade ? gradeLabel(selectedGrade) : "";

    if (refs.addSectionValue) refs.addSectionValue.value = selectedSection;
    if (refs.addGradeLevelValue) refs.addGradeLevelValue.value = resolvedGrade;
    if (refs.addGradeLevelDisplay) refs.addGradeLevelDisplay.value = resolvedGrade;
  };

  const renderAddSectionAssignments = () => {
    if (!refs.addSectionSelect) return;
    const previousValue = String(refs.addSectionSelect.value || "");
    const entries = flattenSectionAssignments();

    const optionMarkup = entries.map(({ gradeKey: gradeValue, section }) => {
      const encodedValue = buildSectionAssignmentValue(gradeValue, section);
      return `<option value="${esc(encodedValue)}">${esc(gradeLabel(gradeValue))} - ${esc(section)}</option>`;
    }).join("");

    refs.addSectionSelect.innerHTML = `<option value="">${entries.length ? "Select Grade + Section" : "No sections available"}</option>${optionMarkup}`;
    if (previousValue && entries.some((item) => buildSectionAssignmentValue(item.gradeKey, item.section) === previousValue)) {
      refs.addSectionSelect.value = previousValue;
    } else {
      refs.addSectionSelect.value = "";
    }
    syncAddSectionAssignment();
  };

  const normalizeParentContactInput = (rawValue, keepPrefix = true) => {
    const raw = String(rawValue || "").trim();
    if (!raw) return keepPrefix ? PH_CONTACT_PREFIX : "";

    let compact = raw.replace(/[^\d+]/g, "");
    if (compact.startsWith("+63")) {
      compact = `+63${compact.slice(3).replace(/\D/g, "")}`;
    } else if (compact.startsWith("63")) {
      compact = `+63${compact.slice(2).replace(/\D/g, "")}`;
    } else if (compact.startsWith("09")) {
      compact = `+63${compact.slice(1).replace(/\D/g, "")}`;
    } else if (compact.startsWith("9")) {
      compact = `+63${compact.replace(/\D/g, "")}`;
    } else {
      compact = `+63${compact.replace(/\D/g, "")}`;
    }

    const tail = compact.slice(3).replace(/\D/g, "").slice(0, 10);
    const normalized = `${PH_CONTACT_PREFIX}${tail}`;
    if (!keepPrefix && normalized === PH_CONTACT_PREFIX) return "";
    return normalized;
  };

  const isValidParentContact = (value) => {
    const contact = String(value || "").trim();
    return !contact || /^\+639\d{9}$/.test(contact);
  };

  const showToast = (message, isError = false) => {
    if (!refs.toast) return;
    refs.toast.textContent = message;
    refs.toast.className = `fixed top-5 right-5 z-[100] rounded-xl px-4 py-3 text-sm shadow-lg border ${isError ? "bg-red-50 border-red-200 text-red-700" : "bg-emerald-50 border-emerald-200 text-emerald-700"}`;
    refs.toast.classList.remove("hidden");
    setTimeout(() => refs.toast.classList.add("hidden"), 2800);
  };

  const setAddImportSummary = (message, isError = false) => {
    if (!refs.addImportSummary) return;
    refs.addImportSummary.textContent = String(message || "").trim();
    refs.addImportSummary.className = `mt-3 rounded-xl border px-3 py-2 text-xs ${isError ? "border-rose-200 bg-rose-50 text-rose-700" : "border-emerald-200 bg-emerald-50 text-emerald-700"}`;
    refs.addImportSummary.classList.toggle("hidden", !String(message || "").trim());
  };

  const setStatValue = (el, value) => {
    if (!el) return;
    const numeric = Number.parseInt(value ?? 0, 10);
    if (Number.isFinite(numeric)) {
      el.textContent = numeric.toLocaleString();
      return;
    }
    el.textContent = String(value ?? "0");
  };

  const showFaceUpdateSuccessAnimation = () => {
    const overlay = refs.faceUpdateSuccessOverlay;
    const badge = refs.faceUpdateSuccessBadge;
    const text = refs.faceUpdateSuccessText;
    if (!overlay || !badge || !text) return;

    overlay.classList.remove("hidden");
    overlay.classList.add("flex");

    requestAnimationFrame(() => {
      badge.classList.remove("opacity-0", "scale-75");
      badge.classList.add("opacity-100", "scale-100");
      text.classList.remove("opacity-0");
      text.classList.add("opacity-100");
    });

    setTimeout(() => {
      badge.classList.remove("opacity-100", "scale-100");
      badge.classList.add("opacity-0", "scale-75");
      text.classList.remove("opacity-100");
      text.classList.add("opacity-0");
    }, 900);

    setTimeout(() => {
      overlay.classList.remove("flex");
      overlay.classList.add("hidden");
    }, 1250);
  };

  const showCenteredSuccess = (message) => {
    const overlay = refs.centerSuccessOverlay;
    const pulse = refs.centerSuccessPulse;
    const badge = refs.centerSuccessBadge;
    const text = refs.centerSuccessText;
    if (!overlay || !badge || !text) return;

    if (centeredSuccessExitTimer) clearTimeout(centeredSuccessExitTimer);
    if (centeredSuccessHideTimer) clearTimeout(centeredSuccessHideTimer);

    text.textContent = String(message || "Success");
    overlay.classList.remove("hidden");
    overlay.classList.add("flex");
    badge.classList.remove("opacity-100", "scale-100", "translate-y-0");
    badge.classList.add("opacity-0", "scale-75", "translate-y-2");
    text.classList.remove("opacity-100", "translate-y-0");
    text.classList.add("opacity-0", "translate-y-1");
    if (pulse) {
      pulse.classList.remove("opacity-100", "scale-125");
      pulse.classList.add("opacity-0", "scale-75");
    }

    requestAnimationFrame(() => {
      badge.classList.remove("opacity-0", "scale-75", "translate-y-2");
      badge.classList.add("opacity-100", "scale-100", "translate-y-0");
      text.classList.remove("opacity-0", "translate-y-1");
      text.classList.add("opacity-100", "translate-y-0");
      if (pulse) {
        pulse.classList.remove("opacity-0", "scale-75");
        pulse.classList.add("opacity-100", "scale-125");
      }
    });

    centeredSuccessExitTimer = setTimeout(() => {
      badge.classList.remove("opacity-100", "scale-100", "translate-y-0");
      badge.classList.add("opacity-0", "scale-75", "translate-y-2");
      text.classList.remove("opacity-100", "translate-y-0");
      text.classList.add("opacity-0", "translate-y-1");
      if (pulse) {
        pulse.classList.remove("opacity-100", "scale-125");
        pulse.classList.add("opacity-0", "scale-75");
      }
    }, 1100);

    centeredSuccessHideTimer = setTimeout(() => {
      overlay.classList.remove("flex");
      overlay.classList.add("hidden");
      centeredSuccessExitTimer = null;
      centeredSuccessHideTimer = null;
    }, 1550);
  };

  const formatStatsUpdatedAt = (value) => {
    if (!value) return "-";
    const parsed = new Date(value);
    if (Number.isNaN(parsed.getTime())) return "-";
    return parsed.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
  };

  const renderSectionStats = () => {
    if (!refs.sectionStatsTitle) return;
    const grade = String(state.sectionStats.grade || "").trim();
    const section = String(state.sectionStats.section || "").trim();
    const hasSelection = Boolean(grade && section);

    if (!hasSelection) {
      refs.sectionStatsTitle.textContent = "No section selected";
      refs.sectionStatsSubtitle.textContent = "Click a section chip to view detailed gender statistics.";
      refs.sectionStatsTotal.textContent = "0";
      refs.sectionStatsMale.textContent = "0";
      refs.sectionStatsFemale.textContent = "0";
      refs.sectionStatsNote.textContent = state.sectionStats.note || "Live counts from MongoDB.";
      if (refs.sectionStatsUpdated) refs.sectionStatsUpdated.textContent = "Last updated: -";
      return;
    }

    refs.sectionStatsTitle.textContent = `${gradeLabel(gradeKey(grade))} - ${section}`;
    refs.sectionStatsSubtitle.textContent = state.sectionStats.loading
      ? "Loading section statistics..."
      : "Live gender distribution from MongoDB.";
    refs.sectionStatsTotal.textContent = Number.parseInt(state.sectionStats.total || 0, 10).toLocaleString();
    refs.sectionStatsMale.textContent = Number.parseInt(state.sectionStats.male || 0, 10).toLocaleString();
    refs.sectionStatsFemale.textContent = Number.parseInt(state.sectionStats.female || 0, 10).toLocaleString();
    refs.sectionStatsNote.textContent = state.sectionStats.note || "Live counts from MongoDB.";
    if (refs.sectionStatsUpdated) {
      refs.sectionStatsUpdated.textContent = `Last updated: ${formatStatsUpdatedAt(state.sectionStats.updatedAt)}`;
    }
  };

  const clearSectionStats = (note = "Live counts from MongoDB.") => {
    sectionStatsRequestToken += 1;
    state.sectionStats = {
      grade: "",
      section: "",
      total: 0,
      male: 0,
      female: 0,
      loading: false,
      note,
      updatedAt: "",
    };
    renderSectionStats();
  };

  const api = async (url, options = {}) => {
    const config = { method: "GET", headers: { Accept: "application/json" }, ...options };
    if (config.body && !(config.body instanceof FormData)) {
      config.headers["Content-Type"] = "application/json";
      config.body = JSON.stringify(config.body);
    }
    const response = await fetch(url, config);
    const data = await response.json().catch(() => ({}));
    if (!response.ok || data.status !== "ok") {
      throw new Error(data.message || "Request failed.");
    }
    return data;
  };

  const formPayload = (form) => Object.fromEntries(Array.from(new FormData(form).entries()).map(([k, v]) => [k, String(v || "").trim()]));

  const getFocusableElements = (container) => {
    if (!container) return [];
    return Array.from(container.querySelectorAll("button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), a[href], [tabindex]:not([tabindex='-1'])"))
      .filter((el) => !el.hasAttribute("hidden") && el.offsetParent !== null);
  };

  const showModal = (id) => {
    const modal = document.getElementById(id);
    if (!modal) return;
    state.lastFocus = document.activeElement;
    state.activeModal = id;
    modal.classList.remove("hidden");
    document.body.classList.add("overflow-hidden");
    const focusables = getFocusableElements(modal);
    if (focusables.length) focusables[0].focus();
  };

  const closeModal = (id) => {
    const modal = document.getElementById(id);
    if (!modal) return;
    modal.classList.add("hidden");
    if (state.activeModal === id) {
      state.activeModal = null;
      document.body.classList.remove("overflow-hidden");
      if (state.lastFocus && typeof state.lastFocus.focus === "function") state.lastFocus.focus();
    }
    if (id === "faceModal") stopFaceCapture();
    if (id === "deleteModal") state.deleteTarget = { id: "", label: "" };
  };

  const trapFocus = (event) => {
    if (!state.activeModal || event.key !== "Tab") return;
    const modal = document.getElementById(state.activeModal);
    const focusables = getFocusableElements(modal);
    if (!focusables.length) return;
    const first = focusables[0];
    const last = focusables[focusables.length - 1];
    if (event.shiftKey && document.activeElement === first) {
      event.preventDefault();
      last.focus();
    } else if (!event.shiftKey && document.activeElement === last) {
      event.preventDefault();
      first.focus();
    }
  };
  const renderRows = () => {
    if (!state.students.length) {
      refs.studentsTableBody.innerHTML = '<tr><td colspan="8" class="px-4 py-8 text-center text-slate-500 text-sm">No students found.</td></tr>';
      return;
    }

    refs.studentsTableBody.innerHTML = state.students.map((student) => {
      const statusText = student.face_registered ? "Registered" : "Not Registered";
      const statusClass = student.face_registered ? "bg-emerald-100 text-emerald-700" : "bg-rose-100 text-rose-700";
      const faceActionText = student.face_registered ? "Update Face" : "Register Face";
      const faceActionClass = student.face_registered ? "bg-blue-600 hover:bg-blue-700" : "bg-emerald-600 hover:bg-emerald-700";
      const faceMode = student.face_registered ? "update" : "register";

      const photo = student.profile_photo
        ? `<img src="${esc(student.profile_photo)}" alt="${esc(student.name)}" class="h-10 w-10 rounded-lg object-cover border border-slate-200">`
        : '<div class="h-10 w-10 rounded-lg border border-dashed border-slate-300 text-[10px] text-slate-400 flex items-center justify-center">No Photo</div>';

      const studentLrn = student.lrn || student.student_id || "";

      return `<tr class="hover:bg-slate-50" data-id="${esc(student._id)}">
        <td class="px-4 py-3">${photo}</td>
        <td class="px-4 py-3 text-sm font-medium">${esc(studentLrn)}</td>
        <td class="px-4 py-3 text-sm">${esc(student.name)}</td>
        <td class="px-4 py-3 text-sm">${esc(student.grade_level || "-")}</td>
        <td class="px-4 py-3 text-sm">${esc(student.section || "-")}</td>
        <td class="px-4 py-3 text-sm">${esc(student.parent_contact || "-")}</td>
        <td class="px-4 py-3"><span class="inline-flex rounded-full px-2.5 py-1 text-xs font-semibold ${statusClass}">${statusText}</span></td>
        <td class="px-4 py-3">
          <div class="flex items-center justify-end gap-2">
            <div class="relative group">
              <button type="button" data-act="edit" data-id="${esc(student._id)}" class="h-8 w-8 inline-flex items-center justify-center rounded-lg border border-slate-300 bg-white text-slate-700 hover:bg-slate-100" aria-label="Edit Student">
                <svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
                  <path stroke-linecap="round" stroke-linejoin="round" d="M16.862 4.487a2.1 2.1 0 113.03 2.915L9.75 17.75 6 18l.25-3.75L16.862 4.487z" />
                </svg>
              </button>
              <span class="pointer-events-none absolute -top-9 left-1/2 -translate-x-1/2 whitespace-nowrap rounded-md bg-slate-900 px-2 py-1 text-[10px] text-white opacity-0 group-hover:opacity-100 transition">Edit Student</span>
            </div>

            <div class="relative group">
              <button type="button" data-act="delete" data-id="${esc(student._id)}" data-name="${esc(student.name)}" class="h-8 w-8 inline-flex items-center justify-center rounded-lg border border-rose-200 bg-rose-50 text-rose-700 hover:bg-rose-100" aria-label="Delete Student">
                <svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
                  <path stroke-linecap="round" stroke-linejoin="round" d="M6 7h12M9 7V5a1 1 0 011-1h4a1 1 0 011 1v2m-7 0l.75 12a1 1 0 001 .94h4.5a1 1 0 001-.94L17 7" />
                </svg>
              </button>
              <span class="pointer-events-none absolute -top-9 left-1/2 -translate-x-1/2 whitespace-nowrap rounded-md bg-slate-900 px-2 py-1 text-[10px] text-white opacity-0 group-hover:opacity-100 transition">Delete Student</span>
            </div>

            <button type="button" data-act="face" data-id="${esc(student._id)}" data-mode="${faceMode}" class="rounded-lg ${faceActionClass} px-2.5 py-1.5 text-xs font-semibold text-white">${faceActionText}</button>
          </div>
        </td>
      </tr>`;
    }).join("");
  };

  const renderPagination = () => {
    const total = state.pagination.total;
    const page = state.pagination.page;
    const limit = state.pagination.limit;
    const pages = Math.max(state.pagination.pages, 1);

    const start = total === 0 ? 0 : ((page - 1) * limit) + 1;
    const end = total === 0 ? 0 : Math.min(total, page * limit);
    refs.paginationSummary.textContent = `Showing ${start}-${end} of ${total} students`;

    const btnBase = "px-3 py-1.5 rounded-lg text-xs font-semibold border";
    const btnEnabled = "border-slate-300 bg-white text-slate-700 hover:bg-slate-100";
    const btnDisabled = "border-slate-200 bg-slate-100 text-slate-400 cursor-not-allowed";

    let html = `<button type="button" data-page="${page - 1}" class="${btnBase} ${page <= 1 ? btnDisabled : btnEnabled}" ${page <= 1 ? "disabled" : ""}>Previous</button>`;

    const from = Math.max(1, page - 2);
    const to = Math.min(pages, page + 2);
    for (let p = from; p <= to; p += 1) {
      html += `<button type="button" data-page="${p}" class="${btnBase} ${p === page ? "bg-emerald-600 border-emerald-600 text-white" : btnEnabled}">${p}</button>`;
    }

    html += `<button type="button" data-page="${page + 1}" class="${btnBase} ${page >= pages ? btnDisabled : btnEnabled}" ${page >= pages ? "disabled" : ""}>Next</button>`;
    refs.paginationControls.innerHTML = html;
  };

  const renderSections = () => {
    const keys = Object.keys(state.sectionsByGrade).sort((a, b) => {
      const an = Number.parseInt(a, 10);
      const bn = Number.parseInt(b, 10);
      if (!Number.isNaN(an) && !Number.isNaN(bn)) return an - bn;
      return String(a).localeCompare(String(b));
    });

    if (!keys.length) {
      refs.sectionsPanel.innerHTML = '<p class="text-sm text-slate-500">No sections available.</p>';
      refs.clearSectionBtn.classList.toggle("hidden", !state.filters.section);
      return;
    }

    const chip = (grade, section) => {
      const selected = gradeKey(state.filters.grade) === grade && state.filters.section === section;
      const cls = selected ? "bg-emerald-600 border-emerald-600 text-white" : "bg-white border-slate-300 text-slate-700 hover:border-emerald-300 hover:text-emerald-700";
      return `<div class="inline-flex items-center gap-1 rounded-full border border-slate-200 bg-white px-1 py-1">
        <button type="button" class="section-chip rounded-full border px-3 py-1.5 text-xs font-semibold ${cls}" data-grade="${esc(grade)}" data-section="${esc(section)}">Grade ${esc(grade)} - ${esc(section)}</button>
        <button type="button" class="section-clear-btn inline-flex h-7 w-7 items-center justify-center rounded-full border border-rose-200 bg-rose-50 text-rose-700 hover:bg-rose-100" title="Remove all students from this section" data-grade="${esc(grade)}" data-section="${esc(section)}" aria-label="Remove students from Grade ${esc(grade)} - ${esc(section)}">
          <svg xmlns="http://www.w3.org/2000/svg" class="h-3.5 w-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
            <path stroke-linecap="round" stroke-linejoin="round" d="M6 7h12M9 7V5a1 1 0 011-1h4a1 1 0 011 1v2m-7 0l.75 12a1 1 0 001 .94h4.5a1 1 0 001-.94L17 7" />
          </svg>
        </button>
      </div>`;
    };

    if (state.filters.grade) {
      const only = gradeKey(state.filters.grade);
      const sections = state.sectionsByGrade[only] || [];
      refs.sectionsPanel.innerHTML = `<div><p class="text-xs font-semibold uppercase tracking-wide text-slate-500 mb-2">${esc(gradeLabel(only))}</p><div class="flex flex-wrap gap-2">${sections.map((section) => chip(only, section)).join("")}</div></div>`;
    } else {
      refs.sectionsPanel.innerHTML = keys.map((grade) => `<div><p class="text-xs font-semibold uppercase tracking-wide text-slate-500 mb-2">${esc(gradeLabel(grade))}</p><div class="flex flex-wrap gap-2">${(state.sectionsByGrade[grade] || []).map((section) => chip(grade, section)).join("")}</div></div>`).join("");
    }

    refs.clearSectionBtn.classList.toggle("hidden", !state.filters.section);
  };

  const loadSectionStats = async ({ grade = "", section = "", silent = false } = {}) => {
    const gradeValue = String(grade || "").trim();
    const sectionValue = String(section || "").trim();
    if (!gradeValue || !sectionValue) {
      clearSectionStats("Live counts from MongoDB.");
      return;
    }

    const requestToken = sectionStatsRequestToken + 1;
    sectionStatsRequestToken = requestToken;
    state.sectionStats = {
      grade: gradeValue,
      section: sectionValue,
      total: 0,
      male: 0,
      female: 0,
      loading: true,
      note: "Fetching section statistics...",
      updatedAt: "",
    };
    renderSectionStats();

    try {
      const params = new URLSearchParams();
      params.set("grade", gradeValue);
      params.set("section", sectionValue);
      const data = await api(`/api/sections/stats?${params.toString()}`);
      if (requestToken !== sectionStatsRequestToken) return;
      const stats = data.stats || {};
      state.sectionStats = {
        grade: data.grade_level || gradeValue,
        section: data.section || sectionValue,
        total: Number.parseInt(stats.total || 0, 10) || 0,
        male: Number.parseInt(stats.male || 0, 10) || 0,
        female: Number.parseInt(stats.female || 0, 10) || 0,
        loading: false,
        note: "Updated from MongoDB.",
        updatedAt: new Date().toISOString(),
      };
      renderSectionStats();
    } catch (error) {
      if (requestToken !== sectionStatsRequestToken) return;
      state.sectionStats.loading = false;
      state.sectionStats.note = "Unable to load section statistics.";
      renderSectionStats();
      if (!silent) showToast(error.message, true);
    }
  };

  const loadSections = async ({ silent = false, force = false } = {}) => {
    if (state.requests.sections) {
      if (force) state.requests.sectionsQueuedReload = true;
      return state.requests.sections;
    }
    state.requests.sectionsQueuedReload = false;

    state.requests.sections = (async () => {
      try {
        const data = await api("/api/sections");
        state.sectionsByGrade = data.sections_by_grade || {};
        renderSections();
        renderAddSectionAssignments();
      } catch (error) {
        if (!silent) {
          showToast(error.message, true);
        } else {
          console.error("Realtime sections refresh failed:", error);
        }
      }
    })();

    try {
      await state.requests.sections;
    } finally {
      state.requests.sections = null;
      if (state.requests.sectionsQueuedReload) {
        state.requests.sectionsQueuedReload = false;
        await loadSections({ silent: true });
      }
    }
    return null;
  };

  const loadStudents = async ({ silent = false, force = false } = {}) => {
    if (state.requests.students) {
      if (force) state.requests.studentsQueuedReload = true;
      return state.requests.students;
    }
    state.requests.studentsQueuedReload = false;

    state.requests.students = (async () => {
      const params = new URLSearchParams();
      if (state.filters.q) params.set("q", state.filters.q);
      if (state.filters.grade) params.set("grade", state.filters.grade);
      if (state.filters.section) params.set("section", state.filters.section);
      params.set("page", String(state.pagination.page));
      params.set("limit", String(state.pagination.limit));

      if (!silent) {
        refs.studentsTableBody.innerHTML = '<tr><td colspan="8" class="px-4 py-8 text-center text-slate-500 text-sm">Loading students...</td></tr>';
      }

      try {
        const data = await api(`/api/students?${params.toString()}`);
        state.students = Array.isArray(data.students) ? data.students : [];
        state.pagination.page = Number.parseInt(data.page || state.pagination.page, 10);
        state.pagination.limit = Number.parseInt(data.limit || state.pagination.limit, 10);
        state.pagination.total = Number.parseInt(data.total || 0, 10);
        state.pagination.pages = Number.parseInt(data.pages || 1, 10);
        renderRows();
        renderPagination();
      } catch (error) {
        if (!silent) {
          showToast(error.message, true);
          refs.studentsTableBody.innerHTML = '<tr><td colspan="8" class="px-4 py-8 text-center text-rose-600 text-sm">Unable to load students.</td></tr>';
          state.pagination.total = 0;
          state.pagination.pages = 1;
          renderPagination();
        } else {
          console.error("Realtime students refresh failed:", error);
        }
      }
    })();

    try {
      await state.requests.students;
    } finally {
      state.requests.students = null;
      if (state.requests.studentsQueuedReload) {
        state.requests.studentsQueuedReload = false;
        await loadStudents({ silent: true });
      }
    }
    return null;
  };

  const loadStudentStats = async ({ silent = false, force = false } = {}) => {
    if (state.requests.stats) {
      if (force) state.requests.statsQueuedReload = true;
      return state.requests.stats;
    }
    state.requests.statsQueuedReload = false;

    state.requests.stats = (async () => {
      try {
        const data = await api("/api/students/stats");
        const stats = data.stats || {};
        setStatValue(refs.statTotalStudents, stats.total);
        setStatValue(refs.statActiveStudents, stats.active);
        setStatValue(refs.statInactiveStudents, stats.inactive);
        setStatValue(refs.statAddedToday, stats.new_today);
      } catch (error) {
        if (!silent) {
          showToast(error.message, true);
        } else {
          console.error("Realtime students stats refresh failed:", error);
        }
      }
    })();

    try {
      await state.requests.stats;
    } finally {
      state.requests.stats = null;
      if (state.requests.statsQueuedReload) {
        state.requests.statsQueuedReload = false;
        await loadStudentStats({ silent: true });
      }
    }
    return null;
  };

  const queueRealtimeRefresh = ({ students = false, sections = false } = {}) => {
    if (students) state.realtime.pending.students = true;
    if (sections) state.realtime.pending.sections = true;
    if (state.realtime.flushTimer) return;

    state.realtime.flushTimer = setTimeout(async () => {
      state.realtime.flushTimer = null;
      const shouldReloadSections = state.realtime.pending.sections;
      const shouldReloadStudents = state.realtime.pending.students || shouldReloadSections;
      state.realtime.pending.students = false;
      state.realtime.pending.sections = false;

      if (shouldReloadSections) await loadSections({ silent: true });
      if (shouldReloadStudents) await loadStudents({ silent: true });
      if (shouldReloadStudents) await loadStudentStats({ silent: true });
      if (shouldReloadStudents && state.filters.grade && state.filters.section) {
        await loadSectionStats({ grade: state.filters.grade, section: state.filters.section, silent: true });
      }

      if (state.realtime.pending.students || state.realtime.pending.sections) {
        queueRealtimeRefresh({});
      }
    }, 250);
  };

  const consumeRealtimeSnapshot = (payload) => {
    const snapshot = {
      revision: Number.parseInt(payload?.revision || 0, 10) || 0,
      students: Number.parseInt(payload?.students || 0, 10) || 0,
      sections: Number.parseInt(payload?.sections || 0, 10) || 0,
    };

    if (!state.realtime.initialized) {
      state.realtime.snapshot = snapshot;
      state.realtime.initialized = true;
      return;
    }

    if (snapshot.revision <= state.realtime.snapshot.revision) return;

    const studentsChanged = snapshot.students !== state.realtime.snapshot.students;
    const sectionsChanged = snapshot.sections !== state.realtime.snapshot.sections;
    state.realtime.snapshot = snapshot;

    if (studentsChanged || sectionsChanged) {
      queueRealtimeRefresh({ students: studentsChanged, sections: sectionsChanged });
    }
  };

  const closeRealtimeStream = () => {
    if (state.realtime.stream) {
      state.realtime.stream.close();
      state.realtime.stream = null;
    }
    if (state.realtime.reconnectTimer) {
      clearTimeout(state.realtime.reconnectTimer);
      state.realtime.reconnectTimer = null;
    }
    if (state.realtime.flushTimer) {
      clearTimeout(state.realtime.flushTimer);
      state.realtime.flushTimer = null;
    }
  };

  const startRealtimeUpdates = () => {
    if (!("EventSource" in window)) return;

    const connect = () => {
      closeRealtimeStream();
      const stream = new EventSource("/api/changes/stream");
      state.realtime.stream = stream;

      stream.addEventListener("data_change", (event) => {
        try {
          const payload = JSON.parse(event.data || "{}");
          consumeRealtimeSnapshot(payload);
        } catch (error) {
          console.error("Failed to parse data_change event:", error);
        }
      });

      stream.onerror = () => {
        closeRealtimeStream();
        if (state.realtime.reconnectTimer) return;
        state.realtime.reconnectTimer = setTimeout(() => {
          state.realtime.reconnectTimer = null;
          connect();
        }, 4000);
      };
    };

    connect();
    window.addEventListener("beforeunload", closeRealtimeStream);
  };

  const fillEditForm = (student) => {
    refs.editForm.elements._id.value = student._id || "";
    refs.editForm.elements.lrn.value = student.lrn || student.student_id || "";
    refs.editForm.elements.name.value = student.name || "";
    refs.editForm.elements.grade_level.value = student.grade_level || "";
    refs.editForm.elements.section.value = student.section || "";
    refs.editForm.elements.parent_contact.value = student.parent_contact || "";
    refs.editForm.elements.gender.value = student.gender || "";
    refs.editForm.elements.status.value = student.status || "Active";
  };

  const openEditModal = async (studentId) => {
    try {
      const data = await api(`/api/students/${studentId}`);
      fillEditForm(data.student || {});
      showModal("editModal");
    } catch (error) {
      showToast(error.message, true);
    }
  };

  const openDeleteModal = (studentId, studentName) => {
    state.deleteTarget = { id: studentId, label: studentName || "Selected student" };
    refs.deleteStudentLabel.textContent = state.deleteTarget.label;
    showModal("deleteModal");
  };

  const confirmDelete = async () => {
    if (!state.deleteTarget.id) return;
    try {
      await api(`/api/students/${state.deleteTarget.id}`, { method: "DELETE" });
      closeModal("deleteModal");
      showToast("Student deleted successfully.");
      if (state.pagination.page > 1 && state.students.length === 1) state.pagination.page -= 1;
      await loadSections();
      await loadStudents();
      await loadStudentStats({ silent: true });
      if (state.filters.grade && state.filters.section) {
        await loadSectionStats({ grade: state.filters.grade, section: state.filters.section, silent: true });
      }
    } catch (error) {
      showToast(error.message, true);
    }
  };

  const clearSectionStudents = async (gradeKeyValue, sectionValue) => {
    const grade = gradeLabel(gradeKeyValue);
    const section = String(sectionValue || "").trim();
    if (!grade || !section) return;

    const confirmed = window.confirm(`Remove all students from ${grade} - ${section}? This cannot be undone.`);
    if (!confirmed) return;

    try {
      const response = await api("/api/sections/clear-students", {
        method: "POST",
        body: { grade, section },
      });
      showToast(response.message || "Section students removed.");
      if (state.filters.section === section && gradeKey(state.filters.grade) === gradeKeyValue) {
        state.filters.section = "";
        clearSectionStats("Select a section to view updated counts.");
      }
      state.pagination.page = 1;
      await loadSections();
      await loadStudents();
      await loadStudentStats({ silent: true });
      if (state.filters.grade && state.filters.section) {
        await loadSectionStats({ grade: state.filters.grade, section: state.filters.section, silent: true });
      }
    } catch (error) {
      showToast(error.message, true);
    }
  };
  const evaluateStep = (stepKey, yaw, pitch) => {
    if (stepKey === "front") return Math.abs(yaw) < 0.035 && Math.abs(pitch) < 0.035;
    if (stepKey === "left") return yaw > 0.035 && Math.abs(pitch) < 0.08;
    if (stepKey === "right") return yaw < -0.035 && Math.abs(pitch) < 0.08;
    if (stepKey === "up") return pitch < -0.018 && Math.abs(yaw) < 0.09;
    if (stepKey === "down") return pitch > 0.018 && Math.abs(yaw) < 0.09;
    return false;
  };

  const drawOverlay = (ctx, width, height, aligned, hasFace) => {
    ctx.clearRect(0, 0, width, height);

    const centerX = width / 2;
    const centerY = height / 2;
    const ovalWidth = width * 0.43;
    const ovalHeight = height * 0.62;

    ctx.beginPath();
    ctx.ellipse(centerX, centerY, ovalWidth / 2, ovalHeight / 2, 0, 0, Math.PI * 2);
    ctx.strokeStyle = !hasFace ? "rgba(148,163,184,0.95)" : (aligned ? "rgba(34,197,94,0.95)" : "rgba(148,163,184,0.95)");
    ctx.lineWidth = 4;
    ctx.stroke();
  };

  const renderFaceState = () => {
    refs.captureProgressText.textContent = `${state.face.captures.length} / ${steps.length}`;
    refs.submitFaceBtn.disabled = state.face.captures.length < steps.length;

    refs.stepTags.innerHTML = steps.map((step, index) => {
      let cls = "bg-slate-100 border-slate-200 text-slate-500";
      if (index < state.face.captures.length) cls = "bg-emerald-100 border-emerald-300 text-emerald-700";
      if (index === state.face.captures.length) cls = "bg-blue-100 border-blue-300 text-blue-700";
      return `<div class="rounded-lg border px-2 py-1 text-xs font-semibold ${cls}">${step.label}</div>`;
    }).join("");

    refs.captureGrid.innerHTML = steps.map((step, index) => {
      const image = state.face.captures[index];
      return image
        ? `<div class="rounded-lg border p-1"><img src="${image}" alt="${step.label}" class="h-16 w-full object-cover rounded"></div>`
        : `<div class="h-16 rounded-lg border border-dashed bg-slate-50 text-[10px] text-slate-400 flex items-center justify-center">${step.label}</div>`;
    }).join("");
  };

  const stopCameraTracks = () => {
    if (state.face.stream) {
      state.face.stream.getTracks().forEach((track) => track.stop());
      state.face.stream = null;
    }
    refs.faceVideo.srcObject = null;
  };

  const stopFaceCapture = () => {
    if (state.face.rafId) {
      cancelAnimationFrame(state.face.rafId);
      state.face.rafId = null;
    }
    stopCameraTracks();
    state.face.mesh = null;
    state.face.lastResults = null;
    state.face.processing = false;
  };

  const captureCurrentFrame = () => {
    if (!refs.faceVideo.videoWidth || !refs.faceVideo.videoHeight) return;
    refs.faceCaptureCanvas.width = refs.faceVideo.videoWidth;
    refs.faceCaptureCanvas.height = refs.faceVideo.videoHeight;
    refs.faceCaptureCanvas.getContext("2d").drawImage(refs.faceVideo, 0, 0, refs.faceCaptureCanvas.width, refs.faceCaptureCanvas.height);
    state.face.captures.push(refs.faceCaptureCanvas.toDataURL("image/jpeg", 0.92));

    state.face.alignFrames = 0;
    state.face.cooldownUntil = Date.now() + 900;
    if (state.face.captures.length >= steps.length) {
      refs.faceStatus.textContent = "Capture sequence complete. Save face registration.";
      stopCameraTracks();
      if (state.face.rafId) {
        cancelAnimationFrame(state.face.rafId);
        state.face.rafId = null;
      }
    } else {
      refs.faceStatus.textContent = `Captured ${state.face.captures.length}/${steps.length}. Moving to next angle.`;
    }
    renderFaceState();
  };

  const processFaceFrame = async () => {
    if (!state.face.mesh || !state.face.stream) {
      state.face.rafId = null;
      return;
    }
    if (state.face.processing) {
      state.face.rafId = requestAnimationFrame(processFaceFrame);
      return;
    }

    const video = refs.faceVideo;
    if (!video || video.readyState < 2) {
      state.face.rafId = requestAnimationFrame(processFaceFrame);
      return;
    }

    try {
      state.face.processing = true;
      await state.face.mesh.send({ image: video });
      const results = state.face.lastResults || {};

      const width = video.videoWidth || video.clientWidth;
      const height = video.videoHeight || video.clientHeight;
      if (!width || !height) {
        state.face.processing = false;
        state.face.rafId = requestAnimationFrame(processFaceFrame);
        return;
      }

      if (refs.faceOverlay.width !== width) refs.faceOverlay.width = width;
      if (refs.faceOverlay.height !== height) refs.faceOverlay.height = height;
      const context = refs.faceOverlay.getContext("2d");

      const step = steps[state.face.captures.length];
      if (!step) {
        drawOverlay(context, width, height, true, true);
        state.face.processing = false;
        state.face.rafId = requestAnimationFrame(processFaceFrame);
        return;
      }

      refs.guideText.textContent = step.instruction;

      const faces = results.multiFaceLandmarks || [];
      if (faces.length !== 1) {
        state.face.alignFrames = 0;
        drawOverlay(context, width, height, false, false);
        refs.faceStatus.textContent = faces.length > 1 ? "Multiple faces detected. Keep one face in frame." : "Waiting for face detection.";
        state.face.processing = false;
        state.face.rafId = requestAnimationFrame(processFaceFrame);
        return;
      }

      const landmarks = faces[0];
      const left = landmarks[234];
      const right = landmarks[454];
      const nose = landmarks[1];
      const top = landmarks[10];
      const bottom = landmarks[152];

      const yaw = nose.x - ((left.x + right.x) / 2);
      const pitch = nose.y - ((top.y + bottom.y) / 2);
      const aligned = evaluateStep(step.key, yaw, pitch);

      drawOverlay(context, width, height, aligned, true);

      if (Date.now() >= state.face.cooldownUntil) {
        state.face.alignFrames = aligned ? state.face.alignFrames + 1 : 0;
      }

      refs.faceStatus.textContent = aligned ? `Alignment OK for ${step.label}. Hold still...` : `Adjust for ${step.label}.`;

      if (state.face.alignFrames >= 4 && Date.now() >= state.face.cooldownUntil) {
        captureCurrentFrame();
      }
    } catch (_error) {
      refs.faceStatus.textContent = "Face detection processing error.";
    } finally {
      state.face.processing = false;
      state.face.rafId = requestAnimationFrame(processFaceFrame);
    }
  };

  const startFaceCapture = async () => {
    stopFaceCapture();
    state.face.lastResults = null;
    renderFaceState();
    refs.guideText.textContent = steps[state.face.captures.length]?.instruction || "Preparing capture...";
    refs.faceStatus.textContent = "Starting camera...";

    if (typeof FaceMesh === "undefined") {
      refs.faceStatus.textContent = "FaceMesh library failed to load.";
      showToast("FaceMesh library failed to load.", true);
      return;
    }

    try {
      state.face.stream = await navigator.mediaDevices.getUserMedia({
        video: { width: { ideal: 960 }, height: { ideal: 720 }, facingMode: "user" },
        audio: false,
      });
      refs.faceVideo.srcObject = state.face.stream;
      await refs.faceVideo.play();

      state.face.mesh = new FaceMesh({ locateFile: (file) => `https://cdn.jsdelivr.net/npm/@mediapipe/face_mesh/${file}` });
      state.face.mesh.setOptions({
        maxNumFaces: 1,
        refineLandmarks: true,
        minDetectionConfidence: 0.6,
        minTrackingConfidence: 0.6,
      });
      state.face.mesh.onResults((results) => {
        state.face.lastResults = results || null;
      });

      refs.faceStatus.textContent = "Camera ready. Follow the guide.";
      state.face.rafId = requestAnimationFrame(processFaceFrame);
    } catch (_error) {
      refs.faceStatus.textContent = "Unable to access camera. Check browser permissions.";
      showToast("Unable to access camera.", true);
    }
  };

  const openFaceModal = async (studentId, mode) => {
    try {
      const data = await api(`/api/students/${studentId}`);
      const student = data.student || {};
      state.face.studentId = student._id || studentId;
      state.face.mode = mode === "update" ? "update" : "register";
      state.face.captures = [];
      state.face.alignFrames = 0;
      state.face.cooldownUntil = 0;

      refs.faceTitle.textContent = state.face.mode === "update" ? "Update Face Registration" : "Register Face";
      refs.faceSubtitle.textContent = `${student.name || ""} (${student.lrn || student.student_id || ""})`;

      showModal("faceModal");
      await startFaceCapture();
    } catch (error) {
      showToast(error.message, true);
    }
  };

  const submitFaceRegistration = async () => {
    if (!state.face.studentId || state.face.captures.length < steps.length) {
      showToast("Complete all required face angles first.", true);
      return;
    }

    const updateMode = state.face.mode === "update";
    const url = updateMode
      ? `/api/students/${state.face.studentId}/face/update`
      : `/api/students/${state.face.studentId}/face/register`;

    try {
      await api(url, { method: updateMode ? "PUT" : "POST", body: { faces: state.face.captures } });
      closeModal("faceModal");
      if (updateMode) showFaceUpdateSuccessAnimation();
      showToast(updateMode ? "Face updated successfully." : "Face registered successfully.");
      await loadStudents();
    } catch (error) {
      showToast(error.message, true);
    }
  };
  const onKeyDown = (event) => {
    if (event.key === "Escape" && state.activeModal) {
      closeModal(state.activeModal);
      return;
    }
    trapFocus(event);
  };

  const initEvents = () => {
    refs.searchInput.addEventListener("input", debounce(() => {
      state.filters.q = refs.searchInput.value.trim();
      state.pagination.page = 1;
      loadStudents();
    }, 320));

    refs.gradeFilter.addEventListener("change", () => {
      state.filters.grade = refs.gradeFilter.value;
      state.pagination.page = 1;
      if (state.filters.section) {
        const allowed = state.sectionsByGrade[gradeKey(state.filters.grade)] || [];
        if (state.filters.grade && !allowed.includes(state.filters.section)) {
          state.filters.section = "";
          clearSectionStats("Select a section to view updated counts.");
        }
      }
      renderSections();
      loadStudents();
      if (state.filters.grade && state.filters.section) {
        loadSectionStats({ grade: state.filters.grade, section: state.filters.section, silent: true });
      } else {
        clearSectionStats("Click a section chip to view detailed gender statistics.");
      }
    });

    refs.clearSectionBtn.addEventListener("click", () => {
      state.filters.section = "";
      state.pagination.page = 1;
      renderSections();
      loadStudents();
      clearSectionStats("Select a section chip to view detailed gender statistics.");
    });

    refs.sectionsPanel.addEventListener("click", (event) => {
      const clearBtn = event.target.closest(".section-clear-btn");
      if (clearBtn) {
        clearSectionStudents(clearBtn.dataset.grade, clearBtn.dataset.section);
        return;
      }
      const chip = event.target.closest(".section-chip");
      if (!chip) return;
      state.filters.grade = gradeLabel(chip.dataset.grade);
      state.filters.section = chip.dataset.section || "";
      state.pagination.page = 1;
      refs.gradeFilter.value = state.filters.grade;
      renderSections();
      loadStudents();
      loadSectionStats({ grade: state.filters.grade, section: state.filters.section });
    });

    refs.openAddBtn.addEventListener("click", () => {
      refs.addForm.reset();
      if (refs.addForm?.elements?.parent_contact) {
        refs.addForm.elements.parent_contact.value = PH_CONTACT_PREFIX;
      }
      if (refs.addSectionSelect) refs.addSectionSelect.value = "";
      syncAddSectionAssignment();
      refs.addImportForm?.reset();
      setAddImportSummary("");
      showModal("addModal");
    });

    refs.addSectionBtn?.addEventListener("click", async () => {
      const grade = String(refs.newSectionGrade?.value || "").trim();
      const section = String(refs.newSectionName?.value || "").trim();
      if (!grade) {
        showToast("Please select a grade for the section.", true);
        refs.newSectionGrade?.focus();
        return;
      }
      if (!section) {
        showToast("Section name is required.", true);
        refs.newSectionName?.focus();
        return;
      }

      try {
        await api("/api/sections", { method: "POST", body: { grade, section } });
        refs.newSectionName.value = "";
        showToast("Section saved successfully.");
        await loadSections();
      } catch (error) {
        showToast(error.message, true);
      }
    });

    refs.newSectionName?.addEventListener("keydown", (event) => {
      if (event.key === "Enter") {
        event.preventDefault();
        refs.addSectionBtn?.click();
      }
    });

    refs.addSectionSelect?.addEventListener("change", () => {
      syncAddSectionAssignment();
    });

    refs.addForm.elements.parent_contact?.addEventListener("input", () => {
      refs.addForm.elements.parent_contact.value = normalizeParentContactInput(
        refs.addForm.elements.parent_contact.value,
        true,
      );
    });

    refs.editForm.elements.parent_contact?.addEventListener("input", () => {
      refs.editForm.elements.parent_contact.value = normalizeParentContactInput(
        refs.editForm.elements.parent_contact.value,
        false,
      );
    });

    refs.addImportForm?.addEventListener("submit", async (event) => {
      event.preventDefault();
      const selectedFile = refs.addImportFile?.files?.[0];
      if (!selectedFile) {
        showToast("Please select an Excel (.xlsx) file first.", true);
        refs.addImportFile?.focus();
        return;
      }

      if (!/\.xlsx$/i.test(selectedFile.name || "")) {
        showToast("Only .xlsx files are supported.", true);
        refs.addImportFile.value = "";
        refs.addImportFile?.focus();
        return;
      }

      const formData = new FormData(refs.addImportForm);
      formData.set("file", selectedFile);
      if (refs.addImportSubmitBtn) refs.addImportSubmitBtn.disabled = true;
      setAddImportSummary("Import in progress...");

      try {
        const response = await api("/api/students/import", {
          method: "POST",
          body: formData,
        });
        const rowsRead = Number.parseInt(response.total_rows_read || 0, 10) || 0;
        const imported = Number.parseInt(response.imported_count || 0, 10) || 0;
        const skipped = Number.parseInt(response.skipped_count || 0, 10) || 0;
        const duplicates = Number.parseInt(response.duplicate_count || 0, 10) || 0;
        const invalid = Number.parseInt(response.invalid_count || 0, 10) || 0;
        const summarySkipped = Number.parseInt(response.summary_skipped_count || 0, 10) || 0;

        const summary = [
          rowsRead > 0 ? `Rows read: ${rowsRead}.` : "",
          `Imported ${imported} student(s).`,
          `Skipped/failed ${skipped} row(s).`,
          duplicates > 0 ? `${duplicates} duplicate row(s) skipped.` : "",
          invalid > 0 ? `${invalid} invalid row(s) skipped.` : "",
          summarySkipped > 0 ? `${summarySkipped} summary row(s) skipped.` : "",
        ].filter(Boolean).join(" ");

        const hasOnlySkippedRows = imported === 0 && skipped > 0;
        showToast(response.message || summary, hasOnlySkippedRows);
        const importNotice = imported > 0 ? "Import Successful" : "Import Completed";
        closeModal("addModal");
        showCenteredSuccess(importNotice);
        refs.addForm?.reset();
        if (refs.addForm?.elements?.parent_contact) {
          refs.addForm.elements.parent_contact.value = PH_CONTACT_PREFIX;
        }
        if (refs.addSectionSelect) refs.addSectionSelect.value = "";
        syncAddSectionAssignment();
        refs.addImportForm?.reset();
        setAddImportSummary("");
        state.pagination.page = 1;
        await loadSections();
        await loadStudents();
        await loadStudentStats({ silent: true });
        if (state.filters.grade && state.filters.section) {
          await loadSectionStats({ grade: state.filters.grade, section: state.filters.section, silent: true });
        }
      } catch (error) {
        showToast(error.message, true);
        setAddImportSummary(error.message, true);
      } finally {
        if (refs.addImportSubmitBtn) refs.addImportSubmitBtn.disabled = false;
      }
    });

    refs.addForm.addEventListener("submit", async (event) => {
      event.preventDefault();
      syncAddSectionAssignment();
      const payload = formPayload(refs.addForm);
      if (!payload.section || !payload.grade_level) {
        showToast("Please select a section assignment first.", true);
        refs.addSectionSelect?.focus();
        return;
      }
      payload.parent_contact = normalizeParentContactInput(payload.parent_contact, true);
      if (payload.parent_contact === PH_CONTACT_PREFIX) payload.parent_contact = "";
      if (!isValidParentContact(payload.parent_contact)) {
        showToast("Parent contact must be in +639XXXXXXXXX format.", true);
        refs.addForm.elements.parent_contact?.focus();
        return;
      }
      try {
        await api("/api/students", { method: "POST", body: payload });
        closeModal("addModal");
        refs.addForm.reset();
        refs.addForm.elements.parent_contact.value = PH_CONTACT_PREFIX;
        if (refs.addSectionSelect) refs.addSectionSelect.value = "";
        syncAddSectionAssignment();
        refs.addImportForm?.reset();
        setAddImportSummary("");
        showCenteredSuccess("Student Added Successfully");
        showToast("Student created successfully.");
        state.pagination.page = 1;
        await loadSections();
        await loadStudents();
        await loadStudentStats({ silent: true });
        if (state.filters.grade && state.filters.section) {
          await loadSectionStats({ grade: state.filters.grade, section: state.filters.section, silent: true });
        }
      } catch (error) {
        showToast(error.message, true);
      }
    });

    refs.editForm.addEventListener("submit", async (event) => {
      event.preventDefault();
      const studentId = refs.editForm.elements._id.value;
      if (!studentId) return;
      const payload = formPayload(refs.editForm);
      payload.parent_contact = normalizeParentContactInput(payload.parent_contact, false);
      if (!isValidParentContact(payload.parent_contact)) {
        showToast("Parent contact must be in +639XXXXXXXXX format.", true);
        refs.editForm.elements.parent_contact?.focus();
        return;
      }
      try {
        await api(`/api/students/${studentId}`, { method: "PUT", body: payload });
        closeModal("editModal");
        showToast("Student updated successfully.");
        await loadSections();
        await loadStudents();
        await loadStudentStats({ silent: true });
        if (state.filters.grade && state.filters.section) {
          await loadSectionStats({ grade: state.filters.grade, section: state.filters.section, silent: true });
        }
      } catch (error) {
        showToast(error.message, true);
      }
    });

    refs.studentsTableBody.addEventListener("click", (event) => {
      const actionButton = event.target.closest("button[data-act]");
      if (actionButton) {
        event.stopPropagation();
        const studentId = actionButton.dataset.id;
        const action = actionButton.dataset.act;
        if (action === "edit") openEditModal(studentId);
        if (action === "delete") openDeleteModal(studentId, actionButton.dataset.name || "Selected student");
        if (action === "face") openFaceModal(studentId, actionButton.dataset.mode);
      }
    });

    refs.confirmDeleteBtn.addEventListener("click", confirmDelete);

    refs.paginationControls.addEventListener("click", (event) => {
      const button = event.target.closest("button[data-page]");
      if (!button || button.disabled) return;
      const page = Number.parseInt(button.dataset.page || "1", 10);
      if (Number.isNaN(page) || page < 1 || page > state.pagination.pages) return;
      state.pagination.page = page;
      loadStudents();
    });

    refs.resetCaptureBtn.addEventListener("click", async () => {
      state.face.captures = [];
      state.face.alignFrames = 0;
      state.face.cooldownUntil = 0;
      await startFaceCapture();
    });

    refs.submitFaceBtn.addEventListener("click", submitFaceRegistration);

    document.querySelectorAll("[data-close]").forEach((button) => {
      button.addEventListener("click", () => closeModal(button.dataset.close));
    });

    document.querySelectorAll("[data-overlay]").forEach((overlay) => {
      overlay.addEventListener("click", (event) => {
        if (event.target === overlay) closeModal(overlay.id);
      });
    });

    document.addEventListener("keydown", onKeyDown);
  };

  initEvents();
  renderFaceState();
  renderSectionStats();
  renderAddSectionAssignments();
  syncAddSectionAssignment();
  startRealtimeUpdates();
  loadSections();
  loadStudents();
  loadStudentStats();
})();
