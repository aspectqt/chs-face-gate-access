(() => {
  const STANDARD_FACE_CAPTURE_STEPS = [
    { key: "front", label: "Front", instruction: "Look straight at the camera.", targetYaw: 0, yawTolerance: 0.09, targetPitch: 0, pitchTolerance: 0.12, minScore: 0.72 },
    { key: "left_soft", label: "Soft Left", instruction: "Turn slightly to your left.", targetYaw: 0.12, yawTolerance: 0.08, targetPitch: 0, pitchTolerance: 0.16, minScore: 0.7 },
    { key: "left", label: "Left", instruction: "Turn farther to your left.", targetYaw: 0.24, yawTolerance: 0.09, targetPitch: 0, pitchTolerance: 0.17, minScore: 0.72 },
    { key: "right_soft", label: "Soft Right", instruction: "Turn slightly to your right.", targetYaw: -0.12, yawTolerance: 0.08, targetPitch: 0, pitchTolerance: 0.16, minScore: 0.7 },
    { key: "right", label: "Right", instruction: "Turn farther to your right.", targetYaw: -0.24, yawTolerance: 0.09, targetPitch: 0, pitchTolerance: 0.17, minScore: 0.72 },
    { key: "up", label: "Up", instruction: "Lift your chin slightly.", targetYaw: 0, yawTolerance: 0.11, targetPitch: -0.13, pitchTolerance: 0.1, minScore: 0.7 },
    { key: "down", label: "Down", instruction: "Lower your chin slightly.", targetYaw: 0, yawTolerance: 0.11, targetPitch: 0.13, pitchTolerance: 0.11, minScore: 0.7 },
    { key: "up_left", label: "Up Left", instruction: "Look up and to your left.", targetYaw: 0.14, yawTolerance: 0.09, targetPitch: -0.11, pitchTolerance: 0.1, minScore: 0.68 },
    { key: "up_right", label: "Up Right", instruction: "Look up and to your right.", targetYaw: -0.14, yawTolerance: 0.09, targetPitch: -0.11, pitchTolerance: 0.1, minScore: 0.68 },
    { key: "down_center", label: "Low Center", instruction: "Keep centered and lower your chin a little more.", targetYaw: 0, yawTolerance: 0.12, targetPitch: 0.2, pitchTolerance: 0.1, minScore: 0.68 },
  ];
  const SIMILAR_FACE_CAPTURE_EXTRA_STEPS = [
    { key: "down_left", label: "Down Left", instruction: "Look down and to your left.", targetYaw: 0.15, yawTolerance: 0.09, targetPitch: 0.15, pitchTolerance: 0.11, minScore: 0.68 },
    { key: "down_right", label: "Down Right", instruction: "Look down and to your right.", targetYaw: -0.15, yawTolerance: 0.09, targetPitch: 0.15, pitchTolerance: 0.11, minScore: 0.68 },
    { key: "profile_left", label: "Profile Left", instruction: "Turn to a deeper left angle.", targetYaw: 0.34, yawTolerance: 0.1, targetPitch: 0, pitchTolerance: 0.18, minScore: 0.72 },
    { key: "profile_right", label: "Profile Right", instruction: "Turn to a deeper right angle.", targetYaw: -0.34, yawTolerance: 0.1, targetPitch: 0, pitchTolerance: 0.18, minScore: 0.72 },
    { key: "high_left", label: "High Left", instruction: "Lift your chin and turn left.", targetYaw: 0.18, yawTolerance: 0.1, targetPitch: -0.18, pitchTolerance: 0.11, minScore: 0.68 },
    { key: "high_right", label: "High Right", instruction: "Lift your chin and turn right.", targetYaw: -0.18, yawTolerance: 0.1, targetPitch: -0.18, pitchTolerance: 0.11, minScore: 0.68 },
    { key: "chin_left", label: "Chin Left", instruction: "Lower your chin and turn left.", targetYaw: 0.18, yawTolerance: 0.1, targetPitch: 0.18, pitchTolerance: 0.11, minScore: 0.68 },
    { key: "chin_right", label: "Chin Right", instruction: "Lower your chin and turn right.", targetYaw: -0.18, yawTolerance: 0.1, targetPitch: 0.18, pitchTolerance: 0.11, minScore: 0.68 },
    { key: "high_center", label: "High Center", instruction: "Look higher while staying centered.", targetYaw: 0, yawTolerance: 0.12, targetPitch: -0.24, pitchTolerance: 0.1, minScore: 0.68 },
    { key: "low_center", label: "Low Center", instruction: "Look lower while staying centered.", targetYaw: 0, yawTolerance: 0.12, targetPitch: 0.26, pitchTolerance: 0.1, minScore: 0.68 },
  ];
  const FACE_CAPTURE_PROFILES = {
    standard: {
      key: "standard",
      label: "Standard Profile",
      target: STANDARD_FACE_CAPTURE_STEPS.length,
      badgeText: "10 guided angles",
      steps: STANDARD_FACE_CAPTURE_STEPS,
    },
    similar_faces: {
      key: "similar_faces",
      label: "Similar Faces Mode",
      target: STANDARD_FACE_CAPTURE_STEPS.length + SIMILAR_FACE_CAPTURE_EXTRA_STEPS.length,
      badgeText: "20 guided angles",
      steps: [...STANDARD_FACE_CAPTURE_STEPS, ...SIMILAR_FACE_CAPTURE_EXTRA_STEPS],
    },
  };
  const FACE_GUIDE_RING_CLASSES = {
    idle: "h-[72%] w-[48%] rounded-[999px] border-2 border-white/70 shadow-[0_0_0_999px_rgba(2,6,23,0.58)] transition-all duration-300",
    detected: "h-[72%] w-[48%] rounded-[999px] border-2 border-sky-300/85 shadow-[0_0_0_999px_rgba(2,6,23,0.58)] transition-all duration-300",
    aligned: "h-[72%] w-[48%] rounded-[999px] border-2 border-emerald-400 shadow-[0_0_0_999px_rgba(2,6,23,0.54),0_0_24px_rgba(52,211,153,0.3)] transition-all duration-300",
    warning: "h-[72%] w-[48%] rounded-[999px] border-2 border-amber-300 shadow-[0_0_0_999px_rgba(2,6,23,0.6)] transition-all duration-300",
  };
  const FACE_CAPTURE_BRIGHTNESS_MIN = 20;
  const FACE_CAPTURE_BRIGHTNESS_MAX = 244;
  const FACE_CAPTURE_CONTRAST_MIN = 8.5;
  const FACE_CAPTURE_SHARPNESS_MIN = 5.5;
  const FACE_CAPTURE_MIN_HAMMING_DISTANCE = 4;
  const FACE_CAPTURE_DETECTION_MAX_WIDTH = 640;
  const FACE_CAPTURE_TRANSITION_POSE_DELTA = 0.022;
  const FACE_CAPTURE_GUIDE_PADDING = 1.08;
  const FACE_CAPTURE_GUIDE_CORE_PADDING = 1.01;
  const FACE_CAPTURE_GUIDE_MIN_POINTS = 4;
  const FACE_CAPTURE_GUIDE_MIN_BOUNDARY_POINTS = 2;
  const FACE_CAPTURE_GUIDE_MIN_FACE_HEIGHT_RATIO = 0.22;
  const FACE_CAPTURE_GUIDE_MAX_FACE_HEIGHT_RATIO = 1.24;
  const FACE_CAPTURE_GUIDE_MIN_FACE_WIDTH_RATIO = 0.19;
  const FACE_CAPTURE_GUIDE_MAX_FACE_WIDTH_RATIO = 1.08;
  const FACE_CAPTURE_STABLE_FRAME_TARGET = 1;
  const FACE_CAPTURE_SOFT_FRAME_TARGET = 2;
  const FACE_CAPTURE_DUPLICATE_POSE_DELTA = 0.055;
  const FACE_CAPTURE_SOFT_SCORE_DELTA = 0.28;
  const FACE_CAPTURE_RETRY_COOLDOWN_MS = 70;
  const FACE_CAPTURE_SUCCESS_COOLDOWN_MS = 120;
  const FACE_CAPTURE_LOCKED_HOLD_MS = 35;
  const FACE_CAPTURE_SOFT_HOLD_MS = 75;
  const FACE_CAPTURE_MISSED_FRAME_TOLERANCE = 2;
  const FACE_CAPTURE_STATUS_HOLD_MS = 280;
  const SECTION_STATS_EMPTY_NOTE = "Select a year level and section to view detailed gender statistics.";
  const REENROLL_ASSIGNMENT_NEW_SECTION_VALUE = "__new_section__";

  const state = {
    students: [],
    schoolYears: [],
    schoolYear: {
      selected: String(document.body?.dataset?.selectedSchoolYear || "").trim(),
      current: String(document.body?.dataset?.currentSchoolYear || "").trim(),
      archivedView: String(document.body?.dataset?.archivedView || "") === "1",
    },
    sectionsByGrade: {},
    filters: { q: "", grade: "", section: "", faceStatus: "" },
    sectionControls: {
      selectedGradeKey: "",
      activeSection: "",
    },
    pagination: { page: 1, limit: 5, total: 0, pages: 1 },
    activeModal: null,
    modalStack: [],
    lastFocus: null,
    deleteTarget: { id: "", label: "" },
    exportPdf: {
      scope: "grade",
      grade: "",
      sectionGrade: "",
      section: "",
      studentQuery: "",
      selectedStudent: null,
      studentResults: [],
      studentLoading: false,
      studentSearchToken: 0,
    },
    face: {
      studentId: "",
      mode: "register",
      captureProfile: "standard",
      captures: [],
      captureMeta: [],
      invalidIndices: [],
      invalidCaptureDetails: {},
      submitting: false,
      stream: null,
      mesh: null,
      lastResults: null,
      rafId: null,
      rafMode: "",
      processing: false,
      captureInFlight: false,
      alignFrames: 0,
      cooldownUntil: 0,
      started: false,
      stableSince: 0,
      stableMode: "",
      missedFrames: 0,
      statusLockUntil: 0,
      statusLockMessage: "",
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
      note: SECTION_STATS_EMPTY_NOTE,
      updatedAt: "",
    },
    addModalView: "manual",
    reenroll: {
      sourceSchoolYear: "",
      candidates: [],
      loading: false,
      submitting: false,
      filters: {
        grade: "",
        section: "",
      },
    },
  };

  let sectionStatsRequestToken = 0;
  let centeredSuccessExitTimer = null;
  let centeredSuccessHideTimer = null;
  let toastHideTimer = null;

  const refs = {
    toast: document.getElementById("toast"),
    toastTitle: document.getElementById("toastTitle"),
    toastMessage: document.getElementById("toastMessage"),
    toastDetails: document.getElementById("toastDetails"),
    toastCloseBtn: document.getElementById("toastCloseBtn"),
    studentsExportBtn: document.getElementById("studentsExportBtn"),
    studentsExportModal: document.getElementById("studentsExportModal"),
    studentsExportScopeGrade: document.getElementById("studentsExportScopeGrade"),
    studentsExportScopeSection: document.getElementById("studentsExportScopeSection"),
    studentsExportScopeStudent: document.getElementById("studentsExportScopeStudent"),
    studentsExportGradeFields: document.getElementById("studentsExportGradeFields"),
    studentsExportGradeLevel: document.getElementById("studentsExportGradeLevel"),
    studentsExportSectionFields: document.getElementById("studentsExportSectionFields"),
    studentsExportSectionGrade: document.getElementById("studentsExportSectionGrade"),
    studentsExportSection: document.getElementById("studentsExportSection"),
    studentsExportStudentFields: document.getElementById("studentsExportStudentFields"),
    studentsExportStudentSearch: document.getElementById("studentsExportStudentSearch"),
    studentsExportStudentHint: document.getElementById("studentsExportStudentHint"),
    studentsExportStudentSelected: document.getElementById("studentsExportStudentSelected"),
    studentsExportStudentResults: document.getElementById("studentsExportStudentResults"),
    studentsExportSummary: document.getElementById("studentsExportSummary"),
    studentsExportDownloadBtn: document.getElementById("studentsExportDownloadBtn"),
    studentsExportPrintBtn: document.getElementById("studentsExportPrintBtn"),
    schoolYearSelect: document.getElementById("schoolYearSelect"),
    schoolYearHint: document.getElementById("schoolYearHint"),
    schoolYearModeBadge: document.getElementById("schoolYearModeBadge"),
    currentSchoolYearLabel: document.getElementById("currentSchoolYearLabel"),
    createSchoolYearBtn: document.getElementById("createSchoolYearBtn"),
    openReenrollBtn: document.getElementById("openReenrollBtn"),
    schoolYearForm: document.getElementById("schoolYearForm"),
    schoolYearLabel: document.getElementById("schoolYearLabel"),
    schoolYearFormAlert: document.getElementById("schoolYearFormAlert"),
    schoolYearSubmitBtn: document.getElementById("schoolYearSubmitBtn"),
    reenrollSourceYearSelect: document.getElementById("reenrollSourceYearSelect"),
    reenrollGradeFilter: document.getElementById("reenrollGradeFilter"),
    reenrollSectionFilter: document.getElementById("reenrollSectionFilter"),
    reenrollTargetYearLabel: document.getElementById("reenrollTargetYearLabel"),
    reenrollTargetYearBadge: document.getElementById("reenrollTargetYearBadge"),
    reenrollSelectAll: document.getElementById("reenrollSelectAll"),
    reenrollSelectionSummary: document.getElementById("reenrollSelectionSummary"),
    reloadReenrollCandidatesBtn: document.getElementById("reloadReenrollCandidatesBtn"),
    reenrollCandidates: document.getElementById("reenrollCandidates"),
    reenrollAlert: document.getElementById("reenrollAlert"),
    reenrollSubmitBtn: document.getElementById("reenrollSubmitBtn"),
    reenrollAssignmentForm: document.getElementById("reenrollAssignmentForm"),
    reenrollAssignmentTargetYear: document.getElementById("reenrollAssignmentTargetYear"),
    reenrollAssignmentSummary: document.getElementById("reenrollAssignmentSummary"),
    reenrollAssignmentAlert: document.getElementById("reenrollAssignmentAlert"),
    reenrollAssignmentGrade: document.getElementById("reenrollAssignmentGrade"),
    reenrollAssignmentSection: document.getElementById("reenrollAssignmentSection"),
    reenrollAssignmentSectionNewWrap: document.getElementById("reenrollAssignmentSectionNewWrap"),
    reenrollAssignmentSectionNew: document.getElementById("reenrollAssignmentSectionNew"),
    reenrollAssignmentSectionHint: document.getElementById("reenrollAssignmentSectionHint"),
    reenrollAssignmentConfirmBtn: document.getElementById("reenrollAssignmentConfirmBtn"),
    statTotalStudents: document.getElementById("statTotalStudents"),
    statActiveStudents: document.getElementById("statActiveStudents"),
    statInactiveStudents: document.getElementById("statInactiveStudents"),
    statAddedToday: document.getElementById("statAddedToday"),
    searchInput: document.getElementById("searchInput"),
    faceRegistrationFilter: document.getElementById("faceRegistrationFilter"),
    gradeFilter: document.getElementById("gradeFilter"),
    openAddSectionBtn: document.getElementById("openAddSectionBtn"),
    newSectionGrade: document.getElementById("newSectionGrade"),
    newSectionName: document.getElementById("newSectionName"),
    addSectionForm: document.getElementById("addSectionForm"),
    addSectionBtn: document.getElementById("addSectionBtn"),
    sectionsGradeSelect: document.getElementById("sectionsGradeSelect"),
    sectionsPanelTitle: document.getElementById("sectionsPanelTitle"),
    sectionsCountBadge: document.getElementById("sectionsCountBadge"),
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
    addManualTabBtn: document.getElementById("addManualTabBtn"),
    addImportTabBtn: document.getElementById("addImportTabBtn"),
    addManualPanel: document.getElementById("addManualPanel"),
    addImportPanel: document.getElementById("addImportPanel"),
    addForm: document.getElementById("addForm"),
    addFormAlert: document.getElementById("addFormAlert"),
    addFormSubmitBtn: document.getElementById("addFormSubmitBtn"),
    addFormSubmitLabel: document.getElementById("addFormSubmitLabel"),
    addLrnError: document.getElementById("addLrnError"),
    addNameError: document.getElementById("addNameError"),
    addSectionError: document.getElementById("addSectionError"),
    addParentContactError: document.getElementById("addParentContactError"),
    addGenderError: document.getElementById("addGenderError"),
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
    startCaptureBtn: document.getElementById("startCaptureBtn"),
    faceCaptureProfileStandard: document.getElementById("faceCaptureProfileStandard"),
    faceCaptureProfileSimilar: document.getElementById("faceCaptureProfileSimilar"),
    faceCaptureTarget: document.getElementById("faceCaptureTarget"),
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

  if (!refs.studentsTableBody) {
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
  const ADD_MODAL_MANUAL_VIEW = "manual";
  const ADD_MODAL_IMPORT_VIEW = "import";
  const SCHOOL_YEAR_PATTERN = /^\d{4}-\d{4}$/;

  const getSelectedSchoolYear = () => String(state.schoolYear.selected || state.schoolYear.current || "").trim();
  const getCurrentSchoolYear = () => String(state.schoolYear.current || "").trim();
  const isArchivedView = () => Boolean(state.schoolYear.archivedView);
  const canManageStudents = () => String(document.body?.dataset?.canManageStudents || "") === "1";
  const canRegisterFaces = () => String(document.body?.dataset?.canRegisterFaces || "") === "1";

  const applySchoolYearViewState = () => {
    const archived = isArchivedView();
    const selectedYear = getSelectedSchoolYear();
    const currentYear = getCurrentSchoolYear();
    const hasSourceYears = listSchoolYearOptions().some((item) => item.label && item.label !== selectedYear);

    if (refs.currentSchoolYearLabel) refs.currentSchoolYearLabel.textContent = currentYear || selectedYear || "-";
    if (refs.schoolYearHint) {
      refs.schoolYearHint.textContent = archived
        ? `Viewing archived records for ${selectedYear}. Archived school years are read-only.`
        : `Managing active enrollment records for ${selectedYear}.`;
    }
    if (refs.schoolYearModeBadge) {
      refs.schoolYearModeBadge.textContent = archived ? "Archived View" : "Current School Year";
      refs.schoolYearModeBadge.className = `inline-flex items-center rounded-full border px-2.5 py-1 text-[11px] font-semibold ${archived ? "border-amber-200 bg-amber-50 text-amber-700" : "border-emerald-200 bg-emerald-50 text-emerald-700"}`;
    }
    if (refs.openAddBtn) refs.openAddBtn.disabled = archived || !canManageStudents();
    if (refs.openAddSectionBtn) refs.openAddSectionBtn.disabled = archived || !canManageStudents();
    if (refs.openReenrollBtn) refs.openReenrollBtn.disabled = archived || !hasSourceYears || !canManageStudents();
    if (refs.reenrollTargetYearLabel) refs.reenrollTargetYearLabel.textContent = selectedYear || "-";
    if (refs.reenrollTargetYearBadge) refs.reenrollTargetYearBadge.textContent = selectedYear || "-";
  };

  const buildStudentsPageUrl = (schoolYear) => {
    const params = new URLSearchParams(window.location.search || "");
    const normalized = String(schoolYear || "").trim();
    if (normalized) params.set("school_year", normalized);
    else params.delete("school_year");
    const query = params.toString();
    return query ? `/students?${query}` : "/students";
  };

  const broadcastSchoolYearSelection = (schoolYear) => {
    try {
      window.AppSchoolYear?.broadcastSelection?.(schoolYear);
    } catch (_error) {
      // Ignore optional cross-tab sync failures.
    }
  };

  const appendSelectedSchoolYearParam = (params, paramName = "school_year") => {
    const schoolYear = getSelectedSchoolYear();
    if (schoolYear) params.set(paramName, schoolYear);
    return params;
  };

  const suggestNextSchoolYearLabel = (label = "") => {
    const fallback = String(label || getCurrentSchoolYear() || getSelectedSchoolYear() || "").trim();
    const match = fallback.match(/^(\d{4})-(\d{4})$/);
    if (!match) return "";
    const startYear = Number.parseInt(match[1], 10);
    const endYear = Number.parseInt(match[2], 10);
    if (!Number.isFinite(startYear) || !Number.isFinite(endYear)) return "";
    return `${startYear + 1}-${endYear + 1}`;
  };

  const validateSchoolYearLabel = (value) => {
    const normalized = String(value || "").trim().replaceAll("/", "-");
    if (!SCHOOL_YEAR_PATTERN.test(normalized)) {
      return { valid: false, message: "Use the school year format YYYY-YYYY." };
    }
    const [startText, endText] = normalized.split("-");
    const startYear = Number.parseInt(startText, 10);
    const endYear = Number.parseInt(endText, 10);
    if (!Number.isFinite(startYear) || !Number.isFinite(endYear) || endYear !== startYear + 1) {
      return { valid: false, message: "The ending year must be exactly one year after the starting year." };
    }
    return { valid: true, value: `${startYear}-${endYear}` };
  };

  const setSchoolYearSubmitting = (isSubmitting) => {
    if (!refs.schoolYearSubmitBtn) return;
    refs.schoolYearSubmitBtn.disabled = Boolean(isSubmitting);
    refs.schoolYearSubmitBtn.innerHTML = isSubmitting
      ? `<svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4 animate-spin" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
          <path stroke-linecap="round" stroke-linejoin="round" d="M12 4v4m0 8v4m8-8h-4M8 12H4m12.95 4.95-2.83-2.83M9.88 9.88 7.05 7.05m9.9 0-2.83 2.83M9.88 14.12l-2.83 2.83" />
        </svg>
        Saving...`
      : `<svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
          <path stroke-linecap="round" stroke-linejoin="round" d="M12 4v16m8-8H4" />
        </svg>
        Save School Year`;
  };

  const setReenrollSubmitting = (isSubmitting) => {
    state.reenroll.submitting = Boolean(isSubmitting);
    if (refs.reenrollSubmitBtn) refs.reenrollSubmitBtn.disabled = state.reenroll.submitting || collectReenrollSelections().length === 0;
    if (refs.reenrollSubmitBtn) {
      refs.reenrollSubmitBtn.innerHTML = state.reenroll.submitting
        ? `<svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4 animate-spin" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
            <path stroke-linecap="round" stroke-linejoin="round" d="M12 4v4m0 8v4m8-8h-4M8 12H4m12.95 4.95-2.83-2.83M9.88 9.88 7.05 7.05m9.9 0-2.83 2.83M9.88 14.12l-2.83 2.83" />
          </svg>
          Saving...`
        : `<svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
            <path stroke-linecap="round" stroke-linejoin="round" d="M5 13l4 4L19 7" />
          </svg>
          Save Enrollment`;
    }
    if (refs.reenrollSelectAll) refs.reenrollSelectAll.disabled = state.reenroll.submitting || state.reenroll.loading;
    if (refs.reloadReenrollCandidatesBtn) refs.reloadReenrollCandidatesBtn.disabled = state.reenroll.submitting || state.reenroll.loading;
    if (refs.reenrollGradeFilter) refs.reenrollGradeFilter.disabled = state.reenroll.submitting || state.reenroll.loading || refs.reenrollGradeFilter.options.length <= 1;
    if (refs.reenrollSectionFilter) refs.reenrollSectionFilter.disabled = state.reenroll.submitting || state.reenroll.loading || !state.reenroll.filters.grade || refs.reenrollSectionFilter.options.length <= 1;
    if (refs.reenrollAssignmentGrade) refs.reenrollAssignmentGrade.disabled = state.reenroll.submitting;
    if (refs.reenrollAssignmentSection) refs.reenrollAssignmentSection.disabled = state.reenroll.submitting;
    if (refs.reenrollAssignmentSectionNew) refs.reenrollAssignmentSectionNew.disabled = state.reenroll.submitting;
    if (refs.reenrollAssignmentConfirmBtn) {
      refs.reenrollAssignmentConfirmBtn.disabled = state.reenroll.submitting;
      refs.reenrollAssignmentConfirmBtn.innerHTML = state.reenroll.submitting
        ? `<svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4 animate-spin" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
            <path stroke-linecap="round" stroke-linejoin="round" d="M12 4v4m0 8v4m8-8h-4M8 12H4m12.95 4.95-2.83-2.83M9.88 9.88 7.05 7.05m9.9 0-2.83 2.83M9.88 14.12l-2.83 2.83" />
          </svg>
          Saving...`
        : `<svg xmlns="http://www.w3.org/2000/svg" class="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" stroke-width="2">
            <path stroke-linecap="round" stroke-linejoin="round" d="M5 13l4 4L19 7" />
          </svg>
          Confirm Enrollment`;
    }
  };

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

  const getSelectedSectionGradeKey = () => String(state.sectionControls.selectedGradeKey || "").trim();

  const setSelectedSectionGradeKey = (gradeValue) => {
    state.sectionControls.selectedGradeKey = String(gradeValue || "").trim();
  };

  const getActiveSectionSelection = () => ({
    gradeKey: getSelectedSectionGradeKey(),
    section: String(state.sectionControls.activeSection || "").trim(),
  });

  const setActiveSectionSelection = (gradeValue, sectionValue) => {
    setSelectedSectionGradeKey(gradeValue);
    state.sectionControls.activeSection = String(sectionValue || "").trim();
  };

  const syncStudentTableFiltersFromSectionSelection = () => {
    const selectedGrade = getSelectedSectionGradeKey();
    const activeSection = String(state.sectionControls.activeSection || "").trim();
    state.filters.grade = selectedGrade ? gradeLabel(selectedGrade) : "";
    state.filters.section = activeSection;
  };

  const syncSectionSelectionState = () => {
    const selectedGrade = getSelectedSectionGradeKey();
    if (!selectedGrade) {
      state.sectionControls.activeSection = "";
      clearSectionStats();
      syncStudentTableFiltersFromSectionSelection();
      return;
    }

    const allowed = Array.isArray(state.sectionsByGrade[selectedGrade]) ? state.sectionsByGrade[selectedGrade] : [];
    if (state.sectionControls.activeSection && !allowed.includes(state.sectionControls.activeSection)) {
      state.sectionControls.activeSection = "";
      clearSectionStats();
    }
    syncStudentTableFiltersFromSectionSelection();
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

  const listSchoolYearOptions = () => Array.from(refs.schoolYearSelect?.options || []).map((option) => ({
    label: String(option.value || "").trim(),
    text: String(option.textContent || "").trim(),
  })).filter((item) => item.label);

  const renderReenrollSourceOptions = () => {
    if (!refs.reenrollSourceYearSelect) return;
    const selectedYear = getSelectedSchoolYear();
    const options = listSchoolYearOptions().filter((item) => item.label && item.label !== selectedYear);
    state.schoolYears = listSchoolYearOptions();
    refs.reenrollSourceYearSelect.innerHTML = `<option value="">Select Source School Year</option>${options.map((item) => `<option value="${esc(item.label)}">${esc(item.text)}</option>`).join("")}`;
    if (state.reenroll.sourceSchoolYear && options.some((item) => item.label === state.reenroll.sourceSchoolYear)) {
      refs.reenrollSourceYearSelect.value = state.reenroll.sourceSchoolYear;
    } else {
      const newestOption = options[0]?.label || "";
      state.reenroll.sourceSchoolYear = newestOption;
      refs.reenrollSourceYearSelect.value = newestOption;
    }
    if (refs.openReenrollBtn) refs.openReenrollBtn.disabled = isArchivedView() || options.length === 0 || !canManageStudents();
  };

  const setSchoolYearFormAlert = (message = "") => {
    if (!refs.schoolYearFormAlert) return;
    const text = String(message || "").trim();
    refs.schoolYearFormAlert.textContent = text;
    refs.schoolYearFormAlert.classList.toggle("hidden", !text);
  };

  const setReenrollAlert = (message = "", isError = false) => {
    if (!refs.reenrollAlert) return;
    const text = String(message || "").trim();
    refs.reenrollAlert.textContent = text;
    refs.reenrollAlert.className = `rounded-xl border px-3 py-2 text-xs font-medium ${isError ? "border-rose-200 bg-rose-50 text-rose-700" : "border-emerald-200 bg-emerald-50 text-emerald-700"}`;
    refs.reenrollAlert.classList.toggle("hidden", !text);
  };

  const setReenrollAssignmentAlert = (message = "", isError = false) => {
    if (!refs.reenrollAssignmentAlert) return;
    const text = String(message || "").trim();
    refs.reenrollAssignmentAlert.textContent = text;
    refs.reenrollAssignmentAlert.className = `rounded-xl border px-3 py-2 text-xs font-medium ${isError ? "border-rose-200 bg-rose-50 text-rose-700" : "border-emerald-200 bg-emerald-50 text-emerald-700"}`;
    refs.reenrollAssignmentAlert.classList.toggle("hidden", !text);
  };

  const gradeOptionsMarkup = (selectedValue = "") => {
    const selected = String(selectedValue || "").trim();
    const options = Array.from(refs.editForm?.elements?.grade_level?.options || []);
    return options.map((option) => {
      const value = String(option.value || "").trim();
      if (!value) return "";
      const isSelected = value === selected ? " selected" : "";
      return `<option value="${esc(value)}"${isSelected}>${esc(option.textContent || value)}</option>`;
    }).join("");
  };

  const getSelectedReenrollCandidates = () => state.reenroll.candidates
    .filter((candidate) => candidate.selected && !candidate.already_enrolled);

  const getDefaultReenrollAssignmentGrade = (selectedCandidates = []) => {
    const values = Array.from(new Set(selectedCandidates
      .map((candidate) => String(candidate.promoted_grade_level || candidate.grade_level || "").trim())
      .filter(Boolean)));
    return values[0] || "";
  };

  const getDefaultReenrollAssignmentSection = (selectedCandidates = []) => {
    const values = Array.from(new Set(selectedCandidates
      .map((candidate) => String(candidate.target_section || "").trim())
      .filter(Boolean)));
    return values.length === 1 ? values[0] : "";
  };

  const toggleReenrollAssignmentNewSectionInput = (showInput, presetValue = "") => {
    if (refs.reenrollAssignmentSectionNewWrap) refs.reenrollAssignmentSectionNewWrap.classList.toggle("hidden", !showInput);
    if (refs.reenrollAssignmentSectionNew) {
      refs.reenrollAssignmentSectionNew.value = showInput ? String(presetValue || "").trim() : "";
      refs.reenrollAssignmentSectionNew.disabled = state.reenroll.submitting || !showInput;
    }
  };

  const getReenrollAssignmentSectionValue = () => {
    const selectedOption = String(refs.reenrollAssignmentSection?.value || "").trim();
    if (selectedOption === REENROLL_ASSIGNMENT_NEW_SECTION_VALUE) {
      return String(refs.reenrollAssignmentSectionNew?.value || "").trim();
    }
    return selectedOption;
  };

  const renderReenrollAssignmentSectionOptions = (preferredSection = "") => {
    const selectedGradeKey = gradeKey(refs.reenrollAssignmentGrade?.value || "");
    const sectionOptions = selectedGradeKey
      ? Array.from(state.sectionsByGrade[selectedGradeKey] || [])
      : []
    const sortedSectionOptions = sectionOptions
      .sort((left, right) => left.localeCompare(right, undefined, { sensitivity: "base" }));
    const targetSection = String(preferredSection || "").trim();

    if (refs.reenrollAssignmentSection) {
      const optionMarkup = sortedSectionOptions
        .map((section) => `<option value="${esc(section)}">${esc(section)}</option>`)
        .join("");
      refs.reenrollAssignmentSection.innerHTML = `<option value="">Select Section</option>${optionMarkup}<option value="${REENROLL_ASSIGNMENT_NEW_SECTION_VALUE}">Add New Section</option>`;
      refs.reenrollAssignmentSection.disabled = !selectedGradeKey || state.reenroll.submitting;

      if (!selectedGradeKey) {
        refs.reenrollAssignmentSection.value = "";
        toggleReenrollAssignmentNewSectionInput(false);
      } else if (targetSection && sortedSectionOptions.includes(targetSection)) {
        refs.reenrollAssignmentSection.value = targetSection;
        toggleReenrollAssignmentNewSectionInput(false);
      } else if (targetSection) {
        refs.reenrollAssignmentSection.value = REENROLL_ASSIGNMENT_NEW_SECTION_VALUE;
        toggleReenrollAssignmentNewSectionInput(true, targetSection);
      } else {
        refs.reenrollAssignmentSection.value = "";
        toggleReenrollAssignmentNewSectionInput(false);
      }
    }

    if (refs.reenrollAssignmentSectionHint) {
      if (!selectedGradeKey) {
        refs.reenrollAssignmentSectionHint.textContent = "Choose the target grade level first, then select a section from the dropdown.";
      } else if (sortedSectionOptions.length) {
        refs.reenrollAssignmentSectionHint.textContent = `Available sections for ${gradeLabel(selectedGradeKey)}: ${sortedSectionOptions.join(", ")}.`;
      } else {
        refs.reenrollAssignmentSectionHint.textContent = `No sections exist yet for ${gradeLabel(selectedGradeKey)}. Choose Add New Section to create one during enrollment.`;
      }
    }
  };

  const openReenrollAssignmentModal = () => {
    const selectedCandidates = getSelectedReenrollCandidates();
    if (!selectedCandidates.length) {
      setReenrollAlert("Select at least one student to enroll.", true);
      return;
    }

    const defaultGrade = getDefaultReenrollAssignmentGrade(selectedCandidates);
    const defaultSection = getDefaultReenrollAssignmentSection(selectedCandidates);

    if (refs.reenrollAssignmentTargetYear) refs.reenrollAssignmentTargetYear.textContent = getSelectedSchoolYear() || "-";
    if (refs.reenrollAssignmentSummary) {
      refs.reenrollAssignmentSummary.textContent = `${selectedCandidates.length} selected student(s) will be enrolled in ${getSelectedSchoolYear() || "the current school year"}.`;
    }
    if (refs.reenrollAssignmentGrade) {
      refs.reenrollAssignmentGrade.innerHTML = `<option value="">Select Grade Level</option>${gradeOptionsMarkup(defaultGrade)}`;
      refs.reenrollAssignmentGrade.value = defaultGrade;
    }
    setReenrollAssignmentAlert("");
    renderReenrollAssignmentSectionOptions(defaultSection);
    showModal("reenrollAssignModal");
  };

  const submitReenrollment = async (selectedStudents) => {
    const response = await api("/api/students/reenroll", {
      method: "POST",
      body: {
        source_school_year: state.reenroll.sourceSchoolYear,
        target_school_year: getSelectedSchoolYear(),
        students: selectedStudents,
      },
    });
    state.sectionsByGrade = response.sections_by_grade || state.sectionsByGrade || {};
    renderSections();
    renderAddSectionAssignments();
    renderReenrollCandidates();
    state.pagination.page = 1;
    await loadSections({ silent: true, force: true });
    await loadStudents({ force: true });
    await loadStudentStats({ silent: true, force: true });
    const activeSection = getActiveSectionSelection();
    if (activeSection.gradeKey && activeSection.section) {
      await loadSectionStats({ grade: gradeLabel(activeSection.gradeKey), section: activeSection.section, silent: true });
    }
    if ((Number.parseInt(response.skipped_count || 0, 10) || 0) > 0 || (response.errors || []).length) {
      closeModal("reenrollAssignModal");
      await loadReenrollCandidates({ silent: true, preserveFilters: true });
      setReenrollAlert(response.message || "Enrollment saved with some warnings.", true);
      showToast(response.message || "Enrollment saved with some warnings.", true, {
        title: "Re-enrollment Completed",
        duration: 10000,
        details: response.errors || [],
      });
      return response;
    }
    closeModal("reenrollAssignModal");
    closeModal("reenrollModal");
    showCenteredSuccess("Enrollment Saved");
    showToast(response.message || "Selected students enrolled successfully.");
    return response;
  };

  const resetReenrollFilters = () => {
    state.reenroll.filters.grade = "";
    state.reenroll.filters.section = "";
    if (refs.reenrollGradeFilter) refs.reenrollGradeFilter.value = "";
    if (refs.reenrollSectionFilter) refs.reenrollSectionFilter.value = "";
  };

  const buildReenrollVisibleEntries = () => {
    const selectedGradeKey = String(state.reenroll.filters.grade || "").trim();
    const selectedSection = String(state.reenroll.filters.section || "").trim();
    return state.reenroll.candidates
      .map((candidate, index) => ({ candidate, index }))
      .filter(({ candidate }) => {
        if (candidate.already_enrolled) return false;
        if (selectedGradeKey && gradeKey(candidate.grade_level) !== selectedGradeKey) return false;
        if (selectedSection && String(candidate.section || "").trim() !== selectedSection) return false;
        return true;
      });
  };

  const renderReenrollFilterOptions = () => {
    if (!refs.reenrollGradeFilter || !refs.reenrollSectionFilter) return;

    const gradeGroups = {};
    state.reenroll.candidates.forEach((candidate) => {
      const currentGradeKey = gradeKey(candidate.grade_level);
      const currentSection = String(candidate.section || "").trim();
      if (!currentGradeKey || !currentSection) return;
      if (!gradeGroups[currentGradeKey]) gradeGroups[currentGradeKey] = new Set();
      gradeGroups[currentGradeKey].add(currentSection);
    });

    const gradeKeys = Object.keys(gradeGroups).sort((left, right) => {
      const leftNumeric = Number.parseInt(left, 10);
      const rightNumeric = Number.parseInt(right, 10);
      if (Number.isFinite(leftNumeric) && Number.isFinite(rightNumeric)) return leftNumeric - rightNumeric;
      return left.localeCompare(right, undefined, { numeric: true, sensitivity: "base" });
    });
    const selectedGradeKey = String(state.reenroll.filters.grade || "").trim();
    const validGradeKey = gradeKeys.includes(selectedGradeKey) ? selectedGradeKey : "";
    state.reenroll.filters.grade = validGradeKey;

    refs.reenrollGradeFilter.innerHTML = `<option value="">Select Grade Level</option>${gradeKeys.map((currentGradeKey) => `<option value="${esc(currentGradeKey)}">${esc(gradeLabel(currentGradeKey))}</option>`).join("")}`;
    refs.reenrollGradeFilter.value = validGradeKey;
    refs.reenrollGradeFilter.disabled = gradeKeys.length === 0 || state.reenroll.loading || state.reenroll.submitting;

    const sectionOptions = validGradeKey
      ? Array.from(gradeGroups[validGradeKey] || []).sort((left, right) => left.localeCompare(right, undefined, { sensitivity: "base" }))
      : [];
    const selectedSection = String(state.reenroll.filters.section || "").trim();
    const validSection = sectionOptions.includes(selectedSection) ? selectedSection : "";
    state.reenroll.filters.section = validSection;

    refs.reenrollSectionFilter.innerHTML = `<option value="">Select Section</option>${sectionOptions.map((section) => `<option value="${esc(section)}">${esc(section)}</option>`).join("")}`;
    refs.reenrollSectionFilter.value = validSection;
    refs.reenrollSectionFilter.disabled = !validGradeKey || sectionOptions.length === 0 || state.reenroll.loading || state.reenroll.submitting;
  };

  const updateReenrollSelectionSummary = () => {
    const totalEligible = state.reenroll.candidates.filter((item) => !item.already_enrolled).length;
    const totalSelected = state.reenroll.candidates.filter((item) => item.selected && !item.already_enrolled).length;
    const visibleEntries = buildReenrollVisibleEntries();
    const visibleCandidates = visibleEntries.map((entry) => entry.candidate);
    const selectedGradeKey = String(state.reenroll.filters.grade || "").trim();
    const selectedSection = String(state.reenroll.filters.section || "").trim();
    const visibleSelectable = visibleCandidates.filter((item) => !item.already_enrolled).length;
    const visibleSelected = visibleCandidates.filter((item) => item.selected && !item.already_enrolled).length;
    if (refs.reenrollSelectionSummary) {
      if (!state.reenroll.candidates.length) {
        refs.reenrollSelectionSummary.textContent = "No students loaded yet.";
      } else if (totalEligible === 0) {
        refs.reenrollSelectionSummary.textContent = "All students in this source school year are already enrolled.";
      } else if (!selectedGradeKey) {
        refs.reenrollSelectionSummary.textContent = "Choose a grade level to display students.";
      } else if (!selectedSection) {
        refs.reenrollSelectionSummary.textContent = "Choose a section to display students.";
      } else {
        refs.reenrollSelectionSummary.textContent = `${visibleSelected} of ${visibleSelectable} visible student(s) selected. Total queued: ${totalSelected}.`;
      }
    }
    if (refs.reenrollSelectAll) {
      const allVisibleSelected = visibleSelectable > 0 && visibleSelected === visibleSelectable;
      const canToggleVisible = !state.reenroll.loading && !state.reenroll.submitting && Boolean(selectedGradeKey) && Boolean(selectedSection) && visibleSelectable > 0;
      refs.reenrollSelectAll.disabled = !canToggleVisible;
      refs.reenrollSelectAll.textContent = allVisibleSelected ? "Unselect All" : "Select All";
      refs.reenrollSelectAll.setAttribute("aria-pressed", allVisibleSelected ? "true" : "false");
      refs.reenrollSelectAll.className = `inline-flex w-full items-center justify-center rounded-xl border px-3 py-2 text-xs font-semibold uppercase tracking-[0.14em] transition disabled:cursor-not-allowed disabled:opacity-60 sm:w-auto ${
        allVisibleSelected
          ? "border-slate-300 bg-slate-900 text-white hover:bg-slate-800"
          : "border-emerald-200 bg-white text-emerald-700 hover:bg-emerald-50"
      }`;
    }
    if (refs.reenrollSubmitBtn) refs.reenrollSubmitBtn.disabled = totalSelected === 0 || state.reenroll.submitting;
  };

  const renderReenrollCandidates = () => {
    if (!refs.reenrollCandidates) return;
    renderReenrollFilterOptions();
    if (state.reenroll.loading) {
      refs.reenrollCandidates.innerHTML = '<div class="rounded-xl border border-dashed border-slate-300 bg-white px-4 py-8 text-center text-sm text-slate-500">Loading source year students...</div>';
      updateReenrollSelectionSummary();
      return;
    }
    if (!state.reenroll.candidates.length) {
      refs.reenrollCandidates.innerHTML = '<div class="rounded-xl border border-dashed border-slate-300 bg-white px-4 py-8 text-center text-sm text-slate-500">No students were found for the selected source school year.</div>';
      updateReenrollSelectionSummary();
      return;
    }
    if (!state.reenroll.filters.grade) {
      refs.reenrollCandidates.innerHTML = '<div class="rounded-xl border border-dashed border-slate-300 bg-white px-4 py-8 text-center text-sm text-slate-500">Select a grade level to display its sections and students.</div>';
      updateReenrollSelectionSummary();
      return;
    }
    if (!state.reenroll.filters.section) {
      refs.reenrollCandidates.innerHTML = '<div class="rounded-xl border border-dashed border-slate-300 bg-white px-4 py-8 text-center text-sm text-slate-500">Select a section to display the students assigned to it.</div>';
      updateReenrollSelectionSummary();
      return;
    }

    const visibleEntries = buildReenrollVisibleEntries();
    if (!visibleEntries.length) {
      refs.reenrollCandidates.innerHTML = '<div class="rounded-xl border border-dashed border-slate-300 bg-white px-4 py-8 text-center text-sm text-slate-500">No eligible students were found for the selected grade level and section.</div>';
      updateReenrollSelectionSummary();
      return;
    }

    refs.reenrollCandidates.innerHTML = `
      <div class="overflow-x-auto rounded-xl border border-slate-200 bg-white">
        <table class="min-w-full divide-y divide-slate-200">
          <thead class="bg-slate-100 text-slate-700">
            <tr class="text-xs uppercase tracking-[0.12em]">
              <th class="px-4 py-3 text-left">Select</th>
              <th class="px-4 py-3 text-left">LRN</th>
              <th class="px-4 py-3 text-left">Student Name</th>
              <th class="px-4 py-3 text-left">Grade</th>
              <th class="px-4 py-3 text-left">Section</th>
            </tr>
          </thead>
          <tbody class="divide-y divide-slate-100">
            ${visibleEntries.map(({ candidate, index }) => {
              const checkedAttr = candidate.selected ? " checked" : "";
              return `<tr class="hover:bg-slate-50" data-reenroll-index="${index}">
                <td class="px-4 py-3">
                  <input type="checkbox" class="h-4 w-4 rounded border-slate-300 text-emerald-600 focus:ring-emerald-500 reenroll-row-check" data-index="${index}"${checkedAttr}>
                </td>
                <td class="px-4 py-3 text-sm text-slate-700">${esc(candidate.lrn || candidate.student_id || "-")}</td>
                <td class="px-4 py-3 text-sm font-medium text-slate-900">${esc(candidate.name || "Unknown Student")}</td>
                <td class="px-4 py-3 text-sm text-slate-700">${esc(candidate.grade_level || "-")}</td>
                <td class="px-4 py-3 text-sm text-slate-700">${esc(candidate.section || "-")}</td>
              </tr>`;
            }).join("")}
          </tbody>
        </table>
      </div>`;
    updateReenrollSelectionSummary();
  };

  const loadReenrollCandidates = async ({ silent = false, preserveFilters = false } = {}) => {
    if (!refs.reenrollSourceYearSelect) return;
    const sourceSchoolYear = String(refs.reenrollSourceYearSelect.value || "").trim();
    state.reenroll.sourceSchoolYear = sourceSchoolYear;
    if (!sourceSchoolYear) {
      state.reenroll.candidates = [];
      resetReenrollFilters();
      renderReenrollCandidates();
      setReenrollAlert("Select a source school year first.", true);
      return;
    }

    state.reenroll.loading = true;
    if (refs.reloadReenrollCandidatesBtn) refs.reloadReenrollCandidatesBtn.disabled = true;
    setReenrollAlert("");
    renderReenrollCandidates();

    try {
      const params = new URLSearchParams();
      params.set("source_school_year", sourceSchoolYear);
      params.set("target_school_year", getSelectedSchoolYear());
      const data = await api(`/api/students/reenrollment-candidates?${params.toString()}`);
      state.sectionsByGrade = data.sections_by_grade || state.sectionsByGrade || {};
      renderSections();
      renderAddSectionAssignments();
      state.reenroll.candidates = Array.isArray(data.candidates)
        ? data.candidates.map((candidate) => ({
          ...candidate,
          selected: false,
          promoted_grade_level: candidate.promoted_grade_level || candidate.grade_level || "",
          target_section: "",
        }))
        : [];
      if (!preserveFilters) resetReenrollFilters();
      renderReenrollCandidates();
      if (!silent) {
        setReenrollAlert(`Loaded ${state.reenroll.candidates.length} student(s) from ${sourceSchoolYear}.`);
      }
    } catch (error) {
      state.reenroll.candidates = [];
      renderReenrollCandidates();
      setReenrollAlert(error.message, true);
      if (!silent) showToast(error.message, true);
    } finally {
      state.reenroll.loading = false;
      if (refs.reloadReenrollCandidatesBtn) refs.reloadReenrollCandidatesBtn.disabled = state.reenroll.submitting;
      renderReenrollCandidates();
    }
  };

  const collectReenrollSelections = () => state.reenroll.candidates
    .filter((candidate) => candidate.selected && !candidate.already_enrolled)
    .map((candidate) => ({
      record_id: candidate._id,
      selected: true,
      grade_level: String(candidate.promoted_grade_level || candidate.grade_level || "").trim(),
      section: String(candidate.target_section || "").trim(),
      status: "Active",
    }));

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

  const limitDetailMessages = (details, maxVisible = 6) => {
    const items = Array.isArray(details)
      ? details.map((item) => String(item || "").trim()).filter(Boolean)
      : [];
    if (items.length <= maxVisible) return items;
    const remaining = items.length - maxVisible;
    return [...items.slice(0, maxVisible), `And ${remaining} more row issue(s).`];
  };

  const hideToast = () => {
    if (!refs.toast) return;
    if (toastHideTimer) {
      clearTimeout(toastHideTimer);
      toastHideTimer = null;
    }
    refs.toast.classList.add("hidden");
  };

  const showToast = (message, isError = false, options = {}) => {
    if (!refs.toast) return;

    const text = String(message || "").trim();
    const title = String(options.title || (isError ? "Error" : "Success")).trim();
    const detailItems = limitDetailMessages(options.details, options.maxDetails || 6);
    const parsedDuration = Number.parseInt(options.duration, 10);
    const duration = Number.isFinite(parsedDuration) && parsedDuration > 0
      ? parsedDuration
      : (isError ? 10000 : 2800);

    if (refs.toastTitle) refs.toastTitle.textContent = title;
    if (refs.toastMessage) refs.toastMessage.textContent = text;

    if (refs.toastDetails) {
      refs.toastDetails.innerHTML = detailItems.map((item) => `<li>${esc(item)}</li>`).join("");
      refs.toastDetails.classList.toggle("hidden", detailItems.length === 0);
    }

    refs.toast.setAttribute("role", isError ? "alert" : "status");
    refs.toast.setAttribute("aria-live", isError ? "assertive" : "polite");
    refs.toast.className = `fixed top-5 right-5 z-[100] w-full max-w-lg rounded-2xl border shadow-xl ${isError ? "border-rose-200 bg-rose-50 text-rose-700" : "border-emerald-200 bg-emerald-50 text-emerald-700"}`;
    refs.toast.classList.remove("hidden");

    if (toastHideTimer) clearTimeout(toastHideTimer);
    toastHideTimer = setTimeout(() => {
      toastHideTimer = null;
      hideToast();
    }, duration);
  };

  const setAddImportSummary = (message, isError = false, details = []) => {
    if (!refs.addImportSummary) return;
    const text = String(message || "").trim();
    const detailItems = limitDetailMessages(details, 8);
    const detailsMarkup = detailItems.length
      ? `<ul class="mt-2 list-disc space-y-1 pl-5 text-[11px] leading-5">${detailItems.map((item) => `<li>${esc(item)}</li>`).join("")}</ul>`
      : "";
    refs.addImportSummary.innerHTML = text
      ? `<p class="leading-5">${esc(text)}</p>${detailsMarkup}`
      : "";
    refs.addImportSummary.className = `mt-3 rounded-xl border px-3 py-2 text-xs ${isError ? "border-rose-200 bg-rose-50 text-rose-700" : "border-emerald-200 bg-emerald-50 text-emerald-700"}`;
    refs.addImportSummary.classList.toggle("hidden", !text);
  };

  const addFormFieldElements = () => ({
    lrn: refs.addForm?.elements?.lrn || null,
    name: refs.addForm?.elements?.name || null,
    section: refs.addSectionSelect || null,
    parent_contact: refs.addForm?.elements?.parent_contact || null,
    gender: refs.addForm?.elements?.gender || null,
  });

  const addFormErrorElements = {
    lrn: refs.addLrnError,
    name: refs.addNameError,
    section: refs.addSectionError,
    parent_contact: refs.addParentContactError,
    gender: refs.addGenderError,
  };

  const clearAddFormAlert = () => {
    if (!refs.addFormAlert) return;
    refs.addFormAlert.textContent = "";
    refs.addFormAlert.classList.add("hidden");
  };

  const setAddFormAlert = (message) => {
    if (!refs.addFormAlert) return;
    const text = String(message || "").trim();
    refs.addFormAlert.textContent = text;
    refs.addFormAlert.classList.toggle("hidden", !text);
  };

  const setAddFieldError = (fieldKey, message = "") => {
    const errors = addFormErrorElements;
    const fields = addFormFieldElements();
    const field = fields[fieldKey];
    const errorNode = errors[fieldKey];
    const text = String(message || "").trim();
    const hasError = Boolean(text);

    if (errorNode) {
      errorNode.textContent = text;
      errorNode.classList.toggle("hidden", !hasError);
    }

    if (field) {
      field.classList.toggle("border-rose-400", hasError);
      field.classList.toggle("bg-rose-50", hasError);
      field.setAttribute("aria-invalid", hasError ? "true" : "false");
    }
  };

  const clearAddFormValidation = () => {
    ["lrn", "name", "section", "parent_contact", "gender"].forEach((key) => setAddFieldError(key, ""));
    clearAddFormAlert();
  };

  const setAddTabButtonState = (button, isActive) => {
    if (!button) return;
    button.classList.toggle("bg-white", isActive);
    button.classList.toggle("text-slate-900", isActive);
    button.classList.toggle("shadow-sm", isActive);
    button.classList.toggle("text-slate-600", !isActive);
    button.classList.toggle("hover:text-slate-900", !isActive);
  };

  const switchAddModalView = (view) => {
    const targetView = view === ADD_MODAL_IMPORT_VIEW ? ADD_MODAL_IMPORT_VIEW : ADD_MODAL_MANUAL_VIEW;
    state.addModalView = targetView;

    const showManual = targetView === ADD_MODAL_MANUAL_VIEW;
    if (refs.addManualPanel) refs.addManualPanel.classList.toggle("hidden", !showManual);
    if (refs.addImportPanel) refs.addImportPanel.classList.toggle("hidden", showManual);
    setAddTabButtonState(refs.addManualTabBtn, showManual);
    setAddTabButtonState(refs.addImportTabBtn, !showManual);
  };

  const setAddFormSubmitting = (isSubmitting) => {
    if (refs.addFormSubmitBtn) refs.addFormSubmitBtn.disabled = Boolean(isSubmitting);
    if (refs.addFormSubmitLabel) refs.addFormSubmitLabel.textContent = isSubmitting ? "Saving..." : "Save Student";
  };

  const validateAddForm = ({ focusFirst = false, showAlert = false } = {}) => {
    syncAddSectionAssignment();
    const payload = formPayload(refs.addForm);
    const issues = {};
    const fieldOrder = ["lrn", "name", "section", "gender", "parent_contact"];
    const fields = addFormFieldElements();

    if (!payload.lrn) {
      issues.lrn = "LRN is required.";
    } else if (!/^[A-Za-z0-9_-]+$/.test(payload.lrn)) {
      issues.lrn = "LRN may contain only letters, numbers, dashes, and underscores.";
    }

    if (!payload.name) {
      issues.name = "Name is required.";
    }

    if (!payload.section || !payload.grade_level) {
      issues.section = "Please select a section assignment.";
    }

    if (!payload.gender) {
      issues.gender = "Sex / Gender is required.";
    }

    const normalizedContact = normalizeParentContactInput(payload.parent_contact, true);
    if (fields.parent_contact) fields.parent_contact.value = normalizedContact;
    payload.parent_contact = normalizedContact === PH_CONTACT_PREFIX ? "" : normalizedContact;
    if (!isValidParentContact(payload.parent_contact)) {
      issues.parent_contact = "Parent contact must be in +639XXXXXXXXX format.";
    }

    fieldOrder.forEach((fieldKey) => setAddFieldError(fieldKey, issues[fieldKey] || ""));
    if (showAlert) {
      setAddFormAlert(Object.keys(issues).length ? "Please review the highlighted fields before saving." : "");
    } else {
      clearAddFormAlert();
    }

    if (focusFirst) {
      const firstFieldKey = fieldOrder.find((fieldKey) => Boolean(issues[fieldKey]));
      const firstField = firstFieldKey ? fields[firstFieldKey] : null;
      if (firstField && typeof firstField.focus === "function") firstField.focus();
    }

    return {
      valid: Object.keys(issues).length === 0,
      payload,
      issues,
    };
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
      refs.sectionStatsSubtitle.textContent = SECTION_STATS_EMPTY_NOTE;
      refs.sectionStatsTotal.textContent = "0";
      refs.sectionStatsMale.textContent = "0";
      refs.sectionStatsFemale.textContent = "0";
      refs.sectionStatsNote.textContent = state.sectionStats.note || SECTION_STATS_EMPTY_NOTE;
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

  const clearSectionStats = (note = SECTION_STATS_EMPTY_NOTE) => {
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
      const error = new Error(data.message || "Request failed.");
      error.data = data;
      error.status = response.status;
      error.field = data.field || "";
      throw error;
    }
    return data;
  };

  const buildInlinePdfUrl = (url) => {
    try {
      const parsed = new URL(String(url || ""), window.location.origin);
      parsed.searchParams.set("disposition", "inline");
      return `${parsed.pathname}${parsed.search}${parsed.hash}`;
    } catch (_error) {
      return url;
    }
  };

  const getStudentsExportSectionsForGrade = (gradeValue) => {
    const key = gradeKey(gradeValue);
    return key && Array.isArray(state.sectionsByGrade[key]) ? state.sectionsByGrade[key] : [];
  };

  const clearStudentsExportStudentSelection = () => {
    state.exportPdf.selectedStudent = null;
  };

  const setStudentsExportStudentSelection = (student) => {
    if (!student || !student._id) {
      clearStudentsExportStudentSelection();
      return;
    }
    state.exportPdf.selectedStudent = {
      _id: String(student._id || "").trim(),
      student_id: String(student.student_id || student.lrn || "").trim(),
      lrn: String(student.lrn || student.student_id || "").trim(),
      name: String(student.name || "").trim(),
      grade_level: String(student.grade_level || student.grade || "").trim(),
      section: String(student.section || "").trim(),
    };
    state.exportPdf.studentQuery = state.exportPdf.selectedStudent.name
      ? `${state.exportPdf.selectedStudent.name} (${state.exportPdf.selectedStudent.student_id || state.exportPdf.selectedStudent.lrn || "No ID"})`
      : (state.exportPdf.selectedStudent.student_id || state.exportPdf.selectedStudent.lrn || "");
    if (refs.studentsExportStudentSearch) refs.studentsExportStudentSearch.value = state.exportPdf.studentQuery;
  };

  const renderStudentsExportSectionOptions = () => {
    if (!refs.studentsExportSection) return;
    const sections = getStudentsExportSectionsForGrade(state.exportPdf.sectionGrade);
    const currentValue = String(state.exportPdf.section || "").trim();
    if (currentValue && !sections.includes(currentValue)) {
      state.exportPdf.section = "";
    }

    const options = ['<option value="">Select Section</option>'];
    sections.forEach((section) => {
      const selected = state.exportPdf.section === section ? " selected" : "";
      options.push(`<option value="${esc(section)}"${selected}>${esc(section)}</option>`);
    });
    refs.studentsExportSection.innerHTML = options.join("");
    refs.studentsExportSection.disabled = sections.length === 0;
  };

  const renderStudentsExportStudentSelection = () => {
    if (!refs.studentsExportStudentSelected) return;
    const student = state.exportPdf.selectedStudent;
    if (!student || !student._id) {
      refs.studentsExportStudentSelected.classList.add("hidden");
      refs.studentsExportStudentSelected.innerHTML = "";
      return;
    }
    refs.studentsExportStudentSelected.classList.remove("hidden");
    refs.studentsExportStudentSelected.innerHTML = `
      <div class="flex items-start justify-between gap-3">
        <div class="min-w-0">
          <p class="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">Selected Student</p>
          <p class="mt-1 text-sm font-semibold text-slate-900">${esc(student.name || student.student_id || "Selected student")}</p>
          <p class="mt-1 text-xs text-slate-500">${esc(student.student_id || student.lrn || "No ID")} | ${esc(student.grade_level || "No Grade")} | ${esc(student.section || "No Section")}</p>
        </div>
        <button type="button" id="studentsExportClearStudentBtn" class="inline-flex h-8 shrink-0 items-center justify-center rounded-lg border border-slate-300 bg-white px-3 text-xs font-semibold text-slate-700 transition hover:bg-slate-100">
          Change
        </button>
      </div>`;
  };

  const renderStudentsExportStudentResults = () => {
    if (!refs.studentsExportStudentResults) return;
    const query = String(state.exportPdf.studentQuery || "").trim();
    if (state.exportPdf.selectedStudent?._id) {
      refs.studentsExportStudentResults.innerHTML = "";
      return;
    }
    if (state.exportPdf.studentLoading) {
      refs.studentsExportStudentResults.innerHTML = '<div class="rounded-2xl border border-slate-200 bg-slate-50 px-4 py-4 text-sm text-slate-500">Searching students...</div>';
      return;
    }
    if (query.length < 2) {
      refs.studentsExportStudentResults.innerHTML = "";
      return;
    }
    if (!state.exportPdf.studentResults.length) {
      refs.studentsExportStudentResults.innerHTML = '<div class="rounded-2xl border border-dashed border-slate-300 bg-slate-50 px-4 py-4 text-sm text-slate-500">No matching students found.</div>';
      return;
    }

    refs.studentsExportStudentResults.innerHTML = state.exportPdf.studentResults.map((student) => `
      <button type="button" class="students-export-student-result flex w-full items-start justify-between gap-3 rounded-2xl border border-slate-200 bg-white px-4 py-3 text-left transition hover:border-slate-400 hover:bg-slate-50" data-student-id="${esc(student._id)}">
        <div class="min-w-0">
          <p class="text-sm font-semibold text-slate-900">${esc(student.name || student.student_id || "Unnamed student")}</p>
          <p class="mt-1 text-xs text-slate-500">${esc(student.student_id || student.lrn || "No ID")} | ${esc(student.grade_level || "No Grade")} | ${esc(student.section || "No Section")}</p>
        </div>
        <span class="shrink-0 rounded-full border border-slate-200 px-2.5 py-1 text-[10px] font-semibold uppercase tracking-[0.16em] text-slate-600">Select</span>
      </button>`).join("");
  };

  const getStudentsExportValidationMessage = () => {
    const scope = String(state.exportPdf.scope || "grade").trim();
    if (scope === "grade") {
      return state.exportPdf.grade ? "" : "Select a grade level to continue.";
    }
    if (scope === "section") {
      if (!state.exportPdf.sectionGrade) return "Select a grade level for the section export.";
      return state.exportPdf.section ? "" : "Select a section to continue.";
    }
    if (scope === "student") {
      return state.exportPdf.selectedStudent?._id ? "" : "Search and select one student to continue.";
    }
    return "Choose a valid export type.";
  };

  const buildStudentsExportRequestUrl = ({ inline = false } = {}) => {
    const validationMessage = getStudentsExportValidationMessage();
    if (validationMessage) return "";

    const params = new URLSearchParams();
    const exportBaseUrl = String(refs.studentsExportBtn?.dataset.exportUrlBase || "/students/export_pdf").trim() || "/students/export_pdf";
    appendSelectedSchoolYearParam(params);
    params.set("scope", String(state.exportPdf.scope || "grade").trim());

    if (state.exportPdf.scope === "grade") {
      params.set("grade", state.exportPdf.grade);
    } else if (state.exportPdf.scope === "section") {
      params.set("grade", state.exportPdf.sectionGrade);
      params.set("section", state.exportPdf.section);
    } else if (state.exportPdf.scope === "student") {
      params.set("student_record_id", state.exportPdf.selectedStudent._id);
      if (state.exportPdf.selectedStudent.student_id) params.set("student_id", state.exportPdf.selectedStudent.student_id);
    }

    const url = `${exportBaseUrl}?${params.toString()}`;
    return inline ? buildInlinePdfUrl(url) : url;
  };

  const getStudentsExportSummaryText = () => {
    const scope = String(state.exportPdf.scope || "grade").trim();
    const validationMessage = getStudentsExportValidationMessage();
    if (validationMessage) return validationMessage;
    if (scope === "grade") {
      return `Export all student records for ${state.exportPdf.grade}.`;
    }
    if (scope === "section") {
      return `Export all student records for ${state.exportPdf.sectionGrade} - ${state.exportPdf.section}.`;
    }
    const student = state.exportPdf.selectedStudent;
    return `Export the individual student record for ${student?.name || student?.student_id || "the selected student"}.`;
  };

  const updateStudentsExportLink = () => {
    renderStudentsExportSectionOptions();
    renderStudentsExportStudentSelection();
    renderStudentsExportStudentResults();

    const scope = String(state.exportPdf.scope || "grade").trim();
    if (refs.studentsExportScopeGrade) refs.studentsExportScopeGrade.checked = scope === "grade";
    if (refs.studentsExportScopeSection) refs.studentsExportScopeSection.checked = scope === "section";
    if (refs.studentsExportScopeStudent) refs.studentsExportScopeStudent.checked = scope === "student";
    if (refs.studentsExportGradeFields) refs.studentsExportGradeFields.classList.toggle("hidden", scope !== "grade");
    if (refs.studentsExportSectionFields) refs.studentsExportSectionFields.classList.toggle("hidden", scope !== "section");
    if (refs.studentsExportStudentFields) refs.studentsExportStudentFields.classList.toggle("hidden", scope !== "student");

    if (refs.studentsExportGradeLevel) refs.studentsExportGradeLevel.value = state.exportPdf.grade || "";
    if (refs.studentsExportSectionGrade) refs.studentsExportSectionGrade.value = state.exportPdf.sectionGrade || "";
    if (refs.studentsExportStudentSearch && document.activeElement !== refs.studentsExportStudentSearch) {
      refs.studentsExportStudentSearch.value = state.exportPdf.studentQuery || "";
    }

    const validationMessage = getStudentsExportValidationMessage();
    const downloadUrl = buildStudentsExportRequestUrl();
    const printUrl = buildStudentsExportRequestUrl({ inline: true });

    if (refs.studentsExportSummary) refs.studentsExportSummary.textContent = getStudentsExportSummaryText();
    if (refs.studentsExportDownloadBtn) {
      refs.studentsExportDownloadBtn.disabled = Boolean(validationMessage);
      refs.studentsExportDownloadBtn.dataset.downloadUrl = downloadUrl;
    }
    if (refs.studentsExportPrintBtn) {
      refs.studentsExportPrintBtn.disabled = Boolean(validationMessage);
      refs.studentsExportPrintBtn.dataset.printUrl = printUrl;
    }
    if (refs.studentsExportStudentHint) {
      refs.studentsExportStudentHint.textContent = scope === "student"
        ? (state.exportPdf.selectedStudent?._id
          ? "Selected student is ready for export."
          : "Type at least 2 characters to search for a student in the selected school year.")
        : "Choose a valid export scope to enable the PDF actions.";
    }
  };

  const syncStudentsExportDefaults = () => {
    const activeSection = getActiveSectionSelection();
    const activeGradeLabel = String(state.filters.grade || (activeSection.gradeKey ? gradeLabel(activeSection.gradeKey) : "")).trim();
    const activeSectionValue = String(state.filters.section || activeSection.section || "").trim();

    if (!["grade", "section", "student"].includes(String(state.exportPdf.scope || "").trim())) {
      state.exportPdf.scope = "grade";
    }
    if (!state.exportPdf.grade) state.exportPdf.grade = activeGradeLabel;
    if (!state.exportPdf.sectionGrade) state.exportPdf.sectionGrade = activeGradeLabel;
    if (!state.exportPdf.section) state.exportPdf.section = activeSectionValue;
  };

  const loadStudentsExportSearchResults = async (queryText) => {
    const query = String(queryText || "").trim();
    const requestToken = state.exportPdf.studentSearchToken + 1;
    state.exportPdf.studentSearchToken = requestToken;

    if (query.length < 2) {
      state.exportPdf.studentLoading = false;
      state.exportPdf.studentResults = [];
      renderStudentsExportStudentResults();
      updateStudentsExportLink();
      return;
    }

    state.exportPdf.studentLoading = true;
    renderStudentsExportStudentResults();

    try {
      const params = new URLSearchParams();
      params.set("q", query);
      params.set("page", "1");
      params.set("limit", "20");
      appendSelectedSchoolYearParam(params);
      const data = await api(`/api/students?${params.toString()}`);
      if (requestToken !== state.exportPdf.studentSearchToken) return;
      state.exportPdf.studentResults = Array.isArray(data.students) ? data.students : [];
    } catch (error) {
      if (requestToken !== state.exportPdf.studentSearchToken) return;
      state.exportPdf.studentResults = [];
      showToast(error.message, true);
    } finally {
      if (requestToken === state.exportPdf.studentSearchToken) {
        state.exportPdf.studentLoading = false;
        renderStudentsExportStudentResults();
        updateStudentsExportLink();
      }
    }
  };

  const openStudentsExportModal = async () => {
    syncStudentsExportDefaults();
    renderStudentsExportSectionOptions();
    updateStudentsExportLink();
    showModal("studentsExportModal");
    if (state.exportPdf.scope === "grade" && refs.studentsExportGradeLevel) {
      refs.studentsExportGradeLevel.focus();
    } else if (state.exportPdf.scope === "section" && refs.studentsExportSectionGrade) {
      refs.studentsExportSectionGrade.focus();
    } else if (refs.studentsExportStudentSearch) {
      refs.studentsExportStudentSearch.focus();
    }
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
    const lastFocus = document.activeElement;
    state.lastFocus = lastFocus;
    state.modalStack = state.modalStack.filter((entry) => entry.id !== id);
    state.modalStack.push({ id, lastFocus });
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
    const stackIndex = state.modalStack.findIndex((entry) => entry.id === id);
    const [closedEntry] = stackIndex >= 0 ? state.modalStack.splice(stackIndex, 1) : [];
    const nextActiveModal = state.modalStack[state.modalStack.length - 1] || null;
    state.activeModal = nextActiveModal?.id || null;
    if (!nextActiveModal) {
      document.body.classList.remove("overflow-hidden");
    } else {
      document.body.classList.add("overflow-hidden");
    }
    const restoreTarget = closedEntry?.lastFocus || state.lastFocus;
    if (restoreTarget && typeof restoreTarget.focus === "function") restoreTarget.focus();
    if (id === "faceModal") resetFaceCaptureSession({ clearStudent: true });
    if (id === "deleteModal") state.deleteTarget = { id: "", label: "" };
    if (id === "addModal") {
      clearAddFormValidation();
      setAddFormSubmitting(false);
      switchAddModalView(ADD_MODAL_MANUAL_VIEW);
    }
    if (id === "schoolYearModal") {
      refs.schoolYearForm?.reset();
      setSchoolYearFormAlert("");
      setSchoolYearSubmitting(false);
    }
    if (id === "reenrollModal") {
      setReenrollAlert("");
      setReenrollSubmitting(false);
    }
    if (id === "reenrollAssignModal") {
      setReenrollAssignmentAlert("");
      if (refs.reenrollAssignmentForm) refs.reenrollAssignmentForm.reset();
      toggleReenrollAssignmentNewSectionInput(false);
      renderReenrollAssignmentSectionOptions();
    }
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
      const archivedView = isArchivedView();
      const statusText = student.face_registered ? "Registered" : "Not Registered";
      const statusClass = student.face_registered ? "bg-emerald-100 text-emerald-700" : "bg-rose-100 text-rose-700";
      const faceActionText = student.face_registered ? "Update Face" : "Register Face";
      const faceActionClass = student.face_registered ? "bg-blue-600 hover:bg-blue-700" : "bg-emerald-600 hover:bg-emerald-700";
      const faceMode = student.face_registered ? "update" : "register";

      const photo = student.profile_photo
        ? `<img src="${esc(student.profile_photo)}" alt="${esc(student.name)}" class="h-10 w-10 rounded-lg object-cover border border-slate-200">`
        : '<div class="h-10 w-10 rounded-lg border border-dashed border-slate-300 text-[10px] text-slate-400 flex items-center justify-center">No Photo</div>';

      const studentLrn = student.lrn || student.student_id || "";
      let actionsMarkup = '<span class="inline-flex rounded-full border border-slate-200 bg-slate-100 px-2.5 py-1 text-[11px] font-semibold text-slate-500">No Actions</span>';
      if (archivedView) {
        actionsMarkup = '<span class="inline-flex rounded-full border border-slate-200 bg-slate-100 px-2.5 py-1 text-[11px] font-semibold text-slate-500">Read Only Archive</span>';
      } else if (canManageStudents()) {
        actionsMarkup = `<div class="flex items-center justify-end gap-2">
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

            ${canRegisterFaces() ? `<button type="button" data-act="face" data-id="${esc(student._id)}" data-mode="${faceMode}" class="rounded-lg ${faceActionClass} px-2.5 py-1.5 text-xs font-semibold text-white">${faceActionText}</button>` : ""}
          </div>`;
      } else if (canRegisterFaces()) {
        actionsMarkup = `<div class="flex items-center justify-end">
            <button type="button" data-act="face" data-id="${esc(student._id)}" data-mode="${faceMode}" class="rounded-lg ${faceActionClass} px-2.5 py-1.5 text-xs font-semibold text-white">${faceActionText}</button>
          </div>`;
      }

      return `<tr class="hover:bg-slate-50" data-id="${esc(student._id)}">
        <td class="px-4 py-3">${photo}</td>
        <td class="px-4 py-3 text-sm font-medium">${esc(studentLrn)}</td>
        <td class="px-4 py-3 text-sm">${esc(student.name)}</td>
        <td class="px-4 py-3 text-sm">${esc(student.grade_level || "-")}</td>
        <td class="px-4 py-3 text-sm">${esc(student.section || "-")}</td>
        <td class="px-4 py-3 text-sm">${esc(student.parent_contact || "-")}</td>
        <td class="px-4 py-3"><span class="inline-flex rounded-full px-2.5 py-1 text-xs font-semibold ${statusClass}">${statusText}</span></td>
        <td class="px-4 py-3">
          ${actionsMarkup}
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
    const selectedGrade = getSelectedSectionGradeKey();
    const active = getActiveSectionSelection();

    if (refs.sectionsGradeSelect) {
      refs.sectionsGradeSelect.value = selectedGrade ? gradeLabel(selectedGrade) : "";
    }

    if (!selectedGrade) {
      if (refs.sectionsPanelTitle) refs.sectionsPanelTitle.textContent = "Select a year level";
      if (refs.sectionsCountBadge) refs.sectionsCountBadge.textContent = "0 Sections";
      refs.sectionsPanel.innerHTML = '<div class="w-full rounded-xl border border-dashed border-slate-300 bg-white px-3 py-4 text-center text-xs text-slate-500">Choose a year level to load its sections.</div>';
      return;
    }

    const sections = Array.isArray(state.sectionsByGrade[selectedGrade]) ? state.sectionsByGrade[selectedGrade] : [];
    if (refs.sectionsPanelTitle) refs.sectionsPanelTitle.textContent = `${gradeLabel(selectedGrade)} Sections`;
    if (refs.sectionsCountBadge) refs.sectionsCountBadge.textContent = `${sections.length} ${sections.length === 1 ? "Section" : "Sections"}`;

    if (!sections.length) {
      refs.sectionsPanel.innerHTML = `<div class="w-full rounded-xl border border-dashed border-slate-300 bg-white px-3 py-4 text-center text-xs text-slate-500">No sections available for ${esc(gradeLabel(selectedGrade))} yet.</div>`;
      return;
    }

    refs.sectionsPanel.innerHTML = sections.map((section) => {
      const isActiveSelection = active.gradeKey === selectedGrade && active.section === section;
      const buttonClass = isActiveSelection
        ? "border-emerald-600 bg-emerald-600 text-white shadow-sm"
        : "border-slate-300 bg-white text-slate-700 hover:border-emerald-300 hover:text-emerald-700";
      return `<button type="button" class="section-chip inline-flex items-center gap-1.5 rounded-lg border px-2.5 py-1.5 text-xs font-semibold transition ${buttonClass}" data-grade="${esc(selectedGrade)}" data-section="${esc(section)}" aria-pressed="${isActiveSelection ? "true" : "false"}">
        <span>${esc(section)}</span>
        <span class="text-[11px] font-medium ${isActiveSelection ? "text-emerald-100" : "text-slate-400"}">${esc(gradeLabel(selectedGrade))}</span>
      </button>`;
    }).join("");
  };

  const loadSectionStats = async ({ grade = "", section = "", silent = false } = {}) => {
    const gradeValue = String(grade || "").trim();
    const sectionValue = String(section || "").trim();
    if (!gradeValue || !sectionValue) {
      clearSectionStats();
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
      appendSelectedSchoolYearParam(params);
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
        const params = appendSelectedSchoolYearParam(new URLSearchParams());
        const data = await api(`/api/sections?${params.toString()}`);
        state.sectionsByGrade = data.sections_by_grade || {};
        syncSectionSelectionState();
        renderSections();
        updateStudentsExportLink();
        renderAddSectionAssignments();
        renderReenrollCandidates();
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
    updateStudentsExportLink();

    state.requests.students = (async () => {
      const params = new URLSearchParams();
      if (state.filters.q) params.set("q", state.filters.q);
      if (state.filters.grade) params.set("grade", state.filters.grade);
      if (state.filters.section) params.set("section", state.filters.section);
      if (state.filters.faceStatus) params.set("face_status", state.filters.faceStatus);
      params.set("page", String(state.pagination.page));
      params.set("limit", String(state.pagination.limit));
      appendSelectedSchoolYearParam(params);

      if (!silent) {
        refs.studentsTableBody.innerHTML = '<tr><td colspan="8" class="px-4 py-8 text-center text-slate-500 text-sm">Loading students...</td></tr>';
      }

      try {
        const data = await api(`/api/students?${params.toString()}`);
        state.students = Array.isArray(data.students) ? data.students : [];
        state.schoolYear.selected = String(data.school_year || getSelectedSchoolYear()).trim();
        state.schoolYear.archivedView = Boolean(data.archived_view);
        state.pagination.page = Number.parseInt(data.page || state.pagination.page, 10);
        state.pagination.limit = Number.parseInt(data.limit || state.pagination.limit, 10);
        state.pagination.total = Number.parseInt(data.total || 0, 10);
        state.pagination.pages = Number.parseInt(data.pages || 1, 10);
        renderReenrollSourceOptions();
        applySchoolYearViewState();
        updateStudentsExportLink();
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
        const params = appendSelectedSchoolYearParam(new URLSearchParams());
        const data = await api(`/api/students/stats?${params.toString()}`);
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
      const activeSection = getActiveSectionSelection();
      if (shouldReloadStudents && activeSection.gradeKey && activeSection.section) {
        await loadSectionStats({ grade: gradeLabel(activeSection.gradeKey), section: activeSection.section, silent: true });
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
    if (isArchivedView()) {
      showToast("Archived school years are read-only.", true);
      return;
    }
    try {
      const data = await api(`/api/students/${studentId}`);
      fillEditForm(data.student || {});
      showModal("editModal");
    } catch (error) {
      showToast(error.message, true);
    }
  };

  const openDeleteModal = (studentId, studentName) => {
    if (isArchivedView()) {
      showToast("Archived school years are read-only.", true);
      return;
    }
    state.deleteTarget = { id: studentId, label: studentName || "Selected student" };
    refs.deleteStudentLabel.textContent = state.deleteTarget.label;
    showModal("deleteModal");
  };

  const confirmDelete = async () => {
    if (!canManageStudents()) {
      showToast("Staff access is limited to face registration only.", true);
      return;
    }
    if (isArchivedView()) {
      showToast("Archived school years are read-only.", true);
      return;
    }
    if (!state.deleteTarget.id) return;
    try {
      await api(`/api/students/${state.deleteTarget.id}`, { method: "DELETE" });
      closeModal("deleteModal");
      showToast("Student deleted successfully.");
      if (state.pagination.page > 1 && state.students.length === 1) state.pagination.page -= 1;
      await loadSections();
      await loadStudents();
      await loadStudentStats({ silent: true });
      const activeSection = getActiveSectionSelection();
      if (activeSection.gradeKey && activeSection.section) {
        await loadSectionStats({ grade: gradeLabel(activeSection.gradeKey), section: activeSection.section, silent: true });
      }
    } catch (error) {
      showToast(error.message, true);
    }
  };

  const clampNumber = (value, min, max) => Math.min(max, Math.max(min, value));

  const normalizeFaceCaptureProfile = (value) => {
    const text = String(value || "standard").trim().toLowerCase();
    return text === "similar_faces" ? "similar_faces" : "standard";
  };

  const getFaceCaptureProfileConfig = () => FACE_CAPTURE_PROFILES[normalizeFaceCaptureProfile(state.face.captureProfile)] || FACE_CAPTURE_PROFILES.standard;
  const getFaceCaptureSteps = () => getFaceCaptureProfileConfig().steps;
  const getCompletedFaceCaptureCount = () => state.face.captures.reduce((count, capture) => (capture ? count + 1 : count), 0);
  const getCurrentFaceStepIndex = () => {
    const steps = getFaceCaptureSteps();
    for (let index = 0; index < steps.length; index += 1) {
      if (!state.face.captures[index]) return index;
    }
    return -1;
  };
  const getCurrentFaceStep = () => {
    const steps = getFaceCaptureSteps();
    const index = getCurrentFaceStepIndex();
    return index >= 0 ? steps[index] : null;
  };
  const getPreviousFaceCaptureMeta = (fromIndex) => {
    const startIndex = Math.min(Number(fromIndex || 0) - 1, state.face.captureMeta.length - 1);
    for (let index = startIndex; index >= 0; index -= 1) {
      if (state.face.captures[index] && state.face.captureMeta[index]) {
        return state.face.captureMeta[index];
      }
    }
    return null;
  };
  const normalizeFaceCaptureIndices = (indices, maxCount) => {
    if (!Array.isArray(indices)) return [];
    const seen = new Set();
    const normalized = [];
    indices.forEach((value) => {
      const index = Number.parseInt(value, 10);
      if (!Number.isInteger(index) || index < 0 || index >= maxCount || seen.has(index)) return;
      seen.add(index);
      normalized.push(index);
    });
    normalized.sort((left, right) => left - right);
    return normalized;
  };
  const describeInvalidFaceCapture = (reason) => {
    if (reason === "duplicate") return "Too similar to another accepted angle.";
    return "This angle failed server validation. Retake it with a clearer face view.";
  };

  const setGuideRingState = (stateKey = "idle") => {
    const ring = document.getElementById("faceGuideRing");
    if (ring) {
      ring.className = FACE_GUIDE_RING_CLASSES[stateKey] || FACE_GUIDE_RING_CLASSES.idle;
    }
  };

  const getGuideMetrics = (width, height) => ({
    cx: width / 2,
    cy: height / 2,
    rx: width * 0.24,
    ry: height * 0.36,
  });

  const getFaceBounds = (landmarks, width, height) => {
    let minX = width;
    let minY = height;
    let maxX = 0;
    let maxY = 0;
    landmarks.forEach((landmark) => {
      const x = clampNumber(landmark.x * width, 0, width);
      const y = clampNumber(landmark.y * height, 0, height);
      minX = Math.min(minX, x);
      minY = Math.min(minY, y);
      maxX = Math.max(maxX, x);
      maxY = Math.max(maxY, y);
    });
    return {
      minX,
      minY,
      maxX,
      maxY,
      width: Math.max(0, maxX - minX),
      height: Math.max(0, maxY - minY),
      centerX: (minX + maxX) / 2,
      centerY: (minY + maxY) / 2,
    };
  };

  const pointInsideGuide = (point, guide, padding = 0.92) => {
    const dx = (point.x - guide.cx) / (guide.rx * padding);
    const dy = (point.y - guide.cy) / (guide.ry * padding);
    return (dx * dx) + (dy * dy) <= 1;
  };

  const analyzeFaceGuideFit = (landmarks, width, height) => {
    const guide = getGuideMetrics(width, height);
    const bounds = getFaceBounds(landmarks, width, height);
    const keyPoints = [1, 10, 152, 234, 454, 33, 263].map((index) => ({
      x: landmarks[index].x * width,
      y: landmarks[index].y * height,
    }));
    const boundaryPoints = [keyPoints[1], keyPoints[2], keyPoints[3], keyPoints[4]];
    const corePoints = [keyPoints[0], keyPoints[5], keyPoints[6]];
    const centerInside = pointInsideGuide({ x: bounds.centerX, y: bounds.centerY }, guide, FACE_CAPTURE_GUIDE_PADDING);
    const insideCount = keyPoints.filter((point) => pointInsideGuide(point, guide, FACE_CAPTURE_GUIDE_PADDING)).length;
    const boundaryInsideCount = boundaryPoints.filter((point) => pointInsideGuide(point, guide, FACE_CAPTURE_GUIDE_PADDING)).length;
    const coreInsideCount = corePoints.filter((point) => pointInsideGuide(point, guide, FACE_CAPTURE_GUIDE_CORE_PADDING)).length;

    const faceHeightRatio = bounds.height / (guide.ry * 2);
    const faceWidthRatio = bounds.width / (guide.rx * 2);
    const visibleRatio = insideCount / Math.max(1, keyPoints.length);
    const boundaryRatio = boundaryInsideCount / Math.max(1, boundaryPoints.length);
    const sizeOk = faceHeightRatio >= FACE_CAPTURE_GUIDE_MIN_FACE_HEIGHT_RATIO
      && faceHeightRatio <= FACE_CAPTURE_GUIDE_MAX_FACE_HEIGHT_RATIO
      && faceWidthRatio >= FACE_CAPTURE_GUIDE_MIN_FACE_WIDTH_RATIO
      && faceWidthRatio <= FACE_CAPTURE_GUIDE_MAX_FACE_WIDTH_RATIO;
    const inside = sizeOk
      && centerInside
      && insideCount >= FACE_CAPTURE_GUIDE_MIN_POINTS
      && boundaryInsideCount >= FACE_CAPTURE_GUIDE_MIN_BOUNDARY_POINTS
      && visibleRatio >= 0.57
      && boundaryRatio >= 0.5
      && coreInsideCount >= 1;

    let reason = "Keep the face inside the oval guide.";
    if (faceHeightRatio < FACE_CAPTURE_GUIDE_MIN_FACE_HEIGHT_RATIO || faceWidthRatio < FACE_CAPTURE_GUIDE_MIN_FACE_WIDTH_RATIO) {
      reason = "Move a little closer so the face fills the oval guide.";
    } else if (faceHeightRatio > FACE_CAPTURE_GUIDE_MAX_FACE_HEIGHT_RATIO || faceWidthRatio > FACE_CAPTURE_GUIDE_MAX_FACE_WIDTH_RATIO) {
      reason = "Move slightly back so the full face stays inside the oval guide.";
    } else if (!centerInside) {
      reason = "Keep your face roughly centered inside the oval guide.";
    } else if (insideCount < FACE_CAPTURE_GUIDE_MIN_POINTS || boundaryInsideCount < FACE_CAPTURE_GUIDE_MIN_BOUNDARY_POINTS) {
      reason = "Keep more of the face visible inside the oval guide.";
    }

    return {
      inside,
      reason,
      bounds,
      centerInside,
      insideCount,
      boundaryInsideCount,
      coreInsideCount,
      visibleRatio,
      boundaryRatio,
      faceHeightRatio,
      faceWidthRatio,
    };
  };

  const calculateFacePose = (landmarks) => {
    const left = landmarks[234];
    const right = landmarks[454];
    const nose = landmarks[1];
    const leftEye = landmarks[33];
    const rightEye = landmarks[263];
    const mouthLeft = landmarks[61];
    const mouthRight = landmarks[291];
    const upperLip = landmarks[13];
    const lowerLip = landmarks[14];
    const faceWidth = Math.max(Math.abs(right.x - left.x), 0.0001);
    const mouthCenterY = (mouthLeft.y + mouthRight.y + upperLip.y + lowerLip.y) / 4;
    const mouthCenterX = (mouthLeft.x + mouthRight.x + upperLip.x + lowerLip.x) / 4;
    const centerX = (left.x + right.x) / 2;
    const eyeCenterY = (leftEye.y + rightEye.y) / 2;
    const eyeWidth = Math.max(Math.abs(rightEye.x - leftEye.x), 0.0001);
    const eyeToMouthHeight = Math.max(Math.abs(mouthCenterY - eyeCenterY), 0.0001);

    return {
      yaw: (nose.x - centerX) / faceWidth,
      pitch: ((nose.y - eyeCenterY) / eyeToMouthHeight) - 0.62,
      roll: (rightEye.y - leftEye.y) / eyeWidth,
      faceWidth,
      faceHeight: eyeToMouthHeight,
      mouthOffsetX: (nose.x - mouthCenterX) / eyeWidth,
    };
  };

  const evaluateStep = (step, pose) => {
    if (!step || !pose) {
      return { aligned: false, score: 0, yawScore: 0, pitchScore: 0, rollScore: 0 };
    }
    const yawTolerance = Math.max(Number(step.yawTolerance || 0.1), 0.01);
    const pitchTolerance = Math.max(Number(step.pitchTolerance || 0.1), 0.01);
    const targetYaw = Number(step.targetYaw || 0);
    const targetPitch = Number(step.targetPitch || 0);
    const yawScore = Math.max(0, 1 - (Math.abs(pose.yaw - targetYaw) / yawTolerance));
    const pitchScore = Math.max(0, 1 - (Math.abs(pose.pitch - targetPitch) / pitchTolerance));
    const rollScore = Math.max(0, 1 - (Math.abs(pose.roll) / 0.22));
    const score = (yawScore * 0.45) + (pitchScore * 0.4) + (rollScore * 0.15);
    const aligned = score >= Number(step.minScore || 0.7) && rollScore >= 0.2;
    return { aligned, score, yawScore, pitchScore, rollScore };
  };

  const drawOverlay = (ctx, width, height, stateKey = "idle") => {
    if (ctx) ctx.clearRect(0, 0, width, height);
    setGuideRingState(stateKey);
  };

  const clearFaceStatusLock = () => {
    state.face.statusLockUntil = 0;
    state.face.statusLockMessage = "";
  };

  const setFaceStatus = (message, { holdMs = 0 } = {}) => {
    if (refs.faceStatus) refs.faceStatus.textContent = message;
    if (holdMs > 0) {
      state.face.statusLockUntil = Date.now() + holdMs;
      state.face.statusLockMessage = message;
    } else {
      clearFaceStatusLock();
    }
  };

  const getLockedFaceStatus = () => {
    if (state.face.statusLockUntil && Date.now() < state.face.statusLockUntil) {
      return state.face.statusLockMessage;
    }
    if (state.face.statusLockUntil) {
      clearFaceStatusLock();
    }
    return "";
  };

  const resetFaceAlignmentState = ({ clearMisses = true } = {}) => {
    state.face.alignFrames = 0;
    state.face.stableSince = 0;
    state.face.stableMode = "";
    if (clearMisses) state.face.missedFrames = 0;
  };

  const buildCaptureFingerprint = (canvas) => {
    const fingerprintCanvas = document.createElement("canvas");
    fingerprintCanvas.width = 8;
    fingerprintCanvas.height = 8;
    const fingerprintContext = fingerprintCanvas.getContext("2d");
    fingerprintContext.drawImage(canvas, 0, 0, 8, 8);
    const { data } = fingerprintContext.getImageData(0, 0, 8, 8);
    const grayscale = [];
    for (let index = 0; index < data.length; index += 4) {
      grayscale.push((data[index] * 0.299) + (data[index + 1] * 0.587) + (data[index + 2] * 0.114));
    }
    const average = grayscale.reduce((sum, value) => sum + value, 0) / Math.max(1, grayscale.length);
    return grayscale.map((value) => (value >= average ? "1" : "0")).join("");
  };

  const getHammingDistance = (left, right) => {
    if (!left || !right || left.length !== right.length) return Number.MAX_SAFE_INTEGER;
    let distance = 0;
    for (let index = 0; index < left.length; index += 1) {
      if (left[index] !== right[index]) distance += 1;
    }
    return distance;
  };

  const buildFaceCropCanvas = (video, landmarks, width, height) => {
    const bounds = getFaceBounds(landmarks, width, height);
    const baseSize = Math.max(bounds.width, bounds.height);
    if (!baseSize) return null;
    const cropSize = clampNumber(baseSize * 1.85, 220, Math.min(width, height));
    const centerX = clampNumber(bounds.centerX, cropSize / 2, width - (cropSize / 2));
    const centerY = clampNumber(bounds.centerY - (baseSize * 0.05), cropSize / 2, height - (cropSize / 2));
    const sx = clampNumber(centerX - (cropSize / 2), 0, Math.max(0, width - cropSize));
    const sy = clampNumber(centerY - (cropSize / 2), 0, Math.max(0, height - cropSize));
    const canvas = document.createElement("canvas");
    canvas.width = 384;
    canvas.height = 384;
    const context = canvas.getContext("2d");
    context.drawImage(video, sx, sy, cropSize, cropSize, 0, 0, canvas.width, canvas.height);
    return canvas;
  };

  const buildFaceDetectionFrame = (video) => {
    const width = Number(video?.videoWidth || 0);
    const height = Number(video?.videoHeight || 0);
    if (!width || !height || !refs.faceCaptureCanvas) {
      return video;
    }

    const targetWidth = Math.min(width, FACE_CAPTURE_DETECTION_MAX_WIDTH);
    const targetHeight = Math.max(1, Math.round((height / width) * targetWidth));
    if (refs.faceCaptureCanvas.width !== targetWidth) refs.faceCaptureCanvas.width = targetWidth;
    if (refs.faceCaptureCanvas.height !== targetHeight) refs.faceCaptureCanvas.height = targetHeight;

    const context = refs.faceCaptureCanvas.getContext("2d", { alpha: false, willReadFrequently: false });
    if (!context) {
      return video;
    }
    context.drawImage(video, 0, 0, targetWidth, targetHeight);
    return refs.faceCaptureCanvas;
  };

  const analyzeCaptureQuality = (canvas) => {
    const context = canvas.getContext("2d");
    const width = canvas.width;
    const height = canvas.height;
    const { data } = context.getImageData(0, 0, width, height);
    const grayscale = new Float32Array(width * height);
    let total = 0;
    let totalSquared = 0;

    for (let index = 0, pixel = 0; index < data.length; index += 4, pixel += 1) {
      const value = (data[index] * 0.299) + (data[index + 1] * 0.587) + (data[index + 2] * 0.114);
      grayscale[pixel] = value;
      total += value;
      totalSquared += value * value;
    }

    const count = grayscale.length || 1;
    const brightness = total / count;
    const variance = Math.max(0, (totalSquared / count) - (brightness * brightness));
    const contrast = Math.sqrt(variance);

    let sharpnessTotal = 0;
    let sharpnessCount = 0;
    for (let y = 1; y < height - 1; y += 1) {
      for (let x = 1; x < width - 1; x += 1) {
        const index = (y * width) + x;
        sharpnessTotal += Math.abs(grayscale[index] - grayscale[index + 1]) + Math.abs(grayscale[index] - grayscale[index + width]);
        sharpnessCount += 1;
      }
    }
    const sharpness = sharpnessCount ? sharpnessTotal / sharpnessCount : 0;

    let reason = "";
    if (brightness < FACE_CAPTURE_BRIGHTNESS_MIN && contrast < FACE_CAPTURE_CONTRAST_MIN * 1.6) {
      reason = "Increase lighting before capturing the next angle.";
    } else if (brightness > FACE_CAPTURE_BRIGHTNESS_MAX && contrast < FACE_CAPTURE_CONTRAST_MIN * 1.4) {
      reason = "Reduce glare or strong backlight before capturing.";
    } else if (contrast < FACE_CAPTURE_CONTRAST_MIN) {
      reason = "Add more contrast so the face is easier to separate from the background.";
    } else if (sharpness < FACE_CAPTURE_SHARPNESS_MIN) {
      reason = "Hold still for a sharper capture.";
    }

    if (!reason) {
      return { ok: true, blocking: false, brightness, contrast, sharpness };
    }

    const blocking = brightness < FACE_CAPTURE_BRIGHTNESS_MIN * 0.5
      || brightness > FACE_CAPTURE_BRIGHTNESS_MAX * 1.03
      || contrast < FACE_CAPTURE_CONTRAST_MIN * 0.46
      || sharpness < FACE_CAPTURE_SHARPNESS_MIN * 0.24;
    return { ok: false, blocking, reason, brightness, contrast, sharpness };
  };

  const renderFaceState = () => {
    const profile = getFaceCaptureProfileConfig();
    const steps = profile.steps;
    const completedCount = getCompletedFaceCaptureCount();
    const currentStepIndex = getCurrentFaceStepIndex();
    const registrationComplete = completedCount >= steps.length;

    if (refs.captureProgressText) refs.captureProgressText.textContent = `${completedCount} / ${steps.length} captures`;
    if (refs.submitFaceBtn) {
      refs.submitFaceBtn.disabled = completedCount < steps.length || state.face.submitting;
      refs.submitFaceBtn.textContent = state.face.submitting ? "Saving..." : "Save Face Registration";
    }
    if (refs.faceCaptureTarget) refs.faceCaptureTarget.textContent = profile.badgeText;

    if (refs.startCaptureBtn) {
      refs.startCaptureBtn.disabled = state.face.started || registrationComplete || state.face.submitting;
      refs.startCaptureBtn.textContent = registrationComplete ? "Registration Complete" : "Start Registration";
    }

    if (refs.resetCaptureBtn) {
      refs.resetCaptureBtn.disabled = state.face.submitting || (!state.face.started && completedCount === 0);
    }

    if (refs.faceCaptureProfileStandard) {
      refs.faceCaptureProfileStandard.checked = profile.key === "standard";
      refs.faceCaptureProfileStandard.disabled = state.face.started || completedCount > 0 || state.face.submitting;
    }
    if (refs.faceCaptureProfileSimilar) {
      refs.faceCaptureProfileSimilar.checked = profile.key === "similar_faces";
      refs.faceCaptureProfileSimilar.disabled = state.face.started || completedCount > 0 || state.face.submitting;
    }

    if (refs.stepTags) {
      refs.stepTags.innerHTML = steps.map((step, index) => {
        let toneClass = "border-slate-200 bg-slate-50 text-slate-500";
        const invalid = state.face.invalidIndices.includes(index);
        if (invalid) toneClass = "border-rose-200 bg-rose-50 text-rose-700";
        else if (state.face.captures[index]) toneClass = "border-emerald-200 bg-emerald-50 text-emerald-700";
        else if (index === currentStepIndex && !registrationComplete) toneClass = "border-sky-200 bg-sky-50 text-sky-700";
        return `<div class="rounded-2xl border ${toneClass} px-3 py-2">
            <p class="text-[10px] font-semibold uppercase tracking-[0.24em]">${index + 1}</p>
            <p class="mt-1 text-sm font-semibold text-slate-900">${esc(step.label)}</p>
            <p class="mt-1 text-[11px] ${invalid ? "text-rose-600" : "text-slate-500"}">${esc(invalid ? describeInvalidFaceCapture(state.face.invalidCaptureDetails[index]?.reason) : step.instruction)}</p>
          </div>`;
      }).join("");
    }

    if (refs.captureGrid) {
      refs.captureGrid.innerHTML = steps.map((step, index) => {
        const image = state.face.captures[index];
        const meta = state.face.captureMeta[index];
        const invalid = state.face.invalidIndices.includes(index);
        const invalidReason = describeInvalidFaceCapture(state.face.invalidCaptureDetails[index]?.reason);
        return image
          ? `<div class="overflow-hidden rounded-2xl border border-slate-200 bg-white">
              <img src="${image}" alt="${esc(step.label)}" class="h-24 w-full object-cover">
              <div class="px-2 py-2">
                <p class="text-[11px] font-semibold uppercase tracking-[0.22em] text-emerald-600">${index + 1}</p>
                <p class="mt-1 text-xs font-semibold text-slate-900">${esc(meta?.label || step.label)}</p>
              </div>
            </div>`
          : `<div class="flex h-24 flex-col items-center justify-center rounded-2xl border ${invalid ? "border-dashed border-rose-200 bg-rose-50 text-rose-600" : "border-dashed border-slate-200 bg-slate-50 text-slate-400"} px-2 text-center">
              <p class="text-[11px] font-semibold">${esc(step.label)}</p>
              <p class="mt-1 text-[10px] font-medium">${esc(invalid ? invalidReason : "Waiting for capture")}</p>
            </div>`;
      }).join("");
    }
  };

  const applyFaceValidationFailure = async (errorData) => {
    const steps = getFaceCaptureSteps();
    const invalidIndices = normalizeFaceCaptureIndices(errorData?.invalid_capture_indices, steps.length);
    if (!invalidIndices.length) {
      return false;
    }

    const invalidCaptureDetails = {};
    if (Array.isArray(errorData?.invalid_captures)) {
      errorData.invalid_captures.forEach((row) => {
        const index = Number.parseInt(row?.index, 10);
        if (!Number.isInteger(index) || index < 0 || index >= steps.length) return;
        invalidCaptureDetails[index] = {
          reason: String(row?.reason || "validation_failed").trim(),
          label: String(row?.label || steps[index]?.label || "").trim(),
        };
      });
    }

    invalidIndices.forEach((index) => {
      delete state.face.captures[index];
      delete state.face.captureMeta[index];
    });
    state.face.invalidIndices = invalidIndices;
    state.face.invalidCaptureDetails = invalidCaptureDetails;
    state.face.started = false;
    state.face.captureInFlight = false;
    state.face.cooldownUntil = 0;
    clearFaceStatusLock();
    resetFaceAlignmentState();
    renderFaceState();

    const currentStep = getCurrentFaceStep();
    const invalidLabels = invalidIndices.map((index) => steps[index]?.label || `Angle ${index + 1}`);
    refs.guideText.textContent = currentStep?.instruction || "Retake the highlighted angles.";
    setFaceStatus(
      `Retake ${invalidIndices.length} rejected angle${invalidIndices.length > 1 ? "s" : ""}. Next: ${currentStep?.label || invalidLabels[0] || "Continue"}.`,
      { holdMs: 1800 },
    );
    showToast(
      `Only the rejected angle${invalidIndices.length > 1 ? "s need" : " needs"} to be captured again.`,
      true,
      { title: "Retake Required", details: invalidLabels, duration: 7000 },
    );

    try {
      await startFaceCapture();
    } catch (_error) {
      return true;
    }
    return true;
  };

  const stopCameraTracks = () => {
    if (state.face.stream) {
      state.face.stream.getTracks().forEach((track) => track.stop());
      state.face.stream = null;
    }
    if (refs.faceVideo) refs.faceVideo.srcObject = null;
  };

  const scheduleFaceFrameProcessing = () => {
    if (!state.face.started || !state.face.mesh || !state.face.stream) {
      state.face.rafId = null;
      state.face.rafMode = "";
      return;
    }
    const video = refs.faceVideo;
    if (video && typeof video.requestVideoFrameCallback === "function") {
      state.face.rafMode = "video";
      state.face.rafId = video.requestVideoFrameCallback(() => {
        state.face.rafId = null;
        state.face.rafMode = "";
        processFaceFrame();
      });
      return;
    }
    state.face.rafMode = "raf";
    state.face.rafId = requestAnimationFrame(() => {
      state.face.rafId = null;
      state.face.rafMode = "";
      processFaceFrame();
    });
  };

  const stopFaceCapture = () => {
    if (state.face.rafId) {
      if (state.face.rafMode === "video" && refs.faceVideo && typeof refs.faceVideo.cancelVideoFrameCallback === "function") {
        refs.faceVideo.cancelVideoFrameCallback(state.face.rafId);
      } else {
        cancelAnimationFrame(state.face.rafId);
      }
      state.face.rafId = null;
    }
    state.face.rafMode = "";
    stopCameraTracks();
    state.face.mesh = null;
    state.face.lastResults = null;
    state.face.processing = false;
    state.face.captureInFlight = false;
    clearFaceStatusLock();
    resetFaceAlignmentState();
    if (refs.faceOverlay) {
      const overlayContext = refs.faceOverlay.getContext("2d");
      overlayContext?.clearRect(0, 0, refs.faceOverlay.width || 0, refs.faceOverlay.height || 0);
    }
    setGuideRingState("idle");
  };

  const resetFaceCaptureSession = ({ clearStudent = false } = {}) => {
    stopFaceCapture();
    state.face.captures = [];
    state.face.captureMeta = [];
    state.face.invalidIndices = [];
    state.face.invalidCaptureDetails = {};
    state.face.cooldownUntil = 0;
    state.face.started = false;
    state.face.submitting = false;
    state.face.captureInFlight = false;
    clearFaceStatusLock();
    resetFaceAlignmentState();
    if (clearStudent) {
      state.face.studentId = "";
      state.face.mode = "register";
      state.face.captureProfile = "standard";
    }
    if (refs.guideText) refs.guideText.textContent = "Press Start Registration to begin.";
    setFaceStatus("Choose a capture profile, then start. Capture happens automatically once a clear face is inside the oval.");
    renderFaceState();
  };

  const captureCurrentFrame = (step, landmarks, width, height, pose) => {
    const stepIndex = getCurrentFaceStepIndex();
    if (stepIndex < 0) {
      setFaceStatus("Capture sequence complete. Save face registration.");
      return false;
    }
    const cropCanvas = buildFaceCropCanvas(refs.faceVideo, landmarks, width, height);
    if (!cropCanvas) {
      resetFaceAlignmentState();
      state.face.cooldownUntil = Date.now() + FACE_CAPTURE_RETRY_COOLDOWN_MS;
      setFaceStatus("Move closer so the face fills the oval guide.", { holdMs: FACE_CAPTURE_STATUS_HOLD_MS });
      return false;
    }

    const quality = analyzeCaptureQuality(cropCanvas);
    if (quality.blocking) {
      resetFaceAlignmentState();
      state.face.cooldownUntil = Date.now() + FACE_CAPTURE_RETRY_COOLDOWN_MS;
      setFaceStatus(quality.reason, { holdMs: FACE_CAPTURE_STATUS_HOLD_MS });
      return false;
    }

    const fingerprint = buildCaptureFingerprint(cropCanvas);
    const duplicate = state.face.captureMeta.some((meta, index) => {
      if (!meta || index === stepIndex || !state.face.captures[index]) return false;
      const fingerprintDistance = getHammingDistance(meta.fingerprint, fingerprint);
      const yawDelta = Math.abs(Number(meta.yaw || 0) - Number(pose.yaw || 0));
      const pitchDelta = Math.abs(Number(meta.pitch || 0) - Number(pose.pitch || 0));
      return fingerprintDistance < FACE_CAPTURE_MIN_HAMMING_DISTANCE
        && yawDelta < FACE_CAPTURE_DUPLICATE_POSE_DELTA
        && pitchDelta < FACE_CAPTURE_DUPLICATE_POSE_DELTA;
    });
    if (duplicate) {
      resetFaceAlignmentState();
      state.face.cooldownUntil = Date.now() + FACE_CAPTURE_RETRY_COOLDOWN_MS;
      setFaceStatus("That angle is too similar. Shift to the next guided position.", { holdMs: FACE_CAPTURE_STATUS_HOLD_MS });
      return false;
    }

    state.face.captures[stepIndex] = cropCanvas.toDataURL("image/jpeg", 0.96);
    state.face.captureMeta[stepIndex] = {
      step_key: step.key,
      label: step.label,
      instruction: step.instruction,
      yaw: Number(pose.yaw.toFixed(4)),
      pitch: Number(pose.pitch.toFixed(4)),
      brightness: Number(quality.brightness.toFixed(2)),
      contrast: Number(quality.contrast.toFixed(2)),
      sharpness: Number(quality.sharpness.toFixed(2)),
      quality_ok: Boolean(quality.ok),
      fingerprint,
    };
    state.face.invalidIndices = state.face.invalidIndices.filter((index) => index !== stepIndex);
    if (Object.prototype.hasOwnProperty.call(state.face.invalidCaptureDetails, stepIndex)) {
      delete state.face.invalidCaptureDetails[stepIndex];
    }

    resetFaceAlignmentState();
    state.face.cooldownUntil = Date.now() + FACE_CAPTURE_SUCCESS_COOLDOWN_MS;
    const steps = getFaceCaptureSteps();
    const completedCount = getCompletedFaceCaptureCount();
    if (completedCount >= steps.length) {
      state.face.started = false;
      refs.guideText.textContent = "All required captures are complete.";
      setFaceStatus("Capture sequence complete. Save face registration.", { holdMs: FACE_CAPTURE_STATUS_HOLD_MS });
      stopFaceCapture();
    } else {
      const nextStep = getCurrentFaceStep();
      refs.guideText.textContent = nextStep?.instruction || "Continue with the guided sequence.";
      setFaceStatus(`Captured ${completedCount}/${steps.length}. Next: ${nextStep?.label || "Save registration"}.`, { holdMs: FACE_CAPTURE_STATUS_HOLD_MS });
    }
    renderFaceState();
    return true;
  };

  const processFaceFrame = async () => {
    if (!state.face.started || !state.face.mesh || !state.face.stream) {
      state.face.rafId = null;
      state.face.rafMode = "";
      return;
    }
    if (state.face.processing) {
      scheduleFaceFrameProcessing();
      return;
    }

    const video = refs.faceVideo;
    if (!video || video.readyState < 2) {
      scheduleFaceFrameProcessing();
      return;
    }

    try {
      state.face.processing = true;
      const detectionFrame = buildFaceDetectionFrame(video);
      await state.face.mesh.send({ image: detectionFrame });
      const results = state.face.lastResults || {};

      const width = video.videoWidth || video.clientWidth;
      const height = video.videoHeight || video.clientHeight;
      if (!width || !height) {
        return;
      }

      if (refs.faceOverlay.width !== width) refs.faceOverlay.width = width;
      if (refs.faceOverlay.height !== height) refs.faceOverlay.height = height;
      const context = refs.faceOverlay.getContext("2d");

      const step = getCurrentFaceStep();
      if (!step) {
        state.face.started = false;
        setFaceStatus("Capture sequence complete. Save face registration.");
        stopFaceCapture();
        renderFaceState();
        return;
      }

      refs.guideText.textContent = step.instruction;
      const currentStepIndex = getCurrentFaceStepIndex();

      const faces = results.multiFaceLandmarks || [];
      if (faces.length !== 1) {
        state.face.missedFrames += 1;
        if (faces.length > 1 || state.face.missedFrames > FACE_CAPTURE_MISSED_FRAME_TOLERANCE) {
          resetFaceAlignmentState({ clearMisses: false });
        }
        drawOverlay(context, width, height, faces.length > 1 ? "warning" : "idle");
        refs.faceStatus.textContent = faces.length > 1 ? "Keep only one face inside the oval guide." : "Waiting for one face inside the oval guide.";
        return;
      }

      state.face.missedFrames = 0;
      const landmarks = faces[0];
      const guideFit = analyzeFaceGuideFit(landmarks, width, height);
      if (!guideFit.inside) {
        resetFaceAlignmentState({ clearMisses: false });
        drawOverlay(context, width, height, "warning");
        refs.faceStatus.textContent = guideFit.reason;
        return;
      }

      const pose = calculateFacePose(landmarks);
      const alignment = evaluateStep(step, pose);
      const lastMeta = getPreviousFaceCaptureMeta(currentStepIndex);
      const yawDeltaFromLastCapture = Math.abs(Number(lastMeta?.yaw || 0) - Number(pose.yaw || 0));
      const pitchDeltaFromLastCapture = Math.abs(Number(lastMeta?.pitch || 0) - Number(pose.pitch || 0));
      const movedEnoughFromLastCapture = !lastMeta
        || alignment.aligned
        || yawDeltaFromLastCapture >= FACE_CAPTURE_TRANSITION_POSE_DELTA
        || pitchDeltaFromLastCapture >= FACE_CAPTURE_TRANSITION_POSE_DELTA;

      drawOverlay(context, width, height, alignment.aligned ? "aligned" : "detected");

      const softThreshold = Math.max((step.minScore || 0.7) - FACE_CAPTURE_SOFT_SCORE_DELTA, 0.36);
      const stableEnough = movedEnoughFromLastCapture
        && alignment.score >= softThreshold
        && alignment.yawScore >= 0.12
        && alignment.pitchScore >= 0.12
        && alignment.rollScore >= 0.04;
      const nowMs = Date.now();
      const cooldownActive = nowMs < state.face.cooldownUntil;

      if (!cooldownActive) {
        if (!stableEnough) {
          resetFaceAlignmentState({ clearMisses: false });
        } else {
          state.face.alignFrames += 1;
          if (!state.face.stableSince) {
            state.face.stableSince = nowMs;
          }
          state.face.stableMode = alignment.aligned ? "locked" : (state.face.stableMode || "soft");
        }
      }

      const lockedStable = state.face.stableMode === "locked" || alignment.aligned;
      const requiredStableFrames = lockedStable ? FACE_CAPTURE_STABLE_FRAME_TARGET : FACE_CAPTURE_SOFT_FRAME_TARGET;
      const requiredStableMs = lockedStable ? FACE_CAPTURE_LOCKED_HOLD_MS : FACE_CAPTURE_SOFT_HOLD_MS;
      const stableLongEnough = state.face.stableSince && (nowMs - state.face.stableSince) >= requiredStableMs;
      const captureReady = stableEnough
        && !cooldownActive
        && !state.face.captureInFlight
        && (
          alignment.aligned
          || (stableLongEnough && state.face.alignFrames >= requiredStableFrames)
        );
      if (captureReady) {
        state.face.captureInFlight = true;
        setFaceStatus(`Capturing ${step.label}...`, { holdMs: FACE_CAPTURE_STATUS_HOLD_MS });
        try {
          captureCurrentFrame(step, landmarks, width, height, pose);
        } finally {
          state.face.captureInFlight = false;
        }
        return;
      }

      const lockedStatus = getLockedFaceStatus();
      if (lockedStatus) {
        if (refs.faceStatus) refs.faceStatus.textContent = lockedStatus;
      } else if (!movedEnoughFromLastCapture) {
        if (refs.faceStatus) refs.faceStatus.textContent = `Shift a little more for ${step.label}.`;
      } else if (cooldownActive && stableEnough) {
        if (refs.faceStatus) refs.faceStatus.textContent = `${step.label} angle locked. Preparing auto-capture.`;
      } else if (stableEnough) {
        if (refs.faceStatus) refs.faceStatus.textContent = `${step.label} angle is clear. Hold briefly for auto-capture.`;
      } else {
        if (refs.faceStatus) refs.faceStatus.textContent = `Adjust for ${step.label}.`;
      }
    } catch (_error) {
      clearFaceStatusLock();
      if (refs.faceStatus) refs.faceStatus.textContent = "Face detection processing error.";
      setGuideRingState("warning");
    } finally {
      state.face.processing = false;
      state.face.captureInFlight = false;
      if (state.face.started && state.face.mesh && state.face.stream) {
        scheduleFaceFrameProcessing();
      } else {
        state.face.rafId = null;
        state.face.rafMode = "";
      }
    }
  };

  const startFaceCapture = async () => {
    if (state.face.started) return;
    const step = getCurrentFaceStep();
    if (!step) {
      setFaceStatus("Capture sequence is already complete.");
      return;
    }
    stopFaceCapture();
    state.face.lastResults = null;
    state.face.cooldownUntil = 0;
    state.face.started = true;
    state.face.captureInFlight = false;
    clearFaceStatusLock();
    resetFaceAlignmentState();
    renderFaceState();
    refs.guideText.textContent = step.instruction;
    setFaceStatus("Starting camera...");

    if (typeof FaceMesh === "undefined") {
      state.face.started = false;
      setFaceStatus("FaceMesh library failed to load.");
      renderFaceState();
      showToast("FaceMesh library failed to load.", true);
      return;
    }

    try {
      const constraints = {
        video: {
          width: { ideal: 960 },
          height: { ideal: 720 },
          frameRate: { ideal: 30, max: 30 },
          facingMode: "user",
        },
        audio: false,
      };
      state.face.stream = await navigator.mediaDevices.getUserMedia(constraints);
      const videoTrack = state.face.stream.getVideoTracks()[0];
      if (videoTrack && typeof videoTrack.applyConstraints === "function") {
        const capabilities = videoTrack.getCapabilities ? videoTrack.getCapabilities() : {};
        if (capabilities.focusMode && capabilities.focusMode.includes("continuous")) {
          await videoTrack.applyConstraints({ advanced: [{ focusMode: "continuous" }] });
        }
      }
      refs.faceVideo.srcObject = state.face.stream;
      await refs.faceVideo.play();

      state.face.mesh = new FaceMesh({ locateFile: (file) => `https://cdn.jsdelivr.net/npm/@mediapipe/face_mesh/${file}` });
      state.face.mesh.setOptions({
        maxNumFaces: 1,
        refineLandmarks: false,
        minDetectionConfidence: 0.4,
        minTrackingConfidence: 0.4,
      });
      state.face.mesh.onResults((results) => {
        state.face.lastResults = results || null;
      });

      setFaceStatus("Camera ready. Keep one clear face inside the oval guide for auto-capture.");
      setGuideRingState("idle");
      scheduleFaceFrameProcessing();
    } catch (_error) {
      state.face.started = false;
      stopFaceCapture();
      setFaceStatus("Unable to access camera. Check browser permissions.");
      renderFaceState();
      showToast("Unable to access camera.", true);
    }
  };

  const openFaceModal = async (studentId, mode) => {
    if (isArchivedView()) {
      showToast("Archived school years are read-only.", true);
      return;
    }
    try {
      const data = await api(`/api/students/${studentId}`);
      const student = data.student || {};
      resetFaceCaptureSession({ clearStudent: true });
      state.face.studentId = student.student_ref_id || studentId;
      if (!state.face.studentId) {
        showToast("Student profile was not found for face registration.", true);
        return;
      }
      state.face.mode = mode === "update" ? "update" : "register";

      refs.faceTitle.textContent = state.face.mode === "update" ? "Update Face Registration" : "Register Face";
      refs.faceSubtitle.textContent = `${student.name || ""} (${student.lrn || student.student_id || ""})`;
      refs.guideText.textContent = "Press Start Registration to begin.";
      setFaceStatus("Choose a capture profile, then start. Capture happens automatically once a clear face is inside the oval.");

      showModal("faceModal");
      renderFaceState();
    } catch (error) {
      showToast(error.message, true);
    }
  };

  const submitFaceRegistration = async () => {
    const steps = getFaceCaptureSteps();
    if (state.face.submitting) return;
    if (!state.face.studentId || getCompletedFaceCaptureCount() < steps.length) {
      showToast("Complete all required face captures first.", true);
      return;
    }

    const updateMode = state.face.mode === "update";
    const url = updateMode
      ? `/api/students/${state.face.studentId}/face/update`
      : `/api/students/${state.face.studentId}/face/register`;

    try {
      state.face.submitting = true;
      setFaceStatus("Saving face registration. Please wait...");
      renderFaceState();
      await api(url, {
        method: updateMode ? "PUT" : "POST",
        body: {
          faces: state.face.captures,
          capture_profile: state.face.captureProfile,
          capture_target: steps.length,
          capture_meta: state.face.captureMeta.map(({ fingerprint, ...meta }) => meta),
        },
      });
      closeModal("faceModal");
      if (updateMode) showFaceUpdateSuccessAnimation();
      showToast(updateMode ? "Face updated successfully." : "Face registered successfully.");
      void loadStudents();
    } catch (error) {
      const handled = await applyFaceValidationFailure(error.data);
      if (!handled) {
        showToast(error.message, true);
      }
    } finally {
      state.face.submitting = false;
      renderFaceState();
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
    const triggerStudentSearch = debounce(() => {
      loadStudents();
    }, 320);
    const triggerStudentsExportSearch = debounce(() => {
      loadStudentsExportSearchResults(state.exportPdf.studentQuery);
    }, 260);

    refs.schoolYearSelect?.addEventListener("change", () => {
      const selectedSchoolYear = String(refs.schoolYearSelect.value || "").trim();
      if (!selectedSchoolYear || selectedSchoolYear === getSelectedSchoolYear()) return;
      broadcastSchoolYearSelection(selectedSchoolYear);
      window.location.href = buildStudentsPageUrl(selectedSchoolYear);
    });

    refs.createSchoolYearBtn?.addEventListener("click", () => {
      if (!canManageStudents()) {
        showToast("Staff access is limited to face registration only.", true);
        return;
      }
      setSchoolYearFormAlert("");
      refs.schoolYearForm?.reset();
      if (refs.schoolYearLabel) refs.schoolYearLabel.value = suggestNextSchoolYearLabel();
      setSchoolYearSubmitting(false);
      showModal("schoolYearModal");
      refs.schoolYearLabel?.focus();
    });

    refs.schoolYearForm?.addEventListener("submit", async (event) => {
      event.preventDefault();
      if (!canManageStudents()) {
        setSchoolYearFormAlert("Staff access is limited to face registration only.");
        return;
      }
      const validation = validateSchoolYearLabel(refs.schoolYearLabel?.value || "");
      if (!validation.valid) {
        setSchoolYearFormAlert(validation.message);
        refs.schoolYearLabel?.focus();
        return;
      }

      setSchoolYearFormAlert("");
      setSchoolYearSubmitting(true);
      try {
        const response = await api("/api/school-years", {
          method: "POST",
          body: { school_year: validation.value },
        });
        closeModal("schoolYearModal");
        showToast(response.message || `School Year ${validation.value} created successfully.`);
        const nextSchoolYear = response.selected_school_year || validation.value;
        broadcastSchoolYearSelection(nextSchoolYear);
        window.location.href = buildStudentsPageUrl(nextSchoolYear);
      } catch (error) {
        setSchoolYearFormAlert(error.message);
      } finally {
        setSchoolYearSubmitting(false);
      }
    });

    refs.openReenrollBtn?.addEventListener("click", async () => {
      if (!canManageStudents()) {
        showToast("Staff access is limited to face registration only.", true);
        return;
      }
      if (isArchivedView()) {
        showToast("Archived school years are read-only.", true);
        return;
      }
      renderReenrollSourceOptions();
      state.reenroll.candidates = [];
      resetReenrollFilters();
      renderReenrollCandidates();
      setReenrollAlert("");
      setReenrollSubmitting(false);
      showModal("reenrollModal");
      if (!state.reenroll.sourceSchoolYear) {
        setReenrollAlert("Create or select a previous school year first.", true);
        return;
      }
      await loadReenrollCandidates({ silent: true });
    });

    refs.reloadReenrollCandidatesBtn?.addEventListener("click", () => {
      if (!canManageStudents()) {
        showToast("Staff access is limited to face registration only.", true);
        return;
      }
      loadReenrollCandidates();
    });

    refs.reenrollSourceYearSelect?.addEventListener("change", () => {
      resetReenrollFilters();
      loadReenrollCandidates();
    });

    refs.reenrollGradeFilter?.addEventListener("change", () => {
      state.reenroll.filters.grade = gradeKey(refs.reenrollGradeFilter.value);
      state.reenroll.filters.section = "";
      renderReenrollCandidates();
    });

    refs.reenrollSectionFilter?.addEventListener("change", () => {
      state.reenroll.filters.section = String(refs.reenrollSectionFilter.value || "").trim();
      renderReenrollCandidates();
    });

    refs.reenrollAssignmentGrade?.addEventListener("change", () => {
      setReenrollAssignmentAlert("");
      renderReenrollAssignmentSectionOptions("");
    });

    refs.reenrollAssignmentSection?.addEventListener("change", () => {
      const selectedValue = String(refs.reenrollAssignmentSection?.value || "").trim();
      toggleReenrollAssignmentNewSectionInput(selectedValue === REENROLL_ASSIGNMENT_NEW_SECTION_VALUE);
      setReenrollAssignmentAlert("");
    });

    refs.reenrollAssignmentSectionNew?.addEventListener("input", () => {
      setReenrollAssignmentAlert("");
    });

    refs.reenrollSelectAll?.addEventListener("click", () => {
      const visibleEntries = buildReenrollVisibleEntries();
      const visibleIndexes = new Set(visibleEntries.map((entry) => entry.index));
      const visibleSelectable = visibleEntries.filter((entry) => !entry.candidate.already_enrolled);
      if (!visibleSelectable.length) return;
      const shouldSelect = visibleSelectable.some((entry) => !entry.candidate.selected);
      state.reenroll.candidates = state.reenroll.candidates.map((candidate, index) => {
        if (!visibleIndexes.has(index) || candidate.already_enrolled) return candidate;
        return { ...candidate, selected: shouldSelect };
      });
      renderReenrollCandidates();
    });

    refs.reenrollCandidates?.addEventListener("change", (event) => {
      const index = Number.parseInt(event.target?.dataset?.index || "-1", 10);
      if (!Number.isInteger(index) || index < 0 || index >= state.reenroll.candidates.length) return;
      const currentCandidate = state.reenroll.candidates[index];
      if (!currentCandidate || currentCandidate.already_enrolled) return;

      if (event.target.classList.contains("reenroll-row-check")) {
        state.reenroll.candidates[index] = { ...currentCandidate, selected: Boolean(event.target.checked) };
        updateReenrollSelectionSummary();
        return;
      }
    });

    refs.reenrollSubmitBtn?.addEventListener("click", async () => {
      if (!canManageStudents()) {
        showToast("Staff access is limited to face registration only.", true);
        return;
      }
      if (isArchivedView()) {
        showToast("Archived school years are read-only.", true);
        return;
      }
      if (!getSelectedReenrollCandidates().length) {
        setReenrollAlert("Select at least one student to enroll.", true);
        return;
      }
      setReenrollAlert("");
      openReenrollAssignmentModal();
    });

    refs.reenrollAssignmentForm?.addEventListener("submit", async (event) => {
      event.preventDefault();
      if (!canManageStudents()) {
        setReenrollAssignmentAlert("Staff access is limited to face registration only.", true);
        return;
      }
      if (isArchivedView()) {
        showToast("Archived school years are read-only.", true);
        return;
      }

      const selectedCandidates = getSelectedReenrollCandidates();
      if (!selectedCandidates.length) {
        setReenrollAssignmentAlert("Select at least one student to enroll.", true);
        return;
      }

      const targetGrade = String(refs.reenrollAssignmentGrade?.value || "").trim();
      const targetSection = getReenrollAssignmentSectionValue();
      if (!targetGrade) {
        setReenrollAssignmentAlert("Please select the target grade level.", true);
        refs.reenrollAssignmentGrade?.focus();
        return;
      }
      if (!targetSection) {
        setReenrollAssignmentAlert("Please select or enter the target section.", true);
        if (String(refs.reenrollAssignmentSection?.value || "").trim() === REENROLL_ASSIGNMENT_NEW_SECTION_VALUE) {
          refs.reenrollAssignmentSectionNew?.focus();
        } else {
          refs.reenrollAssignmentSection?.focus();
        }
        return;
      }

      state.reenroll.candidates = state.reenroll.candidates.map((candidate) => {
        if (!candidate.selected || candidate.already_enrolled) return candidate;
        return {
          ...candidate,
          promoted_grade_level: targetGrade,
          target_section: targetSection,
        };
      });

      const selectedStudents = collectReenrollSelections();
      setReenrollAlert("");
      setReenrollAssignmentAlert("");
      setReenrollSubmitting(true);
      try {
        await submitReenrollment(selectedStudents);
      } catch (error) {
        setReenrollAlert(error.message, true);
        setReenrollAssignmentAlert(error.message, true);
        showToast(error.message, true);
      } finally {
        setReenrollSubmitting(false);
      }
    });

    refs.searchInput?.addEventListener("input", () => {
      state.filters.q = refs.searchInput.value.trim();
      state.pagination.page = 1;
      updateStudentsExportLink();
      triggerStudentSearch();
    });

    refs.faceRegistrationFilter?.addEventListener("change", () => {
      state.filters.faceStatus = String(refs.faceRegistrationFilter.value || "").trim();
      state.pagination.page = 1;
      updateStudentsExportLink();
      loadStudents();
    });

    refs.gradeFilter?.addEventListener("change", () => {
      state.filters.grade = refs.gradeFilter.value;
      state.pagination.page = 1;
      loadStudents();
    });

    refs.sectionsGradeSelect?.addEventListener("change", () => {
      const selectedGrade = gradeKey(refs.sectionsGradeSelect.value);
      setSelectedSectionGradeKey(selectedGrade);
      state.sectionControls.activeSection = "";
      syncStudentTableFiltersFromSectionSelection();
      state.pagination.page = 1;
      renderSections();
      clearSectionStats();
      loadStudents();
    });

    refs.sectionsPanel.addEventListener("click", (event) => {
      const chip = event.target.closest(".section-chip");
      if (!chip) return;
      const grade = String(chip.dataset.grade || "").trim();
      const section = String(chip.dataset.section || "").trim();
      if (!grade || !section) return;
      setActiveSectionSelection(grade, section);
      syncStudentTableFiltersFromSectionSelection();
      state.pagination.page = 1;
      renderSections();
      loadStudents();
      loadSectionStats({ grade: gradeLabel(grade), section });
    });

    refs.studentsExportBtn?.addEventListener("click", () => {
      openStudentsExportModal();
    });

    [refs.studentsExportScopeGrade, refs.studentsExportScopeSection, refs.studentsExportScopeStudent].forEach((input) => {
      input?.addEventListener("change", () => {
        if (!input.checked) return;
        state.exportPdf.scope = String(input.value || "grade").trim();
        updateStudentsExportLink();
      });
    });

    refs.studentsExportGradeLevel?.addEventListener("change", () => {
      state.exportPdf.grade = String(refs.studentsExportGradeLevel.value || "").trim();
      updateStudentsExportLink();
    });

    refs.studentsExportSectionGrade?.addEventListener("change", () => {
      state.exportPdf.sectionGrade = String(refs.studentsExportSectionGrade.value || "").trim();
      state.exportPdf.section = "";
      renderStudentsExportSectionOptions();
      updateStudentsExportLink();
    });

    refs.studentsExportSection?.addEventListener("change", () => {
      state.exportPdf.section = String(refs.studentsExportSection.value || "").trim();
      updateStudentsExportLink();
    });

    refs.studentsExportStudentSearch?.addEventListener("input", () => {
      const nextQuery = String(refs.studentsExportStudentSearch.value || "").trim();
      state.exportPdf.studentQuery = nextQuery;
      const selectedStudent = state.exportPdf.selectedStudent;
      const selectedDisplay = selectedStudent
        ? `${selectedStudent.name} (${selectedStudent.student_id || selectedStudent.lrn || "No ID"})`
        : "";
      if (!selectedStudent || (nextQuery !== selectedStudent.name && nextQuery !== selectedDisplay)) {
        clearStudentsExportStudentSelection();
      }
      updateStudentsExportLink();
      triggerStudentsExportSearch();
    });

    refs.studentsExportStudentResults?.addEventListener("click", (event) => {
      const button = event.target.closest(".students-export-student-result");
      if (!button) return;
      const studentId = String(button.dataset.studentId || "").trim();
      if (!studentId) return;
      const selectedStudent = state.exportPdf.studentResults.find((student) => String(student._id || "").trim() === studentId);
      if (!selectedStudent) return;
      setStudentsExportStudentSelection(selectedStudent);
      state.exportPdf.studentResults = [];
      updateStudentsExportLink();
    });

    refs.studentsExportStudentSelected?.addEventListener("click", (event) => {
      const clearButton = event.target.closest("#studentsExportClearStudentBtn");
      if (!clearButton) return;
      clearStudentsExportStudentSelection();
      state.exportPdf.studentQuery = "";
      state.exportPdf.studentResults = [];
      if (refs.studentsExportStudentSearch) {
        refs.studentsExportStudentSearch.value = "";
        refs.studentsExportStudentSearch.focus();
      }
      updateStudentsExportLink();
    });

    refs.studentsExportDownloadBtn?.addEventListener("click", () => {
      if (!refs.studentsExportDownloadBtn.disabled) closeModal("studentsExportModal");
    });

    refs.studentsExportPrintBtn?.addEventListener("click", () => {
      if (!refs.studentsExportPrintBtn.disabled) closeModal("studentsExportModal");
    });

    refs.openAddBtn.addEventListener("click", () => {
      if (!canManageStudents()) {
        showToast("Staff access is limited to face registration only.", true);
        return;
      }
      if (isArchivedView()) {
        showToast("Archived school years are read-only.", true);
        return;
      }
      refs.addForm.reset();
      if (refs.addForm?.elements?.parent_contact) {
        refs.addForm.elements.parent_contact.value = PH_CONTACT_PREFIX;
      }
      if (refs.addSectionSelect) refs.addSectionSelect.value = "";
      syncAddSectionAssignment();
      refs.addImportForm?.reset();
      setAddImportSummary("");
      clearAddFormValidation();
      setAddFormSubmitting(false);
      switchAddModalView(ADD_MODAL_MANUAL_VIEW);
      showModal("addModal");
      refs.addForm?.elements?.lrn?.focus();
    });

    refs.addManualTabBtn?.addEventListener("click", () => {
      switchAddModalView(ADD_MODAL_MANUAL_VIEW);
    });

    refs.addImportTabBtn?.addEventListener("click", () => {
      switchAddModalView(ADD_MODAL_IMPORT_VIEW);
    });

    refs.openAddSectionBtn?.addEventListener("click", () => {
      if (!canManageStudents()) {
        showToast("Staff access is limited to face registration only.", true);
        return;
      }
      if (isArchivedView()) {
        showToast("Archived school years are read-only.", true);
        return;
      }
      refs.addSectionForm?.reset();
      showModal("addSectionModal");
      refs.newSectionGrade?.focus();
    });

    refs.addSectionForm?.addEventListener("submit", async (event) => {
      event.preventDefault();
      if (!canManageStudents()) {
        showToast("Staff access is limited to face registration only.", true);
        return;
      }
      if (isArchivedView()) {
        showToast("Archived school years are read-only.", true);
        return;
      }
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
        await api("/api/sections", {
          method: "POST",
          body: { grade, section, school_year: getSelectedSchoolYear() },
        });
        refs.addSectionForm?.reset();
        closeModal("addSectionModal");
        showToast("Section saved successfully.");
        await loadSections();
      } catch (error) {
        showToast(error.message, true);
      }
    });

    refs.addSectionSelect?.addEventListener("change", () => {
      syncAddSectionAssignment();
      validateAddForm({ showAlert: false });
    });

    refs.addForm.elements.parent_contact?.addEventListener("input", () => {
      refs.addForm.elements.parent_contact.value = normalizeParentContactInput(
        refs.addForm.elements.parent_contact.value,
        true,
      );
      validateAddForm({ showAlert: false });
    });

    refs.addForm.elements.lrn?.addEventListener("input", () => {
      validateAddForm({ showAlert: false });
    });

    refs.addForm.elements.name?.addEventListener("input", () => {
      validateAddForm({ showAlert: false });
    });

    refs.addForm.elements.gender?.addEventListener("change", () => {
      validateAddForm({ showAlert: false });
    });

    refs.editForm.elements.parent_contact?.addEventListener("input", () => {
      refs.editForm.elements.parent_contact.value = normalizeParentContactInput(
        refs.editForm.elements.parent_contact.value,
        false,
      );
    });

    refs.addImportForm?.addEventListener("submit", async (event) => {
      event.preventDefault();
      if (!canManageStudents()) {
        showToast("Staff access is limited to face registration only.", true, { title: "Import Error" });
        return;
      }
      if (isArchivedView()) {
        showToast("Archived school years are read-only.", true, { title: "Import Error" });
        return;
      }
      const selectedFile = refs.addImportFile?.files?.[0];
      if (!selectedFile) {
        showToast("Please select an Excel (.xlsx) file first.", true, { title: "Import Error" });
        refs.addImportFile?.focus();
        return;
      }

      if (!/\.xlsx$/i.test(selectedFile.name || "")) {
        showToast("Only .xlsx files are supported.", true, { title: "Import Error" });
        refs.addImportFile.value = "";
        refs.addImportFile?.focus();
        return;
      }

      const formData = new FormData(refs.addImportForm);
      formData.set("file", selectedFile);
      formData.set("school_year", getSelectedSchoolYear());
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
        const errorDetails = Array.isArray(response.errors)
          ? response.errors.map((item) => String(item || "").trim()).filter(Boolean)
          : [];
        const hasImportIssues = duplicates > 0 || invalid > 0 || errorDetails.length > 0;

        const summary = [
          rowsRead > 0 ? `Rows read: ${rowsRead}.` : "",
          `Imported ${imported} student(s).`,
          `Skipped/failed ${skipped} row(s).`,
          duplicates > 0 ? `${duplicates} duplicate row(s) skipped.` : "",
          invalid > 0 ? `${invalid} invalid row(s) skipped.` : "",
          summarySkipped > 0 ? `${summarySkipped} summary row(s) skipped.` : "",
        ].filter(Boolean).join(" ");

        if (hasImportIssues) {
          const importErrorMessage = imported > 0
            ? "Some student rows were not imported. Review the duplicate or invalid entries below."
            : "The import could not be completed. Review the duplicate or invalid entries below.";
          showToast(importErrorMessage, true, {
            title: "Import Error",
            duration: 10000,
            details: errorDetails,
            maxDetails: 6,
          });
          setAddImportSummary(summary, true, errorDetails);
          switchAddModalView(ADD_MODAL_IMPORT_VIEW);
        } else {
          showToast(response.message || summary, false, { title: "Import Successful" });
          closeModal("addModal");
          showCenteredSuccess("Import Successful");
          refs.addForm?.reset();
          if (refs.addForm?.elements?.parent_contact) {
            refs.addForm.elements.parent_contact.value = PH_CONTACT_PREFIX;
          }
          if (refs.addSectionSelect) refs.addSectionSelect.value = "";
          syncAddSectionAssignment();
          refs.addImportForm?.reset();
          setAddImportSummary("");
        }

        state.pagination.page = 1;
        await loadSections();
        await loadStudents();
        await loadStudentStats({ silent: true });
        const activeSection = getActiveSectionSelection();
        if (activeSection.gradeKey && activeSection.section) {
          await loadSectionStats({ grade: gradeLabel(activeSection.gradeKey), section: activeSection.section, silent: true });
        }
      } catch (error) {
        showToast(error.message, true, { title: "Import Error" });
        setAddImportSummary(error.message, true);
      } finally {
        if (refs.addImportSubmitBtn) refs.addImportSubmitBtn.disabled = false;
      }
    });

    refs.addForm.addEventListener("submit", async (event) => {
      event.preventDefault();
      if (!canManageStudents()) {
        showToast("Staff access is limited to face registration only.", true);
        return;
      }
      if (isArchivedView()) {
        showToast("Archived school years are read-only.", true);
        return;
      }
      const validation = validateAddForm({ focusFirst: true, showAlert: true });
      if (!validation.valid) {
        const firstIssue = Object.values(validation.issues || {}).find(Boolean) || "Please complete all required fields.";
        showToast(firstIssue, true);
        return;
      }
      const payload = {
        ...validation.payload,
        school_year: getSelectedSchoolYear(),
      };
      setAddFormSubmitting(true);
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
        const activeSection = getActiveSectionSelection();
        if (activeSection.gradeKey && activeSection.section) {
          await loadSectionStats({ grade: gradeLabel(activeSection.gradeKey), section: activeSection.section, silent: true });
        }
      } catch (error) {
        showToast(error.message, true);
      } finally {
        setAddFormSubmitting(false);
      }
    });

    refs.editForm.addEventListener("submit", async (event) => {
      event.preventDefault();
      if (!canManageStudents()) {
        showToast("Staff access is limited to face registration only.", true);
        return;
      }
      if (isArchivedView()) {
        showToast("Archived school years are read-only.", true);
        return;
      }
      const studentId = refs.editForm.elements._id.value;
      if (!studentId) return;
      const payload = formPayload(refs.editForm);
      payload.parent_contact = normalizeParentContactInput(payload.parent_contact, false);
      payload.school_year = getSelectedSchoolYear();
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
        const activeSection = getActiveSectionSelection();
        if (activeSection.gradeKey && activeSection.section) {
          await loadSectionStats({ grade: gradeLabel(activeSection.gradeKey), section: activeSection.section, silent: true });
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
        if (action === "edit") {
          if (!canManageStudents()) {
            showToast("Staff access is limited to face registration only.", true);
            return;
          }
          openEditModal(studentId);
        }
        if (action === "delete") {
          if (!canManageStudents()) {
            showToast("Staff access is limited to face registration only.", true);
            return;
          }
          openDeleteModal(studentId, actionButton.dataset.name || "Selected student");
        }
        if (action === "face") {
          if (!canRegisterFaces()) {
            showToast("Face registration is not available for this account.", true);
            return;
          }
          openFaceModal(studentId, actionButton.dataset.mode);
        }
      }
    });

    refs.confirmDeleteBtn.addEventListener("click", confirmDelete);
    refs.toastCloseBtn?.addEventListener("click", hideToast);

    refs.paginationControls.addEventListener("click", (event) => {
      const button = event.target.closest("button[data-page]");
      if (!button || button.disabled) return;
      const page = Number.parseInt(button.dataset.page || "1", 10);
      if (Number.isNaN(page) || page < 1 || page > state.pagination.pages) return;
      state.pagination.page = page;
      loadStudents();
    });

    refs.faceCaptureProfileStandard?.addEventListener("change", () => {
      state.face.captureProfile = "standard";
      renderFaceState();
    });

    refs.faceCaptureProfileSimilar?.addEventListener("change", () => {
      state.face.captureProfile = "similar_faces";
      renderFaceState();
    });

    refs.startCaptureBtn?.addEventListener("click", startFaceCapture);

    refs.resetCaptureBtn?.addEventListener("click", () => {
      const studentId = state.face.studentId;
      const mode = state.face.mode;
      const captureProfile = state.face.captureProfile;
      resetFaceCaptureSession();
      state.face.studentId = studentId;
      state.face.mode = mode;
      state.face.captureProfile = captureProfile;
      renderFaceState();
    });

    refs.submitFaceBtn?.addEventListener("click", submitFaceRegistration);

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
  applySchoolYearViewState();
  renderReenrollSourceOptions();
  renderFaceState();
  renderSectionStats();
  switchAddModalView(ADD_MODAL_MANUAL_VIEW);
  clearAddFormValidation();
  setAddFormSubmitting(false);
  setSchoolYearSubmitting(false);
  setReenrollSubmitting(false);
  renderAddSectionAssignments();
  renderReenrollCandidates();
  syncAddSectionAssignment();
  updateStudentsExportLink();
  startRealtimeUpdates();
  loadSections();
  loadStudents();
  loadStudentStats();
})();
