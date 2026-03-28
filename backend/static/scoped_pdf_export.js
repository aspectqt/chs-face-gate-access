(function () {
  "use strict";

  function normalizeText(value) {
    return String(value || "").trim();
  }

  function gradeKey(value) {
    const match = normalizeText(value).match(/\d+/);
    return match ? match[0] : "";
  }

  function buildInlinePdfUrl(url) {
    try {
      const parsed = new URL(String(url || ""), window.location.origin);
      parsed.searchParams.set("disposition", "inline");
      return `${parsed.pathname}${parsed.search}${parsed.hash}`;
    } catch (_error) {
      return url;
    }
  }

  function fallbackNotify(instance, message, type = "error") {
    if (typeof instance.showToast === "function") {
      instance.showToast(message, type);
      return;
    }
    if (typeof instance.setToast === "function") {
      instance.setToast(message, type);
      return;
    }
    window.alert(String(message || "Unable to continue."));
  }

  function createScopedPdfExportController(config) {
    const options = config || {};
    const baseUrl = normalizeText(options.baseUrl) || "/";
    const baseQuery = normalizeText(options.baseQuery);
    const schoolYear = normalizeText(options.schoolYear);
    const gradeOptions = Array.isArray(options.gradeOptions) ? options.gradeOptions.map((value) => normalizeText(value)).filter(Boolean) : [];
    const sectionsByGrade = options.sectionsByGrade && typeof options.sectionsByGrade === "object" ? options.sectionsByGrade : {};
    const studentSearchUrl = normalizeText(options.studentSearchUrl) || "/api/students";
    const emptyResultsLabel = normalizeText(options.emptyResultsLabel) || "No matching students found.";

    return {
      pdfExportModalOpen: false,
      pdfExportScope: "grade",
      pdfExportGrade: "",
      pdfExportSectionGrade: "",
      pdfExportSection: "",
      pdfExportStudentQuery: "",
      pdfExportSelectedStudent: null,
      pdfExportStudentResults: [],
      pdfExportStudentLoading: false,
      pdfExportStudentSearchToken: 0,
      pdfExportBaseUrl: baseUrl,
      pdfExportBaseQuery: baseQuery,
      pdfExportSchoolYear: schoolYear,
      pdfExportGradeOptions: gradeOptions,
      pdfExportSectionsByGrade: sectionsByGrade,
      pdfExportEmptyResultsLabel: emptyResultsLabel,

      pdfExportOpenModal() {
        this.pdfExportModalOpen = true;
      },

      pdfExportCloseModal() {
        this.pdfExportModalOpen = false;
      },

      pdfExportSetScope(scope) {
        const nextScope = normalizeText(scope).toLowerCase();
        this.pdfExportScope = ["grade", "section", "student"].includes(nextScope) ? nextScope : "grade";
        if (this.pdfExportScope !== "student") {
          this.pdfExportStudentResults = [];
          this.pdfExportStudentLoading = false;
        }
        if (this.pdfExportScope !== "section") {
          this.pdfExportSection = "";
        }
      },

      pdfExportSectionsForGrade(gradeValue) {
        const key = gradeKey(gradeValue);
        return key && Array.isArray(this.pdfExportSectionsByGrade[key]) ? this.pdfExportSectionsByGrade[key] : [];
      },

      pdfExportRefreshSectionOptions() {
        const sections = this.pdfExportSectionsForGrade(this.pdfExportSectionGrade);
        if (this.pdfExportSection && !sections.includes(this.pdfExportSection)) {
          this.pdfExportSection = "";
        }
      },

      pdfExportValidationMessage() {
        if (this.pdfExportScope === "grade") {
          return this.pdfExportGrade ? "" : "Select a grade level to continue.";
        }
        if (this.pdfExportScope === "section") {
          if (!this.pdfExportSectionGrade) return "Select a grade level for the section export.";
          return this.pdfExportSection ? "" : "Select a section to continue.";
        }
        if (this.pdfExportScope === "student") {
          return this.pdfExportSelectedStudent && this.pdfExportSelectedStudent._id
            ? ""
            : "Search and select one student to continue.";
        }
        return "Choose a valid export type.";
      },

      pdfExportSummaryText() {
        const validationMessage = this.pdfExportValidationMessage();
        if (validationMessage) return validationMessage;
        if (this.pdfExportScope === "grade") {
          return `Export records for students in ${this.pdfExportGrade}.`;
        }
        if (this.pdfExportScope === "section") {
          return `Export records for ${this.pdfExportSectionGrade} - ${this.pdfExportSection}.`;
        }
        const student = this.pdfExportSelectedStudent || {};
        return `Export records for ${student.name || student.student_id || "the selected student"}.`;
      },

      pdfExportStudentHint() {
        if (this.pdfExportScope !== "student") {
          return "Choose a valid export scope to enable the PDF actions.";
        }
        if (this.pdfExportSelectedStudent && this.pdfExportSelectedStudent._id) {
          return "Selected student is ready for export.";
        }
        return "Type at least 2 characters to search for a student in the selected school year.";
      },

      pdfExportBuildUrl(inline) {
        const validationMessage = this.pdfExportValidationMessage();
        if (validationMessage) return "";

        const params = new URLSearchParams(this.pdfExportBaseQuery || "");
        if (this.pdfExportSchoolYear && !params.get("school_year")) {
          params.set("school_year", this.pdfExportSchoolYear);
        }
        params.set("scope", this.pdfExportScope);

        if (this.pdfExportScope === "grade") {
          params.set("grade", this.pdfExportGrade);
          params.delete("section");
          params.delete("student_record_id");
          params.delete("student_id");
        } else if (this.pdfExportScope === "section") {
          params.set("grade", this.pdfExportSectionGrade);
          params.set("section", this.pdfExportSection);
          params.delete("student_record_id");
          params.delete("student_id");
        } else if (this.pdfExportScope === "student" && this.pdfExportSelectedStudent) {
          params.set("student_record_id", this.pdfExportSelectedStudent._id);
          if (this.pdfExportSelectedStudent.student_id) {
            params.set("student_id", this.pdfExportSelectedStudent.student_id);
          } else {
            params.delete("student_id");
          }
          params.delete("grade");
          params.delete("section");
        }

        const builtUrl = `${this.pdfExportBaseUrl}?${params.toString()}`;
        return inline ? buildInlinePdfUrl(builtUrl) : builtUrl;
      },

      pdfExportDownloadUrl() {
        return this.pdfExportBuildUrl(false);
      },

      pdfExportOpenUrl() {
        return this.pdfExportDownloadUrl();
      },

      pdfExportPrintUrl() {
        return this.pdfExportBuildUrl(true);
      },

      pdfExportSelectStudent(student) {
        if (!student || !student._id) {
          this.pdfExportSelectedStudent = null;
          return;
        }
        this.pdfExportSelectedStudent = {
          _id: normalizeText(student._id),
          student_id: normalizeText(student.student_id || student.lrn),
          lrn: normalizeText(student.lrn || student.student_id),
          name: normalizeText(student.name),
          grade_level: normalizeText(student.grade_level || student.grade),
          section: normalizeText(student.section),
        };
        this.pdfExportStudentQuery = this.pdfExportSelectedStudent.name
          ? `${this.pdfExportSelectedStudent.name} (${this.pdfExportSelectedStudent.student_id || this.pdfExportSelectedStudent.lrn || "No ID"})`
          : (this.pdfExportSelectedStudent.student_id || this.pdfExportSelectedStudent.lrn || "");
        this.pdfExportStudentResults = [];
      },

      pdfExportClearStudentSelection() {
        this.pdfExportSelectedStudent = null;
        this.pdfExportStudentQuery = "";
        this.pdfExportStudentResults = [];
      },

      async pdfExportHandleStudentSearch() {
        const queryText = normalizeText(this.pdfExportStudentQuery);
        const requestToken = this.pdfExportStudentSearchToken + 1;
        this.pdfExportStudentSearchToken = requestToken;
        const selectedStudentLabel = this.pdfExportSelectedStudent
          ? normalizeText(
            this.pdfExportSelectedStudent.name
              ? `${this.pdfExportSelectedStudent.name} (${this.pdfExportSelectedStudent.student_id || this.pdfExportSelectedStudent.lrn || "No ID"})`
              : (this.pdfExportSelectedStudent.student_id || this.pdfExportSelectedStudent.lrn || "")
          )
          : "";

        if (this.pdfExportSelectedStudent && queryText === selectedStudentLabel) {
          this.pdfExportStudentResults = [];
          return;
        }

        if (this.pdfExportSelectedStudent && queryText !== selectedStudentLabel) {
          this.pdfExportSelectedStudent = null;
        }
        if (queryText.length < 2) {
          this.pdfExportSelectedStudent = null;
          this.pdfExportStudentResults = [];
          this.pdfExportStudentLoading = false;
          return;
        }

        this.pdfExportSelectedStudent = null;
        this.pdfExportStudentLoading = true;
        try {
          const params = new URLSearchParams();
          params.set("q", queryText);
          params.set("limit", "8");
          if (this.pdfExportSchoolYear) {
            params.set("school_year", this.pdfExportSchoolYear);
          }
          const response = await fetch(`${studentSearchUrl}?${params.toString()}`, {
            headers: { Accept: "application/json" },
            credentials: "same-origin",
          });
          const payload = await response.json().catch(() => ({}));
          if (requestToken !== this.pdfExportStudentSearchToken) return;
          if (!response.ok || payload.status !== "ok") {
            throw new Error(payload.message || "Unable to search students right now.");
          }
          this.pdfExportStudentResults = Array.isArray(payload.students) ? payload.students : [];
        } catch (error) {
          if (requestToken !== this.pdfExportStudentSearchToken) return;
          this.pdfExportStudentResults = [];
          fallbackNotify(this, error instanceof Error ? error.message : "Unable to search students right now.", "error");
        } finally {
          if (requestToken === this.pdfExportStudentSearchToken) {
            this.pdfExportStudentLoading = false;
          }
        }
      },
    };
  }

  window.createScopedPdfExportController = createScopedPdfExportController;
})();
