(function () {
  const STORAGE_KEY = "chs_theme_preference";
  const FULLSCREEN_PREF_KEY = "chs_fullscreen_preference";
  const VALID_THEMES = new Set(["light", "dark"]);

  const docEl = document.documentElement;
  let currentTheme = "light";
  let initialized = false;
  let selectObserver = null;
  let fullscreenEventsBound = false;
  let fullscreenResumeBound = false;
  let fullscreenResumeHandler = null;
  let logoutModalEl = null;
  let logoutPendingHref = "/logout";

  const DROPDOWN_OPEN_KEYS = new Set([
    "Enter",
    " ",
    "ArrowDown",
    "ArrowUp",
    "Home",
    "End",
    "PageUp",
    "PageDown",
  ]);
  const FULLSCREEN_EVENTS = ["fullscreenchange", "webkitfullscreenchange", "msfullscreenchange"];

  function normalizeTheme(value) {
    const normalized = String(value || "").trim().toLowerCase();
    return VALID_THEMES.has(normalized) ? normalized : "";
  }

  function isAuthenticatedPage() {
    const body = document.body;
    if (!body) return false;
    return String(body.dataset.authenticated || "") === "1";
  }

  function getServerTheme() {
    const bodyTheme = normalizeTheme(document.body?.dataset.userTheme || "");
    if (bodyTheme) return bodyTheme;
    return normalizeTheme(window.__SERVER_THEME || "");
  }

  function getLocalTheme() {
    try {
      return normalizeTheme(window.localStorage.getItem(STORAGE_KEY));
    } catch (_err) {
      return "";
    }
  }

  function getSystemTheme() {
    return window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches
      ? "dark"
      : "light";
  }

  function chooseInitialTheme() {
    return getServerTheme() || getLocalTheme() || getSystemTheme();
  }

  function updateToggleButtons(theme) {
    document.querySelectorAll("[data-theme-toggle]").forEach((btn) => {
      const sunIcon = btn.querySelector('[data-theme-icon="sun"]');
      const moonIcon = btn.querySelector('[data-theme-icon="moon"]');
      const labelEl = btn.querySelector("[data-theme-label]");
      const darkMode = theme === "dark";

      btn.setAttribute("aria-pressed", darkMode ? "true" : "false");
      btn.setAttribute("title", darkMode ? "Switch to Light Mode" : "Switch to Dark Mode");
      btn.setAttribute("aria-label", darkMode ? "Switch to light mode" : "Switch to dark mode");

      if (sunIcon) sunIcon.classList.toggle("hidden", darkMode);
      if (moonIcon) moonIcon.classList.toggle("hidden", !darkMode);
      if (labelEl) labelEl.textContent = darkMode ? "Dark" : "Light";
    });
  }

  function emitThemeEvent(theme) {
    window.dispatchEvent(new CustomEvent("app-theme-change", { detail: { theme } }));
  }

  async function persistThemeToServer(theme) {
    if (!isAuthenticatedPage()) return;
    try {
      const res = await fetch("/api/profile/theme", {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ theme }),
      });
      if (!res.ok) return;
      const body = document.body;
      if (body) body.dataset.userTheme = theme;
    } catch (_err) {
      // Silent fallback to local persistence only.
    }
  }

  function applyTheme(theme, options = {}) {
    const { saveLocal = true, persistRemote = false, emit = true } = options;
    const resolvedTheme = normalizeTheme(theme) || "light";
    currentTheme = resolvedTheme;
    docEl.setAttribute("data-theme", resolvedTheme);

    if (saveLocal) {
      try {
        window.localStorage.setItem(STORAGE_KEY, resolvedTheme);
      } catch (_err) {
        // Ignore localStorage errors.
      }
    }

    updateToggleButtons(resolvedTheme);
    if (emit) emitThemeEvent(resolvedTheme);
    if (persistRemote) persistThemeToServer(resolvedTheme);
  }

  function toggleTheme() {
    const nextTheme = currentTheme === "dark" ? "light" : "dark";
    applyTheme(nextTheme, { saveLocal: true, persistRemote: true, emit: true });
  }

  function bindToggleButtons() {
    document.querySelectorAll("[data-theme-toggle]").forEach((btn) => {
      if (btn.dataset.themeBound === "1") return;
      btn.dataset.themeBound = "1";
      btn.addEventListener("click", () => toggleTheme());
    });
  }

  function fullscreenSupported() {
    const root = document.documentElement;
    return Boolean(
      root.requestFullscreen
      || root.webkitRequestFullscreen
      || root.msRequestFullscreen,
    );
  }

  function fullscreenElement() {
    return document.fullscreenElement || document.webkitFullscreenElement || document.msFullscreenElement || null;
  }

  function fullscreenActive() {
    return Boolean(fullscreenElement());
  }

  function rootFullscreenActive() {
    return fullscreenElement() === docEl;
  }

  function getFullscreenPreference() {
    try {
      return window.localStorage.getItem(FULLSCREEN_PREF_KEY) === "1";
    } catch (_err) {
      return false;
    }
  }

  function setFullscreenPreference(active) {
    try {
      window.localStorage.setItem(FULLSCREEN_PREF_KEY, active ? "1" : "0");
    } catch (_err) {
      // Ignore localStorage errors.
    }
  }

  async function enterFullscreen() {
    const root = document.documentElement;
    if (root.requestFullscreen) {
      await root.requestFullscreen();
      return;
    }
    if (root.webkitRequestFullscreen) {
      root.webkitRequestFullscreen();
      return;
    }
    if (root.msRequestFullscreen) {
      root.msRequestFullscreen();
    }
  }

  async function exitFullscreen() {
    if (document.exitFullscreen) {
      await document.exitFullscreen();
      return;
    }
    if (document.webkitExitFullscreen) {
      document.webkitExitFullscreen();
      return;
    }
    if (document.msExitFullscreen) {
      document.msExitFullscreen();
    }
  }

  function updateFullscreenButtons() {
    const active = rootFullscreenActive();
    const supported = fullscreenSupported();

    document.querySelectorAll("[data-fullscreen-toggle]").forEach((btn) => {
      const enterIcon = btn.querySelector('[data-fullscreen-icon="enter"]');
      const exitIcon = btn.querySelector('[data-fullscreen-icon="exit"]');

      if (!supported) {
        btn.disabled = true;
        btn.setAttribute("aria-disabled", "true");
        btn.setAttribute("title", "Full Screen is not supported in this browser");
        btn.classList.add("opacity-60", "cursor-not-allowed");
        if (enterIcon) enterIcon.classList.remove("hidden");
        if (exitIcon) exitIcon.classList.add("hidden");
        return;
      }

      btn.disabled = false;
      btn.removeAttribute("aria-disabled");
      btn.classList.remove("opacity-60", "cursor-not-allowed");
      btn.setAttribute("aria-pressed", active ? "true" : "false");
      btn.setAttribute("aria-label", active ? "Exit full screen" : "Enter full screen");
      btn.setAttribute("title", active ? "Exit Full Screen" : "Enter Full Screen");

      if (enterIcon) enterIcon.classList.toggle("hidden", active);
      if (exitIcon) exitIcon.classList.toggle("hidden", !active);
    });
  }

  async function toggleFullscreen() {
    if (!fullscreenSupported()) return;
    try {
      if (fullscreenActive()) {
        setFullscreenPreference(false);
        await exitFullscreen();
      } else {
        setFullscreenPreference(true);
        await enterFullscreen();
      }
    } catch (_err) {
      if (!fullscreenActive()) {
        bindFullscreenAutoResume();
      }
    } finally {
      updateFullscreenButtons();
    }
  }

  function unbindFullscreenAutoResume() {
    if (!fullscreenResumeBound || !fullscreenResumeHandler) return;
    ["pointerdown", "keydown", "touchstart"].forEach((eventName) => {
      window.removeEventListener(eventName, fullscreenResumeHandler, true);
    });
    fullscreenResumeHandler = null;
    fullscreenResumeBound = false;
  }

  async function tryResumeFullscreen() {
    if (!fullscreenSupported()) return false;
    if (!getFullscreenPreference()) {
      unbindFullscreenAutoResume();
      return false;
    }
    if (fullscreenActive()) {
      unbindFullscreenAutoResume();
      updateFullscreenButtons();
      return true;
    }

    try {
      await enterFullscreen();
      if (fullscreenActive()) {
        unbindFullscreenAutoResume();
        updateFullscreenButtons();
        return true;
      }
    } catch (_err) {
      // Will retry on next user interaction while preference remains enabled.
    }

    updateFullscreenButtons();
    return false;
  }

  function bindFullscreenAutoResume() {
    if (fullscreenResumeBound) return;
    fullscreenResumeHandler = () => {
      tryResumeFullscreen();
    };

    ["pointerdown", "keydown", "touchstart"].forEach((eventName) => {
      window.addEventListener(eventName, fullscreenResumeHandler, true);
    });
    fullscreenResumeBound = true;
  }

  function bindFullscreenButtons() {
    document.querySelectorAll("[data-fullscreen-toggle]").forEach((btn) => {
      if (btn.dataset.fullscreenBound === "1") return;
      btn.dataset.fullscreenBound = "1";
      btn.addEventListener("click", () => toggleFullscreen());
    });

    if (!fullscreenEventsBound) {
      FULLSCREEN_EVENTS.forEach((eventName) => {
        document.addEventListener(eventName, () => {
          const activeElement = fullscreenElement();
          if (activeElement === docEl) {
            unbindFullscreenAutoResume();
          } else if (activeElement) {
            // Keep app-level fullscreen preference tied to the global fullscreen button only.
            setFullscreenPreference(false);
            unbindFullscreenAutoResume();
          } else if (getFullscreenPreference()) {
            bindFullscreenAutoResume();
            tryResumeFullscreen();
          } else {
            unbindFullscreenAutoResume();
          }
          updateFullscreenButtons();
        });
      });
      fullscreenEventsBound = true;
    }

    if (getFullscreenPreference()) {
      bindFullscreenAutoResume();
      tryResumeFullscreen();
    } else {
      unbindFullscreenAutoResume();
    }

    updateFullscreenButtons();
  }

  function isLogoutHref(value) {
    const href = String(value || "").trim();
    if (!href) return false;
    if (href === "/logout") return true;
    try {
      const parsed = new URL(href, window.location.origin);
      return parsed.pathname === "/logout";
    } catch (_err) {
      return false;
    }
  }

  function buildLogoutModal() {
    const modal = document.createElement("div");
    modal.id = "globalLogoutConfirmModal";
    modal.className = "fixed inset-0 z-[220] hidden items-center justify-center bg-slate-900/55 p-4";
    modal.innerHTML = `
      <div class="w-full max-w-md rounded-2xl border border-slate-200 bg-white shadow-2xl">
        <div class="border-b border-slate-200 px-5 py-4">
          <h3 class="text-base font-semibold text-slate-900">Confirm Logout</h3>
          <p class="mt-1 text-sm text-slate-500">Are you sure you want to log out?</p>
        </div>
        <div class="flex items-center justify-end gap-2 px-5 py-4">
          <button type="button" data-logout-cancel class="rounded-xl border border-slate-300 bg-white px-4 py-2 text-sm font-semibold text-slate-700 hover:bg-slate-100">Cancel</button>
          <button type="button" data-logout-confirm class="rounded-xl border border-rose-300 bg-rose-600 px-4 py-2 text-sm font-semibold text-white hover:bg-rose-700">Confirm Logout</button>
        </div>
      </div>
    `;

    const cancelBtn = modal.querySelector("[data-logout-cancel]");
    const confirmBtn = modal.querySelector("[data-logout-confirm]");

    cancelBtn?.addEventListener("click", () => closeLogoutModal());
    confirmBtn?.addEventListener("click", () => {
      const href = logoutPendingHref || "/logout";
      closeLogoutModal();
      window.location.assign(href);
    });

    modal.addEventListener("click", (event) => {
      if (event.target === modal) closeLogoutModal();
    });

    document.addEventListener("keydown", (event) => {
      if (event.key !== "Escape") return;
      if (modal.classList.contains("hidden")) return;
      closeLogoutModal();
    });

    document.body.appendChild(modal);
    return modal;
  }

  function ensureLogoutModal() {
    if (logoutModalEl) return logoutModalEl;
    logoutModalEl = document.getElementById("globalLogoutConfirmModal") || buildLogoutModal();
    return logoutModalEl;
  }

  function openLogoutModal(href) {
    const modal = ensureLogoutModal();
    logoutPendingHref = String(href || "/logout");
    modal.classList.remove("hidden");
    modal.classList.add("flex");
    const cancelBtn = modal.querySelector("[data-logout-cancel]");
    cancelBtn?.focus();
  }

  function closeLogoutModal() {
    const modal = ensureLogoutModal();
    modal.classList.remove("flex");
    modal.classList.add("hidden");
    logoutPendingHref = "/logout";
  }

  function bindLogoutLinks(root = document) {
    const scope = root instanceof Element || root instanceof Document ? root : document;
    const links = [];

    if (root instanceof HTMLAnchorElement) links.push(root);
    if (root instanceof Element && root.matches("a")) links.push(root);

    scope.querySelectorAll("a[href], a[data-logout-link]").forEach((link) => {
      links.push(link);
    });

    links.forEach((link) => {
      if (!(link instanceof HTMLAnchorElement)) return;
      if (link.dataset.logoutBound === "1") return;
      if (!isLogoutHref(link.getAttribute("href"))) return;

      link.dataset.logoutBound = "1";
      link.dataset.logoutLink = "1";
      link.addEventListener("click", (event) => {
        if (event.defaultPrevented) return;
        if (event.button !== 0) return;
        if (event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
        event.preventDefault();
        openLogoutModal(link.href || "/logout");
      });
    });
  }

  function createSelectArrowElement() {
    const arrow = document.createElement("span");
    arrow.className = "ui-select-arrow";
    arrow.setAttribute("aria-hidden", "true");
    arrow.innerHTML = "<svg viewBox='0 0 24 24' fill='none' stroke='currentColor' stroke-width='2.2' stroke-linecap='round' stroke-linejoin='round'><path d='m6 9 6 6 6-6'/></svg>";
    return arrow;
  }

  function ensureSelectWrapper(select) {
    const parent = select.parentElement;
    if (parent && parent.classList.contains("ui-select-wrap")) {
      if (!parent.querySelector(".ui-select-arrow")) {
        parent.appendChild(createSelectArrowElement());
      }
      return parent;
    }

    const wrapper = document.createElement("span");
    wrapper.className = "ui-select-wrap";
    if (select.classList.contains("w-full")) {
      wrapper.classList.add("ui-select-wrap-full");
    }

    select.parentNode.insertBefore(wrapper, select);
    wrapper.appendChild(select);
    wrapper.appendChild(createSelectArrowElement());
    return wrapper;
  }

  function enhanceSelectElement(select) {
    if (!(select instanceof HTMLSelectElement)) return;
    if (select.dataset.dropdownEnhanced === "1") return;
    const wrapper = ensureSelectWrapper(select);
    const isOpen = () => wrapper.classList.contains("ui-dropdown-open");
    const setOpenState = (open) => {
      wrapper.classList.toggle("ui-dropdown-open", Boolean(open));
    };

    select.dataset.dropdownEnhanced = "1";
    select.addEventListener("pointerdown", () => {
      setOpenState(!isOpen());
    });
    select.addEventListener("keydown", (event) => {
      if (DROPDOWN_OPEN_KEYS.has(event.key)) {
        setOpenState(true);
      }
      if (event.key === "Escape") {
        setOpenState(false);
      }
    });
    select.addEventListener("change", () => {
      setOpenState(false);
    });
    select.addEventListener("blur", () => {
      setOpenState(false);
    });
  }

  function enhanceSelectElements(root = document) {
    if (root instanceof HTMLSelectElement) {
      enhanceSelectElement(root);
      return;
    }

    if (root instanceof Element && root.matches("select")) {
      enhanceSelectElement(root);
    }

    const scope = root instanceof Element || root instanceof Document ? root : document;
    scope.querySelectorAll("select").forEach((select) => enhanceSelectElement(select));
  }

  function observeSelectInsertions() {
    if (selectObserver || typeof MutationObserver === "undefined") return;
    if (!document.body) return;

    selectObserver = new MutationObserver((mutations) => {
      mutations.forEach((mutation) => {
        mutation.addedNodes.forEach((node) => {
          if (!(node instanceof Element)) return;
          enhanceSelectElements(node);
          bindLogoutLinks(node);
          bindFullscreenButtons();
        });
      });
    });
    selectObserver.observe(document.body, { childList: true, subtree: true });
  }

  function isDropdownOpen(menu) {
    if (!menu) return false;
    return !menu.classList.contains("hidden") && !menu.classList.contains("ui-dropdown-hidden");
  }

  function setButtonArrowState(button, open) {
    if (!button) return;
    button.querySelectorAll("[data-dropdown-arrow], .ui-dropdown-arrow").forEach((arrow) => {
      arrow.classList.toggle("ui-dropdown-arrow-open", Boolean(open));
    });
  }

  function openDropdown(button, menu) {
    if (!menu) return false;
    menu.classList.add("ui-dropdown-menu");
    menu.classList.remove("hidden", "ui-dropdown-hidden", "ui-dropdown-closing");
    window.requestAnimationFrame(() => {
      menu.classList.add("ui-dropdown-visible");
    });
    if (button) button.setAttribute("aria-expanded", "true");
    setButtonArrowState(button, true);
    return true;
  }

  function closeDropdown(button, menu) {
    if (!menu) return false;
    if (!isDropdownOpen(menu)) {
      menu.classList.add("hidden", "ui-dropdown-hidden");
      menu.classList.remove("ui-dropdown-visible", "ui-dropdown-closing");
      if (button) button.setAttribute("aria-expanded", "false");
      setButtonArrowState(button, false);
      return false;
    }

    menu.classList.remove("ui-dropdown-visible");
    menu.classList.add("ui-dropdown-closing");
    if (button) button.setAttribute("aria-expanded", "false");
    setButtonArrowState(button, false);

    let done = false;
    const finalize = () => {
      if (done) return;
      done = true;
      menu.classList.remove("ui-dropdown-closing");
      menu.classList.add("hidden", "ui-dropdown-hidden");
      menu.removeEventListener("transitionend", finalize);
    };

    menu.addEventListener("transitionend", finalize);
    window.setTimeout(finalize, 210);
    return true;
  }

  function toggleDropdown(button, menu) {
    if (isDropdownOpen(menu)) {
      closeDropdown(button, menu);
      return false;
    }
    openDropdown(button, menu);
    return true;
  }

  async function syncThemeFromServerIfNeeded() {
    if (!isAuthenticatedPage() || getServerTheme()) return;
    try {
      const res = await fetch("/api/profile/theme", { method: "GET" });
      const payload = await res.json().catch(() => ({}));
      const remoteTheme = normalizeTheme(payload.theme);
      if (!res.ok || !remoteTheme) return;
      applyTheme(remoteTheme, { saveLocal: true, persistRemote: false, emit: true });
      const body = document.body;
      if (body) body.dataset.userTheme = remoteTheme;
    } catch (_err) {
      // Silent fallback to local/system preference.
    }
  }

  function initTheme() {
    if (initialized) return;
    initialized = true;
    applyTheme(chooseInitialTheme(), { saveLocal: true, persistRemote: false, emit: false });
    bindToggleButtons();
    bindFullscreenButtons();
    enhanceSelectElements(document);
    bindLogoutLinks(document);
    ensureLogoutModal();
    observeSelectInsertions();
    syncThemeFromServerIfNeeded();
    emitThemeEvent(currentTheme);
    window.requestAnimationFrame(() => docEl.classList.add("theme-ready"));
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initTheme);
  } else {
    initTheme();
  }

  window.AppTheme = {
    init: initTheme,
    getTheme: function () {
      return currentTheme;
    },
    setTheme: function (theme, options = {}) {
      applyTheme(theme, { saveLocal: true, persistRemote: false, emit: true, ...options });
    },
    toggleTheme,
    toggleFullscreen,
    enhanceSelects: enhanceSelectElements,
  };

  window.AppDropdown = {
    isOpen: isDropdownOpen,
    open: openDropdown,
    close: closeDropdown,
    toggle: toggleDropdown,
  };
})();
