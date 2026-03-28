// Sidebar enhancement (moved from inline template)
(function () {
  // Configurable visual tweaks
  const CONFIG = {
    pillWidth: 5,
    pillHeight: 30,
    pillLeftOffset: 14,
    pillBlur: 6,
    pillRadius: 9999,
  };

  function applyVariables() {
    // No-op: CSS variables are defined by the shared sidebar partial.
  }

  function closeMobileSidebar() {
    try {
      if (!window.matchMedia || !window.matchMedia("(max-width: 767px)").matches) return;
      if (!window.Alpine || !document.body || !document.body.hasAttribute("x-data")) return;
      window.Alpine.evaluate(document.body, "mobileMenuOpen = false");
    } catch (_err) {
      // Ignore mobile close failures and keep navigation working.
    }
  }

  function enhanceSidebar() {
    try {
      const sidebar = document.querySelector("aside.app-sidebar");
      if (!sidebar) return;

      const links = Array.from(sidebar.querySelectorAll("a"));
      const locPath = window.location.pathname || "/";
      const locHash = window.location.hash || "";

      const normalize = (href) => {
        if (!href) return "";
        try {
          const url = new URL(href, window.location.origin);
          return url.pathname + (url.hash || "");
        } catch (_err) {
          return href;
        }
      };

      let activeCount = 0;

      links.forEach((link) => {
        if (!link.classList.contains("sidebar-link")) link.classList.add("sidebar-link");
        const href = link.getAttribute("href") || "";
        const normalized = normalize(href);
        const isActive = Boolean(
          normalized && (
            normalized === (locPath + locHash)
            || normalized === locPath
            || (href.startsWith("#") && locHash === href)
          )
        );

        if (link.dataset.sidebarMobileBound !== "1") {
          link.dataset.sidebarMobileBound = "1";
          link.addEventListener("click", () => {
            closeMobileSidebar();
            window.requestAnimationFrame(enhanceSidebar);
          });
        }

        if (isActive) {
          link.classList.add("sidebar-active");
          link.classList.remove("sidebar-inactive");
          link.setAttribute("aria-current", "page");
          activeCount += 1;
        } else {
          link.classList.remove("sidebar-active");
          link.classList.add("sidebar-inactive");
          link.removeAttribute("aria-current");
        }
      });

      sidebar.dataset.sidebarLinks = String(links.length);
      sidebar.dataset.sidebarActive = String(activeCount);
    } catch (error) {
      console.error("Sidebar enhancement failed", error);
    }
  }

  document.addEventListener("DOMContentLoaded", () => {
    applyVariables();
    enhanceSidebar();
  });

  window.addEventListener("app-theme-change", () => {
    applyVariables();
    enhanceSidebar();
  });

  window.addEventListener("hashchange", enhanceSidebar);
  window.addEventListener("popstate", enhanceSidebar);

  window.SidebarEnhance = {
    enhance: enhanceSidebar,
    closeMobile: closeMobileSidebar,
    config: CONFIG,
  };
})();
