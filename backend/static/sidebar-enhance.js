// Sidebar enhancement (moved from inline template)
(function () {
  // Configurable visual tweaks
  const CONFIG = {
    pillWidth: 6,       // px
    pillHeight: 36,     // px
    pillLeftOffset: 9,  // px from left edge
    pillBlur: 6,        // shadow blur
    pillRadius: 9999,   // large for rounded pill
  };

  function applyVariables() {
    // No-op: CSS variables are defined globally in theme.css. CONFIG kept for runtime reference.
  }

  function enhanceSidebar() {
    try {
      const sidebar = document.querySelector('aside.app-sidebar');
      if (!sidebar) return;

      const links = Array.from(sidebar.querySelectorAll('a'));
      const locPath = window.location.pathname || '/';
      const locHash = window.location.hash || '';

      const normalize = (href) => {
        if (!href) return '';
        try {
          const url = new URL(href, window.location.origin);
          return url.pathname + (url.hash || '');
        } catch (err) {
          return href;
        }
      };

      let activeCount = 0;

      links.forEach((link) => {
        if (!link.classList.contains('sidebar-link')) link.classList.add('sidebar-link');
        const href = link.getAttribute('href') || '';
        const normalized = normalize(href);

        const isActive = (normalized && (normalized === (locPath + locHash) || normalized === locPath || (href.startsWith('#') && locHash === href)));

        if (isActive) {
          link.classList.add('sidebar-active');
          link.setAttribute('aria-current', 'page');
          activeCount++;
        } else {
          link.classList.remove('sidebar-active');
          link.classList.add('sidebar-inactive');
          link.removeAttribute('aria-current');
        }
      });

      // Small self-test: expose counts for debugging
      sidebar.dataset.sidebarLinks = links.length;
      sidebar.dataset.sidebarActive = activeCount;

    } catch (e) {
      console.error('Sidebar enhancement failed', e);
    }
  }

  // Run on DOMContentLoaded and on app-theme-change since sidebar may be re-rendered
  document.addEventListener('DOMContentLoaded', () => {
    applyVariables();
    enhanceSidebar();
  });

  window.addEventListener('app-theme-change', () => {
    applyVariables();
    enhanceSidebar();
  });

  // Expose for manual tweaks in console
  window.SidebarEnhance = {
    enhance: enhanceSidebar,
    config: CONFIG,
  };
})();
