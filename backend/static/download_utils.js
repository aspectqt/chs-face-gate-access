(function () {
  "use strict";
  let activeDownloads = 0;

  function emitDownloadLifecycleEvent(name, detail) {
    document.dispatchEvent(new CustomEvent(name, {
      bubbles: true,
      detail: detail || {},
    }));
  }

  function parseContentDispositionFilename(headerValue) {
    const raw = String(headerValue || "");
    if (!raw) return "";

    // RFC 5987 / RFC 6266 format: filename*=UTF-8''...
    const utf8Match = raw.match(/filename\*\s*=\s*UTF-8''([^;]+)/i);
    if (utf8Match && utf8Match[1]) {
      try {
        return decodeURIComponent(utf8Match[1].trim()).split('"').join("");
      } catch (_error) {
        return utf8Match[1].trim().split('"').join("");
      }
    }

    const quotedMatch = raw.match(/filename\s*=\s*"([^"]+)"/i);
    if (quotedMatch && quotedMatch[1]) {
      return quotedMatch[1].trim();
    }

    const plainMatch = raw.match(/filename\s*=\s*([^;]+)/i);
    if (plainMatch && plainMatch[1]) {
      return plainMatch[1].trim().split('"').join("");
    }

    return "";
  }

  function inferFilenameFromUrl(url, fallback = "download") {
    try {
      const parsed = new URL(String(url || ""), window.location.origin);
      const pathname = parsed.pathname || "";
      const leaf = pathname.split("/").filter(Boolean).pop() || "";
      return leaf || fallback;
    } catch (_error) {
      return fallback;
    }
  }

  function triggerBlobDownload(blob, filename) {
    const objectUrl = URL.createObjectURL(blob);
    const anchor = document.createElement("a");
    anchor.href = objectUrl;
    anchor.download = filename;
    anchor.style.display = "none";
    document.body.appendChild(anchor);
    anchor.click();
    anchor.remove();
    setTimeout(() => URL.revokeObjectURL(objectUrl), 1500);
  }

  async function downloadFile(url, preferredFilename = "") {
    const response = await fetch(url, {
      method: "GET",
      credentials: "same-origin",
      headers: {
        Accept: "*/*",
      },
    });

    if (!response.ok) {
      throw new Error(`Download failed with status ${response.status}.`);
    }

    const blob = await response.blob();
    const contentDisposition = response.headers.get("Content-Disposition");
    const headerFilename = parseContentDispositionFilename(contentDisposition);
    const fallbackFilename = inferFilenameFromUrl(url, "download");
    const filename = String(preferredFilename || headerFilename || fallbackFilename || "download").trim();

    triggerBlobDownload(blob, filename);
    return { filename };
  }

  function isUnmodifiedClick(event) {
    return !event.metaKey && !event.ctrlKey && !event.shiftKey && !event.altKey;
  }

  function resolveDownloadTrigger(eventTarget) {
    if (eventTarget instanceof Element) {
      return eventTarget.closest("[data-file-download]");
    }
    const parent = eventTarget && eventTarget.parentElement ? eventTarget.parentElement : null;
    return parent ? parent.closest("[data-file-download]") : null;
  }

  async function onDownloadLinkClick(event) {
    const link = resolveDownloadTrigger(event.target);
    if (!link) return;
    if (!isUnmodifiedClick(event)) return;

    const url = String(link.dataset.downloadUrl || link.getAttribute("href") || "").trim();
    if (!url) return;

    if (typeof event.preventDefault === "function") {
      event.preventDefault();
    }
    if (typeof event.stopPropagation === "function") {
      event.stopPropagation();
    }
    if (typeof event.stopImmediatePropagation === "function") {
      event.stopImmediatePropagation();
    }
    if (link.dataset.downloading === "1") return;

    const originalTitle = link.getAttribute("title") || "";
    link.dataset.downloading = "1";
    link.setAttribute("aria-busy", "true");
    link.classList.add("pointer-events-none", "opacity-70");
    link.setAttribute("title", "Downloading...");
    activeDownloads += 1;
    emitDownloadLifecycleEvent("app-file-download-start", { url });

    try {
      await downloadFile(url, link.dataset.filename || "");
      link.dispatchEvent(new CustomEvent("file-download-success", {
        bubbles: true,
        detail: { url },
      }));
    } catch (error) {
      console.error("File download failed:", error);
      link.dispatchEvent(new CustomEvent("file-download-error", {
        bubbles: true,
        detail: { url, error: error instanceof Error ? error.message : String(error || "Download failed.") },
      }));
      window.alert("Unable to download the file right now. Please try again.");
    } finally {
      link.dataset.downloading = "0";
      link.removeAttribute("aria-busy");
      link.classList.remove("pointer-events-none", "opacity-70");
      activeDownloads = Math.max(0, activeDownloads - 1);
      emitDownloadLifecycleEvent("app-file-download-end", { url, activeDownloads });
      if (originalTitle) {
        link.setAttribute("title", originalTitle);
      } else {
        link.removeAttribute("title");
      }
    }
  }

  // Capture phase ensures we intercept before default navigation handlers.
  document.addEventListener("click", onDownloadLinkClick, true);

  window.AppFileDownload = Object.freeze({
    downloadFile,
    isBusy: () => activeDownloads > 0,
  });
})();
