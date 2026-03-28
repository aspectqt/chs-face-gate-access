(function () {
  "use strict";
  let activeDownloads = 0;
  const NATIVE_DOWNLOAD_SETTLE_MS = 10000;

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

  function sanitizeFilename(value, fallback = "download") {
    const fallbackName = String(fallback || "download").trim() || "download";
    const cleaned = String(value || "")
      .trim()
      .replace(/[<>:"/\\|?*\u0000-\u001f]+/g, "_")
      .replace(/\s+/g, " ");
    return cleaned || fallbackName;
  }

  function resolveResponseFilename({
    preferredFilename = "",
    contentDisposition = "",
    contentType = "",
    url = "",
    fallback = "download",
  } = {}) {
    const filename = sanitizeFilename(
      preferredFilename || parseContentDispositionFilename(contentDisposition) || inferFilenameFromUrl(url, fallback),
      fallback,
    );
    if (/\.[A-Za-z0-9]{2,8}$/.test(filename)) {
      return filename;
    }

    const normalizedContentType = String(contentType || "").toLowerCase();
    if (normalizedContentType.includes("application/pdf")) {
      return `${filename}.pdf`;
    }
    if (normalizedContentType.includes("spreadsheetml")) {
      return `${filename}.xlsx`;
    }
    if (normalizedContentType.includes("csv")) {
      return `${filename}.csv`;
    }
    return filename;
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

  function ensureBrowserDownloadFrame() {
    let frame = document.getElementById("appBrowserDownloadFrame");
    if (frame) return frame;

    frame = document.createElement("iframe");
    frame.id = "appBrowserDownloadFrame";
    frame.name = "appBrowserDownloadFrame";
    frame.hidden = true;
    frame.setAttribute("aria-hidden", "true");
    frame.style.display = "none";
    document.body.appendChild(frame);
    return frame;
  }

  function triggerBrowserManagedDownload(url) {
    try {
      const parsed = new URL(String(url || ""), window.location.origin);
      const frame = ensureBrowserDownloadFrame();
      const form = document.createElement("form");
      form.method = "GET";
      form.action = `${parsed.origin}${parsed.pathname}`;
      form.target = frame.name;
      form.style.display = "none";

      parsed.searchParams.forEach((value, key) => {
        const input = document.createElement("input");
        input.type = "hidden";
        input.name = key;
        input.value = value;
        form.appendChild(input);
      });

      document.body.appendChild(form);
      form.submit();
      window.setTimeout(() => form.remove(), 0);
    } catch (_error) {
      window.location.assign(String(url || ""));
    }
  }

  function openFileInNewTab(url) {
    const popup = window.open("about:blank", "_blank", "noopener");
    if (!popup) {
      throw new Error("Popup blocked. Allow popups to open the PDF in a new tab.");
    }

    try {
      popup.opener = null;
    } catch (_error) {
      // Some browsers expose a read-only opener reference.
    }

    try {
      popup.location.replace(url);
    } catch (_error) {
      popup.location.href = url;
    }
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
    const filename = resolveResponseFilename({
      preferredFilename,
      contentDisposition: response.headers.get("Content-Disposition"),
      contentType: response.headers.get("Content-Type"),
      url,
      fallback: "download",
    });

    triggerBlobDownload(blob, filename);
    return { filename };
  }

  function resolvePrintTrigger(eventTarget) {
    if (eventTarget instanceof Element) {
      return eventTarget.closest("[data-pdf-print]");
    }
    const parent = eventTarget && eventTarget.parentElement ? eventTarget.parentElement : null;
    return parent ? parent.closest("[data-pdf-print]") : null;
  }

  function escapeHtml(value) {
    return String(value || "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function openPrintWindow(title) {
    const popup = window.open("about:blank", "_blank");
    if (!popup) {
      throw new Error("Popup blocked. Allow popups to print the PDF.");
    }

    const safeTitle = escapeHtml(title || "Print PDF");
    popup.document.open();
    popup.document.write(`<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>${safeTitle}</title>
  <style>
    :root { color-scheme: light; }
    html, body { height: 100%; margin: 0; background: #ffffff; color: #111827; font-family: "Segoe UI", Arial, sans-serif; }
    body { display: flex; flex-direction: column; }
    .toolbar { display: flex; align-items: center; justify-content: space-between; gap: 1rem; padding: 0.9rem 1.1rem; border-bottom: 1px solid #d1d5db; background: #f9fafb; }
    .toolbar-copy { min-width: 0; }
    .toolbar-title { font-size: 0.98rem; font-weight: 700; }
    .toolbar-status { margin-top: 0.15rem; font-size: 0.82rem; color: #4b5563; }
    .toolbar-filename { margin-top: 0.12rem; font-size: 0.76rem; color: #6b7280; word-break: break-word; }
    .toolbar-actions { display: flex; align-items: center; gap: 0.6rem; }
    .print-button { border: 1px solid #111827; background: #111827; color: #ffffff; border-radius: 999px; padding: 0.62rem 1rem; font-size: 0.82rem; font-weight: 700; cursor: pointer; }
    .print-button[disabled] { opacity: 0.45; cursor: wait; }
    iframe { flex: 1 1 auto; width: 100%; border: 0; background: white; }
  </style>
</head>
<body>
  <div class="toolbar">
    <div class="toolbar-copy">
      <div class="toolbar-title">Print-ready PDF</div>
      <div id="pdfPrintStatus" class="toolbar-status">Preparing the PDF preview...</div>
      <div id="pdfPrintFilename" class="toolbar-filename"></div>
    </div>
    <div class="toolbar-actions">
      <button id="pdfPrintNowButton" type="button" class="print-button" disabled>Print Now</button>
    </div>
  </div>
  <iframe id="pdfPrintFrame" title="${safeTitle}"></iframe>
</body>
</html>`);
    popup.document.close();
    try {
      popup.focus();
    } catch (_error) {
      // Ignore focus failures; the tab is still usable.
    }
    return popup;
  }

  function setPrintWindowStatus(popup, message) {
    try {
      const statusNode = popup.document.getElementById("pdfPrintStatus");
      if (statusNode) statusNode.textContent = String(message || "");
    } catch (_error) {
      // Ignore cross-document access failures.
    }
  }

  function setPrintWindowFilename(popup, filename) {
    try {
      const text = String(filename || "").trim();
      popup.document.title = text || popup.document.title || "Print PDF";
      const filenameNode = popup.document.getElementById("pdfPrintFilename");
      if (filenameNode) filenameNode.textContent = text;
    } catch (_error) {
      // Ignore cross-document access failures.
    }
  }

  function attemptPrint(popup, frame) {
    try {
      if (frame && frame.contentWindow) {
        frame.contentWindow.focus();
        frame.contentWindow.print();
        return true;
      }
    } catch (_error) {
      // Fall through to the popup-level print attempt.
    }

    try {
      popup.focus();
      popup.print();
      return true;
    } catch (_error) {
      return false;
    }
  }

  async function fetchPdfBlob(url) {
    const response = await fetch(url, {
      method: "GET",
      credentials: "same-origin",
      headers: {
        Accept: "application/pdf",
      },
    });

    if (!response.ok) {
      throw new Error(`Print request failed with status ${response.status}.`);
    }

    const blob = await response.blob();
    const filename = resolveResponseFilename({
      contentDisposition: response.headers.get("Content-Disposition"),
      contentType: response.headers.get("Content-Type"),
      url,
      fallback: "report.pdf",
    });
    return { blob, filename };
  }

  async function printPdf(url, title) {
    const popup = openPrintWindow(title);
    let objectUrl = "";

    try {
      const { blob, filename } = await fetchPdfBlob(url);
      objectUrl = URL.createObjectURL(blob);
      const frame = popup.document.getElementById("pdfPrintFrame");
      const printButton = popup.document.getElementById("pdfPrintNowButton");
      if (!frame) {
        throw new Error("Unable to prepare the print frame.");
      }
      if (!printButton) {
        throw new Error("Unable to prepare the print controls.");
      }

      setPrintWindowFilename(popup, filename);
      setPrintWindowStatus(popup, "Loading PDF preview...");

      const requestPrint = () => {
        const didPrint = attemptPrint(popup, frame);
        setPrintWindowStatus(
          popup,
          didPrint
            ? "Print dialog opened. If your browser blocks it, use the Print Now button or Ctrl/Cmd+P."
            : "Use the Print Now button or Ctrl/Cmd+P if the browser blocks automatic printing.",
        );
      };

      frame.addEventListener("load", () => {
        setPrintWindowStatus(popup, "PDF ready. Opening the print dialog...");
        setTimeout(() => {
          requestPrint();
        }, 350);
      }, { once: true });

      printButton.disabled = false;
      printButton.addEventListener("click", requestPrint);

      popup.addEventListener("beforeunload", () => {
        if (objectUrl) {
          URL.revokeObjectURL(objectUrl);
        }
      }, { once: true });

      frame.src = objectUrl;
    } catch (error) {
      if (objectUrl) {
        URL.revokeObjectURL(objectUrl);
      }
      popup.document.body.innerHTML = `<div style="padding: 24px; font-family: 'Segoe UI', Arial, sans-serif;">
        <h1 style="margin: 0 0 12px; font-size: 20px; color: #111827;">Unable to prepare the PDF</h1>
        <p style="margin: 0; color: #374151;">${escapeHtml(error instanceof Error ? error.message : String(error || "Print failed."))}</p>
      </div>`;
      throw error;
    }
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

  function resolveDownloadMode(trigger) {
    const explicit = String(trigger?.dataset?.downloadMode || "").trim().toLowerCase();
    if (explicit) return explicit;
    return trigger?.hasAttribute("data-open-in-new-tab") ? "new-tab" : "download";
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
    if (link.dataset.downloading === "1") return;

    const originalTitle = link.getAttribute("title") || "";
    const downloadMode = resolveDownloadMode(link);
    let deferLifecycleEnd = false;
    let lifecycleClosed = false;
    const closeDownloadLifecycle = () => {
      if (lifecycleClosed) return;
      lifecycleClosed = true;
      activeDownloads = Math.max(0, activeDownloads - 1);
      emitDownloadLifecycleEvent("app-file-download-end", { url, activeDownloads });
    };
    link.dataset.downloading = "1";
    link.setAttribute("aria-busy", "true");
    link.classList.add("pointer-events-none", "opacity-70");
    link.setAttribute("title", "Downloading...");
    activeDownloads += 1;
    emitDownloadLifecycleEvent("app-file-download-start", { url });

    try {
      if (downloadMode === "native") {
        triggerBrowserManagedDownload(url);
        deferLifecycleEnd = true;
        window.setTimeout(closeDownloadLifecycle, NATIVE_DOWNLOAD_SETTLE_MS);
      } else if (downloadMode === "new-tab") {
        openFileInNewTab(url);
      } else {
        await downloadFile(url, link.dataset.filename || "");
      }
      link.dispatchEvent(new CustomEvent("file-download-success", {
        bubbles: true,
        detail: { url, mode: downloadMode },
      }));
    } catch (error) {
      console.error("File download failed:", error);
      const fallbackErrorMessage = downloadMode === "new-tab"
        ? "Unable to open the PDF in a new tab right now. Please try again."
        : "Unable to download the file right now. Please try again.";
      const errorEvent = new CustomEvent("file-download-error", {
        bubbles: true,
        cancelable: true,
        detail: {
          url,
          mode: downloadMode,
          error: error instanceof Error ? error.message : String(error || "Download failed."),
        },
      });
      const defaultPrevented = !link.dispatchEvent(errorEvent);
      if (!defaultPrevented) {
        window.alert(link.dataset.errorMessage || fallbackErrorMessage);
      }
    } finally {
      link.dataset.downloading = "0";
      link.removeAttribute("aria-busy");
      link.classList.remove("pointer-events-none", "opacity-70");
      if (!deferLifecycleEnd) {
        closeDownloadLifecycle();
      }
      if (originalTitle) {
        link.setAttribute("title", originalTitle);
      } else {
        link.removeAttribute("title");
      }
    }
  }

  async function onPrintButtonClick(event) {
    const trigger = resolvePrintTrigger(event.target);
    if (!trigger) return;
    if (!isUnmodifiedClick(event)) return;

    const url = String(trigger.dataset.printUrl || "").trim();
    if (!url) return;

    if (typeof event.preventDefault === "function") {
      event.preventDefault();
    }
    if (trigger.dataset.printing === "1") return;

    trigger.dataset.printing = "1";
    trigger.disabled = true;
    trigger.classList.add("pointer-events-none", "opacity-70");

    try {
      await printPdf(url, trigger.dataset.printTitle || "Print PDF");
    } catch (error) {
      console.error("PDF print failed:", error);
      window.alert(error instanceof Error ? error.message : "Unable to open the print-ready PDF right now.");
    } finally {
      trigger.dataset.printing = "0";
      trigger.disabled = false;
      trigger.classList.remove("pointer-events-none", "opacity-70");
    }
  }

  // Capture phase ensures we intercept before default navigation handlers.
  document.addEventListener("click", onDownloadLinkClick, true);
  document.addEventListener("click", onPrintButtonClick, true);

  window.AppFileDownload = Object.freeze({
    downloadFile,
    isBusy: () => activeDownloads > 0,
  });
})();
