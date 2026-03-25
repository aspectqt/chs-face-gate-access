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
    const popup = window.open("", "_blank", "noopener,noreferrer,width=1080,height=860");
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
    html, body { height: 100%; margin: 0; background: #f8fafc; color: #0f172a; font-family: "Segoe UI", Arial, sans-serif; }
    body { display: flex; flex-direction: column; }
    .status { display: flex; align-items: center; justify-content: center; gap: 0.75rem; padding: 1rem 1.25rem; border-bottom: 1px solid #e2e8f0; background: linear-gradient(135deg, #fff1f2, #ffffff); font-size: 0.95rem; }
    .status-badge { display: inline-flex; align-items: center; justify-content: center; width: 2rem; height: 2rem; border-radius: 0.75rem; background: #e11d48; color: white; font-weight: 700; letter-spacing: 0.08em; font-size: 0.75rem; }
    .status-note { font-size: 0.8rem; color: #64748b; }
    iframe { flex: 1 1 auto; width: 100%; border: 0; background: white; }
  </style>
</head>
<body>
  <div class="status">
    <span class="status-badge">PDF</span>
    <div>
      <div>Preparing a print-ready PDF...</div>
      <div class="status-note">If the print dialog does not open automatically, use Ctrl/Cmd+P.</div>
    </div>
  </div>
  <iframe id="pdfPrintFrame" title="${safeTitle}"></iframe>
</body>
</html>`);
    popup.document.close();
    return popup;
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

    return response.blob();
  }

  async function printPdf(url, title) {
    const popup = openPrintWindow(title);
    let objectUrl = "";

    try {
      const blob = await fetchPdfBlob(url);
      objectUrl = URL.createObjectURL(blob);
      const frame = popup.document.getElementById("pdfPrintFrame");
      if (!frame) {
        throw new Error("Unable to prepare the print frame.");
      }

      frame.addEventListener("load", () => {
        setTimeout(() => {
          try {
            if (frame.contentWindow) {
              frame.contentWindow.focus();
              frame.contentWindow.print();
              return;
            }
          } catch (_error) {
            // Fall back to printing the popup window itself.
          }
          try {
            popup.focus();
            popup.print();
          } catch (_error) {
            // Let the user print manually if the browser blocks programmatic printing.
          }
        }, 350);
      }, { once: true });

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
        <h1 style="margin: 0 0 12px; font-size: 20px; color: #991b1b;">Unable to prepare the PDF</h1>
        <p style="margin: 0; color: #475569;">${escapeHtml(error instanceof Error ? error.message : String(error || "Print failed."))}</p>
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

  async function onPrintButtonClick(event) {
    const trigger = resolvePrintTrigger(event.target);
    if (!trigger) return;
    if (!isUnmodifiedClick(event)) return;

    const url = String(trigger.dataset.printUrl || "").trim();
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
