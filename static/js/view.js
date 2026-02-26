// ================================================================
// view.js — Column resize from HEADER only
//
// Features:
//   • Resize handle injected into every <th> as a grip icon (⋮⋮)
//   • Vertical blue line tracks cursor while dragging
//   • Text aligned left throughout
//   • Double-click handle to auto-fit column
// ================================================================

document.addEventListener("DOMContentLoaded", function () {

    const table = document.querySelector(".view-table");
    if (!table) return;

    const headers = Array.from(table.querySelectorAll("thead th"));
    if (!headers.length) return;

    table.style.tableLayout = "fixed";

    // ── Drag line element (blue vertical bar constrained to table) ──
    // Appended inside scroll wrapper so it clips at table boundaries
    const tableScroll = table.closest(".view-table-scroll") || table.parentElement;
    tableScroll.style.position = "relative";

    const dragLine = document.createElement("div");
    dragLine.id = "col-resize-line";
    dragLine.style.cssText = `
        position: absolute;
        top: 0;
        width: 2px;
        height: 100%;
        background: #2563eb;
        z-index: 99999;
        pointer-events: none;
        display: none;
        box-shadow: 0 0 4px rgba(37,99,235,0.4);
    `;
    tableScroll.appendChild(dragLine);

    // ── State ────────────────────────────────────────────────────────
    let resizing       = false;
    let resizingIndex  = null;
    let startX         = 0;
    let startWidth     = 0;

    // ── Set initial column widths ────────────────────────────────────
    headers.forEach((th, idx) => {
        th.style.width    = "160px";
        th.style.minWidth = "80px";
        th.style.position = "relative";

        // Also set body cells
        table.querySelectorAll(`tbody tr td:nth-child(${idx + 1})`).forEach(td => {
            td.style.width    = "160px";
            td.style.minWidth = "80px";
        });
    });

    // ── Inject resize handle into each <th> ──────────────────────────
    headers.forEach((th, idx) => {
        // Skip last column — nothing to resize into
        if (idx === headers.length - 1) return;

        const handle = document.createElement("div");
        handle.className = "col-resize-handle";
        handle.setAttribute("title", "Drag to resize · Double-click to auto-fit");
        handle.innerHTML = `
            <span class="col-resize-grip">
                <svg width="4" height="14" viewBox="0 0 4 14" fill="none">
                    <circle cx="1" cy="2"  r="1" fill="currentColor"/>
                    <circle cx="1" cy="7"  r="1" fill="currentColor"/>
                    <circle cx="1" cy="12" r="1" fill="currentColor"/>
                    <circle cx="3" cy="2"  r="1" fill="currentColor"/>
                    <circle cx="3" cy="7"  r="1" fill="currentColor"/>
                    <circle cx="3" cy="12" r="1" fill="currentColor"/>
                </svg>
            </span>
        `;

        // ── Mousedown: start resize ──────────────────────────────────
        handle.addEventListener("mousedown", function (e) {
            e.preventDefault();
            e.stopPropagation();

            resizing      = true;
            resizingIndex = idx;
            startX        = e.clientX;
            startWidth    = th.offsetWidth;

        // Move drag line — position relative to scroll container
        const scrollRect = tableScroll.getBoundingClientRect();
        const lineLeft   = e.clientX - scrollRect.left + tableScroll.scrollLeft;
        dragLine.style.left    = lineLeft + "px";
        dragLine.style.display = "block";

            document.body.style.cursor     = "col-resize";
            document.body.style.userSelect = "none";

            th.classList.add("resizing");
        });

        // ── Double-click: auto-fit ───────────────────────────────────
        handle.addEventListener("dblclick", function (e) {
            e.preventDefault();
            e.stopPropagation();
            autoFit(idx, th);
        });

        th.appendChild(handle);
    });

    // ── Global mousemove ─────────────────────────────────────────────
    document.addEventListener("mousemove", function (e) {
        if (!resizing || resizingIndex === null) return;

        const diff     = e.clientX - startX;
        const newWidth = Math.max(80, startWidth + diff);
        const th       = headers[resizingIndex];

        // Move drag line
        const scrollRect = tableScroll.getBoundingClientRect();
        dragLine.style.left = (e.clientX - scrollRect.left + tableScroll.scrollLeft) + "px";

        // Resize header
        th.style.width    = newWidth + "px";
        th.style.minWidth = newWidth + "px";

        // Resize all body cells in this column
        table.querySelectorAll(
            `tbody tr td:nth-child(${resizingIndex + 1})`
        ).forEach(td => {
            td.style.width    = newWidth + "px";
            td.style.minWidth = newWidth + "px";
        });

        e.preventDefault();
    });

    // ── Global mouseup ───────────────────────────────────────────────
    document.addEventListener("mouseup", function () {
        if (!resizing) return;

        resizing = false;

        if (headers[resizingIndex]) {
            headers[resizingIndex].classList.remove("resizing");
        }

        resizingIndex = null;

        dragLine.style.display      = "none";
        document.body.style.cursor     = "";
        document.body.style.userSelect = "";
    });

    // ── Auto-fit column to content ───────────────────────────────────
    function autoFit(idx, th) {
        let max = 80;

        // Measure header text
        const span = th.querySelector(".th-label");
        const headerText = span ? span.textContent : th.textContent.trim();
        max = Math.max(max, measureText(headerText, th) + 48);

        // Measure body cells
        table.querySelectorAll(
            `tbody tr td:nth-child(${idx + 1})`
        ).forEach(td => {
            max = Math.max(max, measureText(td.textContent.trim(), td) + 32);
        });

        max = Math.min(max, 600);

        th.style.width    = max + "px";
        th.style.minWidth = max + "px";

        table.querySelectorAll(
            `tbody tr td:nth-child(${idx + 1})`
        ).forEach(td => {
            td.style.width    = max + "px";
            td.style.minWidth = max + "px";
        });
    }

    function measureText(text, el) {
        const c   = document.createElement("canvas");
        const ctx = c.getContext("2d");
        const s   = window.getComputedStyle(el);
        ctx.font  = `${s.fontWeight} ${s.fontSize} ${s.fontFamily}`;
        return ctx.measureText(text).width;
    }

});