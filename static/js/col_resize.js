/**
 * col_resize.js — Shared column resize for all table pages
 *
 * Usage:
 *   initColResize(table)        — call once after table is in the DOM
 *   initColResize(table, 80)    — optional minWidth in px (default 60)
 */

function initColResize(table, minWidth = 60) {
  if (!table) return;

  const ths = Array.from(table.querySelectorAll('thead tr:first-child th'));
  if (!ths.length) return;

  // ── Step 1: Read natural widths BEFORE switching layout ──────────────
  // CRITICAL: must read offsetWidth while table-layout is still 'auto'.
  // If we switch to 'fixed' first, browser squishes all columns equally.
  const naturalWidths = ths.map(th => th.offsetWidth);

  // ── Step 2: Switch to fixed layout so widths become controllable ─────
  table.style.tableLayout = 'fixed';
  if (!table.style.minWidth) table.style.minWidth = 'max-content';

  // ── Step 3: Lock each th to its natural rendered width ───────────────
  ths.forEach((th, i) => {
    th.style.width     = naturalWidths[i] + 'px';
    th.style.minWidth  = minWidth + 'px';
    th.style.position  = 'relative';
    th.style.userSelect = 'none';
    th.style.overflow  = 'visible';
    th.style.boxSizing = 'border-box';
  });

  // ── Step 4: Inject resize handle into every th ───────────────────────
  ths.forEach((th) => {
    const existing = th.querySelector('.col-resize-handle');
    if (existing) existing.remove();

    const handle = document.createElement('div');
    handle.className = 'col-resize-handle';
    handle.innerHTML = `<span class="col-resize-grip">
      <svg width="4" height="14" viewBox="0 0 4 14" fill="none">
        <circle cx="1" cy="2"  r="1" fill="currentColor"/>
        <circle cx="1" cy="7"  r="1" fill="currentColor"/>
        <circle cx="1" cy="12" r="1" fill="currentColor"/>
        <circle cx="3" cy="2"  r="1" fill="currentColor"/>
        <circle cx="3" cy="7"  r="1" fill="currentColor"/>
        <circle cx="3" cy="12" r="1" fill="currentColor"/>
      </svg>
    </span>`;
    th.appendChild(handle);

    let startX, startW;

    handle.addEventListener('mousedown', (e) => {
      e.preventDefault();
      e.stopPropagation();
      startX = e.clientX;
      startW = th.offsetWidth;
      th.classList.add('resizing');
      document.body.style.cursor     = 'col-resize';
      document.body.style.userSelect = 'none';

      const onMove = (e) => {
        const newW = Math.max(minWidth, startW + (e.clientX - startX));
        th.style.width = newW + 'px';
      };
      const onUp = () => {
        th.classList.remove('resizing');
        document.body.style.cursor     = '';
        document.body.style.userSelect = '';
        document.removeEventListener('mousemove', onMove);
        document.removeEventListener('mouseup',   onUp);
      };
      document.addEventListener('mousemove', onMove);
      document.addEventListener('mouseup',   onUp);
    });

    handle.addEventListener('click', e => e.stopPropagation());
  });
}