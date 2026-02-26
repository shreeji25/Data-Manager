/**
 * cross_relation.js
 * Handles all interactivity for the Cross-File Relations page.
 * - Dropdown toggles (user + file selectors)
 * - Mode switching (combined / phone / email)
 * - Live search filter
 * - Sort
 * - Card expand/collapse with lazy AJAX record loading
 * - Filter apply (re-fetches from server with selected file IDs)
 * - Reset filters
 */

/* ── STATE ── */
let currentMode   = 'combined';
let currentSort   = 'phone';
let loadedCards   = new Set();   // track which cards have been AJAX-loaded

/* ── PAGE LOADER ── */
function showPageLoader(msg = 'Loading...') {
  const loader = document.getElementById('crf-page-loader');
  const text   = document.getElementById('crf-loader-text');
  if (loader) { loader.classList.add('active'); }
  if (text)   { text.textContent = msg; }
  // Disable interactive buttons so user can't double-trigger
  document.querySelectorAll('.crf-sort-btn, .crf-apply-btn, .crf-reset-btn')
    .forEach(b => b.classList.add('crf-btn-loading'));
}

function hidePageLoader() {
  const loader = document.getElementById('crf-page-loader');
  if (loader) loader.classList.remove('active');
  document.querySelectorAll('.crf-sort-btn, .crf-apply-btn, .crf-reset-btn')
    .forEach(b => b.classList.remove('crf-btn-loading'));
}

/* ── SKELETON CARDS (shown while navigating) ── */
function showSkeletonCards(count = 5) {
  const list = document.getElementById('crf-groups-list');
  if (!list) return;
  // Hide real cards and empty states
  list.querySelectorAll('.crf-group-card, .crf-empty-state').forEach(el => el.style.display = 'none');
  // Remove any existing skeleton
  const old = list.querySelector('.crf-skeleton-list');
  if (old) old.remove();

  const skeletonList = document.createElement('div');
  skeletonList.className = 'crf-skeleton-list';
  for (let i = 0; i < count; i++) {
    skeletonList.innerHTML += `
      <div class="crf-skeleton-card">
        <div class="crf-skeleton-num"></div>
        <div class="crf-skeleton-body">
          <div class="crf-skeleton-line long"></div>
          <div class="crf-skeleton-line medium"></div>
        </div>
        <div class="crf-skeleton-right">
          <div class="crf-skeleton-pill"></div>
          <div class="crf-skeleton-pill"></div>
          <div class="crf-skeleton-pill"></div>
          <div class="crf-skeleton-badge"></div>
        </div>
      </div>`;
  }
  list.appendChild(skeletonList);
}

/* ── INIT ── */
document.addEventListener('DOMContentLoaded', () => {
  // Initialize mode from URL params or meta data
  const params = new URLSearchParams(window.location.search);
  const urlMode = params.get('mode') || 'combined';
  
  // If current mode has no visible cards, auto-switch to first mode with cards
  const hasCards = document.querySelectorAll(`.crf-group-card[data-mode="${urlMode}"]`).length > 0;
  if (!hasCards) {
    const availableModes = ['combined', 'phone', 'email'];
    for (const mode of availableModes) {
      if (document.querySelectorAll(`.crf-group-card[data-mode="${mode}"]`).length > 0) {
        currentMode = mode;
        break;
      }
    }
  } else {
    currentMode = urlMode;
  }
  
  // Sync filter bar checkboxes to active file_ids in the URL
  syncCheckboxesToUrl();

  updateVisibleCount();
  updateActiveMode();
  filterCards();  // Apply mode filter to show correct cards
  // Close dropdowns on outside click
  document.addEventListener('click', (e) => {
    if (!e.target.closest('.crf-dropdown-wrap')) {
      closeAllDropdowns();
    }
  });

  // Pagination: intercept clicks and rebuild URL preserving all active filters
  document.querySelectorAll('.crf-page-btn[href]').forEach(link => {
    link.addEventListener('click', (e) => {
      e.preventDefault();
      const targetHref = link.getAttribute('href');
      const targetPage = new URLSearchParams(targetHref.includes('?') ? targetHref.split('?')[1] : '').get('page') || '1';

      // Start from current URL so file_ids, mode, cross_user are preserved
      const params = new URLSearchParams(window.location.search);
      params.set('page', targetPage);
      // Always reflect the current client-side mode in case user switched tabs
      params.set('mode', currentMode);

      showPageLoader('Loading page...');
      showSkeletonCards(5);
      window.location.href = window.location.pathname + '?' + params.toString();
    });
  });

  // Hide loader if user navigates back (bfcache restore)
  window.addEventListener('pageshow', (e) => {
    if (e.persisted) hidePageLoader();
  });
});

/* ════════════════════════════════════════
   DROPDOWN TOGGLES
════════════════════════════════════════ */
function toggleDropdown(type) {
  const panel = document.getElementById(`${type}-dropdown-panel`);
  const isOpen = panel.classList.contains('open');
  closeAllDropdowns();
  if (!isOpen) panel.classList.add('open');
}

function closeAllDropdowns() {
  document.querySelectorAll('.crf-dropdown-panel').forEach(p => p.classList.remove('open'));
}

/* ── Sync filter checkboxes to URL file_ids on page load ── */
function syncCheckboxesToUrl() {
  const params    = new URLSearchParams(window.location.search);
  const urlFileIds = params.getAll('file_ids');
  if (urlFileIds.length === 0) return; // no filter active — leave all checked

  const fileIdSet = new Set(urlFileIds.map(String));

  // Check only the files that are in the URL
  document.querySelectorAll('.file-checkbox').forEach(cb => {
    cb.checked = fileIdSet.has(String(cb.value));
  });

  // Update master file checkbox state
  const allFiles     = document.querySelectorAll('.file-checkbox:not([disabled])');
  const checkedFiles = document.querySelectorAll('.file-checkbox:checked');
  const masterFiles  = document.getElementById('select-all-files');
  if (masterFiles) {
    masterFiles.indeterminate = checkedFiles.length > 0 && checkedFiles.length < allFiles.length;
    masterFiles.checked = checkedFiles.length === allFiles.length;
  }

  // User checkbox = checked if ANY of their files are checked
  document.querySelectorAll('.crf-file-user-group').forEach(group => {
    const userCb     = document.querySelector(`.user-checkbox[value="${group.dataset.userId}"]`);
    const anyChecked = Array.from(group.querySelectorAll('.file-checkbox')).some(cb => cb.checked);
    if (userCb) userCb.checked = anyChecked;
    // Grey out rows for unchecked users
    group.querySelectorAll('.crf-file-row').forEach(row => {
      row.style.opacity = anyChecked ? '1' : '0.35';
    });
  });

  // Update master user checkbox state
  const allUsers     = document.querySelectorAll('.user-checkbox');
  const checkedUsers = document.querySelectorAll('.user-checkbox:checked');
  const masterUsers  = document.getElementById('select-all-users');
  if (masterUsers) {
    masterUsers.indeterminate = checkedUsers.length > 0 && checkedUsers.length < allUsers.length;
    masterUsers.checked = checkedUsers.length === allUsers.length;
  }
}

/* ── Select All: Users ── */
function toggleAllUsers(masterCb) {
  document.querySelectorAll('.user-checkbox').forEach(cb => {
    cb.checked = masterCb.checked;
  });
  // Sync file checkboxes visibility
  syncFilesToUsers();
}

/* ── Select All: Files ── */
function toggleAllFiles(masterCb) {
  document.querySelectorAll('.file-checkbox:not([disabled])').forEach(cb => {
    cb.checked = masterCb.checked;
  });
}

/* When user checkboxes change, grey-out files belonging to unchecked users */
function onUserChange() {
  syncFilesToUsers();
  // Update master checkbox state
  const all = document.querySelectorAll('.user-checkbox');
  const checked = document.querySelectorAll('.user-checkbox:checked');
  const master = document.getElementById('select-all-users');
  if (master) {
    master.indeterminate = checked.length > 0 && checked.length < all.length;
    master.checked = checked.length === all.length;
  }
}

function syncFilesToUsers() {
  document.querySelectorAll('.crf-file-user-group').forEach(group => {
    const userId = group.dataset.userId;
    const userCb = document.querySelector(`.user-checkbox[value="${userId}"]`);
    const fileRows = group.querySelectorAll('.crf-file-row');
    const fileCbs  = group.querySelectorAll('.file-checkbox');
    const active = userCb ? userCb.checked : true;
    fileRows.forEach(row => row.style.opacity = active ? '1' : '0.35');
    fileCbs.forEach(cb => {
      cb.disabled = !active;
      if (!active) cb.checked = false;
    });
  });
}

function onFileChange() {
  const all     = document.querySelectorAll('.file-checkbox:not([disabled])');
  const checked = document.querySelectorAll('.file-checkbox:checked');
  const master  = document.getElementById('select-all-files');
  if (master) {
    master.indeterminate = checked.length > 0 && checked.length < all.length;
    master.checked = checked.length === all.length;
  }
}

/* ════════════════════════════════════════
   APPLY FILTERS (re-fetch from server)
════════════════════════════════════════ */
function applyFilters() {
  closeAllDropdowns();

  const selectedFiles = Array.from(
    document.querySelectorAll('.file-checkbox:checked')
  ).map(cb => cb.value);

  // Need at least 2 files
  if (selectedFiles.length < 2) {
    showMinFilesMessage(true);
    return;
  }
  showMinFilesMessage(false);

  const crossUserOnly = document.getElementById('cross-user-toggle')?.checked || false;

  // Build query params
  const params = new URLSearchParams();
  selectedFiles.forEach(id => params.append('file_ids', id));
  params.set('mode', currentMode);
  params.set('cross_user', crossUserOnly);
  params.set('page', '1');

  // Show loading state before navigation
  showPageLoader('Applying filters...');
  showSkeletonCards(5);

  // Update URL and reload
  window.location.href = `${window.location.pathname}?${params.toString()}`;
}

function showMinFilesMessage(show) {
  const msg    = document.getElementById('crf-min-files-msg');
  const list   = document.getElementById('crf-groups-list');
  const noRes  = document.getElementById('crf-no-results');
  if (msg) msg.style.display = show ? 'block' : 'none';
  if (noRes) noRes.style.display = 'none';
  // Hide all group cards
  document.querySelectorAll('.crf-group-card').forEach(card => {
    card.style.display = show ? 'none' : '';
  });
}

/* ════════════════════════════════════════
   MODE SWITCHING (combined / phone / email)
════════════════════════════════════════ */
function setMode(mode) {
  currentMode = mode;
  updateActiveMode();

  // Server only renders cards for the active mode (paginated server-side).
  // If the requested mode has no cards in the DOM, navigate to reload from server.
  const hasCardsInDom = document.querySelectorAll(`.crf-group-card[data-mode="${mode}"]`).length > 0;

  if (hasCardsInDom) {
    filterCards();
    const params = new URLSearchParams(window.location.search);
    params.set('mode', mode);
    history.replaceState(null, '', `${window.location.pathname}?${params.toString()}`);
  } else {
    // Navigate to server so it renders cards for this mode
    showPageLoader('Switching to ' + mode + ' mode...');
    showSkeletonCards(5);
    const params = new URLSearchParams(window.location.search);
    params.set('mode', mode);
    params.set('page', '1');
    window.location.href = `${window.location.pathname}?${params.toString()}`;
  }
}

function updateActiveMode() {
  ['combined', 'phone', 'email'].forEach(m => {
    const card = document.getElementById(`mode-${m}`);
    if (card) card.classList.toggle('active', m === currentMode);
  });
}

/* ════════════════════════════════════════
   SORT
════════════════════════════════════════ */
function setSort(sortKey) {
  currentSort = sortKey;
  document.querySelectorAll('.crf-sort-btn').forEach(b => b.classList.remove('active'));
  const btn = document.getElementById(`sort-${sortKey}`);
  if (btn) btn.classList.add('active');
  sortCards();
}

function sortCards() {
  const list  = document.getElementById('crf-groups-list');
  const cards = Array.from(list.querySelectorAll('.crf-group-card'));

  cards.sort((a, b) => {
    switch (currentSort) {
      case 'phone':
        return (a.dataset.phone || '').localeCompare(b.dataset.phone || '');
      case 'email':
        return (a.dataset.email || '').localeCompare(b.dataset.email || '');
      case 'count':
        return parseInt(b.dataset.count) - parseInt(a.dataset.count);
      case 'users':
        return (b.dataset.users?.split(',').length || 0) -
               (a.dataset.users?.split(',').length || 0);
      default:
        return 0;
    }
  });

  // Re-append in sorted order
  cards.forEach(card => list.appendChild(card));
  renumberCards();
}

/* ════════════════════════════════════════
   LIVE SEARCH
════════════════════════════════════════ */
function liveSearch(query) {
  query = query.toLowerCase().trim();
  filterCards(query);
}

/* ════════════════════════════════════════
   FILTER CARDS (by mode + search)
════════════════════════════════════════ */
function filterCards(searchQuery = '') {
  const cards   = document.querySelectorAll('.crf-group-card');
  let   visible = 0;

  cards.forEach(card => {
    const modeMatch   = card.dataset.mode === currentMode;
    const phone       = (card.dataset.phone || '').toLowerCase();
    const email       = (card.dataset.email || '').toLowerCase();
    const searchMatch = !searchQuery ||
                        phone.includes(searchQuery) ||
                        email.includes(searchQuery);

    const show = modeMatch && searchMatch;
    card.style.display = show ? '' : 'none';
    if (show) visible++;
  });

  document.getElementById('visible-count').textContent = visible;

  const noResults = document.getElementById('crf-no-results');
  if (noResults) noResults.style.display = visible === 0 ? 'block' : 'none';

  renumberCards();
}

function renumberCards() {
  let n = 1;
  document.querySelectorAll('.crf-group-card:not([style*="display: none"])').forEach(card => {
    const numEl = card.querySelector('.crf-card-num');
    if (numEl) numEl.textContent = String(n++).padStart(2, '0');
  });
}

/* ════════════════════════════════════════
   CARD TOGGLE + LAZY AJAX LOAD
════════════════════════════════════════ */
function toggleCard(headerEl) {
  const card  = headerEl.closest('.crf-group-card');
  const body  = card.querySelector('.crf-card-body');
  const index = card.dataset.index;
  const isOpen = card.classList.contains('crf-card-open');

  if (isOpen) {
    card.classList.remove('crf-card-open');
    body.style.display = 'none';
    return;
  }

  card.classList.add('crf-card-open');
  body.style.display = 'block';

  // Lazy load if not already loaded
  if (!loadedCards.has(index)) {
    loadedCards.add(index);
    loadCardRecords(card, index);
  }
}

async function loadCardRecords(card, index) {
  const inner  = document.getElementById(`card-inner-${index}`);
  const phone  = card.dataset.phone || '';
  const email  = card.dataset.email || '';
  const fileIds = card.dataset.files || '';

  try {
    const params = new URLSearchParams();
    if (phone)   params.set('phone', phone);
    if (email)   params.set('email', email);
    if (fileIds) params.set('file_ids', fileIds);
    params.set('mode', card.dataset.mode);

    const resp = await fetch(`/cross-relations/records?${params.toString()}`);
    if (!resp.ok) throw new Error('Failed to load records');

    const data = await resp.json();
    inner.innerHTML = buildCardBody(data, card.dataset.mode);

  } catch (err) {
    inner.innerHTML = `<div class="crf-card-loading" style="color:#ef4444">
      Failed to load records. Please try again.</div>`;
  }
}

function buildCardBody(data, mode) {
  if (!data.file_groups || data.file_groups.length === 0) {
    return `<div class="crf-card-loading">No records found.</div>`;
  }

  let html = '';

  data.file_groups.forEach(group => {
    // Darken the color for text readability on header
    const headerColor = group.color || '#334155';

    html += `
    <div class="crf-file-block">
      <div class="crf-file-block-header" style="background:${headerColor}">
        <span class="crf-file-icon">📄</span>
        <span class="crf-file-block-fname">${escHtml(group.file_name)}</span>
        ${group.user_name ? `
        <span class="crf-file-block-user">
          <span>👤</span> ${escHtml(group.user_name)}
        </span>` : ''}
      </div>
      <div class="crf-mini-table-wrap">
        <table class="crf-mini-table">
          <thead>
            <tr>
              ${group.columns.map(c => `<th>${escHtml(c)}</th>`).join('')}
            </tr>
          </thead>
          <tbody>
            ${group.records.map(row => `
              <tr>
                ${group.columns.map(col => {
                  const val  = row[col] ?? '—';
                  const isPhone = col === group.phone_col;
                  const isEmail = col === group.email_col;
                  const cls = isPhone ? 'crf-cell-match-phone'
                            : isEmail ? 'crf-cell-match-email' : '';
                  return `<td class="${cls}" title="${escHtml(String(val))}">${escHtml(String(val))}</td>`;
                }).join('')}
              </tr>
            `).join('')}
          </tbody>
        </table>
      </div>
      <div class="crf-file-record-count">
        ${group.records.length} record${group.records.length !== 1 ? 's' : ''} in this file
      </div>
    </div>`;
  });

  return html;
}

/* ════════════════════════════════════════
   RESET FILTERS
════════════════════════════════════════ */
function resetFilters() {
  // Navigate to bare page — drops all file_ids/mode/page params so
  // the server returns the full unfiltered result set fresh.
  showPageLoader('Resetting filters...');
  window.location.href = window.location.pathname;
}

/* ════════════════════════════════════════
   UPDATE VISIBLE COUNT
════════════════════════════════════════ */
function updateVisibleCount() {
  const visible = document.querySelectorAll(
    `.crf-group-card[data-mode="${currentMode}"]`
  ).length;
  const el = document.getElementById('visible-count');
  if (el) el.textContent = visible;
}

/* ── UTILS ── */
function escHtml(str) {
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}
const _crfResizeObserver = new MutationObserver((mutations) => {
  mutations.forEach(m => {
    m.addedNodes.forEach(node => {
      if (!node.querySelectorAll) return;
      node.querySelectorAll('.crf-mini-table').forEach(t => initColResize(t, 80));
      if (node.classList && node.classList.contains('crf-mini-table')) {
        initColResize(node, 80);
      }
    });
  });
});
_crfResizeObserver.observe(document.body, { childList: true, subtree: true });

