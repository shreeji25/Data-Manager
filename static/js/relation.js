/**
 * relation.js — Dashboard Card UI
 *
 * Features:
 *  - Client-side search (live filter on phone/email)
 *  - Client-side sort (phone / email / count)
 *  - Reset Filters button (turns red when active)
 *  - Inline card expand with lazy AJAX record loading
 *  - Inner per-card sort chips (by any column)
 *  - No modal — everything inline
 */

console.log("Relation JS loaded ✅");

// ── STATE ──────────────────────────────────────────────────────
const REL_DEFAULT_SORT = { key: "count", dir: "desc" };
const relSort  = { key: "count", dir: "desc" };
let   relSearch = "";

// track which cards have already had records loaded
const relLoadedCards = new Set();


// ── DOM READY ──────────────────────────────────────────────────
document.addEventListener("DOMContentLoaded", () => {
    relInitSortUI();
    relRenumber();
    relUpdateResetBtn();

    // If search was pre-populated from server (Jinja value attr),
    // apply it client-side too so card visibility is correct
    const input = document.getElementById("relSearchInput");
    if (input && input.value.trim()) {
        relHandleSearch(input.value);
    }
});


// ══════════════════════════════════════════════════════════════
// CARD TOGGLE + LAZY LOAD
// ══════════════════════════════════════════════════════════════

function relToggleCard(card) {
    const isExpanded = card.classList.contains("expanded");

    if (!isExpanded) {
        // expand
        card.classList.add("expanded");
        // lazy load if not yet loaded
        if (!relLoadedCards.has(card)) {
            relLoadCardRecords(card);
        }
    } else {
        card.classList.remove("expanded");
    }

    relRenumber();
}

function relLoadCardRecords(card) {
    const datasetId = card.dataset.datasetId;
    const phone     = card.dataset.phone  || "";
    const email     = card.dataset.email  || "";
    const grid      = card.querySelector(".rel-records-grid");

    if (!grid) return;

    // show loading state
    grid.innerHTML = `
        <div class="rel-card-loading">
            <div class="rel-spinner"></div>
            <span>Loading records…</span>
        </div>`;

    const url = `/get-duplicate-records/${datasetId}`
        + `?phone=${encodeURIComponent(phone)}`
        + `&email=${encodeURIComponent(email)}`;

    fetch(url)
        .then(res => res.json())
        .then(data => {
            if (data.error) {
                grid.innerHTML = `<div class="rel-card-error">⚠️ ${data.error}</div>`;
                return;
            }
            if (!data.records || data.records.length === 0) {
                grid.innerHTML = `<div class="rel-card-error">No records found.</div>`;
                return;
            }

            relRenderRecords(card, grid, data);
            relLoadedCards.add(card);
        })
        .catch(err => {
            console.error("Fetch error:", err);
            grid.innerHTML = `<div class="rel-card-error">⚠️ Failed to load records.</div>`;
        });
}

function relRenderRecords(card, grid, data) {
    const columns = data.columns.filter(c => c !== "__is_duplicate__");

    // Hide sort bar — inner sorting removed
    const sortBar = card.querySelector(".rel-inner-sort-bar");
    if (sortBar) sortBar.style.display = "none";

    // Determine which columns are the "match" columns so we can highlight them
    const phoneCol = (data.phone_column || "").toLowerCase();
    const emailCol = (data.email_column || "").toLowerCase();

    // Build a proper dashboard-style table
    const wrapper = document.createElement("div");
    wrapper.className = "rel-table-wrapper";

    const table = document.createElement("table");
    table.className = "rel-data-table";

    // Header row
    const thead = document.createElement("thead");
    const headerRow = document.createElement("tr");
    columns.forEach(col => {
        const th = document.createElement("th");
        const colLower = col.toLowerCase();
        const isMatch = colLower === phoneCol || colLower === emailCol;
        if (isMatch) th.classList.add("rel-col-match");
        th.title = col.toUpperCase();
        th.textContent = col.toUpperCase();
        headerRow.appendChild(th);
    });
    thead.appendChild(headerRow);
    table.appendChild(thead);

    // Body rows
    const tbody = document.createElement("tbody");
    data.records.forEach(row => {
        const tr = document.createElement("tr");
        tr.className = "rel-dup-row";
        columns.forEach(col => {
            const td = document.createElement("td");
            const colLower = col.toLowerCase();
            const isMatch = colLower === phoneCol || colLower === emailCol;
            if (isMatch) td.classList.add("rel-col-match");
            const val = (row[col] || "").toString().trim();
            td.title = val || "—";
            td.textContent = val || "—";
            if (!val) td.classList.add("rel-cell-empty");
            tr.appendChild(td);
        });
        tbody.appendChild(tr);
    });
    table.appendChild(tbody);
    wrapper.appendChild(table);

    // Footer: record count
    const footer = document.createElement("div");
    footer.className = "rel-table-footer";
    footer.textContent = `Showing ${data.records.length} duplicate record${data.records.length !== 1 ? "s" : ""}`;

    grid.innerHTML = "";
    grid.appendChild(wrapper);
    grid.appendChild(footer);
}


// ══════════════════════════════════════════════════════════════
// INNER SORT (per-card)
// ══════════════════════════════════════════════════════════════



// ══════════════════════════════════════════════════════════════
// OUTER SORT
// ══════════════════════════════════════════════════════════════

function relSetSort(key, btn) {
    if (relSort.key === key) {
        relSort.dir = relSort.dir === "asc" ? "desc" : "asc";
    } else {
        relSort.key = key;
        relSort.dir = key === "count" ? "desc" : "asc";
    }
    relSyncSortUI();
    relSortCards();
    relUpdateResetBtn();
}

function relInitSortUI() {
    relSyncSortUI();
}

function relSyncSortUI() {
    document.querySelectorAll(".rel-sort-chip").forEach(b => {
        b.classList.remove("active");
        b.querySelector(".rel-arrow").textContent = "↕";
    });
    const active = document.querySelector(`.rel-sort-chip[data-sort="${relSort.key}"]`);
    if (active) {
        active.classList.add("active");
        active.querySelector(".rel-arrow").textContent = relSort.dir === "asc" ? "↑" : "↓";
    }
}

function relSortCards() {
    const list  = document.getElementById("relCardsList");
    if (!list) return;
    const cards = Array.from(list.querySelectorAll(".rel-dup-card"));
    const asc   = relSort.dir === "asc";

    cards.sort((a, b) => {
        if (relSort.key === "count") {
            const na = parseInt(a.dataset.count) || 0;
            const nb = parseInt(b.dataset.count) || 0;
            return asc ? na - nb : nb - na;
        }
        const va = (a.dataset[relSort.key] || "").toLowerCase();
        const vb = (b.dataset[relSort.key] || "").toLowerCase();
        if (!va && vb)  return 1;
        if (va  && !vb) return -1;
        return asc ? va.localeCompare(vb) : vb.localeCompare(va);
    });

    cards.forEach(c => list.appendChild(c));
    relRenumber();
}


// ══════════════════════════════════════════════════════════════
// SEARCH (client-side live filter)
// ══════════════════════════════════════════════════════════════

function relHandleSearch(q) {
    relSearch = q.toLowerCase().trim();
    let shown = 0;

    document.querySelectorAll(".rel-dup-card").forEach(c => {
        const phone = (c.dataset.phone || "").toLowerCase();
        const email = (c.dataset.email || "").toLowerCase();
        const match = !relSearch || phone.includes(relSearch) || email.includes(relSearch);
        c.style.display = match ? "" : "none";
        if (match) shown++;
    });

    const label = document.getElementById("relResultsLabel");
    if (label) label.textContent = shown + " group" + (shown !== 1 ? "s" : "");

    relRenumber();
    relUpdateResetBtn();
}


// ══════════════════════════════════════════════════════════════
// RESET FILTERS
// ══════════════════════════════════════════════════════════════

function relResetFilters() {
    // clear search
    relSearch = "";
    const input = document.getElementById("relSearchInput");
    if (input) input.value = "";

    // show all cards
    document.querySelectorAll(".rel-dup-card").forEach(c => c.style.display = "");

    // restore default sort
    relSort.key = REL_DEFAULT_SORT.key;
    relSort.dir = REL_DEFAULT_SORT.dir;
    relSyncSortUI();
    relSortCards();

    // update results label
    const allCards = document.querySelectorAll(".rel-dup-card").length;
    const label = document.getElementById("relResultsLabel");
    if (label) label.textContent = allCards + " group" + (allCards !== 1 ? "s" : "");

    relUpdateResetBtn();
}

function relUpdateResetBtn() {
    const btn = document.getElementById("relResetBtn");
    if (!btn) return;
    const isDefault = relSort.key === REL_DEFAULT_SORT.key
                   && relSort.dir === REL_DEFAULT_SORT.dir
                   && relSearch === "";
    btn.classList.toggle("has-filters", !isDefault);
}


// ══════════════════════════════════════════════════════════════
// UTILITY
// ══════════════════════════════════════════════════════════════

function relRenumber() {
    let i = 1;
    document.querySelectorAll(".rel-dup-card").forEach(c => {
        if (c.style.display !== "none") {
            const idx = c.querySelector(".rel-card-idx");
            if (idx) idx.textContent = String(i++).padStart(2, "0");
        }
    });
}

const _relResizeObserver = new MutationObserver((mutations) => {
  mutations.forEach(m => {
    m.addedNodes.forEach(node => {
      if (!node.querySelectorAll) return;
      // Newly injected table
      node.querySelectorAll('.rel-data-table').forEach(t => initColResize(t, 80));
      // If the node itself is the table
      if (node.classList && node.classList.contains('rel-data-table')) {
        initColResize(node, 80);
      }
    });
  });
});
_relResizeObserver.observe(document.body, { childList: true, subtree: true });