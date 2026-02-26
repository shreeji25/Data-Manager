/**
 * loading.js — Universal page loading overlay
 * 
 * Covers:
 *   1. Upload form submit         → "Uploading your file…"
 *   2. Dashboard View button      → "Loading dataset…"
 *   3. Dashboard Link button      → "Loading duplicate records…"
 *   4. Relation mode switch       → "Switching mode…"
 *   5. Relation / View pagination → "Loading page…"
 *   6. Relation search            → "Searching…"
 *   7. Clean & Export button      → "Preparing export…"
 *
 * Usage: include ONCE in base.html before </body>
 * The overlay HTML is injected automatically — nothing else needed.
 */

(function () {
    'use strict';

    /* ── Inject overlay HTML into DOM ──────────────────────────── */
    function createOverlay() {
        if (document.getElementById('page-loading-overlay')) return;

        const el = document.createElement('div');
        el.id = 'page-loading-overlay';
        el.setAttribute('role', 'status');
        el.setAttribute('aria-live', 'polite');
        el.setAttribute('aria-label', 'Loading');
        el.innerHTML = `
            <div class="loading-ring"></div>
            <div class="loading-body">
                <p class="loading-title" id="loading-title">Loading…</p>
                <p class="loading-subtitle" id="loading-subtitle">Please wait</p>
            </div>
            <div class="loading-progress" id="loading-progress">
                <div class="loading-progress-track">
                    <div class="loading-progress-fill" id="loading-progress-fill"></div>
                </div>
                <p class="loading-progress-label" id="loading-progress-label">0%</p>
            </div>
        `;
        document.body.appendChild(el);
    }

    /* ── Show / hide helpers ────────────────────────────────────── */
    function showLoading(title, subtitle) {
        const overlay = document.getElementById('page-loading-overlay');
        if (!overlay) return;
        document.getElementById('loading-title').textContent    = title    || 'Loading…';
        document.getElementById('loading-subtitle').textContent = subtitle || 'Please wait';
        document.getElementById('loading-progress').classList.remove('visible');
        overlay.classList.add('active');
    }

    function showLoadingWithProgress(title, subtitle) {
        showLoading(title, subtitle);
        document.getElementById('loading-progress').classList.add('visible');
    }

    function hideLoading() {
        const overlay = document.getElementById('page-loading-overlay');
        if (overlay) overlay.classList.remove('active');
    }

    function updateProgress(pct, label) {
        const fill  = document.getElementById('loading-progress-fill');
        const lbl   = document.getElementById('loading-progress-label');
        if (fill) fill.style.width = Math.min(pct, 100) + '%';
        if (lbl)  lbl.textContent  = label || Math.round(pct) + '%';
    }

    /* expose globally so upload.js can call updateProgress */
    window.LoadingOverlay = { show: showLoading, hide: hideLoading, progress: updateProgress, showWithProgress: showLoadingWithProgress };

    /* ── Bind all triggers after DOM ready ──────────────────────── */
    document.addEventListener('DOMContentLoaded', function () {
        createOverlay();

        /* ────────────────────────────────────────────────────────
           1. UPLOAD FORM — /upload  (upload.html)
           Shows progress bar since upload can take minutes.
        ──────────────────────────────────────────────────────── */
        const uploadForm = document.getElementById('uploadForm');
        const uploadBtn  = document.getElementById('upload-btn');

        if (uploadForm && uploadBtn) {
            uploadBtn.addEventListener('click', function () {
                // Let upload.js validate first — only show overlay if form is valid
                setTimeout(function () {
                    const fileInput = document.getElementById('file-input');
                    const category  = document.getElementById('category-select');
                    if (fileInput && fileInput.files.length > 0 &&
                        category  && category.value) {
                        const fileMB = (fileInput.files[0].size / 1024 / 1024).toFixed(1);
                        showLoadingWithProgress(
                            'Uploading your file…',
                            fileMB + ' MB — this may take a moment for large files'
                        );
                        // Fake smooth progress (real XHR progress needs upload.js changes)
                        simulateProgress(0, 90, 8000);
                    }
                }, 100);
            });
        }

        /* ────────────────────────────────────────────────────────
           2. DASHBOARD — View button  (href="/view/ID")
        ──────────────────────────────────────────────────────── */
        document.querySelectorAll('a.btn-view, a[href^="/view/"]').forEach(function (el) {
            el.addEventListener('click', function () {
                showLoading('Loading dataset…', 'Reading file and preparing data');
            });
        });

        /* ────────────────────────────────────────────────────────
           3. DASHBOARD — Relations link  (href="/dataset/ID/relations")
        ──────────────────────────────────────────────────────── */
        document.querySelectorAll('a.btn-relations, a[href*="/relations"]').forEach(function (el) {
            el.addEventListener('click', function () {
                showLoading('Loading duplicate records…', 'Analysing phone and email matches');
            });
        });

        /* ────────────────────────────────────────────────────────
           4. RELATION PAGE — mode switch (stat cards)
           Handles both old class (dup-stat-card) and current (rel-metric-card)
        ──────────────────────────────────────────────────────── */
        document.querySelectorAll('a.dup-stat-card, a.rel-metric-card').forEach(function (el) {
            el.addEventListener('click', function () {
                const mode = (new URL(el.href)).searchParams.get('mode') || 'combined';
                const labels = {
                    combined: 'Loading combined duplicates…',
                    phone:    'Loading phone duplicates…',
                    email:    'Loading email duplicates…',
                };
                showLoading(
                    labels[mode] || 'Switching mode…',
                    'Fetching records from server'
                );
            });
        });

        /* ────────────────────────────────────────────────────────
           4b. RELATION PAGE — pagination (rel-pg-btn)
        ──────────────────────────────────────────────────────── */
        document.querySelectorAll('a.rel-pg-btn:not(.disabled)').forEach(function (el) {
            el.addEventListener('click', function () {
                showLoading('Loading page…', 'Fetching records');
            });
        });

        /* ────────────────────────────────────────────────────────
           4c. RELATION PAGE — Clean & Export (rel-btn-export)
        ──────────────────────────────────────────────────────── */
        document.querySelectorAll('a.rel-btn-export').forEach(function (el) {
            el.addEventListener('click', function () {
                showLoading('Preparing export…', 'Cleaning duplicates and generating file');
            });
        });

        /* ────────────────────────────────────────────────────────
           4d. RELATION PAGE — Back to Dashboard (rel-btn-back)
        ──────────────────────────────────────────────────────── */
        document.querySelectorAll('a.rel-btn-back').forEach(function (el) {
            el.addEventListener('click', function () {
                showLoading('Loading dashboard…', 'Please wait');
            });
        });

        /* ────────────────────────────────────────────────────────
           5. PAGINATION — any .page-btn link (relation + view pages)
        ──────────────────────────────────────────────────────── */
        document.querySelectorAll('a.page-btn:not(.disabled)').forEach(function (el) {
            el.addEventListener('click', function () {
                showLoading('Loading page…', 'Fetching records');
            });
        });

        /* ────────────────────────────────────────────────────────
           6. RELATION SEARCH FORM
        ──────────────────────────────────────────────────────── */
        document.querySelectorAll('form.relation-search-inline').forEach(function (form) {
            form.addEventListener('submit', function () {
                showLoading('Searching…', 'Filtering duplicate records');
            });
        });

        /* ────────────────────────────────────────────────────────
           7. CLEAN & EXPORT BUTTON  (href="/export/clean-relations/ID")
        ──────────────────────────────────────────────────────── */
        document.querySelectorAll('a.clean-btn, a[href*="clean-relations"]').forEach(function (el) {
            el.addEventListener('click', function () {
                showLoading('Preparing export…', 'Cleaning duplicates and generating file');
            });
        });

        /* ────────────────────────────────────────────────────────
           8. VIEW PAGE — export buttons
        ──────────────────────────────────────────────────────── */
        document.querySelectorAll('a[href*="/export/"]').forEach(function (el) {
            // skip clean-relations (already handled above)
            if (el.href.includes('clean-relations')) return;
            el.addEventListener('click', function () {
                showLoading('Preparing export…', 'Generating download file');
            });
        });

        /* ────────────────────────────────────────────────────────
           9. VIEW PAGE — filter/search form submit
        ──────────────────────────────────────────────────────── */
        const filterForm = document.getElementById('filterForm');
        if (filterForm) {
            filterForm.addEventListener('submit', function () {
                showLoading('Applying filters…', 'Fetching matching datasets');
            });
        }

        /* ────────────────────────────────────────────────────────
           SAFETY: hide overlay when page becomes visible
           (handles browser back button — overlay would stay stuck)
        ──────────────────────────────────────────────────────── */
        window.addEventListener('pageshow', function (e) {
            if (e.persisted) hideLoading();
        });
    });

    /* ── Smooth fake progress bar ───────────────────────────────── */
    function simulateProgress(from, to, durationMs) {
        const steps    = 60;
        const interval = durationMs / steps;
        let   current  = from;
        const increment = (to - from) / steps;

        const timer = setInterval(function () {
            current += increment + (Math.random() * increment * 0.4);
            if (current >= to) {
                current = to;
                clearInterval(timer);
            }
            updateProgress(current, Math.round(current) + '%');
        }, interval);

        // Store timer so upload.js can clear it when real progress is known
        window._loadingProgressTimer = timer;
    }

})();