// ===============================
// CORRECTION.JS - COLUMN MAPPING
// ===============================

console.log("🔧 Correction.js loaded");

document.addEventListener("DOMContentLoaded", function () {
    
    console.log("🚀 Initializing correction page...");
    
    // ===============================
    // ELEMENT REFERENCES
    // ===============================
    const autoDetectBtn = document.getElementById('auto-detect-btn');
    const correctionForm = document.getElementById('correction-form');
    const submitBtn = document.getElementById('submit-btn');
    const statusEl = document.getElementById('mapping-status');
    const allSelects = document.querySelectorAll('.mapping-select');
    
    // Safety check
    if (!correctionForm || !submitBtn || !statusEl) {
        console.error("❌ Required elements not found!");
        return;
    }
    
    console.log(`✅ Found ${allSelects.length} columns to map`);
    
    
    // ===============================
    // CHECK MAPPING STATUS
    // ===============================
    function checkMappingStatus() {
        const selects = document.querySelectorAll('.mapping-select');
        
        const mapped = {
            phone: false,
            email: false,
            name: false
        };
        
        const mappings = {};
        
        selects.forEach(select => {
            const value = select.value;
            const originalCol = select.getAttribute('data-original');
            
            if (value && mapped.hasOwnProperty(value)) {
                // Check for duplicate mappings
                if (mappings[value]) {
                    console.warn(`⚠️ Duplicate mapping: ${value} mapped to both ${mappings[value]} and ${originalCol}`);
                }
                
                mapped[value] = true;
                mappings[value] = originalCol;
                select.classList.add('mapped');
            } else {
                select.classList.remove('mapped');
            }
        });
        
        const mappedCount = Object.values(mapped).filter(v => v).length;
        const isComplete = mappedCount === 3;
        
        // Update status display
        if (statusEl) {
            const statusIcon = statusEl.querySelector('.status-icon');
            const statusText = statusEl.querySelector('.status-text');
            
            if (statusText) {
                statusText.textContent = `${mappedCount} of 3 required columns mapped`;
            }
            
            if (isComplete) {
                statusEl.classList.add('complete');
                if (statusIcon) statusIcon.textContent = '✅';
            } else {
                statusEl.classList.remove('complete');
                if (statusIcon) statusIcon.textContent = '⏳';
            }
        }
        
        // Enable/disable submit button
        if (submitBtn) {
            submitBtn.disabled = !isComplete;
        }
        
        console.log(`📊 Mapping status: ${mappedCount}/3`, mapped);
        
        return { mapped, mappings, isComplete };
    }
    
    
    // ===============================
    // AUTO-DETECT COLUMNS
    // ===============================
    if (autoDetectBtn) {
        autoDetectBtn.addEventListener('click', function() {
            console.log("🤖 Auto-detecting columns...");
            
            const selects = document.querySelectorAll('.mapping-select');
            let autoMappedCount = 0;
            
            // Clear existing mappings first
            selects.forEach(select => {
                select.value = '';
            });
            
            // Apply suggestions based on detected types
            selects.forEach(select => {
                const suggestion = select.getAttribute('data-suggestion');
                const originalCol = select.getAttribute('data-original');
                
                if (suggestion && ['phone', 'email', 'name'].includes(suggestion)) {
                    select.value = suggestion;
                    autoMappedCount++;
                    console.log(`   ✅ Auto-mapped: "${originalCol}" → ${suggestion}`);
                }
            });
            
            checkMappingStatus();
            
            // Show result to user
            if (autoMappedCount > 0) {
                alert(`✅ Auto-detection complete!\n\n${autoMappedCount} column(s) mapped automatically.\n\nPlease review the mappings and click "Apply Corrections" when ready.`);
            } else {
                alert('⚠️ Could not auto-detect any columns.\n\nPlease map the columns manually using the dropdowns.');
            }
        });
    }
    
    
    // ===============================
    // LISTEN TO SELECT CHANGES
    // ===============================
    allSelects.forEach(select => {
        select.addEventListener('change', function() {
            const value = this.value;
            const originalCol = this.getAttribute('data-original');
            
            if (value) {
                console.log(`🔄 Mapped: "${originalCol}" → ${value}`);
            } else {
                console.log(`❌ Unmapped: "${originalCol}"`);
            }
            
            checkMappingStatus();
        });
    });
    
    

    
    
    // ===============================
    // INITIAL CHECK ON PAGE LOAD
    // ===============================
    checkMappingStatus();
    
    console.log("✅ Correction page initialized successfully");
});(function () {

  /* ── 1. Filled-state class on inputs ── */
  document.querySelectorAll('.hdr-input').forEach(function (inp) {
    inp.addEventListener('input', function () {
      inp.classList.toggle('filled', inp.value.trim() !== '');
      inp.classList.remove('invalid');
    });
  });

  /* ── 2. Reset button — clear only the inputs, not hidden fields ── */
  document.getElementById('btn-reset').addEventListener('click', function () {
    document.querySelectorAll('.hdr-input').forEach(function (inp) {
      inp.value = '';
      inp.classList.remove('filled', 'invalid');
    });
  });

  /* ── 3. Client-side validation — highlight empty inputs ── */
  document.getElementById('fix-form').addEventListener('submit', function (e) {
    var empty = [];
    document.querySelectorAll('.hdr-input').forEach(function (inp) {
      if (inp.value.trim() === '') {
        inp.classList.add('invalid');
        empty.push(inp);
      }
    });
    if (empty.length > 0) {
      e.preventDefault();
      empty[0].scrollIntoView({ behavior: 'smooth', block: 'center' });
      empty[0].focus();
    }
  });

  /* ── 4. Column resize ── */
  (function initColResize(table) {
    if (!table) return;
    var startX, startW, th;

    table.querySelectorAll('thead tr.orig-row th:not(.rn)').forEach(function (header) {
      var handle = header.querySelector('.rh');
      if (!handle) return;

      handle.addEventListener('mousedown', function (e) {
        e.preventDefault();
        th = header;
        startX = e.pageX;
        startW = th.offsetWidth;
        th.classList.add('resizing');
        document.addEventListener('mousemove', onMove);
        document.addEventListener('mouseup', onUp);
      });
    });

    function onMove(e) {
      if (!th) return;
      var newW = Math.max(80, startW + (e.pageX - startX));
      th.style.width = newW + 'px';
      th.style.minWidth = newW + 'px';
      /* sync input-row th width */
      var idx = Array.from(th.parentNode.children).indexOf(th);
      var inputTh = table.querySelector('thead tr.input-row th:nth-child(' + (idx + 1) + ')');
      if (inputTh) { inputTh.style.width = newW + 'px'; inputTh.style.minWidth = newW + 'px'; }
    }

    function onUp() {
      if (th) th.classList.remove('resizing');
      th = null;
      document.removeEventListener('mousemove', onMove);
      document.removeEventListener('mouseup', onUp);
    }
  })(document.getElementById('fix-table'));

})();