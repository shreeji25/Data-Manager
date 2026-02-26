// ===============================================
// UPLOAD.JS - BULLETPROOF VERSION
// Data Management System
// ===============================================

console.log("🚀 Upload.js loaded");

let previewTable;
let previewSection;
let previewCount;

document.addEventListener("DOMContentLoaded", function () {
    console.log("✅ DOM Content Loaded");

    // ================= ELEMENT REFERENCES =================
    const fileInput = document.getElementById("file-input");
    const dropZone = document.getElementById("drop-zone");
    const uploadBtn = document.getElementById("upload-btn");
    const cancelBtn = document.getElementById("cancel-btn");
    const removeFileBtn = document.getElementById("remove-file-btn");
    
    const fileCard = document.getElementById("file-card");
    const fileNameDisplay = document.getElementById("file-name-display");
    const fileSizeDisplay = document.getElementById("file-size");
    const fileTypeDisplay = document.getElementById("file-type");
    const fileStatus = document.getElementById("file-status");
    
    const progressWrapper = document.getElementById("progress-wrapper");
    const progressBar = document.getElementById("progress-bar");
    const progressText = document.getElementById("progress-text");
    const progressStatus = document.getElementById("progress-status");
    
    previewSection = document.getElementById("preview-section");
    previewTable = document.getElementById("preview-table");
    previewCount = document.getElementById("preview-count");

    const missingColumns = document.getElementById("missing-columns");
    const missingInputs = document.getElementById("missing-inputs");
    
    const metadataSection = document.getElementById("metadata-section");
    const categorySelect = document.getElementById("category-select");
    const descriptionInput = document.getElementById("description-input");
    
    const actionBar = document.getElementById("action-bar");
    const autoDetect = document.getElementById("auto-detect");
    const detectList = document.getElementById("detect-list");
    
    const toast = document.getElementById("toast");
    const toastIcon = document.getElementById("toast-icon");
    const toastMessage = document.getElementById("toast-message");

    let selectedFile = null;
    let isUploading = false;

    // ================= CRITICAL ELEMENT CHECK =================
    console.log("Checking elements:");
    console.log("- fileInput:", fileInput ? "✅" : "❌");
    console.log("- dropZone:", dropZone ? "✅" : "❌");
    console.log("- uploadBtn:", uploadBtn ? "✅" : "❌");

    if (!fileInput || !dropZone || !uploadBtn) {
        console.error("❌ Critical elements missing!");
        return;
    }

    console.log("✅ All critical elements found");

    // ================= FILE INPUT CHANGE EVENT =================
    // Use multiple event types to ensure it triggers
    fileInput.addEventListener("change", handleFileInputChange);
    fileInput.addEventListener("input", handleFileInputChange);
    
    function handleFileInputChange(e) {
        console.log("🔔 File input change event fired!", e.type);
        console.log("Files object:", fileInput.files);
        console.log("Files count:", fileInput.files ? fileInput.files.length : 0);

        if (!fileInput.files || fileInput.files.length === 0) {
            console.log("⚠️ No files selected");
            return;
        }

        const file = fileInput.files[0];
        console.log("📄 File details:", {
            name: file.name,
            size: file.size,
            type: file.type
        });

        handleFileSelect(file);
    }

    // ================= DROP ZONE EVENTS =================
    if (dropZone) {
        // Prevent default behavior for all drag events
        ['dragenter', 'dragover', 'dragleave', 'drop'].forEach(eventName => {
            dropZone.addEventListener(eventName, function(e) {
                e.preventDefault();
                e.stopPropagation();
            }, false);
        });

        dropZone.addEventListener("dragover", function(e) {
            dropZone.classList.add("drag-over");
            console.log("Dragging over");
        });

        dropZone.addEventListener("dragleave", function(e) {
            dropZone.classList.remove("drag-over");
        });

        dropZone.addEventListener("drop", function(e) {
            console.log("📦 File dropped");
            dropZone.classList.remove("drag-over");

            if (e.dataTransfer.files.length > 0) {
                const file = e.dataTransfer.files[0];
                console.log("File from drop:", file.name);
                
                // Manually set to input
                const dataTransfer = new DataTransfer();
                dataTransfer.items.add(file);
                fileInput.files = dataTransfer.files;
                
                handleFileSelect(file);
            }
        });
    }

    // ================= HANDLE FILE SELECT =================
    function handleFileSelect(file) {
        console.log("🔍 handleFileSelect called");
        console.log("File:", file.name);

        // Validate file type
        const validExtensions = ['.csv', '.xlsx', '.xls', '.zip'];
        const ext = getFileExtension(file.name);

        console.log("Extension:", ext);

        if (!validExtensions.includes(ext)) {
            console.log("❌ Invalid file type");
            showToast("Invalid file type. Please upload CSV, XLSX, XLS, or ZIP files.", "error");
            return;
        }

        // No file size limit — server accepts unlimited size

        selectedFile = file;
        console.log("✅ File validated and stored");

        // Update UI
        console.log("Updating UI...");
        displayFileInfo(file);
        showFileCard();
        enableMetadataSection();
        showActionBar();
        checkUploadReady();

        // Hide drop zone
        if (dropZone) {
            dropZone.style.display = 'none';
            console.log("Drop zone hidden");
        }

        // Preview for CSV
        if (ext === ".csv") {
    loadCSVPreview(file);
}
else if (ext === ".xls" || ext === ".xlsx") {
    loadExcelPreview(file);
}
else {
    hidePreview();
}

autoDetectCategory(file.name);

        checkUploadReady();
        console.log("✅ File selection complete");
    }

    // ================= DISPLAY FILE INFO =================
    function displayFileInfo(file) {
        console.log("Displaying file info");
        
        const size = formatFileSize(file.size);
        const ext = getFileExtension(file.name).substring(1).toUpperCase();
        
        if (fileNameDisplay) {
            fileNameDisplay.textContent = file.name;
            console.log("Filename set:", file.name);
        }
        if (fileSizeDisplay) {
            fileSizeDisplay.textContent = size;
            console.log("Size set:", size);
        }
        if (fileTypeDisplay) {
            fileTypeDisplay.textContent = ext;
            console.log("Type set:", ext);
        }
        
        // Update icon color based on type
        const iconBox = document.querySelector('.file-icon-box');
        if (iconBox) {
            const gradients = {
                'CSV': 'linear-gradient(135deg, #10b981 0%, #059669 100%)',
                'XLSX': 'linear-gradient(135deg, #2563eb 0%, #1d4ed8 100%)',
                'XLS': 'linear-gradient(135deg, #2563eb 0%, #1d4ed8 100%)',
                'ZIP': 'linear-gradient(135deg, #8b5cf6 0%, #7c3aed 100%)'
            };
            iconBox.style.background = gradients[ext] || gradients['CSV'];
        }
    }

    // ================= SHOW/HIDE FUNCTIONS =================
    function showFileCard() {
        console.log("Showing file card");
        if (fileCard) {
            fileCard.classList.remove('hidden');
        }
    }

    function hideFileCard() {
        if (fileCard) {
            fileCard.classList.add('hidden');
        }
    }

    function enableMetadataSection() {
        console.log("Enabling metadata section");
        if (metadataSection) {
            metadataSection.classList.remove('disabled');
        }
    }

    function disableMetadataSection() {
        if (metadataSection) {
            metadataSection.classList.add('disabled');
        }
    }

    function showActionBar() {
        console.log("Showing action bar");
        if (actionBar) {
            actionBar.classList.remove('hidden');
        }
    }

    function hideActionBar() {
        if (actionBar) {
            actionBar.classList.add('hidden');
        }
    }

    function hidePreview() {
        if (previewSection) {
            previewSection.classList.add('hidden');
        }
    }

    // ================= CSV PREVIEW =================
    function loadCSVPreview(file) {
        console.log("Loading CSV preview");
        
        const reader = new FileReader();
        
        reader.onload = function(e) {
            try {
                const text = e.target.result;
                const lines = text.split('\n').filter(line => line.trim());
                
                if (lines.length === 0) {
                    console.log("Empty file");
                    return;
                }
                
                // Parse first 6 lines (header + 5 rows)
                const rows = lines.slice(0, 6).map(line => {
                    return line.split(',').map(cell => cell.trim().replace(/^"|"$/g, ''));
                });
                
                if (rows.length > 0 && previewTable) {
                    const headers = rows[0];
                    const dataRows = rows.slice(1);
                    checkMissingColumnsFromArray(headers.map(h => h.toLowerCase()));

                    let html = '<thead><tr>';
                    headers.forEach(header => {
                        html += `<th>${header || '(empty)'}</th>`;
                    });
                    html += '</tr></thead><tbody>';
                    
                    dataRows.forEach(row => {
                        html += '<tr>';
                        row.forEach(cell => {
                            html += `<td>${cell || '-'}</td>`;
                        });
                        html += '</tr>';
                    });
                    html += '</tbody>';
                    
                    previewTable.innerHTML = html;
                    
                    if (previewSection) {
                        previewSection.classList.remove('hidden');
                    }
                    
                    if (previewCount) {
                        previewCount.textContent = `First ${dataRows.length} rows`;
                    }
                    
                    console.log("Preview loaded successfully");
                }
            } catch (error) {
                console.error("Preview error:", error);
            }
        };
        
        reader.onerror = function() {
            console.error("Failed to read file");
        };
        
        reader.readAsText(file);
    }

    // ================= AUTO DETECT CATEGORY =================
    function autoDetectCategory(filename) {
        console.log("Auto-detecting category for:", filename);
        
        const lower = filename.toLowerCase();
        const categories = {
            'customer': ['customer', 'client', 'user'],
            'sales': ['sales', 'order', 'transaction'],
            'inventory': ['inventory', 'stock', 'product'],
            'employee': ['employee', 'staff', 'hr']
        };
        
        for (const [category, keywords] of Object.entries(categories)) {
            if (keywords.some(kw => lower.includes(kw))) {
                console.log("Category detected:", category);
                
                if (categorySelect) {
                    for (let option of categorySelect.options) {
                        if (option.text.toLowerCase().includes(category)) {
                            categorySelect.value = option.value;
                            break;
                        }
                    }
                }
                
                if (autoDetect && detectList) {
                    autoDetect.classList.remove('hidden');
                    detectList.innerHTML = `<li>Category: ${category}</li>`;
                }
                return;
            }
        }
    }

    // ================= REMOVE FILE =================
    if (removeFileBtn) {
        removeFileBtn.addEventListener("click", function() {
            console.log("Remove file clicked");
            resetUpload();
        });
    }

    function resetUpload() {
        console.log("Resetting upload");
        
        selectedFile = null;
        fileInput.value = "";
        
        hideFileCard();
        disableMetadataSection();
        hideActionBar();
        hidePreview();
        
        if (dropZone) {
            dropZone.style.display = 'block';
        }
        
        if (autoDetect) {
            autoDetect.classList.add('hidden');
        }
        
        if (uploadBtn) {
            uploadBtn.disabled = true;
        }
    }

    // ================= CANCEL BUTTON =================
    if (cancelBtn) {
        cancelBtn.addEventListener("click", function() {
            console.log("Cancel clicked");
            resetUpload();
        });
    }

    // ================= CHECK UPLOAD READY =================
    function checkUploadReady() {
        if (uploadBtn && categorySelect) {
            const isReady = selectedFile && categorySelect.value;
            uploadBtn.disabled = !isReady;
            console.log("Upload ready:", isReady);
        }
    }

    // Listen for category changes
    if (categorySelect) {
        categorySelect.addEventListener("change", checkUploadReady);
    }

    // ================= UPLOAD BUTTON =================
    if (uploadBtn) {
        uploadBtn.addEventListener("click", function() {
            console.log("Upload button clicked");
            
            if (!selectedFile) {
                showToast("Please select a file first", "error");
                return;
            }
            
            if (!categorySelect || !categorySelect.value) {
                showToast("Please select a category", "error");
                return;
            }
            
            if (isUploading) {
                console.log("Already uploading");
                return;
            }
            
            performUpload();
        });
    }

    // ================= PERFORM UPLOAD =================
    function performUpload() {

        if (!selectedFile) {
            showToast("Fix column headers first", "error");
            return;
        }

        console.log("Starting upload...");
        
        isUploading = true;
        
        if (uploadBtn) {
            uploadBtn.disabled = true;
            uploadBtn.innerHTML = `
                <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <circle cx="12" cy="12" r="10"></circle>
                    <polyline points="12 6 12 12 16 10"></polyline>
                </svg>
                Uploading...
            `;
        }
        
        if (fileStatus) fileStatus.style.display = 'none';
        if (progressWrapper) progressWrapper.classList.remove('hidden');
        
        // Build form data
        const formData = new FormData();
        formData.append("file", selectedFile);
        formData.append("category_id", categorySelect.value);
        
        if (descriptionInput && descriptionInput.value) {
            formData.append("description", descriptionInput.value);
        }
        
        // Upload via XHR for progress tracking
        const xhr = new XMLHttpRequest();
        xhr.open("POST", "/upload", true);

        // Show loading overlay when upload starts
        if (window.LoadingOverlay) {
            const fileMB = (selectedFile.size / 1024 / 1024).toFixed(1);
            window.LoadingOverlay.showWithProgress(
                'Uploading your file…',
                fileMB + ' MB — please keep this tab open'
            );
            // Clear any fake progress timer from loading.js
            if (window._loadingProgressTimer) {
                clearInterval(window._loadingProgressTimer);
            }
        }

        // Progress
        xhr.upload.onprogress = function(e) {
            if (e.lengthComputable) {
                const percent = Math.round((e.loaded / e.total) * 100);
                updateProgress(percent);
                // Also update loading overlay bar
                if (window.LoadingOverlay) {
                    const label = percent < 100 ? percent + '% uploaded' : 'Processing on server…';
                    window.LoadingOverlay.progress(percent, label);
                    if (percent === 100) {
                        document.getElementById('loading-subtitle').textContent =
                            'File received — processing records…';
                    }
                }
            }
        };
        
        // Success
      xhr.onload = function () {

    const contentType = xhr.getResponseHeader("Content-Type") || "";

    console.log("Upload response type:", contentType);

    // If backend sends correction page
    if (contentType.includes("text/html")) {

        document.open();
        document.write(xhr.responseText);
        document.close();

        return;
    }

    // Normal success
    if (xhr.status === 200 || xhr.status === 303) {

        showSuccess();

        setTimeout(() => {
            window.location.href = "/dashboard";
        }, 1500);

        return;
    }

    showToast("Upload failed. Please try again.", "error");
    if (window.LoadingOverlay) window.LoadingOverlay.hide();
    resetUploadState();
};

        
        // Error
        xhr.onerror = function() {
            console.error("Upload error");
            if (window.LoadingOverlay) window.LoadingOverlay.hide();
            showToast("Network error occurred", 'error');
            resetUploadState();
        };
        
        xhr.send(formData);
    }

    // ================= PROGRESS =================
    function updateProgress(percent) {
        if (progressBar) {
            progressBar.style.width = percent + "%";
        }
        if (progressText) {
            progressText.textContent = percent + "%";
        }
        if (progressStatus) {
            progressStatus.textContent = percent < 100 ? "Uploading..." : "Processing...";
        }
        console.log("Progress:", percent + "%");
    }

    // ================= SUCCESS =================
    function showSuccess() {
        if (progressWrapper) progressWrapper.classList.add('hidden');
        if (fileStatus) {
            fileStatus.style.display = 'inline-flex';
            fileStatus.style.color = '#10b981';
            fileStatus.innerHTML = `
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5">
                    <polyline points="20 6 9 17 4 12"></polyline>
                </svg>
                Success!
            `;
        }
        showToast("Upload successful! Redirecting...", 'success');
    }

    // ================= RESET STATE =================
    function resetUploadState() {
        isUploading = false;
        
        if (uploadBtn) {
            uploadBtn.disabled = false;
            uploadBtn.innerHTML = `
                <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"></path>
                    <polyline points="17 8 12 3 7 8"></polyline>
                    <line x1="12" y1="3" x2="12" y2="15"></line>
                </svg>
                Upload Dataset
            `;
        }
        
        if (progressWrapper) {
            progressWrapper.classList.add('hidden');
        }
        
        if (fileStatus) {
            fileStatus.style.display = 'inline-flex';
        }
    }

    // ================= TOAST =================
    function showToast(message, type = 'error') {
        console.log("Toast:", message, type);
        
        if (!toast || !toastMessage) return;
        
        toastMessage.textContent = message;
        toast.className = `toast ${type}`;
        
        if (toastIcon) {
            if (type === 'error') {
                toastIcon.innerHTML = `
                    <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                        <circle cx="12" cy="12" r="10"></circle>
                        <line x1="15" y1="9" x2="9" y2="15"></line>
                        <line x1="9" y1="9" x2="15" y2="15"></line>
                    </svg>
                `;
            } else if (type === 'success') {
                toastIcon.innerHTML = `
                    <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                        <circle cx="12" cy="12" r="10"></circle>
                        <polyline points="20 6 9 17 4 12"></polyline>
                    </svg>
                `;
            }
        }
        
        toast.classList.remove('hidden');
        
        setTimeout(() => {
            toast.classList.add('hidden');
        }, 5000);
    }

    // Toast close button
    const toastClose = document.querySelector('.toast-close');
    if (toastClose) {
        toastClose.onclick = function() {
            if (toast) toast.classList.add('hidden');
        };
    }

    // ================= UTILITY FUNCTIONS =================
    function formatFileSize(bytes) {
        if (bytes === 0) return '0 Bytes';
        const k = 1024;
        const sizes = ['Bytes', 'KB', 'MB', 'GB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return Math.round((bytes / Math.pow(k, i)) * 100) / 100 + ' ' + sizes[i];
    }

    function getFileExtension(filename) {
        return filename.substring(filename.lastIndexOf('.')).toLowerCase();
    }

    console.log("✅ Upload system ready and waiting for files");
});

function loadExcelPreview(file) {

    const reader = new FileReader();

    reader.onload = function (e) {

        const data = new Uint8Array(e.target.result);
        const workbook = XLSX.read(data, { type: 'array' });

        const sheetName = workbook.SheetNames[0];
        const sheet = workbook.Sheets[sheetName];

        // Convert sheet to array
        const rows = XLSX.utils.sheet_to_json(sheet, { header: 1 });

        if (!rows || rows.length === 0) {
            showToast("Empty Excel file", "error");
            return;
        }

        // First row = header
        const headers = rows[0].map(h =>
            String(h || "").toLowerCase().trim()
        );

        console.log("Excel Headers:", headers);

        // Check headers
        const valid = checkExcelHeaders(headers);

        // Show preview
        renderExcelPreview(rows);

        // If invalid → block upload
        if (!valid) {
            selectedFile = null;
            uploadBtn.disabled = true;
        }
    };

    reader.readAsArrayBuffer(file);
}

function renderExcelPreview(rows) {

    previewTable.innerHTML = "";

    const maxRows = Math.min(6, rows.length);

    for (let i = 0; i < maxRows; i++) {

        const tr = document.createElement("tr");

        rows[i].forEach(cell => {

            const el = document.createElement(i === 0 ? "th" : "td");
            el.textContent = cell || "";
            tr.appendChild(el);

        });

        previewTable.appendChild(tr);
    }

    showPreview(maxRows - 1);
}
function checkMissingColumnsFromArray(headers) {

    // Common valid header words
    const validKeywords = [
        "name", "email", "phone", "mobile",
        "address", "city", "state", "pin",
        "zip", "company", "id"
    ];

    let matchCount = 0;

    headers.forEach(h => {
        validKeywords.forEach(k => {
            if (h.includes(k)) {
                matchCount++;
            }
        });
    });

    console.log("Header match count:", matchCount);

    // If too few matches → probably not headers
    if (matchCount < 2) {
        console.log("❌ No real headers found");
        showCorrectionPage(headers);
        return;
    }

    console.log("✅ Valid headers detected");
}

function showCorrectionPage(headers) {

    console.log("Showing correction page");

    // Hide preview + buttons
    if (previewSection) previewSection.classList.add("hidden");
    if (actionBar) actionBar.classList.add("hidden");

    if (!missingColumns || !missingInputs) return;

    missingInputs.innerHTML = "";

    headers.forEach(h => {

        const div = document.createElement("div");

        div.className = "form-field";

        div.innerHTML = `
            <label class="field-label">Rename column</label>
            <input 
                type="text" 
                value="${h}" 
                class="field-input" 
                required
            />
        `;

        missingInputs.appendChild(div);
    });

    missingColumns.classList.remove("hidden");

    // Block upload
    if (uploadBtn) uploadBtn.disabled = true;
}


function checkExcelHeaders(headers) {

    const validKeywords = [
        "name", "email", "phone", "mobile",
        "address", "city", "state", "pin",
        "zip", "company", "id"
    ];

    let match = 0;

    headers.forEach(h => {
        validKeywords.forEach(k => {
            if (h.includes(k)) {
                match++;
            }
        });
    });

    console.log("Excel header matches:", match);

    // Fake header
    if (match < 2) {

        console.log("❌ Fake header detected");

        showCorrectionPage(headers);

        showToast("Column headers not detected. Please correct them.", "error");

        return false;
    }

    console.log("✅ Valid headers");

    return true;
}

function openNewCategoryModal() {
  const modal = document.getElementById('newCategoryModal');
  modal.style.display = 'flex';
  document.body.style.overflow = 'hidden';
  setTimeout(() => document.getElementById('newCategoryInput').focus(), 50);
}

function closeNewCategoryModal() {
  document.getElementById('newCategoryModal').style.display = 'none';
  document.body.style.overflow = '';
  document.getElementById('newCategoryInput').value = '';
  document.getElementById('newCategoryError').style.display = 'none';
}

async function submitNewCategory() {
  const input = document.getElementById('newCategoryInput');
  const errEl = document.getElementById('newCategoryError');
  const name  = input.value.trim();

  errEl.style.display = 'none';

  if (!name) {
    errEl.textContent = 'Please enter a category name.';
    errEl.style.display = 'block';
    input.focus();
    return;
  }

  const formData = new FormData();
  formData.append('name', name);

  try {
    const res  = await fetch('/category/create', { method: 'POST', body: formData });
    const data = await res.json();

    if (data.success) {
      // Add the new option to the select and select it
      const select = document.getElementById('category-select');
      const opt    = document.createElement('option');
      opt.value    = data.category_id ?? data.id ?? name;
      opt.text     = name.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
      opt.selected = true;
      select.appendChild(opt);
      closeNewCategoryModal();
    } else {
      errEl.textContent = data.error || 'Failed to create category.';
      errEl.style.display = 'block';
    }
  } catch (err) {
    errEl.textContent = 'Network error: ' + err.message;
    errEl.style.display = 'block';
  }
}