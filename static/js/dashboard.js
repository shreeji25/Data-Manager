// ================= CATEGORY MANAGEMENT =================

function addCategory() {
    const categoryName = prompt("Enter new category name:");
    
    if (!categoryName || categoryName.trim() === "") {
        return;
    }
    
    const trimmedName = categoryName.trim();
    
    // Validate length
    if (trimmedName.length > 50) {
        alert("Category name too long (max 50 characters)");
        return;
    }
    
    // Create form data
    const formData = new FormData();
    formData.append("name", trimmedName);
    
    // Show loading state
    const btn = event.target;
    const originalText = btn.innerHTML;
    btn.innerHTML = "Creating...";
    btn.disabled = true;
    
    // Submit to backend
    fetch("/category/create", {
        method: "POST",
        body: formData
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            // Success - reload page
            window.location.reload();
        } else {
            // Error - show message
            alert(data.error || "Failed to create category");
            btn.innerHTML = originalText;
            btn.disabled = false;
        }
    })
    .catch(error => {
        console.error("Error creating category:", error);
        alert("Error creating category. Please try again.");
        btn.innerHTML = originalText;
        btn.disabled = false;
    });
}


function renameCategory(categoryId, currentName) {
    const newName = prompt("Enter new category name:", currentName);
    
    if (!newName || newName.trim() === "") {
        return;
    }
    
    const trimmedName = newName.trim();
    
    // Check if name is unchanged
    if (trimmedName === currentName) {
        return;
    }
    
    // Validate length
    if (trimmedName.length > 50) {
        alert("Category name too long (max 50 characters)");
        return;
    }
    
    // Create form data
    const formData = new FormData();
    formData.append("name", trimmedName);
    
    // Submit to backend
    fetch(`/category/rename/${categoryId}`, {
        method: "POST",
        body: formData
    })
    .then(response => response.json())
    .then(data => {
        if (data.success) {
            // Success - reload page
            window.location.reload();
        } else {
            // Error - show message
            alert(data.error || "Failed to rename category");
        }
    })
    .catch(error => {
        console.error("Error renaming category:", error);
        alert("Error renaming category. Please try again.");
    });
}


// ================= HELPER FUNCTIONS =================

// Prevent double-click on forms
document.addEventListener('DOMContentLoaded', function() {
    const forms = document.querySelectorAll('form');
    forms.forEach(form => {
        form.addEventListener('submit', function() {
            const submitBtn = form.querySelector('button[type="submit"]');
            if (submitBtn) {
                submitBtn.disabled = true;
                setTimeout(() => {
                    submitBtn.disabled = false;
                }, 3000);
            }
        });
    });
});
// Category Management Functions
function renameCategory(categoryId, oldName) {
    const newName = prompt('Rename category:', oldName);
    if (!newName || newName === oldName) return;
    
    const formData = new FormData();
    formData.append('name', newName);
    
    fetch(`/category/rename/${categoryId}`, {
        method: 'POST',
        body: formData
    })
    .then(res => res.json())
    .then(data => {
        if (data.success) {
            location.reload();
        } else {
            alert('Error: ' + (data.error || 'Failed to rename category'));
        }
    })
    .catch(err => {
        alert('Error: ' + err.message);
    });
}

function deleteCategory(categoryId, name) {
    if (!confirm(`Delete category "${name}"?\n\nDatasets in this category will not be deleted.`)) return;
    
    window.location.href = `/category/delete/${categoryId}`;
}