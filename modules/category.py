"""
modules/category.py
-------------------
Category management routes.

All endpoints return JSONResponse (used by frontend JavaScript).
Only delete returns a redirect (called from a plain HTML form/link).

Routes:
  POST /category/create              → create a new category (JSON)
  POST /category/rename/{id}         → rename category + sync dataset.department (JSON)
  GET  /category/delete/{id}         → delete category (redirect)
"""

from fastapi import APIRouter, Depends, Request, HTTPException
from fastapi.responses import RedirectResponse, JSONResponse
from sqlalchemy.orm import Session

from auth import get_current_user
from database import get_db
from models import Category, Dataset
from utils.permissions import get_effective_user

router = APIRouter(tags=["category"])

MAX_NAME_LEN = 50


# ---------------------------------------------------------------------------
# Create
# ---------------------------------------------------------------------------

@router.post("/category/create")
async def create_category(
    request: Request,
    db: Session = Depends(get_db),
):
    """Create a new category for the effective user. Returns JSON."""
    user = get_current_user(request)
    if not user:
        return JSONResponse(status_code=401, content={"success": False, "error": "Not authenticated"})

    effective_user = get_effective_user(request, db)
    if not effective_user:
        return JSONResponse(status_code=403, content={"success": False, "error": "No user selected"})

    form = await request.form()
    name = (form.get("name") or "").strip()

    if not name:
        return JSONResponse(status_code=400, content={"success": False, "error": "Category name is required"})

    if len(name) > MAX_NAME_LEN:
        return JSONResponse(
            status_code=400,
            content={"success": False, "error": f"Category name too long (max {MAX_NAME_LEN} characters)"},
        )

    # Duplicate check
    existing = db.query(Category).filter(
        Category.name == name,
        Category.user_id == effective_user.id,
    ).first()

    if existing:
        return JSONResponse(status_code=400, content={"success": False, "error": "Category already exists"})

    try:
        new_category = Category(name=name, user_id=effective_user.id)
        db.add(new_category)
        db.commit()
        db.refresh(new_category)

        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "message": "Category created successfully",
                "category": {"id": new_category.id, "name": new_category.name},
            },
        )

    except Exception as e:
        db.rollback()
        return JSONResponse(status_code=500, content={"success": False, "error": f"Failed to create category: {str(e)}"})


# ---------------------------------------------------------------------------
# Rename
# ---------------------------------------------------------------------------

@router.post("/category/rename/{category_id}")
async def rename_category(
    category_id: int,
    request: Request,
    db: Session = Depends(get_db),
):
    """
    Rename a category and sync all linked datasets.

    Because datasets are linked to categories by the department name string
    (not by FK), renaming must also update Dataset.department for every
    dataset that belonged to the old category name.
    """
    user = get_current_user(request)
    if not user:
        return JSONResponse(status_code=401, content={"success": False, "error": "Not authenticated"})

    effective_user = get_effective_user(request, db)
    if not effective_user:
        return JSONResponse(status_code=403, content={"success": False, "error": "No user selected"})

    form = await request.form()
    new_name = (form.get("name") or "").strip()

    if not new_name:
        return JSONResponse(status_code=400, content={"success": False, "error": "Category name is required"})

    if len(new_name) > MAX_NAME_LEN:
        return JSONResponse(
            status_code=400,
            content={"success": False, "error": f"Category name too long (max {MAX_NAME_LEN} characters)"},
        )

    # Must belong to effective_user
    category = db.query(Category).filter(
        Category.id == category_id,
        Category.user_id == effective_user.id,
    ).first()

    if not category:
        return JSONResponse(status_code=404, content={"success": False, "error": "Category not found"})

    # Check new name doesn't collide with another category for this user
    clash = db.query(Category).filter(
        Category.name == new_name,
        Category.user_id == effective_user.id,
        Category.id != category_id,
    ).first()

    if clash:
        return JSONResponse(status_code=400, content={"success": False, "error": "Category name already exists"})

    try:
        old_name = category.name
        category.name = new_name

        # Sync dataset.department for all datasets that used the old category name
        # This is required because the link is by name string, not by FK.
        datasets = db.query(Dataset).filter(
            Dataset.department == old_name,
            Dataset.user_id == effective_user.id,
        ).all()

        for ds in datasets:
            ds.department = new_name

        db.commit()

        return JSONResponse(
            status_code=200,
            content={
                "success": True,
                "message": "Category renamed successfully",
                "category": {"id": category.id, "name": category.name},
            },
        )

    except Exception as e:
        db.rollback()
        return JSONResponse(status_code=500, content={"success": False, "error": f"Failed to rename category: {str(e)}"})


# ---------------------------------------------------------------------------
# Delete
# ---------------------------------------------------------------------------

@router.get("/category/delete/{category_id}")
def delete_category(
    category_id: int,
    request: Request,
    db: Session = Depends(get_db),
):
    """
    Delete a category. Returns a redirect to /dashboard.

    Datasets that used this category are NOT deleted — their department
    field is left as-is (orphaned string reference). If you want to
    null them out on delete, swap the pass below for the update query.
    """
    user = get_current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=302)

    effective_user = get_effective_user(request, db)
    if not effective_user:
        raise HTTPException(status_code=403, detail="No user selected")

    # Must belong to effective_user — prevents cross-tenant delete
    category = db.query(Category).filter(
        Category.id == category_id,
        Category.user_id == effective_user.id,
    ).first()

    if not category:
        raise HTTPException(status_code=404, detail="Category not found")

    try:
        db.delete(category)
        db.commit()
        return RedirectResponse("/dashboard", status_code=302)

    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Failed to delete category: {str(e)}")