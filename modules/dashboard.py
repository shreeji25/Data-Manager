"""
modules/dashboard.py
--------------------
Multi-tenant dashboard.

Routes:
  GET  /                            → redirect to /dashboard
  GET  /dashboard                   → main dashboard (paginated, filtered)
  POST /admin/select-user/{user_id} → admin selects a user to view
  POST /admin/exit-view             → admin clears user selection
"""

import math
from datetime import datetime

from fastapi import APIRouter, Depends, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
from sqlalchemy import and_, func

from auth import get_current_user
from database import get_db
from models import Dataset, Category, User
from utils.permissions import get_effective_user, get_sidebar_context

router = APIRouter(tags=["dashboard"])
templates = Jinja2Templates(directory="templates")

PAGE_SIZE = 10


# ---------------------------------------------------------------------------
# Admin: select user
# ---------------------------------------------------------------------------

@router.post("/admin/select-user/{user_id}")
def select_user(
    request: Request,
    user_id: int,
    db: Session = Depends(get_db),
):
    """Admin picks a user by clicking their sidebar button."""
    user = get_current_user(request)
    if not user or user.get("role") != "admin":
        return RedirectResponse("/dashboard", status_code=302)

    # Verify the target user actually exists
    target = db.query(User).filter(User.id == user_id).first()
    if not target:
        return RedirectResponse("/dashboard", status_code=302)

    request.session["selected_user_id"] = user_id
    return RedirectResponse("/dashboard", status_code=302)


# ---------------------------------------------------------------------------
# Admin: exit view (clear selected user)
# ---------------------------------------------------------------------------

@router.post("/admin/exit-view")
def exit_view(request: Request):
    """Admin clears user selection; returns to empty admin dashboard."""
    request.session.pop("selected_user_id", None)
    return RedirectResponse("/dashboard", status_code=302)


# ---------------------------------------------------------------------------
# Main dashboard
# ---------------------------------------------------------------------------

@router.get("/", response_class=HTMLResponse)
@router.get("/dashboard", response_class=HTMLResponse)
def dashboard(
    request: Request,
    page: int = 1,
    q: str = "",
    category: str = "",
    from_date: str = "",
    to_date: str = "",
    min_rows: str = "",
    max_rows: str = "",
    has_duplicates: int = 0,
    db: Session = Depends(get_db),
):
    # ── Auth ─────────────────────────────────────────────────────────────────
    user = get_current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=302)

    current_user = db.query(User).filter(User.id == user["id"]).first()
    if not current_user:
        return RedirectResponse("/login", status_code=302)

    effective_user = get_effective_user(request, db)
    is_admin = current_user.role == "admin"

    # Sidebar context (admin_users with counts, or categories for normal user)
    sidebar = get_sidebar_context(request, db, current_user)

    # ── Admin with no user selected → Show ALL datasets ─────────────────────────
    if is_admin and not effective_user:
        # Admin sees ALL datasets from ALL users
        total_datasets = db.query(func.count(Dataset.id)).scalar()
        
        # Parse filter params
        try:
            min_rows_val = int(min_rows) if min_rows else None
            max_rows_val = int(max_rows) if max_rows else None
        except (ValueError, TypeError):
            min_rows_val = max_rows_val = None

        try:
            from_date_val = datetime.strptime(from_date, "%Y-%m-%d") if from_date else None
            to_date_val   = datetime.strptime(to_date,   "%Y-%m-%d") if to_date   else None
        except (ValueError, TypeError):
            from_date_val = to_date_val = None

        # Build filtered query - ALL users
        query = db.query(Dataset).join(User, Dataset.user_id == User.id)

        filters = []
        if q:
            filters.append(Dataset.file_name.ilike(f"%{q}%"))
        if category:
            filters.append(Dataset.department == category)
        if from_date_val:
            filters.append(Dataset.uploaded_at >= from_date_val)
        if to_date_val:
            filters.append(Dataset.uploaded_at <= to_date_val)
        if min_rows_val is not None:
            filters.append(Dataset.row_count >= min_rows_val)
        if max_rows_val is not None:
            filters.append(Dataset.row_count <= max_rows_val)
        if has_duplicates:
            filters.append(Dataset.duplicate_records > 0)

        if filters:
            query = query.filter(and_(*filters))

        # Pagination
        total = query.count()
        total_pages = max(1, math.ceil(total / PAGE_SIZE))
        page = max(1, min(page, total_pages))
        offset = (page - 1) * PAGE_SIZE

        datasets = (
            query.order_by(Dataset.id.desc())
            .offset(offset)
            .limit(PAGE_SIZE)
            .all()
        )

        # Get all categories from all users for filter dropdown
        all_categories = (
            db.query(Category)
            .order_by(Category.name)
            .all()
        )

        context = {
            "request": request,
            "user": user,
            "datasets": datasets,
            "categories": all_categories,
            "category_counts": {},
            "total_datasets": total_datasets,
            "page": page,
            "total_pages": total_pages,
            "q": q,
            "category": category,
            "from_date": from_date_val.strftime("%Y-%m-%d") if from_date_val else "",
            "to_date":   to_date_val.strftime("%Y-%m-%d")   if to_date_val   else "",
            "min_rows": min_rows_val or "",
            "max_rows": max_rows_val or "",
            "has_duplicates": has_duplicates,
            "admin_mode": True,
            "viewing_user": None,
            "show_header": True,
        }
        context.update(sidebar)
        return templates.TemplateResponse("dashboard.html", context)

    # ── Normal user or admin with selected user ───────────────────────────
    user_id = effective_user.id

    # Category counts (category name → dataset count) for sidebar badges
    category_counts = dict(
        db.query(Category.name, func.count(Dataset.id))
        .outerjoin(
            Dataset,
            and_(
                Dataset.department == Category.name,
                Dataset.user_id == user_id,
            ),
        )
        .filter(Category.user_id == user_id)
        .group_by(Category.name)
        .all()
    )

    # Total unfiltered dataset count for the stats panel
    total_datasets = (
        db.query(func.count(Dataset.id))
        .filter(Dataset.user_id == user_id)
        .scalar()
    )

    # ── Parse filter params ───────────────────────────────────────────────
    try:
        min_rows_val = int(min_rows) if min_rows else None
        max_rows_val = int(max_rows) if max_rows else None
    except (ValueError, TypeError):
        min_rows_val = max_rows_val = None

    try:
        from_date_val = datetime.strptime(from_date, "%Y-%m-%d") if from_date else None
        to_date_val   = datetime.strptime(to_date,   "%Y-%m-%d") if to_date   else None
    except (ValueError, TypeError):
        from_date_val = to_date_val = None

    # ── Build filtered query ──────────────────────────────────────────────
    query = db.query(Dataset).filter(Dataset.user_id == user_id)

    filters = []
    if q:
        filters.append(Dataset.file_name.ilike(f"%{q}%"))
    if category:
        filters.append(Dataset.department == category)
    if from_date_val:
        filters.append(Dataset.uploaded_at >= from_date_val)
    if to_date_val:
        filters.append(Dataset.uploaded_at <= to_date_val)
    if min_rows_val is not None:
        filters.append(Dataset.row_count >= min_rows_val)
    if max_rows_val is not None:
        filters.append(Dataset.row_count <= max_rows_val)
    if has_duplicates:
        filters.append(Dataset.duplicate_records > 0)

    if filters:
        query = query.filter(and_(*filters))

    # ── Pagination ────────────────────────────────────────────────────────
    total = query.count()
    total_pages = max(1, math.ceil(total / PAGE_SIZE))
    page = max(1, min(page, total_pages))
    offset = (page - 1) * PAGE_SIZE

    datasets = (
        query.order_by(Dataset.id.desc())
        .offset(offset)
        .limit(PAGE_SIZE)
        .all()
    )

    # Categories for the filter dropdown (scoped to effective_user)
    user_categories = (
        db.query(Category)
        .filter(Category.user_id == user_id)
        .order_by(Category.name)
        .all()
    )

    # ── Build template context ────────────────────────────────────────────
    context = {
        "request": request,
        "user": user,
        "datasets": datasets,
        "categories": user_categories,
        "category_counts": category_counts,
        "total_datasets": total_datasets,
        "page": page,
        "total_pages": total_pages,
        "q": q,
        "category": category,
        "from_date": from_date_val.strftime("%Y-%m-%d") if from_date_val else "",
        "to_date":   to_date_val.strftime("%Y-%m-%d")   if to_date_val   else "",
        "min_rows": min_rows_val or "",
        "max_rows": max_rows_val or "",
        "has_duplicates": has_duplicates,
        "admin_mode": is_admin,
        "viewing_user": effective_user,
        "show_header": True,
    }
    context.update(sidebar)

    return templates.TemplateResponse("dashboard.html", context)

# ---------------------------------------------------------------------------
# Dataset: change category
# ---------------------------------------------------------------------------

@router.post("/dataset/{dataset_id}/change-category")
async def change_dataset_category(
    dataset_id: int,
    request: Request,
    db: Session = Depends(get_db),
):
    """Change the category/department of a dataset."""
    user = get_current_user(request)
    if not user:
        return JSONResponse({"success": False, "error": "Not authenticated"}, status_code=401)

    form = await request.form()
    new_category = (form.get("category") or "").strip()

    # Scope to current user — users can only change their own datasets
    dataset = db.query(Dataset).filter(
        Dataset.id == dataset_id,
        Dataset.user_id == user["id"],
    ).first()

    if not dataset:
        return JSONResponse({"success": False, "error": "Dataset not found"}, status_code=404)

    # If a category name was provided, verify it belongs to this user
    if new_category:
        category = db.query(Category).filter(
            Category.name == new_category,
            Category.user_id == user["id"],
        ).first()
        if not category:
            return JSONResponse({"success": False, "error": "Invalid category"}, status_code=400)
        dataset.department  = category.name
        dataset.category_id = category.id
    else:
        # Cleared — remove category assignment
        dataset.department  = None
        dataset.category_id = None

    db.commit()
    return JSONResponse({"success": True})


# ---------------------------------------------------------------------------
# Dataset: delete (AJAX version used by the delete modal)
# ---------------------------------------------------------------------------

@router.post("/dataset/{dataset_id}/delete")
def delete_dataset_ajax(
    dataset_id: int,
    request: Request,
    db: Session = Depends(get_db),
):
    """Delete a dataset — AJAX version returning JSON (used by dashboard modal)."""
    user = get_current_user(request)
    if not user:
        return JSONResponse({"success": False, "error": "Not authenticated"}, status_code=401)

    # Admins cannot delete via this route
    if user.get("role") == "admin":
        return JSONResponse({"success": False, "error": "Admins cannot delete user datasets"}, status_code=403)

    dataset = db.query(Dataset).filter(
        Dataset.id == dataset_id,
        Dataset.user_id == user["id"],
    ).first()

    if not dataset:
        return JSONResponse({"success": False, "error": "Dataset not found"}, status_code=404)

    import os
    if os.path.exists(dataset.file_path):
        try:
            os.remove(dataset.file_path)
        except Exception as e:
            print(f"⚠️ Could not remove file {dataset.file_path}: {e}")

    db.delete(dataset)
    db.commit()

    print(f"🗑️ Deleted dataset {dataset_id} for user {user['id']}")
    return JSONResponse({"success": True})


# ---------------------------------------------------------------------------
# Dataset: bulk delete
# ---------------------------------------------------------------------------

@router.post("/dataset/bulk-delete")
async def bulk_delete_datasets(
    request: Request,
    db: Session = Depends(get_db),
):
    """Bulk delete datasets by ID list — used by the bulk delete toolbar."""
    user = get_current_user(request)
    if not user:
        return JSONResponse({"success": False, "error": "Not authenticated"}, status_code=401)

    if user.get("role") == "admin":
        return JSONResponse({"success": False, "error": "Admins cannot delete user datasets"}, status_code=403)

    try:
        body = await request.json()
        ids  = body.get("ids", [])
    except Exception:
        return JSONResponse({"success": False, "error": "Invalid request body"}, status_code=400)

    if not ids:
        return JSONResponse({"success": False, "error": "No IDs provided"}, status_code=400)

    import os
    deleted = 0
    for dataset_id in ids:
        dataset = db.query(Dataset).filter(
            Dataset.id == dataset_id,
            Dataset.user_id == user["id"],
        ).first()
        if not dataset:
            continue
        if os.path.exists(dataset.file_path):
            try:
                os.remove(dataset.file_path)
            except Exception as e:
                print(f"⚠️ Could not remove file {dataset.file_path}: {e}")
        db.delete(dataset)
        deleted += 1

    db.commit()
    print(f"🗑️ Bulk deleted {deleted} datasets for user {user['id']}")
    return JSONResponse({"success": True, "deleted": deleted})