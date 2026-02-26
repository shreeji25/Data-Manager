"""
modules/profile.py
------------------
User profile page — fetches real account data, dataset records,
category counts and aggregate stats.

Route:
  GET /profile  → profile page with full user data
"""

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
from sqlalchemy import func

from auth import get_current_user
from database import get_db
from models import Dataset, Category, User

router = APIRouter(tags=["profile"])
templates = Jinja2Templates(directory="templates")

# How many recent datasets to show on profile (all, or cap if huge)
PROFILE_DATASET_LIMIT = 50


@router.get("/profile", response_class=HTMLResponse)
def profile(
    request: Request,
    db: Session = Depends(get_db),
):
    # ── Auth ────────────────────────────────────────────────────────────────
    user = get_current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=302)

    current_user = db.query(User).filter(User.id == user["id"]).first()
    if not current_user:
        return RedirectResponse("/login", status_code=302)

    user_id = current_user.id

    # ── Fetch datasets (most recent first) ──────────────────────────────────
    datasets = (
        db.query(Dataset)
        .filter(Dataset.user_id == user_id)
        .order_by(Dataset.uploaded_at.desc(), Dataset.id.desc())
        .limit(PROFILE_DATASET_LIMIT)
        .all()
    )

    # ── Aggregate stats ─────────────────────────────────────────────────────
    total_datasets = (
        db.query(func.count(Dataset.id))
        .filter(Dataset.user_id == user_id)
        .scalar() or 0
    )

    total_rows = (
        db.query(func.coalesce(func.sum(Dataset.row_count), 0))
        .filter(Dataset.user_id == user_id)
        .scalar() or 0
    )

    total_duplicates = (
        db.query(func.coalesce(func.sum(Dataset.duplicate_records), 0))
        .filter(Dataset.user_id == user_id)
        .scalar() or 0
    )

    total_categories = (
        db.query(func.count(Category.id))
        .filter(Category.user_id == user_id)
        .scalar() or 0
    )

    # ── Build context ────────────────────────────────────────────────────────
    context = {
        "request": request,
        "user": current_user,          # full ORM object (has .created_at, .last_login, etc.)
        "datasets": datasets,
        "total_datasets": total_datasets,
        "total_rows": total_rows,
        "total_duplicates": total_duplicates,
        "total_categories": total_categories,
        "show_header": True,
        "show_sidebar": False,
        # sidebar not needed on profile, but base.html needs these to avoid errors
        "admin_mode": current_user.role == "admin",
        "admin_users": [],
        "categories": [],
        "category_counts": {},
        "viewing_user": None,
    }

    return templates.TemplateResponse("profile.html", context)