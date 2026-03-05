"""
modules/admin.py

Admin-only routes for user management and user view selection.
"""

from fastapi import APIRouter, Depends, Request, HTTPException
from fastapi.responses import RedirectResponse, HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
from sqlalchemy import func, text
from datetime import datetime, timedelta, date
import json

from auth import get_current_user
from database import get_db
from models import User, Dataset, Category

router = APIRouter(prefix="/admin", tags=["admin"])
templates = Jinja2Templates(directory="templates")


def require_admin(request: Request):
    """Dependency to check if user is admin"""
    user = get_current_user(request)
    if not user or user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    return user


def _build_stats(db: Session, days: int) -> dict:
    """
    Shared helper — builds all chart + stat data for a given time range.
    days=30  → last 30 days
    days=90  → last 90 days
    days=0   → all time (no date filter)
    """
    # Use end of today (23:59:59) so uploads made TODAY are always included
    today_midnight = datetime.combine(date.today(), datetime.min.time())
    today_end = datetime.combine(date.today(), datetime.max.time().replace(microsecond=0))
    since = today_midnight - timedelta(days=days - 1) if days > 0 else None

    def date_filter(col):
        """Returns a SQLAlchemy filter or None depending on range."""
        if since:
            return col >= since
        return None

    # ── Stat Cards ────────────────────────────────────────────────────────
    total_users    = db.query(func.count(User.id)).filter(User.role != "admin").scalar() or 0
    total_datasets_q = db.query(func.count(Dataset.id))
    total_rows_q     = db.query(func.sum(Dataset.row_count))
    if since:
        total_datasets_q = total_datasets_q.filter(Dataset.uploaded_at >= since)
        total_rows_q     = total_rows_q.filter(Dataset.uploaded_at >= since)
    total_datasets = total_datasets_q.scalar() or 0
    total_rows     = total_rows_q.scalar() or 0

    most_active_q = (
        db.query(User.username, func.count(Dataset.id).label("cnt"))
        .join(Dataset, Dataset.user_id == User.id)
        .filter(User.role != "admin")
    )
    if since:
        most_active_q = most_active_q.filter(Dataset.uploaded_at >= since)
    most_active = most_active_q.group_by(User.id).order_by(func.count(Dataset.id).desc()).first()
    most_active_user  = most_active[0] if most_active else "—"
    most_active_count = most_active[1] if most_active else 0

    # ── Category Distribution ─────────────────────────────────────────────
    cat_q = (
        db.query(Dataset.department, func.count(Dataset.id).label("cnt"))
        .filter(Dataset.department.isnot(None))
    )
    if since:
        cat_q = cat_q.filter(Dataset.uploaded_at >= since)
    cat_rows = cat_q.group_by(Dataset.department).order_by(func.count(Dataset.id).desc()).all()
    cat_data = [{"name": r.department, "count": r.cnt} for r in cat_rows]

    uncategorised_q = db.query(func.count(Dataset.id)).filter(
        (Dataset.department.is_(None)) | (Dataset.department == "")
    )
    if since:
        uncategorised_q = uncategorised_q.filter(Dataset.uploaded_at >= since)
    uncategorised = uncategorised_q.scalar() or 0
    if uncategorised > 0:
        cat_data.append({"name": "Uncategorised", "count": uncategorised})

    # ── File Type Breakdown ───────────────────────────────────────────────
    files_q = db.query(Dataset.file_name)
    if since:
        files_q = files_q.filter(Dataset.uploaded_at >= since)
    all_files = files_q.all()
    type_counts = {"CSV": 0, "XLSX": 0, "XLS": 0, "Other": 0}
    for row in all_files:
        if row.file_name:
            ext = row.file_name.rsplit(".", 1)[-1].upper() if "." in row.file_name else ""
            if ext in type_counts:
                type_counts[ext] += 1
            else:
                type_counts["Other"] += 1
    filetype_data = [{"name": k, "count": v} for k, v in type_counts.items() if v > 0]

    # ── User Activity Heatmap ─────────────────────────────────────────────
    # Always show last 84 days (12 weeks) ending TODAY for the activity chart
    heatmap_since = today_midnight - timedelta(days=83)  # 84 days inclusive of today
    from sqlalchemy import cast, Date as SADate
    activity_rows = (
        db.query(
            User.username,
            cast(Dataset.uploaded_at, SADate).label("day"),  # works on PostgreSQL & SQLite
            func.count(Dataset.id).label("cnt"),
        )
        .join(Dataset, Dataset.user_id == User.id)
        .filter(User.role != "admin", Dataset.uploaded_at >= heatmap_since)
        .group_by(User.username, cast(Dataset.uploaded_at, SADate))
        .all()
    )
    heatmap_data = [
        {"user": r.username, "day": str(r.day), "count": r.cnt}  # str(date) → "YYYY-MM-DD"
        for r in activity_rows
    ]

    heatmap_users = [
        u.username for u in
        db.query(User).filter(User.role != "admin").order_by(User.username).all()
    ]

    # ── Top Users ─────────────────────────────────────────────────────────
    top_q = (
        db.query(User.username, func.count(Dataset.id).label("cnt"))
        .outerjoin(Dataset, Dataset.user_id == User.id)
        .filter(User.role != "admin")
    )
    if since:
        top_q = top_q.filter(
            (Dataset.uploaded_at >= since) | (Dataset.id.is_(None))
        )
    top_users_rows = top_q.group_by(User.id).order_by(func.count(Dataset.id).desc()).all()
    top_users_data = [{"user": r.username, "count": r.cnt} for r in top_users_rows]

    # ── Duplicate Rate Distribution ───────────────────────────────────────
    dup_q = db.query(Dataset.row_count, Dataset.duplicate_records).filter(
        Dataset.row_count.isnot(None), Dataset.row_count > 0
    )
    if since:
        dup_q = dup_q.filter(Dataset.uploaded_at >= since)
    datasets_with_rows = dup_q.all()
    buckets = {"0-10": 0, "10-25": 0, "25-40": 0, "40-60": 0, "60+": 0}
    for d in datasets_with_rows:
        rate = (d.duplicate_records or 0) / d.row_count * 100
        if rate <= 10:
            buckets["0-10"] += 1
        elif rate <= 25:
            buckets["10-25"] += 1
        elif rate <= 40:
            buckets["25-40"] += 1
        elif rate <= 60:
            buckets["40-60"] += 1
        else:
            buckets["60+"] += 1
    dup_dist_data = [{"range": k, "count": v} for k, v in buckets.items()]

    return {
        "total_users":      total_users,
        "total_datasets":   total_datasets,
        "total_rows":       f"{total_rows:,}" if total_rows else "0",
        "most_active_user": most_active_user,
        "most_active_count": most_active_count,
        "cat_data":         cat_data,
        "filetype_data":    filetype_data,
        "heatmap_data":     heatmap_data,
        "heatmap_users":    heatmap_users,
        "top_users_data":   top_users_data,
        "dup_dist_data":    dup_dist_data,
    }


@router.get("/overview", response_class=HTMLResponse)
def admin_overview(
    request: Request,
    db: Session = Depends(get_db),
    admin: dict = Depends(require_admin),
):
    """Admin overview dashboard — initial page load (defaults to 30 days)."""

    stats = _build_stats(db, days=30)

    # Sidebar user list
    admin_users = (
        db.query(User, func.count(Dataset.id).label("cnt"))
        .outerjoin(Dataset, Dataset.user_id == User.id)
        .filter(User.role != "admin")
        .group_by(User.id)
        .order_by(User.username)
        .all()
    )

    return templates.TemplateResponse("admin_overview.html", {
        "request":          request,
        "user":             admin,
        "show_header":      True,
        "admin_mode":       True,
        "active_page":      "admin_overview",
        "viewing_user":     None,
        "admin_users":      admin_users,
        # Stat cards
        "total_users":      stats["total_users"],
        "total_datasets":   stats["total_datasets"],
        "total_rows":       stats["total_rows"],
        "most_active_user": stats["most_active_user"],
        "most_active_count": stats["most_active_count"],
        # Chart data (JSON strings for JS)
        "cat_data":         json.dumps(stats["cat_data"]),
        "filetype_data":    json.dumps(stats["filetype_data"]),
        "heatmap_data":     json.dumps(stats["heatmap_data"]),
        "heatmap_users":    json.dumps(stats["heatmap_users"]),
        "top_users_data":   json.dumps(stats["top_users_data"]),
        "dup_dist_data":    json.dumps(stats["dup_dist_data"]),
    })


@router.get("/overview/stats", response_class=JSONResponse)
def admin_overview_stats(
    request: Request,
    days: int = 30,
    db: Session = Depends(get_db),
    admin: dict = Depends(require_admin),
):
    """
    AJAX endpoint — returns updated chart data for the given day range.
    Called by the time-range pills (30 / 90 / 0=All time) without page reload.
    """
    if days not in (30, 90, 0):
        raise HTTPException(status_code=400, detail="days must be 30, 90, or 0")

    stats = _build_stats(db, days=days)
    return JSONResponse(content=stats)


# ── All remaining routes unchanged ───────────────────────────────────────

@router.post("/select-user/{user_id}")
def select_user(
    user_id: int,
    request: Request,
    db: Session = Depends(get_db),
):
    user = get_current_user(request)
    if not user or user.get("role") != "admin":
        return RedirectResponse("/dashboard", status_code=302)
    target = db.query(User).filter(User.id == user_id).first()
    if not target:
        return RedirectResponse("/dashboard", status_code=302)
    request.session["selected_user_id"] = user_id
    return RedirectResponse("/dashboard", status_code=302)


@router.post("/exit-view")
def exit_user_view(request: Request):
    user = get_current_user(request)
    if not user or user.get("role") != "admin":
        return RedirectResponse("/dashboard", status_code=302)
    request.session.pop("selected_user_id", None)
    return RedirectResponse("/dashboard", status_code=302)


@router.get("/panel", response_class=HTMLResponse)
def admin_panel(
    request: Request,
    db: Session = Depends(get_db),
    admin: dict = Depends(require_admin)
):
    users = db.query(
        User,
        func.count(Dataset.id).label('dataset_count')
    ).outerjoin(Dataset).group_by(User.id).order_by(User.created_at.desc()).all()

    users_data = [
        {
            "id": u.id,
            "username": u.username,
            "email": u.email,
            "full_name": u.full_name,
            "role": u.role,
            "is_active": u.is_active,
            "created_at": u.created_at,
            "last_login": u.last_login,
            "dataset_count": count
        }
        for u, count in users
    ]

    return templates.TemplateResponse("admin_panel.html", {
        "request": request,
        "user": admin,
        "users": users_data,
        "show_header": True,
        "active_page": "admin"
    })


@router.post("/users/create")
async def create_user(
    request: Request,
    db: Session = Depends(get_db),
    admin: dict = Depends(require_admin)
):
    form = await request.form()
    username  = form.get("username")
    email     = form.get("email")
    password  = form.get("password")
    full_name = form.get("full_name", "")
    role      = form.get("role", "user")

    if not username or not email or not password:
        return JSONResponse(status_code=400, content={"success": False, "error": "Username, email, and password are required"})
    if role not in ["user", "admin"]:
        return JSONResponse(status_code=400, content={"success": False, "error": "Invalid role"})

    exists = db.query(User).filter((User.username == username) | (User.email == email)).first()
    if exists:
        return JSONResponse(status_code=400, content={"success": False, "error": "Username or email already exists"})

    try:
        user = User(username=username, email=email, password=User.hash_password(password),
                    full_name=full_name, role=role, is_active=True)
        db.add(user)
        db.commit()
        return JSONResponse(status_code=200, content={"success": True, "message": f"User '{username}' created successfully"})
    except Exception as e:
        db.rollback()
        return JSONResponse(status_code=500, content={"success": False, "error": str(e)})


@router.post("/users/{user_id}/delete")
def delete_user(user_id: int, request: Request, db: Session = Depends(get_db), admin: dict = Depends(require_admin)):
    if user_id == admin["id"]:
        return JSONResponse(status_code=400, content={"success": False, "error": "Cannot delete yourself"})
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        return JSONResponse(status_code=404, content={"success": False, "error": "User not found"})
    try:
        user.is_active = False
        db.commit()
        return JSONResponse(status_code=200, content={"success": True, "message": f"User '{user.username}' deactivated"})
    except Exception as e:
        db.rollback()
        return JSONResponse(status_code=500, content={"success": False, "error": str(e)})


@router.post("/users/{user_id}/toggle-active")
def toggle_user_active(user_id: int, request: Request, db: Session = Depends(get_db), admin: dict = Depends(require_admin)):
    if user_id == admin["id"]:
        return JSONResponse(status_code=400, content={"success": False, "error": "Cannot deactivate yourself"})
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        return JSONResponse(status_code=404, content={"success": False, "error": "User not found"})
    try:
        user.is_active = not user.is_active
        db.commit()
        status = "activated" if user.is_active else "deactivated"
        return JSONResponse(status_code=200, content={"success": True, "message": f"User '{user.username}' {status}"})
    except Exception as e:
        db.rollback()
        return JSONResponse(status_code=500, content={"success": False, "error": str(e)})


@router.post("/users/{user_id}/promote")
def promote_to_admin(user_id: int, request: Request, db: Session = Depends(get_db), admin: dict = Depends(require_admin)):
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        return JSONResponse(status_code=404, content={"success": False, "error": "User not found"})
    if user.role == "admin":
        return JSONResponse(status_code=400, content={"success": False, "error": "User is already an admin"})
    try:
        user.role = "admin"
        db.commit()
        return JSONResponse(status_code=200, content={"success": True, "message": f"User '{user.username}' promoted to admin"})
    except Exception as e:
        db.rollback()
        return JSONResponse(status_code=500, content={"success": False, "error": str(e)})


@router.post("/users/{user_id}/demote")
def demote_from_admin(user_id: int, request: Request, db: Session = Depends(get_db), admin: dict = Depends(require_admin)):
    if user_id == admin["id"]:
        return JSONResponse(status_code=400, content={"success": False, "error": "Cannot demote yourself"})
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        return JSONResponse(status_code=404, content={"success": False, "error": "User not found"})
    if user.role != "admin":
        return JSONResponse(status_code=400, content={"success": False, "error": "User is not an admin"})
    try:
        user.role = "user"
        db.commit()
        return JSONResponse(status_code=200, content={"success": True, "message": f"User '{user.username}' demoted to regular user"})
    except Exception as e:
        db.rollback()
        return JSONResponse(status_code=500, content={"success": False, "error": str(e)})