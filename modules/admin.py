"""
modules/admin.py

Admin-only routes for user management and user view selection.
"""

from fastapi import APIRouter, Depends, Request, HTTPException
from fastapi.responses import RedirectResponse, HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
from sqlalchemy import func, text
from datetime import datetime, timedelta
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


@router.get("/overview", response_class=HTMLResponse)
def admin_overview(
    request: Request,
    db: Session = Depends(get_db),
    admin: dict = Depends(require_admin),
):
    """Admin overview dashboard with system-wide charts and stats."""

    # ── Stat Cards ────────────────────────────────────────────────────────
    total_users    = db.query(func.count(User.id)).filter(User.role != "admin").scalar() or 0
    total_datasets = db.query(func.count(Dataset.id)).scalar() or 0
    total_rows     = db.query(func.sum(Dataset.row_count)).scalar() or 0

    most_active = (
        db.query(User.username, func.count(Dataset.id).label("cnt"))
        .join(Dataset, Dataset.user_id == User.id)
        .filter(User.role != "admin")
        .group_by(User.id)
        .order_by(func.count(Dataset.id).desc())
        .first()
    )
    most_active_user  = most_active[0] if most_active else "—"
    most_active_count = most_active[1] if most_active else 0

    # ── Chart 1: Dataset Growth (cumulative by month) ─────────────────────
    datasets_all = (
        db.query(Dataset.uploaded_at)
        .filter(Dataset.uploaded_at.isnot(None))
        .order_by(Dataset.uploaded_at)
        .all()
    )
    # Build cumulative monthly counts
    monthly = {}
    for row in datasets_all:
        if row.uploaded_at:
            key = row.uploaded_at.strftime("%Y-%m")
            monthly[key] = monthly.get(key, 0) + 1
    cumulative = []
    running = 0
    for key in sorted(monthly.keys()):
        running += monthly[key]
        cumulative.append({"month": key, "count": running})
    growth_data = json.dumps(cumulative[-12:] if len(cumulative) > 12 else cumulative)

    # ── Chart 2: Category Distribution ────────────────────────────────────
    cat_rows = (
        db.query(Dataset.department, func.count(Dataset.id).label("cnt"))
        .filter(Dataset.department.isnot(None))
        .group_by(Dataset.department)
        .order_by(func.count(Dataset.id).desc())
        .all()
    )
    cat_data = json.dumps([{"name": r.department, "count": r.cnt} for r in cat_rows])

    uncategorised = db.query(func.count(Dataset.id)).filter(
        (Dataset.department.is_(None)) | (Dataset.department == "")
    ).scalar() or 0
    if uncategorised > 0:
        cat_list = json.loads(cat_data)
        cat_list.append({"name": "Uncategorised", "count": uncategorised})
        cat_data = json.dumps(cat_list)

    # ── Chart 3: File Type Breakdown ──────────────────────────────────────
    all_files = db.query(Dataset.file_name).all()
    type_counts = {"CSV": 0, "XLSX": 0, "XLS": 0, "Other": 0}
    for row in all_files:
        if row.file_name:
            ext = row.file_name.rsplit(".", 1)[-1].upper() if "." in row.file_name else ""
            if ext == "CSV":
                type_counts["CSV"] += 1
            elif ext == "XLSX":
                type_counts["XLSX"] += 1
            elif ext == "XLS":
                type_counts["XLS"] += 1
            else:
                type_counts["Other"] += 1
    filetype_data = json.dumps([{"name": k, "count": v} for k, v in type_counts.items() if v > 0])

    # ── Chart 4: User Activity Heatmap (uploads per user per day, 84 days) ─
    since = datetime.now() - timedelta(days=84)
    activity_rows = (
        db.query(
            User.username,
            func.date(Dataset.uploaded_at).label("day"),
            func.count(Dataset.id).label("cnt"),
        )
        .join(Dataset, Dataset.user_id == User.id)
        .filter(User.role != "admin", Dataset.uploaded_at >= since)
        .group_by(User.username, func.date(Dataset.uploaded_at))
        .all()
    )
    heatmap_data = json.dumps([
        {"user": r.username, "day": str(r.day), "count": r.cnt}
        for r in activity_rows
    ])

    # All non-admin usernames for heatmap y-axis order
    heatmap_users = json.dumps([
        u.username for u in
        db.query(User).filter(User.role != "admin").order_by(User.username).all()
    ])

    # ── Chart 5: Top Users by Dataset Count ───────────────────────────────
    top_users_rows = (
        db.query(User.username, func.count(Dataset.id).label("cnt"))
        .outerjoin(Dataset, Dataset.user_id == User.id)
        .filter(User.role != "admin")
        .group_by(User.id)
        .order_by(func.count(Dataset.id).desc())
        .all()
    )
    top_users_data = json.dumps([{"user": r.username, "count": r.cnt} for r in top_users_rows])

    # ── Chart 6: Duplicate Rate Distribution ──────────────────────────────
    datasets_with_rows = (
        db.query(Dataset.row_count, Dataset.duplicate_records)
        .filter(Dataset.row_count.isnot(None), Dataset.row_count > 0)
        .all()
    )
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
    dup_dist_data = json.dumps([{"range": k, "count": v} for k, v in buckets.items()])

    # ── Admin users list for sidebar ──────────────────────────────────────
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
        "total_users":      total_users,
        "total_datasets":   total_datasets,
        "total_rows":       f"{total_rows:,}" if total_rows else "0",
        "most_active_user": most_active_user,
        "most_active_count": most_active_count,
        # Chart data (JSON strings for JS)
        "cat_data":         cat_data,
        "filetype_data":    filetype_data,
        "heatmap_data":     heatmap_data,
        "heatmap_users":    heatmap_users,
        "top_users_data":   top_users_data,
        "dup_dist_data":    dup_dist_data,
    })


@router.post("/select-user/{user_id}")
def select_user(
    user_id: int,
    request: Request,
    db: Session = Depends(get_db),
):
    """Admin selects a user to view."""
    user = get_current_user(request)
    if not user or user.get("role") != "admin":
        return RedirectResponse("/dashboard", status_code=302)
    
    # Verify target user exists
    target = db.query(User).filter(User.id == user_id).first()
    if not target:
        return RedirectResponse("/dashboard", status_code=302)
    
    request.session["selected_user_id"] = user_id
    return RedirectResponse("/dashboard", status_code=302)


@router.post("/exit-view")
def exit_user_view(request: Request):
    """Admin exits the selected user view."""
    user = get_current_user(request)
    if not user or user.get("role") != "admin":
        return RedirectResponse("/dashboard", status_code=302)
    
    request.session.pop("selected_user_id", None)
    return RedirectResponse("/dashboard", status_code=302)


# ============= NEW: ADMIN PANEL =============

@router.get("/panel", response_class=HTMLResponse)
def admin_panel(
    request: Request,
    db: Session = Depends(get_db),
    admin: dict = Depends(require_admin)
):
    """Admin control panel for user management"""
    
    # Get all users with their dataset counts
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
    """Create a new user"""
    form = await request.form()
    
    username = form.get("username")
    email = form.get("email")
    password = form.get("password")
    full_name = form.get("full_name", "")
    role = form.get("role", "user")
    
    # Validation
    if not username or not email or not password:
        return JSONResponse(
            status_code=400,
            content={"success": False, "error": "Username, email, and password are required"}
        )
    
    if role not in ["user", "admin"]:
        return JSONResponse(
            status_code=400,
            content={"success": False, "error": "Invalid role"}
        )
    
    # Check if exists
    exists = db.query(User).filter(
        (User.username == username) | (User.email == email)
    ).first()
    
    if exists:
        return JSONResponse(
            status_code=400,
            content={"success": False, "error": "Username or email already exists"}
        )
    
    try:
        hashed_password = User.hash_password(password)
        
        user = User(
            username=username,
            email=email,
            password=hashed_password,
            full_name=full_name,
            role=role,
            is_active=True
        )
        
        db.add(user)
        db.commit()
        
        return JSONResponse(
            status_code=200,
            content={"success": True, "message": f"User '{username}' created successfully"}
        )
        
    except Exception as e:
        db.rollback()
        return JSONResponse(
            status_code=500,
            content={"success": False, "error": f"Failed to create user: {str(e)}"}
        )


@router.post("/users/{user_id}/delete")
def delete_user(
    user_id: int,
    request: Request,
    db: Session = Depends(get_db),
    admin: dict = Depends(require_admin)
):
    """Delete a user (soft delete - deactivate)"""
    
    # Prevent self-deletion
    if user_id == admin["id"]:
        return JSONResponse(
            status_code=400,
            content={"success": False, "error": "Cannot delete yourself"}
        )
    
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        return JSONResponse(
            status_code=404,
            content={"success": False, "error": "User not found"}
        )
    
    try:
        # Soft delete - just deactivate
        user.is_active = False
        db.commit()
        
        return JSONResponse(
            status_code=200,
            content={"success": True, "message": f"User '{user.username}' deactivated"}
        )
        
    except Exception as e:
        db.rollback()
        return JSONResponse(
            status_code=500,
            content={"success": False, "error": f"Failed to delete user: {str(e)}"}
        )


@router.post("/users/{user_id}/toggle-active")
def toggle_user_active(
    user_id: int,
    request: Request,
    db: Session = Depends(get_db),
    admin: dict = Depends(require_admin)
):
    """Toggle user active status"""
    
    if user_id == admin["id"]:
        return JSONResponse(
            status_code=400,
            content={"success": False, "error": "Cannot deactivate yourself"}
        )
    
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        return JSONResponse(
            status_code=404,
            content={"success": False, "error": "User not found"}
        )
    
    try:
        user.is_active = not user.is_active
        db.commit()
        
        status = "activated" if user.is_active else "deactivated"
        return JSONResponse(
            status_code=200,
            content={"success": True, "message": f"User '{user.username}' {status}"}
        )
        
    except Exception as e:
        db.rollback()
        return JSONResponse(
            status_code=500,
            content={"success": False, "error": f"Failed to update user: {str(e)}"}
        )


@router.post("/users/{user_id}/promote")
def promote_to_admin(
    user_id: int,
    request: Request,
    db: Session = Depends(get_db),
    admin: dict = Depends(require_admin)
):
    """Promote user to admin"""
    
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        return JSONResponse(
            status_code=404,
            content={"success": False, "error": "User not found"}
        )
    
    if user.role == "admin":
        return JSONResponse(
            status_code=400,
            content={"success": False, "error": "User is already an admin"}
        )
    
    try:
        user.role = "admin"
        db.commit()
        
        return JSONResponse(
            status_code=200,
            content={"success": True, "message": f"User '{user.username}' promoted to admin"}
        )
        
    except Exception as e:
        db.rollback()
        return JSONResponse(
            status_code=500,
            content={"success": False, "error": f"Failed to promote user: {str(e)}"}
        )


@router.post("/users/{user_id}/demote")
def demote_from_admin(
    user_id: int,
    request: Request,
    db: Session = Depends(get_db),
    admin: dict = Depends(require_admin)
):
    """Demote admin to regular user"""
    
    if user_id == admin["id"]:
        return JSONResponse(
            status_code=400,
            content={"success": False, "error": "Cannot demote yourself"}
        )
    
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        return JSONResponse(
            status_code=404,
            content={"success": False, "error": "User not found"}
        )
    
    if user.role != "admin":
        return JSONResponse(
            status_code=400,
            content={"success": False, "error": "User is not an admin"}
        )
    
    try:
        user.role = "user"
        db.commit()
        
        return JSONResponse(
            status_code=200,
            content={"success": True, "message": f"User '{user.username}' demoted to regular user"}
        )
        
    except Exception as e:
        db.rollback()
        return JSONResponse(
            status_code=500,
            content={"success": False, "error": f"Failed to demote user: {str(e)}"}
        )