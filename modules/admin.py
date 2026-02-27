"""
modules/admin.py

Admin-only routes for user management and user view selection.
"""

from fastapi import APIRouter, Depends, Request, HTTPException
from fastapi.responses import RedirectResponse, HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
from sqlalchemy import func

from auth import get_current_user
from database import get_db
from models import User, Dataset

router = APIRouter(prefix="/admin", tags=["admin"])
templates = Jinja2Templates(directory="templates")


def require_admin(request: Request):
    """Dependency to check if user is admin"""
    user = get_current_user(request)
    if not user or user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    return user


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




@router.post("/users/{user_id}/change-password")
async def change_user_password(
    user_id: int,
    request: Request,
    db: Session = Depends(get_db),
    admin: dict = Depends(require_admin)
):
    """Admin changes the password of any user (including themselves)."""
    form = await request.form()
    new_password = form.get("new_password", "").strip()

    # Validation
    if not new_password:
        return JSONResponse(
            status_code=400,
            content={"success": False, "error": "New password is required"}
        )

    if len(new_password) < 6:
        return JSONResponse(
            status_code=400,
            content={"success": False, "error": "Password must be at least 6 characters"}
        )

    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        return JSONResponse(
            status_code=404,
            content={"success": False, "error": "User not found"}
        )

    try:
        user.password = User.hash_password(new_password)
        db.commit()

        return JSONResponse(
            status_code=200,
            content={"success": True, "message": f"Password updated for '{user.username}'"}
        )

    except Exception as e:
        db.rollback()
        return JSONResponse(
            status_code=500,
            content={"success": False, "error": f"Failed to update password: {str(e)}"}
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