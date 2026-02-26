"""
modules/settings.py
-------------------
User settings page — account info, password change, danger zone.

Routes:
  GET  /settings                → render settings page
  POST /settings/account        → update name / email / username / password
  POST /settings/clear-datasets → delete all user datasets (keep categories)
  POST /settings/delete-account → delete account + all data, then logout
"""

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from auth import get_current_user
from database import get_db
from models import Dataset, Category, User

router = APIRouter(tags=["settings"])
templates = Jinja2Templates(directory="templates")


# ---------------------------------------------------------------------------
# GET /settings
# ---------------------------------------------------------------------------

@router.get("/settings", response_class=HTMLResponse)
def settings_page(
    request: Request,
    success: str = "",
    error: str = "",
    db: Session = Depends(get_db),
):
    user = get_current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=302)

    current_user = db.query(User).filter(User.id == user["id"]).first()
    if not current_user:
        return RedirectResponse("/login", status_code=302)

    return templates.TemplateResponse("settings.html", {
        "request":     request,
        "user":        current_user,
        "show_header": True,
        "admin_mode":  current_user.role == "admin",
        "admin_users": [],
        "categories":  [],
        "show_sidebar": False,
        "category_counts": {},
        "viewing_user": None,
        "success_msg": success,
        "error_msg":   error,
    })


# ---------------------------------------------------------------------------
# POST /settings/account  — update profile + optional password change
# ---------------------------------------------------------------------------

@router.post("/settings/account")
def update_account(
    request: Request,
    full_name:        str = Form(""),
    username:         str = Form(...),
    current_password: str = Form(""),
    new_password:     str = Form(""),
    confirm_password: str = Form(""),
    db: Session = Depends(get_db),
):
    user = get_current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=302)

    current_user = db.query(User).filter(User.id == user["id"]).first()
    if not current_user:
        return RedirectResponse("/login", status_code=302)

    # ── Check for username conflict ──────────────────────────────────────
    if username != current_user.username:
        taken = db.query(User).filter(User.username == username, User.id != current_user.id).first()
        if taken:
            return RedirectResponse("/settings?error=Username+already+taken", status_code=302)

    # ── Password change (optional) ──────────────────────────────────────
    if new_password:
        if not current_password:
            return RedirectResponse("/settings?error=Enter+your+current+password+to+change+it", status_code=302)
        if not current_user.verify_password(current_password):
            return RedirectResponse("/settings?error=Current+password+is+incorrect", status_code=302)
        if len(new_password) < 8:
            return RedirectResponse("/settings?error=New+password+must+be+at+least+8+characters", status_code=302)
        if new_password != confirm_password:
            return RedirectResponse("/settings?error=New+passwords+do+not+match", status_code=302)
        current_user.password = User.hash_password(new_password)

    # ── Apply changes ────────────────────────────────────────────────────
    current_user.full_name = full_name.strip() or None
    current_user.username  = username.strip()
    db.commit()

    # Refresh session to reflect updated username/name
    request.session["user"] = {
        "id":        current_user.id,
        "username":  current_user.username,
        "email":     current_user.email,
        "role":      current_user.role,
        "full_name": current_user.full_name,
    }

    return RedirectResponse("/settings?success=Account+updated+successfully", status_code=302)


# ---------------------------------------------------------------------------
# POST /settings/clear-datasets  — delete all datasets (keep categories)
# ---------------------------------------------------------------------------

@router.post("/settings/clear-datasets")
def clear_datasets(
    request: Request,
    db: Session = Depends(get_db),
):
    user = get_current_user(request)
    if not user:
        return JSONResponse({"success": False, "error": "Not authenticated"}, status_code=401)

    db.query(Dataset).filter(Dataset.user_id == user["id"]).delete(synchronize_session=False)
    db.commit()
    return JSONResponse({"success": True})


# ---------------------------------------------------------------------------
# POST /settings/delete-account  — nuke everything
# ---------------------------------------------------------------------------

@router.post("/settings/delete-account")
def delete_account(
    request: Request,
    db: Session = Depends(get_db),
):
    user = get_current_user(request)
    if not user:
        return JSONResponse({"success": False, "error": "Not authenticated"}, status_code=401)

    user_id = user["id"]

    # Delete in dependency order: datasets → categories → user
    db.query(Dataset).filter(Dataset.user_id == user_id).delete(synchronize_session=False)
    db.query(Category).filter(Category.user_id == user_id).delete(synchronize_session=False)
    db.query(User).filter(User.id == user_id).delete(synchronize_session=False)
    db.commit()

    # Clear the session
    request.session.clear()
    return JSONResponse({"success": True})


# ---------------------------------------------------------------------------
# POST /settings/change-email  — change email with password verification
# ---------------------------------------------------------------------------

@router.post("/settings/change-email")
def change_email(
    request: Request,
    new_email: str = Form(...),
    password:  str = Form(...),
    db: Session = Depends(get_db),
):
    user = get_current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=302)

    current_user = db.query(User).filter(User.id == user["id"]).first()
    if not current_user:
        return RedirectResponse("/login", status_code=302)

    # Verify current password
    if not current_user.verify_password(password):
        return RedirectResponse("/settings?error=Incorrect+password", status_code=302)

    new_email = new_email.strip().lower()

    # Check email not already taken
    taken = db.query(User).filter(User.email == new_email, User.id != current_user.id).first()
    if taken:
        return RedirectResponse("/settings?error=Email+already+in+use", status_code=302)

    current_user.email = new_email
    db.commit()

    # Refresh session
    request.session["user"] = {
        "id":        current_user.id,
        "username":  current_user.username,
        "email":     current_user.email,
        "role":      current_user.role,
        "full_name": current_user.full_name,
    }

    return RedirectResponse("/settings?success=Email+updated+successfully", status_code=302)


# ---------------------------------------------------------------------------
# POST /settings/change-password  — dedicated password change route
# ---------------------------------------------------------------------------

@router.post("/settings/change-password")
def change_password(
    request: Request,
    current_password: str = Form(...),
    new_password:     str = Form(...),
    confirm_password: str = Form(...),
    db: Session = Depends(get_db),
):
    user = get_current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=302)

    current_user = db.query(User).filter(User.id == user["id"]).first()
    if not current_user:
        return RedirectResponse("/login", status_code=302)

    if not current_user.verify_password(current_password):
        return RedirectResponse("/settings?error=Current+password+is+incorrect", status_code=302)

    if len(new_password) < 8:
        return RedirectResponse("/settings?error=Password+must+be+at+least+8+characters", status_code=302)

    if new_password != confirm_password:
        return RedirectResponse("/settings?error=Passwords+do+not+match", status_code=302)

    current_user.password = User.hash_password(new_password)
    db.commit()

    return RedirectResponse("/settings?success=Password+changed+successfully", status_code=302)