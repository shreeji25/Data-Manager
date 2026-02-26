"""
utils/permissions.py
--------------------
Authentication and permission utilities for multi-tenant system.

Public API:
  require_login(request)                          → session dict or raises 401
  require_admin(request)                          → session dict (admin only) or raises 401/403
  get_effective_user(request, db)                 → User ORM object or None
  get_sidebar_context(request, db, current_user)  → dict for base.html
"""

from fastapi import Request, HTTPException, Depends
from sqlalchemy.orm import Session

from auth import get_current_user
from database import get_db
from models import User, Dataset


# ---------------------------------------------------------------------------
# Auth guards
# ---------------------------------------------------------------------------

def require_login(request: Request):
    """Return session user dict, or raise 401."""
    user = get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return user


def require_admin(request: Request):
    """Return session user dict (admin only), or raise 401/403."""
    user = get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    if user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    return user


# ---------------------------------------------------------------------------
# Multi-tenant resolver
# ---------------------------------------------------------------------------

def get_effective_user(request: Request, db: Session = Depends(get_db)):
    """
    Returns the User ORM object whose data should be shown:

      Normal user  → themselves (always)
      Admin        → selected user from session, or None if none selected

    Never returns a raw session dict — always a full ORM User or None.
    """
    current_user = get_current_user(request)
    if not current_user:
        return None

    # Normal users always see their own data
    if current_user.get("role") != "admin":
        return db.query(User).filter(User.id == current_user["id"]).first()

    # Admin: check if a user has been selected
    selected_user_id = request.session.get("selected_user_id")
    if not selected_user_id:
        return None  # Admin must select a user first

    return db.query(User).filter(User.id == selected_user_id).first()


# ---------------------------------------------------------------------------
# Sidebar context builder
# ---------------------------------------------------------------------------

def get_sidebar_context(request: Request, db: Session, current_user: User) -> dict:
    """
    Build sidebar variables for base.html.

    Returns dict with:
      admin_users       → list of (User, dataset_count) tuples  [admin only]
      selected_user_id  → int | None                            [admin only]
      categories        → list[Category]                        [normal user only]

    The (User, dataset_count) tuple format is required by the admin sidebar
    badge that shows how many datasets each user has uploaded.
    """
    from models import Category  # local import avoids circular reference

    sidebar_data = {
        "admin_users": [],
        "selected_user_id": None,
        "categories": [],
    }

    role = (current_user.role or "").strip().lower()

    if role == "admin":
        users = (
            db.query(User)
            .filter(User.role != "admin", User.is_active == True)
            .order_by(User.username)
            .all()
        )

        # (User, dataset_count) tuples — sidebar badge needs the count
        admin_users = []
        for u in users:
            count = db.query(Dataset).filter(Dataset.user_id == u.id).count()
            admin_users.append((u, count))

        sidebar_data["admin_users"] = admin_users
        sidebar_data["selected_user_id"] = request.session.get("selected_user_id")

    else:
        # Normal user: load their categories for the sidebar filter links
        categories = (
            db.query(Category)
            .filter(Category.user_id == current_user.id)
            .order_by(Category.name)
            .all()
        )
        sidebar_data["categories"] = categories

    return sidebar_data