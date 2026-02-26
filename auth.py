from fastapi import APIRouter, Depends, Request
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
from database import get_db
from models import User
from datetime import datetime


router = APIRouter()
templates = Jinja2Templates(directory="templates")


# ================= HELPER FUNCTIONS =================

def get_current_user(request: Request):
    """Get user from session"""
    return request.session.get("user")


# ================= LANDING PAGE =================

@router.get("/")
def landing_page(request: Request):
    """
    Public info/landing page — no login required.
    If already logged in, skip it and go straight to dashboard.
    """
    user = get_current_user(request)
    if user:
        return RedirectResponse("/dashboard", status_code=302)

    return templates.TemplateResponse(
        "landing.html",
        {
            "request": request,
            "user": None
        }
    )


# ================= LOGIN =================

@router.get("/login")
def login_page(request: Request):
    user = get_current_user(request)
    if user:
        return RedirectResponse("/dashboard", status_code=302)
    
    return templates.TemplateResponse(
        "login.html", 
        {
            "request": request,
            "user": None
        }
    )


@router.post("/login")
async def login(request: Request, db: Session = Depends(get_db)):
    form = await request.form()
    
    username = form.get("username")
    password = form.get("password")
    
    user = (
        db.query(User)
        .filter((User.username == username) | (User.email == username))
        .first()
    )
    
    if not user or not user.verify_password(password):
        return templates.TemplateResponse(
            "login.html",
            {
                "request": request,
                "user": None,
                "error": "Invalid username or password"
            }
        )
    
    if not user.is_active:
        return templates.TemplateResponse(
            "login.html",
            {
                "request": request,
                "user": None,
                "error": "Your account has been deactivated"
            }
        )
    
    # Update last login
    user.last_login = datetime.now()
    db.commit()
    
    # ✅ CRITICAL FIX: Store user in session
    request.session["user"] = {
        "id": user.id,
        "username": user.username,
        "email": user.email,
        "role": user.role,
        "full_name": user.full_name
    }
    
    return RedirectResponse("/dashboard", status_code=302)


# ================= LOGOUT =================

@router.get("/logout")
def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/login", status_code=302)


# ================= REGISTER =================

@router.get("/register")
def register_page(request: Request):
    user = get_current_user(request)
    if user:
        return RedirectResponse("/dashboard", status_code=302)
    
    return templates.TemplateResponse(
        "register.html",
        {
            "request": request,
            "user": None
        }
    )


@router.post("/register")
async def register(request: Request, db: Session = Depends(get_db)):
    form = await request.form()
    
    username = form.get("username")
    email = form.get("email")
    password = form.get("password")
    full_name = form.get("full_name")
    
    # Validation
    if not username or not email or not password:
        return templates.TemplateResponse(
            "register.html",
            {
                "request": request,
                "user": None,
                "error": "All fields are required"
            }
        )
    
    if len(password) < 6:
        return templates.TemplateResponse(
            "register.html",
            {
                "request": request,
                "user": None,
                "error": "Password must be at least 6 characters"
            }
        )
    
    if len(password.encode("utf-8")) > 72:
        return templates.TemplateResponse(
            "register.html",
            {
                "request": request,
                "user": None,
                "error": "Password must be under 72 characters"
            }
        )
    
    # Check if exists
    exists = db.query(User).filter(
        (User.username == username) | (User.email == email)
    ).first()
    
    if exists:
        return templates.TemplateResponse(
            "register.html",
            {
                "request": request,
                "user": None,
                "error": "Username or email already exists"
            }
        )
    
    # Create user
    try:
        hashed_password = User.hash_password(password)
        
        user = User(
            username=username,
            email=email,
            password=hashed_password,
            full_name=full_name,
            role="user"
        )
        
        db.add(user)
        db.commit()
        
        return RedirectResponse("/login", status_code=302)
        
    except Exception as e:
        db.rollback()
        return templates.TemplateResponse(
            "register.html",
            {
                "request": request,
                "user": None,
                "error": f"Registration failed: {str(e)}"
            }
        )