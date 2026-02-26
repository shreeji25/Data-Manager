from fastapi import FastAPI, Request, Response        # ✅ Request comes from fastapi, NOT h11
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, RedirectResponse
from starlette.middleware.sessions import SessionMiddleware
from starlette.formparsers import MultiPartParser, FormParser

from database import Base, engine

# Routers
from auth import router as auth_router
from modules.dashboard import router as dashboard_router
from modules.upload import router as upload_router
from modules.category import router as category_router
from modules.relation import router as relation_router
from modules.view import router as view_router
from modules.export import router as export_router
from auth import get_current_user
from modules import admin as admin_module
from modules import profile
from modules import settings
from modules import cross_relation
from fastapi.templating import Jinja2Templates

templates = Jinja2Templates(directory="templates")

# ── Remove multipart/form file size limits ──────────────────────
MultiPartParser.max_file_size  = 10 * 1024 * 1024 * 1024   # 10 GB
FormParser.max_fields          = 10_000

app = FastAPI()

# ── Middleware MUST be added before routes ──────────────────────
app.add_middleware(SessionMiddleware, secret_key="CHAYOUR_SUPER_SECRET_KEY_12345_CHANGE_ME")

# ── DB tables ───────────────────────────────────────────────────
Base.metadata.create_all(bind=engine)

# ── Static files ────────────────────────────────────────────────
app.mount("/static", StaticFiles(directory="static"), name="static")

# ── Routers ─────────────────────────────────────────────────────
app.include_router(admin_module.router)
app.include_router(auth_router)
app.include_router(dashboard_router)
app.include_router(upload_router)
app.include_router(category_router)
app.include_router(relation_router)
app.include_router(view_router)
app.include_router(export_router)
app.include_router(profile.router)
app.include_router(settings.router)
app.include_router(cross_relation.router)
# ── Landing page ─────────────────────────────────────────────────
# Not logged in  →  shows landing.html
# Already logged in  →  straight to /dashboard (skip landing)

@app.get("/", response_class=HTMLResponse)
async def landing_page(request: Request):
    user = get_current_user(request)
    if user:
        return RedirectResponse(url="/dashboard", status_code=302)
    return templates.TemplateResponse("landing.html", {"request": request})

# ── About page (landing) for logged-in users via sidebar link ─────
@app.get("/landing", response_class=HTMLResponse)
async def about_page(request: Request):
    return templates.TemplateResponse("landing.html", {"request": request})

# ── Misc ─────────────────────────────────────────────────────────
@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    return Response(status_code=204)