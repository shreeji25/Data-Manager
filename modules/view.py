"""
modules/view.py

Multi-tenant dataset viewer:
- Always fetches dataset scoped to effective_user.id
- Admin can view any user's dataset via get_effective_user
- Returns 404 (not 403) to avoid leaking existence of other users' datasets
"""

from fastapi import APIRouter, Depends, Request, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
import math
import os

from auth import get_current_user
from database import get_db
from models import Dataset, User
from modules import shared
from utils.permissions import get_effective_user

router = APIRouter(tags=["view"])
templates = Jinja2Templates(directory="templates")


@router.get("/view/{dataset_id}", response_class=HTMLResponse)
def view_dataset(
    dataset_id: int,
    request: Request,
    db: Session = Depends(get_db),
):
    # ✅ Check authentication
    user = get_current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=302)
    
    # Get effective user (for admin viewing other users' data)
    effective_user = get_effective_user(request, db)
    is_admin = user.get("role") == "admin"

# Admin with no selected user can open any dataset directly
    if not effective_user and not is_admin:
      raise HTTPException(status_code=403, detail="Select a user first")

# Admin with no selected user → fetch by ID only (no user scope)
    if is_admin and not effective_user:
     dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
    else:
     dataset = db.query(Dataset).filter(
        Dataset.id == dataset_id,
        Dataset.user_id == effective_user.id,
      ).first()
   
    if not dataset:
        raise HTTPException(404, "Dataset not found")
    
    owner_id = effective_user.id if effective_user else dataset.user_id
    print(f"📂 Loading dataset: {dataset.file_name} (owner id={owner_id})")
    
    # Query params
    show = request.query_params.get("show")
    page = int(request.query_params.get("page", 1))
    
    # ── Resolve paths ────────────────────────────────────────────────────
    # Always prefer the cleaned (header-corrected) file if it exists.
    # The correction page saves it as "cleaned_<stem>.csv" in the same dir.
    raw_path     = dataset.file_path
    raw_dir      = os.path.dirname(raw_path)
    raw_stem     = os.path.splitext(os.path.basename(raw_path))[0]
    cleaned_path = os.path.join(raw_dir, f"cleaned_{raw_stem}.csv")

    # Use a cache key that reflects WHICH file is actually loaded so that
    # correcting headers doesn't serve the old cached original file.
    cache_key = f"cleaned_{raw_stem}" if os.path.exists(cleaned_path) else dataset.file_name

    # Load dataframe (cache first, then file)
    df = shared.get_cached_df(cache_key)

    if df is None:
        print("⚠️ Cache miss — reloading from file...")

        load_path = cleaned_path if os.path.exists(cleaned_path) else raw_path

        if not os.path.exists(load_path):
            raise HTTPException(404, "File missing on server")

        print(f"📂 Loading from: {load_path}")

        try:
            df = shared.read_file(load_path)
        except Exception as e:
            print("❌ File load error:", e)
            raise HTTPException(500, "Could not read dataset file")

        shared.set_cached_df(cache_key, df)
        print("✅ Reloaded + cached")
    
    if df is None or df.empty:
        raise HTTPException(500, "Dataset could not be loaded")
    
    print(f"📊 DataFrame loaded: {df.shape}")
    
    # Duplicate detection
    if "__dup_combined__" not in df.columns:
        try:
            df = shared.detect_duplicates(df)
        except Exception as e:
            print("❌ Duplicate detect error:", e)
            df["__dup_combined__"] = False
            df["__dup_phone__"] = False
            df["__dup_email__"] = False
    
    # Ensure all duplicate columns exist
    if "__dup_combined__" not in df.columns:
        df["__dup_combined__"] = False
    if "__dup_phone__" not in df.columns:
        df["__dup_phone__"] = False
    if "__dup_email__" not in df.columns:
        df["__dup_email__"] = False
    
    # Calculate stats BEFORE filtering
    total_records = len(df)
    duplicate_records = int(df["__dup_combined__"].sum())
    phone_duplicates  = int(df["__dup_phone__"].sum())
    email_duplicates  = int(df["__dup_email__"].sum())
    actual_records = total_records - duplicate_records

    print(f"📊 Stats: Total={total_records}, Duplicates={duplicate_records}, Actual={actual_records}")

    # ── Exact duplicate detection (full-row match) ────────────────────────
    exact_dup_data = shared.detect_exact_duplicates(df)
    exact_dup_count = exact_dup_data["count"]
    exact_dup_groups = exact_dup_data["groups"]

    # ── Column fill rates (for visualisation sidebar) ─────────────────────
    column_fill_rates = shared.get_column_fill_rates(df)

    # ── Top duplicate values for sidebar (phone/email) ────────────────────
    import json as _json
    sidebar_stats = {
        "total": total_records,
        "actual": actual_records,
        "combined": duplicate_records,
        "phone": phone_duplicates,
        "email": email_duplicates,
        "exact": exact_dup_count,
        "exact_groups": exact_dup_groups,
        "fill_rates": column_fill_rates,
        "health_score": round((actual_records / total_records) * 100, 1) if total_records else 100,
    }
    
    # Update database with fresh counts (in case they changed)
    dataset.row_count = total_records
    dataset.duplicate_records = duplicate_records
    dataset.actual_records = actual_records
    db.commit()
    print(f"💾 Database updated: row_count={total_records}, duplicates={duplicate_records}, actual={actual_records}")
    
    # Filter based on mode
    if show == "duplicates":
        df = df[df["__dup_combined__"] == True]
        print(f"🔍 Filtered to duplicates only: {len(df)} rows")
    
    # Pagination
    rows_per_page = 10
    total = len(df)
    total_pages = max(1, math.ceil(total / rows_per_page))
    
    start = (page - 1) * rows_per_page
    end = start + rows_per_page
    df_page = df.iloc[start:end]
    
    max_links = 5
    start_page = max(1, page - max_links // 2)
    end_page = min(total_pages, start_page + max_links - 1)
    if end_page - start_page < max_links - 1:
        start_page = max(1, end_page - max_links + 1)
    
    
    return templates.TemplateResponse("view.html", {
        "request": request,
        "user": user,
        "active_page": "dashboard",
        "dataset": dataset,
        "columns": df_page.columns.tolist(),
        "rows": df_page.to_dict(orient="records"),
        "page": page,
        "total_pages": total_pages,
        "start_page": start_page,
        "end_page": end_page,
        "show": show,
        "show_header": False,
        "show_sidebar": False,
        "admin_mode": is_admin,
        "viewing_user": effective_user,
        # Sidebar data
        "sidebar_stats": sidebar_stats,
        "exact_dup_count": exact_dup_count,
        "exact_dup_groups": exact_dup_groups,
        "column_fill_rates": column_fill_rates,
        "phone_duplicates": phone_duplicates,
        "email_duplicates": email_duplicates,
    })


@router.post("/delete/{dataset_id}")
def delete_dataset(
    dataset_id: int,
    request: Request,
    db: Session = Depends(get_db),
):
    """
    Delete is intentionally scoped to current_user (not effective_user).
    Admin cannot delete datasets on behalf of users — only view them.
    """
    # ✅ Check authentication
    user = get_current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=302)
    
    user_id = user["id"]
    
    # Admin cannot delete via this route
    if user.get("role") == "admin":
        raise HTTPException(403, "Admins cannot delete user datasets")
    
    dataset = db.query(Dataset).filter(
        Dataset.id == dataset_id,
        Dataset.user_id == user_id,
    ).first()
    
    if not dataset:
        raise HTTPException(404, "Dataset not found")
    
    # Remove file from disk
    import os
    if os.path.exists(dataset.file_path):
        try:
            os.remove(dataset.file_path)
        except Exception as e:
            print(f"⚠️ Could not remove file {dataset.file_path}: {e}")
    
    db.delete(dataset)
    db.commit()
    
    print(f"🗑️ Deleted dataset {dataset_id} for user {user_id}")
    
    return RedirectResponse("/dashboard", status_code=303)