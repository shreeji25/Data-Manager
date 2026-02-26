import os
import traceback
import pandas as pd
from pathlib import Path
from fastapi import APIRouter, Depends, Request, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from auth import get_current_user
from database import get_db
from models import Dataset
from modules import shared
from utils.permissions import get_effective_user
from utils.duplicate_detector import (
    extract_duplicate_contacts,
    normalize_phone_public,
    normalize_email_public,
)

router = APIRouter(tags=["relation"])
templates = Jinja2Templates(directory="templates")

# In-memory caches
DUPLICATE_CACHE: dict = {}
DATASET_CACHE:   dict = {}
LOOKUP_CACHE:    dict = {}

PAGE_SIZE   = 10
VALID_MODES = {"combined", "phone", "email"}

# Project root — used to resolve relative file paths stored in the DB
BASE_DIR = Path(__file__).resolve().parent.parent


def _resolve_path(raw_path: str) -> str:
    """
    Return an absolute path.
    If raw_path is already absolute and exists → return as-is.
    Otherwise resolve relative to BASE_DIR.
    """
    p = Path(raw_path)
    if p.is_absolute() and p.exists():
        return str(p)
    resolved = BASE_DIR / raw_path
    return str(resolved)


def safe_val(v):
    if v is None:
        return ""
    if isinstance(v, float):
        return ""
    return str(v).strip()


def _build_strict_modes(raw: dict) -> dict:
    """
    Classify duplicate groups into three mutually exclusive modes:
      combined  — same phone AND same email
      phone     — same phone only (all members have different emails)
      email     — same email only (all members have different phones)

    process_dataframe() now provides:
      phone records → include 'emails' list (all emails in that phone group)
      email records → include 'phones' list (all phones in that email group)

    This makes cross-referencing straightforward and accurate.
    """
    from collections import defaultdict

    raw_combined = raw.get("combined", [])
    raw_phone    = raw.get("phone", [])
    raw_email    = raw.get("email", [])

    # Combined groups from process_dataframe are already correct
    # (grouped by phone+email both non-null)
    combined = [
        {**r, "match_type": "both"} for r in raw_combined
    ]

    # Track which phones and emails are already in combined
    combined_phones = {r["phone"] for r in raw_combined if r.get("phone")}
    combined_emails = {r["email"] for r in raw_combined if r.get("email")}

    phone_only = []
    email_only = []

    # ── Phone groups ──────────────────────────────────────────────────────
    # A phone group is phone-only if its phone is NOT already in combined
    # (meaning it doesn't share both phone+email with another record)
    for r in raw_phone:
        ph = r.get("phone") or ""
        if not ph:
            continue
        if ph in combined_phones:
            # This phone is already captured in combined — skip
            continue
        phone_only.append({**r, "match_type": "phone"})

    # ── Email groups ──────────────────────────────────────────────────────
    # An email group is email-only if its email is NOT already in combined
    for r in raw_email:
        em = r.get("email") or ""
        if not em:
            continue
        if em in combined_emails:
            # This email is already captured in combined — skip
            continue
        email_only.append({**r, "match_type": "email"})

    return {
        "combined": combined,
        "phone":    phone_only,
        "email":    email_only,
    }


# ---------------------------------------------------------------------------
# DEBUG ENDPOINT — visit /debug/dataset/1 to diagnose any dataset
# Remove this route after confirming the relation page works.
# ---------------------------------------------------------------------------

@router.get("/debug/dataset/{dataset_id}")
def debug_dataset(
    dataset_id: int,
    request: Request,
    db: Session = Depends(get_db),
):
    """
    Admin-only diagnostic endpoint. Shows:
    - What file_path is stored in the DB
    - Whether that file exists on disk
    - What extract_duplicate_contacts() returns or raises
    """
    user = get_current_user(request)
    if not user or user.get("role") != "admin":
        return JSONResponse({"error": "admin only"}, status_code=403)

    dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
    if not dataset:
        return JSONResponse({"error": "dataset not found"})

    raw_path = dataset.file_path
    abs_path = _resolve_path(raw_path)
    file_ext = Path(abs_path).suffix.lower().replace(".", "")
    exists   = os.path.exists(abs_path)

    result = {
        "dataset_id":    dataset_id,
        "file_name":     dataset.file_name,
        "raw_path":      raw_path,
        "resolved_path": abs_path,
        "file_exists":   exists,
        "file_ext":      file_ext,
        "base_dir":      str(BASE_DIR),
        "cwd":           os.getcwd(),
    }

    if not exists:
        result["error"] = "FILE DOES NOT EXIST at resolved path"
        return JSONResponse(result)

    try:
        raw    = extract_duplicate_contacts(abs_path, file_ext)
        strict = _build_strict_modes(raw)
        result["raw_phone_count"]  = len(raw.get("phone", []))
        result["raw_email_count"]  = len(raw.get("email", []))
        result["combined_count"]   = len(strict["combined"])
        result["phone_only_count"] = len(strict["phone"])
        result["email_only_count"] = len(strict["email"])
        result["combined_sample"]  = strict["combined"][:3]
        result["status"]           = "OK"
    except Exception as e:
        result["error"]     = str(e)
        result["traceback"] = traceback.format_exc()
        result["status"]    = "EXTRACT_FAILED"

    return JSONResponse(result)


# ---------------------------------------------------------------------------
# Relation HTML view (per-dataset)
# ---------------------------------------------------------------------------

@router.get("/dataset/{dataset_id}/relations", response_class=HTMLResponse)
def duplicate_contact_view(
    request: Request,
    dataset_id: int,
    page: int = 1,
    search: str = "",
    mode: str = "combined",
    db: Session = Depends(get_db),
):
    user = get_current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=302)

    effective_user = get_effective_user(request, db)
    if not effective_user:
        raise HTTPException(status_code=403, detail="Select a user first")

    dataset = db.query(Dataset).filter(
        Dataset.id == dataset_id,
        Dataset.user_id == effective_user.id,
    ).first()

    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    # ── Resolve file path ─────────────────────────────────────────────────
    abs_path = _resolve_path(dataset.file_path)
    file_ext = Path(abs_path).suffix.lower().replace(".", "")

    # ── Build duplicate cache ─────────────────────────────────────────────
    extract_error = None

    if dataset_id not in DUPLICATE_CACHE:
        if not os.path.exists(abs_path):
            extract_error = (
                f"File not found on disk.\n"
                f"Path stored in DB: {dataset.file_path}\n"
                f"Resolved to: {abs_path}\n"
                f"Server working directory: {os.getcwd()}\n\n"
                f"Fix: re-upload this file, or check your uploads directory."
            )
            DUPLICATE_CACHE[dataset_id] = {"combined": [], "phone": [], "email": []}
        else:
            # Prefer the header-corrected cleaned file if it exists.
            # upload.py saves corrected headers as "cleaned_<stem>.csv"
            # in the same directory — use that so phone/email columns are
            # detected correctly after a header correction.
            raw_dir      = os.path.dirname(abs_path)
            raw_stem     = os.path.splitext(os.path.basename(abs_path))[0]
            cleaned_path = os.path.join(raw_dir, f"cleaned_{raw_stem}.csv")
            load_path    = cleaned_path if os.path.exists(cleaned_path) else abs_path
            load_ext     = Path(load_path).suffix.lower().replace(".", "")

            try:
                raw = extract_duplicate_contacts(load_path, load_ext)
                DUPLICATE_CACHE[dataset_id] = _build_strict_modes(raw)
            except Exception as e:
                extract_error = (
                    f"{type(e).__name__}: {e}\n\n"
                    f"File: {load_path}\n\n"
                    f"{traceback.format_exc()}"
                )
                DUPLICATE_CACHE[dataset_id] = {"combined": [], "phone": [], "email": []}

    all_results = DUPLICATE_CACHE.get(dataset_id, {})

    if mode not in VALID_MODES:
        mode = "combined"

    results = all_results.get(mode, [])

    # ── Search ────────────────────────────────────────────────────────────
    if search:
        s = search.lower().strip()
        results = [
            r for r in results
            if s in (r.get("phone", "") or "").lower()
            or s in (r.get("email", "") or "").lower()
        ]

    # ── Pagination ────────────────────────────────────────────────────────
    total       = len(results)
    total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
    page        = max(1, min(page, total_pages))
    start       = (page - 1) * PAGE_SIZE
    records     = results[start : start + PAGE_SIZE]

    max_links  = 5
    start_page = max(1, page - max_links // 2)
    end_page   = min(total_pages, start_page + max_links - 1)
    if end_page - start_page < max_links - 1:
        start_page = max(1, end_page - max_links + 1)

    is_admin = user.get("role") == "admin"

    return templates.TemplateResponse(
        "relation_view.html",
        {
            "request":        request,
            "user":           user,
            "dataset":        dataset,
            "records":        records,
            "page":           page,
            "total_pages":    total_pages,
            "total":          total,
            "search":         search,
            "mode":           mode,
            "start_page":     start_page,
            "end_page":       end_page,
            "admin_mode":     is_admin,
            "viewing_user":   effective_user,
            "show_header":    True,
            "show_sidebar":  False,
            "extract_error":  extract_error,
            "combined_count": len(all_results.get("combined", [])),
            "phone_count":    len(all_results.get("phone",    [])),
            "email_count":    len(all_results.get("email",    [])),
        },
    )


# ---------------------------------------------------------------------------
# AJAX drill-down
# ---------------------------------------------------------------------------

@router.get("/get-duplicate-records/{dataset_id}")
def get_duplicate_records(
    dataset_id: int,
    request: Request,
    phone: str = "",
    email: str = "",
    db: Session = Depends(get_db),
):
    user = get_current_user(request)
    if not user:
        return {"error": "Not authenticated"}

    effective_user = get_effective_user(request, db)
    if not effective_user:
        return {"error": "Not authenticated"}

    dataset = db.query(Dataset).filter(
        Dataset.id == dataset_id,
        Dataset.user_id == effective_user.id,
    ).first()

    if not dataset:
        return {"error": "Dataset not found"}

    phone = normalize_phone_public(phone)
    email = normalize_email_public(email)

    _empty = {"", "—", "null", "none", None}
    phone = None if phone in _empty else phone
    email = None if email in _empty else email

    cache_key = f"{dataset_id}|{phone}|{email}"
    if cache_key in LOOKUP_CACHE:
        return LOOKUP_CACHE[cache_key]

    abs_path = _resolve_path(dataset.file_path)

    if dataset_id not in DATASET_CACHE:
        if not os.path.exists(abs_path):
            return {"error": f"File not found: {abs_path}"}
        try:
            # Prefer the header-corrected cleaned file if available
            raw_dir      = os.path.dirname(abs_path)
            raw_stem     = os.path.splitext(os.path.basename(abs_path))[0]
            cleaned_path = os.path.join(raw_dir, f"cleaned_{raw_stem}.csv")
            load_path    = cleaned_path if os.path.exists(cleaned_path) else abs_path
            df = shared.read_file(load_path)
            df.columns = df.columns.str.lower().str.strip()
            df = df.fillna("")

            phone_cols = []
            email_col  = None

            PHONE_KEYWORDS = [
                "phone_no", "contact_no", "phone", "mobile", "cell",
                "tel", "mob", "phoneno", "mobileno", "contactno",
            ]
            PHONE_EXCLUDES = [
                "zip", "pin", "postal", "code", "index", "sr",
                "ref", "two", "four", "wheeler", "company", "fax",
            ]
            EMAIL_KEYWORDS = ["email", "e-mail", "emailid", "emailaddress", "mail"]
            EMAIL_EXCLUDES = ["name", "username", "filename"]

            for col in df.columns:
                lc = col.lower()
                if any(k in lc for k in PHONE_KEYWORDS) and not any(b in lc for b in PHONE_EXCLUDES):
                    phone_cols.append(col)
                if not email_col and any(k in lc for k in EMAIL_KEYWORDS) and not any(b in lc for b in EMAIL_EXCLUDES):
                    email_col = col

            # Merge multiple phone columns
            phone_col = None
            if len(phone_cols) == 1:
                phone_col = phone_cols[0]
            elif len(phone_cols) > 1:
                df["__merged_phone__"] = df[phone_cols].apply(
                    lambda row: next(
                        (
                            str(v).strip().rstrip(".0")
                            for v in row
                            if pd.notna(v)
                            and str(v).strip() not in ("", "nan", "none", "null", "0")
                        ),
                        None,
                    ),
                    axis=1,
                )
                phone_col = "__merged_phone__"

            if phone_col:
                df[phone_col] = df[phone_col].apply(normalize_phone_public)
            if email_col:
                df[email_col] = df[email_col].apply(normalize_email_public)

            DATASET_CACHE[dataset_id] = {
                "df": df, "phone_col": phone_col, "email_col": email_col,
            }

        except Exception as e:
            return {"error": str(e)}

    cache_data = DATASET_CACHE[dataset_id]
    df         = cache_data["df"]
    phone_col  = cache_data["phone_col"]
    email_col  = cache_data["email_col"]

    match_type = "None"

    if phone and email and phone_col and email_col:
        df2 = df[(df[phone_col] == phone) & (df[email_col] == email)]
        match_type = "Phone + Email"
    elif phone and phone_col and not email:
        df2 = df[df[phone_col] == phone]
        match_type = "Phone only"
    elif email and email_col and not phone:
        df2 = df[df[email_col] == email]
        match_type = "Email only"
    elif phone and phone_col:
        df2 = df[df[phone_col] == phone]
        match_type = "Phone fallback"
    elif email and email_col:
        df2 = df[df[email_col] == email]
        match_type = "Email fallback"
    else:
        df2 = df.iloc[0:0]

    if df2.empty:
        result = {
            "records": [], "columns": df.columns.tolist(),
            "total": 0, "match_type": match_type, "info": "No records found",
        }
        LOOKUP_CACHE[cache_key] = result
        return result

    records = [
        {col: safe_val(row[col]) for col in df2.columns}
        for _, row in df2.iterrows()
    ]

    result = {
        "records":      records,
        "columns":      df.columns.tolist(),
        "total":        len(records),
        "match_type":   match_type,
        "phone_column": phone_col,
        "email_column": email_col,
    }
    LOOKUP_CACHE[cache_key] = result
    return result