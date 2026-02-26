"""
cross_relation.py — Cross-File Relations page
Detects matching phone/email records across multiple uploaded files.

Routes:
  GET  /cross-relations          — Main page (employee: own files, admin: all users)
  GET  /cross-relations/records  — AJAX: records for one duplicate group
"""

from fastapi import APIRouter, Depends, Request, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
from typing import Optional
import pandas as pd
import itertools
import logging

from database import get_db
from models import Dataset, User
from auth import get_current_user
from utils.permissions import get_effective_user
from modules.shared import (
    read_file,
    normalize_email,
)
import re as _re

_log = logging.getLogger(__name__)

# ── Phone keywords — matched with word-boundary logic (see _is_phone_col) ──
# Word-boundary matching ensures 'phone' does NOT match 'curphone1' but
# DOES match 'phone1', 'res_phone', 'mobileno' etc.
_PHONE_KEYWORDS = [
    'phone', 'mobile', 'mobileno', 'mob',
    'cell', 'telephone',
    'whatsapp', 'contact_no', 'contactno', 'contact_num',
    'phone_no', 'phoneno', 'phone_num', 'phone_number',
    'mobile_no', 'mob_no', 'mob_num',
    'tel_no', 'telno', 'tel_num',
    'cell_no', 'cellno',
    'resphone', 'res_phone', 'alt_phone',
    'landline', 'phn', 'ph_no', 'phno',
]

_PHONE_EXCLUDES = [
    'zip', 'pin_code', 'pincode', 'postal', 'bus_pin',
    'emp_id', 'employee_id', 'empid',
    'customer_id', 'cust_id', 'custid',
    'order_id', 'orderid',
    'invoice', 'inv_no',
    'account_no', 'accountno', 'acc_no',
    'loan_id', 'loanid',
    'ref_no', 'refno', 'reference',
    'serial_no', 'serialno', 'sr_no', 'srno',
    'two_wheeler', 'four_wheeler',
    'income', 'salary', 'credit_lim', 'amount', 'price',
    'qty', 'quantity', 'age', 'year', 'month',
    'date', 'time', 'rating', 'score', 'rank',
]

_EMAIL_KEYWORDS = [
    'email', 'e_mail', 'emailid', 'email_id', 'email_address',
    'emailadd', 'mail', 'e-mail',
]

_EMAIL_EXCLUDES = [
    'email_verified', 'email_opt', 'email_bounce', 'email_status',
]


def _is_phone_col(col: str) -> bool:
    """
    Keyword must start the column name OR follow an underscore/separator.
    Prevents 'phone' matching 'curphone1' or 'busphone2' while still
    matching 'phone', 'phone_no', 'phone1', 'res_phone', 'mobileno' etc.
    """
    c = col.lower().strip()
    if any(ex in c for ex in _PHONE_EXCLUDES):
        return False
    for kw in _PHONE_KEYWORDS:
        # keyword at start, or after underscore; followed by end, digit, or underscore
        if _re.search(r'(^|_)' + _re.escape(kw) + r'(_|$|\d)', c):
            return True
    return False


def _is_email_col(col: str) -> bool:
    c = col.lower().strip()
    if any(ex in c for ex in _EMAIL_EXCLUDES):
        return False
    return any(kw in c for kw in _EMAIL_KEYWORDS)


def normalize_phone(val) -> "str | None":
    """
    Robust Indian phone normaliser.

    Handles the common pattern in legacy datasets where a single MOB cell
    contains multiple numbers, e.g. "9848360170(M) 08468 229462" or
    "9444077355(M) 91 44 26155182".  The old last-10-digits approach would
    extract a garbage suffix from the landline portion — causing thousands of
    false-positive cross-file matches and breaking the combined/email tabs.

    Priority:
      1. First 10-digit number starting with 6-9 (Indian mobile, no prefix).
      2. First 10-digit mobile prefixed with a lone 0  (e.g. 09444107443).
      3. Single number: strip 91/0091/0 country/STD prefix.
      4. Clean 10-digit number.
      5. 10-12 digit landline/area-code combos → last 10.
      6. Cells with > 12 digits AND no extractable mobile → discard entirely
         (they are multi-number cells with no safe canonical form).
      7. 7-9 digit landlines returned as-is.
    """
    if val is None:
        return None
    s = str(val).strip()
    if not s or s.lower() in ('nan', 'none', '', 'null'):
        return None
    # Strip pandas float suffix
    if s.endswith('.0'):
        s = s[:-2]

    digits_all = _re.sub(r'\D', '', s)
    if not digits_all:
        return None

    # 1. Bare 10-digit mobile anywhere in the string (6-9 start, no adjacent digits)
    m = _re.search(r'(?<!\d)([6-9]\d{9})(?!\d)', s)
    if m:
        return m.group(1)

    # 2. Mobile prefixed with a lone leading 0  (e.g. 09444107443)
    m = _re.search(r'(?<!\d)(0([6-9]\d{9}))(?!\d)', s)
    if m:
        return m.group(2)

    # 3-6. Single-number cases
    if len(digits_all) >= 10:
        # Country code 91 + 10-digit mobile
        if digits_all.startswith('91') and len(digits_all) == 12 and digits_all[2] in '6789':
            return digits_all[2:]
        # STD 0 + 10-digit mobile
        if digits_all.startswith('0') and len(digits_all) == 11 and digits_all[1] in '6789':
            return digits_all[1:]
        # Exact 10 digits
        if len(digits_all) == 10:
            return digits_all
        # Area-code + 7-8 digit landline (≤ 12 digits total)
        if len(digits_all) <= 12:
            return digits_all[-10:]
        # > 12 digits = multi-number cell with no extractable mobile → discard
        return None

    # 7. Short landlines (7-9 digits)
    if 7 <= len(digits_all) < 10:
        return digits_all

    return None


def _detect_cols(df: "pd.DataFrame") -> "tuple[str | None, str | None]":
    """
    Detect phone and email columns using word-boundary keyword matching.
    Multiple phone columns are coalesced into __merged_phone__ (first
    non-null value wins).
    """
    phone_cols = [c for c in df.columns if _is_phone_col(c)]
    email_cols = [c for c in df.columns if _is_email_col(c)]

    phone_col = None
    if phone_cols:
        if len(phone_cols) == 1:
            phone_col = phone_cols[0]
        else:
            # Coalesce: first non-null, non-empty value across all phone columns.
            # Use .iloc by position to always get a Series even if duplicate
            # column names survived (belt-and-suspenders against ValueError).
            def _col_as_series(name: str) -> pd.Series:
                loc = df.columns.get_loc(name)
                s = df.iloc[:, loc] if isinstance(loc, int) else df.iloc[:, loc].iloc[:, 0]
                return s.astype(str)

            merged = _col_as_series(phone_cols[0]).copy()
            for pc in phone_cols[1:]:
                col_str    = _col_as_series(pc)
                needs_fill = merged.isin(['', 'nan', 'None', 'NaN', 'none', 'null'])
                merged     = merged.where(~needs_fill, col_str)
            df['__merged_phone__'] = merged
            phone_col = '__merged_phone__'

    email_col = email_cols[0] if email_cols else None
    return phone_col, email_col


router = APIRouter()
templates = Jinja2Templates(directory="templates")

# ── File colour palette (cycles if more than 10 files) ──────────────────────
FILE_COLORS = [
    "#4f46e5",  # indigo
    "#16a34a",  # green
    "#ea580c",  # orange
    "#dc2626",  # red
    "#7c3aed",  # violet
    "#0891b2",  # cyan
    "#db2777",  # pink
    "#ca8a04",  # yellow
    "#059669",  # emerald
    "#9333ea",  # purple
]

# ── In-memory cache: keyed by frozenset of file_ids ─────────────────────────
CROSS_CACHE: dict = {}


# ════════════════════════════════════════════════════════════
#  HELPERS
# ════════════════════════════════════════════════════════════

def _resolve_path(file_path: str) -> str:
    """Convert relative DB path to absolute disk path."""
    import os
    base = r"D:\Vnnovate Data\project"
    if os.path.isabs(file_path):
        return file_path
    return os.path.join(base, file_path)


def _color_for_index(i: int) -> str:
    return FILE_COLORS[i % len(FILE_COLORS)]


def _load_file_df(dataset: Dataset) -> Optional[pd.DataFrame]:
    """
    Always reads the ORIGINAL raw file — not the cleaned version.
    The cleaned file omits single-file duplicate rows which can still
    match across other files and must be included in cross-file scanning.
    """
    import os

    raw_path = _resolve_path(dataset.file_path)
    if not os.path.exists(raw_path):
        return None

    try:
        df = read_file(raw_path)
        df.columns = df.columns.str.lower().str.strip()
        markers = [c for c in df.columns if c.startswith('__') and c.endswith('__')]
        if markers:
            df = df.drop(columns=markers)
        return df
    except Exception:
        return None


def _build_cross_groups(file_dfs: list[dict]) -> dict:
    """
    Given a list of {dataset_id, file_name, user_id, user_name, color, df},
    find phone/email groups that span at least 2 files.

    Returns:
      {
        'combined': [...],
        'phone':    [...],
        'email':    [...],
        'file_colors': {dataset_id: color}
      }

    Each group dict:
      {
        phone, email, mode,
        total_records,
        file_ids:  [dataset_id, ...],
        user_ids:  [user_id, ...],
        file_data: [{dataset_id, file_name, user_name, color, records:[{col:val}], columns, phone_col, email_col}]
      }
    """
    # ── Step 1: normalise phone+email for every row across every file ────────
    all_rows = []

    for info in file_dfs:
        df     = info["df"]
        did    = info["dataset_id"]
        fname  = info["file_name"]
        uid    = info["user_id"]
        uname  = info["user_name"]
        color  = info["color"]
        phone_col, email_col = _detect_cols(df)
        cols = list(df.columns)  # AFTER _detect_cols so __merged_phone__ is included

        for _, row in df.iterrows():
            p = normalize_phone(row[phone_col]) if phone_col else None
            e = normalize_email(row[email_col]) if email_col else None
            if not p and not e:
                continue
            all_rows.append({
                "dataset_id": did,
                "file_name":  fname,
                "user_id":    uid,
                "user_name":  uname,
                "color":      color,
                "phone_norm": p,
                "email_norm": e,
                "phone_col":  phone_col,
                "email_col":  email_col,
                "columns":    cols,
                "row_dict":   row.to_dict(),
            })

    if not all_rows:
        return {"combined": [], "phone": [], "email": [], "file_colors": {}}

    df_all = pd.DataFrame(all_rows)

    # ── Step 2: group by phone+email (combined) ──────────────────────────────
    combined_groups = []
    phone_groups    = []
    email_groups    = []

    combined_keys: set = set()

    # Combined: both phone & email non-null and match across 2+ files
    has_both_mask = df_all["phone_norm"].notna() & df_all["email_norm"].notna()
    df_both = df_all[has_both_mask].copy()

    if not df_both.empty:
        for (p, e), grp in df_both.groupby(["phone_norm", "email_norm"]):
            if len(grp["dataset_id"].unique()) < 2:
                continue
            combined_keys.add((p, e))
            combined_groups.append(_make_group(grp, p, e, "combined"))

    # Phone-only: phone matches across files, not already combined
    has_phone_mask = df_all["phone_norm"].notna()
    df_phone = df_all[has_phone_mask].copy()

    if not df_phone.empty:
        for p, grp in df_phone.groupby("phone_norm"):
            if len(grp["dataset_id"].unique()) < 2:
                continue
            if combined_keys:
                is_combined = grp.apply(
                    lambda r: (r["phone_norm"], r["email_norm"]) in combined_keys
                    if (r["phone_norm"] and r["email_norm"]) else False, axis=1
                )
                grp_filtered = grp[~is_combined]
            else:
                grp_filtered = grp
            if grp_filtered["dataset_id"].nunique() < 2:
                continue
            phone_groups.append(_make_group(grp_filtered, p, None, "phone"))

    # Email-only: email matches across files, not already combined
    has_email_mask = df_all["email_norm"].notna()
    df_email = df_all[has_email_mask].copy()

    if not df_email.empty:
        for e, grp in df_email.groupby("email_norm"):
            if len(grp["dataset_id"].unique()) < 2:
                continue
            if combined_keys:
                is_combined = grp.apply(
                    lambda r: (r["phone_norm"], r["email_norm"]) in combined_keys
                    if (r["phone_norm"] and r["email_norm"]) else False, axis=1
                )
                grp_filtered = grp[~is_combined]
            else:
                grp_filtered = grp
            if grp_filtered["dataset_id"].nunique() < 2:
                continue
            email_groups.append(_make_group(grp_filtered, None, e, "email"))

    file_colors = {info["dataset_id"]: info["color"] for info in file_dfs}

    return {
        "combined": combined_groups,
        "phone":    phone_groups,
        "email":    email_groups,
        "file_colors": file_colors,
    }


def _make_group(grp: pd.DataFrame, phone, email, mode: str) -> dict:
    """Build a group summary dict from a subset of the all_rows DataFrame."""
    file_ids = list(grp["dataset_id"].unique())
    # use unique() — already deduplicated, no need for set()
    user_ids = list(grp["user_id"].unique())

    file_data = []
    for did, fgrp in grp.groupby("dataset_id"):
        first = fgrp.iloc[0]
        records = [row["row_dict"] for _, row in fgrp.iterrows()]
        file_data.append({
            "dataset_id": int(did),
            "file_name":  first["file_name"],
            "user_name":  first["user_name"],
            "color":      first["color"],
            "phone_col":  first["phone_col"],
            "email_col":  first["email_col"],
            "columns":    first["columns"],
            "records":    records,
        })

    return {
        "phone":         phone,
        "email":         email,
        "mode":          mode,
        "total_records": len(grp),
        "file_ids":      file_ids,
        "user_ids":      user_ids,
        "file_data":     file_data,
    }


def _paginate(items: list, page: int, per_page: int = 10):
    total       = len(items)
    total_pages = max(1, (total + per_page - 1) // per_page)
    page        = max(1, min(page, total_pages))
    start       = (page - 1) * per_page
    return items[start:start + per_page], page, total_pages


def _page_range(page: int, total_pages: int) -> list:
    """Build pagination range with ellipsis."""
    pages = []
    for p in range(1, total_pages + 1):
        if p == 1 or p == total_pages or abs(p - page) <= 2:
            pages.append(p)
        elif pages and pages[-1] != '...':
            pages.append('...')
    return pages


# ════════════════════════════════════════════════════════════
#  MAIN PAGE
# ════════════════════════════════════════════════════════════

@router.get("/cross-relations", response_class=HTMLResponse)
def cross_relations_page(
    request:    Request,
    db:         Session = Depends(get_db),
    page:       int     = Query(1),
    mode:       str     = Query("combined"),
    file_ids:   list[int] = Query(default=[]),
    cross_user: bool    = Query(False),
):
    current_user = get_current_user(request)
    if not current_user:
        from fastapi.responses import RedirectResponse
        return RedirectResponse("/login", status_code=302)

    user_role  = current_user.get("role") if isinstance(current_user, dict) else current_user.role
    user_id    = current_user.get("id")   if isinstance(current_user, dict) else current_user.id
    admin_mode = user_role == "admin"

    # ── Fetch datasets ────────────────────────────────────────────────────────
    if admin_mode:
        all_datasets = db.query(Dataset).all()
        all_users_db = db.query(User).filter(User.role != "admin").all()
    else:
        effective_user = get_effective_user(request, db)
        eff_id         = effective_user.get("id") if isinstance(effective_user, dict) else effective_user.id
        all_datasets   = db.query(Dataset).filter_by(user_id=eff_id).all()
        all_users_db   = []

    color_map = {ds.id: _color_for_index(i) for i, ds in enumerate(all_datasets)}

    active_datasets = (
        [ds for ds in all_datasets if ds.id in file_ids]
        if file_ids else all_datasets
    )

    # ── Build file_dfs ───────────────────────────────────────────────────────
    file_dfs = []
    for ds in active_datasets:
        df = _load_file_df(ds)
        if df is None:
            continue
        owner = db.query(User).filter_by(id=ds.user_id).first()
        file_dfs.append({
            "dataset_id": ds.id,
            "file_name":  ds.file_name,
            "user_id":    ds.user_id,
            "user_name":  owner.full_name or owner.username if owner else "Unknown",
            "color":      color_map.get(ds.id, "#334155"),
            "df":         df,
        })

    # ── Detect cross-file duplicates ─────────────────────────────────────────
    # Cache key includes only the file set (cross_user is a post-filter, not
    # part of the group-building logic — don't double the cache space for it)
    cache_key = frozenset(ds["dataset_id"] for ds in file_dfs)
    if cache_key not in CROSS_CACHE:
        result = _build_cross_groups(file_dfs)
        CROSS_CACHE[cache_key] = result
    else:
        result = CROSS_CACHE[cache_key]

    combined_groups = result["combined"]
    phone_groups    = result["phone"]
    email_groups    = result["email"]
    file_colors     = result["file_colors"]

    # Cross-user filter (admin only, only when toggle is explicitly ON).
    # Uses set() to correctly deduplicate — prevents a single-user group
    # with multiple matching rows from being counted as multi-user.
    if cross_user and admin_mode:
        combined_groups = [g for g in combined_groups if len(set(g["user_ids"])) > 1]
        phone_groups    = [g for g in phone_groups    if len(set(g["user_ids"])) > 1]
        email_groups    = [g for g in email_groups    if len(set(g["user_ids"])) > 1]

    # ── Choose groups for current mode + paginate ────────────────────────────
    # Auto-fallback: if requested mode is empty, switch to first mode with data
    mode_map = {"combined": combined_groups, "phone": phone_groups, "email": email_groups}
    if not mode_map.get(mode):
        for fallback in ("combined", "phone", "email"):
            if mode_map.get(fallback):
                mode = fallback
                break
    mode_groups = mode_map.get(mode, combined_groups)
    paged_groups, page, total_pages = _paginate(mode_groups, page)

    # ── Compute metric percentages ───────────────────────────────────────────
    total_all = len(combined_groups) + len(phone_groups) + len(email_groups) or 1
    def pct(n): return round(n / total_all * 100)

    # ── Build user list for admin dropdown ───────────────────────────────────
    all_users_view = []
    if admin_mode:
        for u in all_users_db:
            u_files = [ds for ds in all_datasets if ds.user_id == u.id]
            all_users_view.append({
                "id":         u.id,
                "username":   u.username,
                "full_name":  u.full_name,
                "file_count": len(u_files),
                "files": [{
                    "id":        ds.id,
                    "file_name": ds.file_name,
                    "color":     color_map.get(ds.id, "#334155"),
                } for ds in u_files],
            })

    all_files_view = [{
        "id":        ds.id,
        "file_name": ds.file_name,
        "color":     color_map.get(ds.id, "#334155"),
    } for ds in all_datasets]

    selected_file_ids = list(file_ids) if file_ids else []

    return templates.TemplateResponse("cross_relation_view.html", {
        "request":           request,
        "user":              current_user,
        "admin_mode":        admin_mode,
        "admin_users":       None,
        "viewing_user":      None,
        "current_mode":      mode,
        "groups":            paged_groups,
        "page":              page,
        "total_pages":       total_pages,
        "page_range":        _page_range(page, total_pages),
        "combined_count":    len(combined_groups),
        "phone_count":       len(phone_groups),
        "email_count":       len(email_groups),
        "combined_pct":      pct(len(combined_groups)),
        "phone_pct":         pct(len(phone_groups)),
        "email_pct":         pct(len(email_groups)),
        "total_files":       len(all_datasets),
        "total_users":       len(all_users_db) if admin_mode else 0,
        "all_users":         all_users_view,
        "all_files":         all_files_view,
        "file_colors":       file_colors,
        "show_header":       True,
        "categories":        [],
        "category_counts":   {},
        "total_datasets":    len(all_datasets),
        "selected_file_ids": selected_file_ids,
        "cross_user":        cross_user,
    })


# ════════════════════════════════════════════════════════════
#  AJAX — RECORD DRILL-DOWN
# ════════════════════════════════════════════════════════════

@router.get("/cross-relations/records")
def cross_relation_records(
    request:  Request,
    db:       Session = Depends(get_db),
    phone:    Optional[str] = Query(None),
    email:    Optional[str] = Query(None),
    file_ids: Optional[str] = Query(None),
    mode:     str           = Query("combined"),
):
    current_user = get_current_user(request)
    if not current_user:
        return JSONResponse({"file_groups": []})
    user_role  = current_user.get("role") if isinstance(current_user, dict) else current_user.role
    admin_mode = user_role == "admin"

    # Parse comma-separated file_ids
    file_id_list = []
    if file_ids:
        try:
            file_id_list = [int(fid.strip()) for fid in file_ids.split(",") if fid.strip()]
        except ValueError:
            return JSONResponse({"file_groups": []})

    if not file_id_list:
        return JSONResponse({"file_groups": []})

    # Fetch only the requested datasets
    if admin_mode:
        datasets = db.query(Dataset).filter(Dataset.id.in_(file_id_list)).all()
    else:
        effective_user = get_effective_user(request, db)
        eff_id = effective_user.get("id") if isinstance(effective_user, dict) else effective_user.id
        datasets = db.query(Dataset).filter(
            Dataset.id.in_(file_id_list),
            Dataset.user_id == eff_id,
        ).all()

    color_map = {ds.id: _color_for_index(i) for i, ds in enumerate(datasets)}

    file_groups = []
    for ds in datasets:
        try:
            df = _load_file_df(ds)
            if df is None:
                continue

            phone_col, email_col = _detect_cols(df)

            # Normalise
            if phone_col:
                df["__phone_norm__"] = df[phone_col].apply(normalize_phone)
            if email_col:
                df["__email_norm__"] = df[email_col].apply(normalize_email)

            # Filter rows matching the group
            mask = pd.Series([True] * len(df), index=df.index)
            if phone and phone_col:
                mask = mask & (df["__phone_norm__"] == phone)
            if email and email_col:
                mask = mask & (df["__email_norm__"] == email)

            matched = df[mask]
            if matched.empty:
                continue

            # Drop internal columns before returning
            display_cols = [c for c in matched.columns if not c.startswith("__")]
            # Convert to native Python types — numpy int64/float64/Timestamp are
            # not JSON-serializable and will crash JSONResponse outside the try/except.
            records = [
                {k: (None if (isinstance(v, float) and v != v)  # NaN → None
                      else v.item() if hasattr(v, 'item')        # numpy scalar → Python
                      else str(v) if hasattr(v, 'isoformat')     # Timestamp → str
                      else v)
                 for k, v in row.items()}
                for row in matched[display_cols].fillna("").to_dict(orient="records")
            ]

            owner = db.query(User).filter_by(id=ds.user_id).first()

            file_groups.append({
                "dataset_id": ds.id,
                "file_name":  ds.file_name,
                "user_name":  (owner.full_name or owner.username) if (owner and admin_mode) else None,
                "color":      color_map.get(ds.id, "#334155"),
                "phone_col":  phone_col,
                "email_col":  email_col,
                "columns":    display_cols,
                "records":    records,
            })

        except Exception as exc:
            _log.exception("cross_relation_records: failed processing dataset %s: %s", ds.id, exc)
            continue

    return JSONResponse({"file_groups": file_groups})