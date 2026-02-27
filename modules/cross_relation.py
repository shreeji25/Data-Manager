from fastapi import APIRouter, Depends, Request, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional
import threading
import sqlite3
import hashlib
import pandas as pd
import logging
import os

from database import get_db
from models import Dataset, User
from auth import get_current_user
from utils.permissions import get_effective_user
from modules.shared import read_file, normalize_email
import re as _re


router = APIRouter()
templates = Jinja2Templates(directory="templates")

# ── How many rows to show per file before "Show All" button ─────
CARD_PREVIEW_LIMIT = 10


_log = logging.getLogger(__name__)

# ── SQLite index database path ───────────────────────────────────────────────
# Stored alongside the project. One small DB, never grows unbounded because
# rows are replaced when a file is re-indexed.
_INDEX_DB = os.path.join(r"D:\Vnnovate Data\project", "cross_rel_index.db")

# ── Phone keywords ───────────────────────────────────────────────────────────
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
    'order_id', 'orderid', 'invoice', 'inv_no',
    'account_no', 'accountno', 'acc_no',
    'loan_id', 'loanid', 'ref_no', 'refno', 'reference',
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

# Pre-compiled regexes
_PHONE_KW_PATTERNS = [
    _re.compile(r'(^|_)' + _re.escape(kw) + r'(_|$|\d)')
    for kw in _PHONE_KEYWORDS
]
_RX_MOBILE_BARE  = _re.compile(r'(?<!\d)([6-9]\d{9})(?!\d)')
_RX_MOBILE_LEAD0 = _re.compile(r'(?<!\d)(0([6-9]\d{9}))(?!\d)')
_RX_NON_DIGIT    = _re.compile(r'\D')


def _is_phone_col(col: str) -> bool:
    c = col.lower().strip()
    if any(ex in c for ex in _PHONE_EXCLUDES):
        return False
    return any(pat.search(c) for pat in _PHONE_KW_PATTERNS)


def _is_email_col(col: str) -> bool:
    c = col.lower().strip()
    if any(ex in c for ex in _EMAIL_EXCLUDES):
        return False
    return any(kw in c for kw in _EMAIL_KEYWORDS)


def normalize_phone(val) -> "str | None":
    if val is None:
        return None
    s = str(val).strip()
    if not s or s.lower() in ('nan', 'none', '', 'null'):
        return None
    if s.endswith('.0'):
        s = s[:-2]
    digits_all = _RX_NON_DIGIT.sub('', s)
    if not digits_all:
        return None
    m = _RX_MOBILE_BARE.search(s)
    if m:
        return m.group(1)
    m = _RX_MOBILE_LEAD0.search(s)
    if m:
        return m.group(2)
    if len(digits_all) >= 10:
        if digits_all.startswith('91') and len(digits_all) == 12 and digits_all[2] in '6789':
            return digits_all[2:]
        if digits_all.startswith('0') and len(digits_all) == 11 and digits_all[1] in '6789':
            return digits_all[1:]
        if len(digits_all) == 10:
            return digits_all
        if len(digits_all) <= 12:
            return digits_all[-10:]
        return None
    if 7 <= len(digits_all) < 10:
        return digits_all
    return None


def _detect_cols(df: "pd.DataFrame") -> "tuple[str | None, str | None]":
    phone_cols = [c for c in df.columns if _is_phone_col(c)]
    email_cols = [c for c in df.columns if _is_email_col(c)]
    phone_col = None
    if phone_cols:
        if len(phone_cols) == 1:
            phone_col = phone_cols[0]
        else:
            def _col_as_series(name):
                loc = df.columns.get_loc(name)
                s = df.iloc[:, loc] if isinstance(loc, int) else df.iloc[:, loc].iloc[:, 0]
                return s.astype(str)
            merged = _col_as_series(phone_cols[0]).copy()
            for pc in phone_cols[1:]:
                needs_fill = merged.isin(['', 'nan', 'None', 'NaN', 'none', 'null'])
                merged = merged.where(~needs_fill, _col_as_series(pc))
            df['__merged_phone__'] = merged
            phone_col = '__merged_phone__'
    email_col = email_cols[0] if email_cols else None
    return phone_col, email_col


router    = APIRouter()
templates = Jinja2Templates(directory="templates")

FILE_COLORS = [
    "#4f46e5", "#16a34a", "#ea580c", "#dc2626", "#7c3aed",
    "#0891b2", "#db2777", "#ca8a04", "#059669", "#9333ea",
]

# Background rebuild tracking: dataset_id -> True while indexing
_REBUILDING: dict  = {}
_rebuild_lock       = threading.Lock()

# In-memory group cache (keyed by frozenset of dataset_ids + cross_user flag)
# Groups are cheap to rebuild from the index (pure SQL), so this is just a
# request-level micro-cache to avoid duplicate SQL in the same request.
CROSS_CACHE: dict  = {}
_cache_lock         = threading.Lock()


# ════════════════════════════════════════════════════════════
#  SQLITE INDEX
# ════════════════════════════════════════════════════════════

def _get_index_conn() -> sqlite3.Connection:
    """Open the index DB with WAL mode for safe concurrent reads/writes."""
    conn = sqlite3.connect(_INDEX_DB, timeout=30, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def _ensure_index_schema():
    """Create index table + indexes if they don't exist yet."""
    with _get_index_conn() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS cross_rel_index (
                dataset_id  INTEGER NOT NULL,
                user_id     INTEGER NOT NULL,
                phone_norm  TEXT,
                email_norm  TEXT,
                mtime       REAL NOT NULL
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_cri_phone ON cross_rel_index(phone_norm)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_cri_email ON cross_rel_index(email_norm)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_cri_ds    ON cross_rel_index(dataset_id)")
        # Metadata table: tracks per-file index state
        conn.execute("""
            CREATE TABLE IF NOT EXISTS cross_rel_meta (
                dataset_id  INTEGER PRIMARY KEY,
                mtime       REAL NOT NULL,
                indexed_at  TEXT NOT NULL
            )
        """)
        conn.commit()


def _get_indexed_mtime(dataset_id: int) -> float:
    """Return the mtime stored when this file was last indexed, or 0."""
    try:
        with _get_index_conn() as conn:
            row = conn.execute(
                "SELECT mtime FROM cross_rel_meta WHERE dataset_id = ?",
                (dataset_id,)
            ).fetchone()
            return row[0] if row else 0.0
    except Exception:
        return 0.0


def _index_dataset(dataset_id: int, user_id: int, file_path: str, mtime: float):
    """
    Read one file and write its normalised phone/email rows into the index.
    Replaces any existing rows for this dataset_id.
    Called from a background thread — never blocks the web request.
    """
    raw_path = _resolve_path(file_path)
    if not os.path.exists(raw_path):
        return
    try:
        df = read_file(raw_path)
        df.columns = df.columns.str.lower().str.strip()
        markers = [c for c in df.columns if c.startswith('__') and c.endswith('__')]
        if markers:
            df = df.drop(columns=markers)
    except Exception as exc:
        _log.error("_index_dataset: failed to read %s: %s", raw_path, exc)
        return

    phone_col, email_col = _detect_cols(df)

    # Vectorised normalisation
    phone_s = (df[phone_col].apply(normalize_phone) if phone_col
               else pd.Series([None] * len(df), dtype=object))
    email_s = (df[email_col].apply(normalize_email) if email_col
               else pd.Series([None] * len(df), dtype=object))

    # Only keep rows with at least one value
    valid = phone_s.notna() | email_s.notna()
    phone_vals = phone_s[valid].where(phone_s[valid].notna(), None).tolist()
    email_vals = email_s[valid].where(email_s[valid].notna(), None).tolist()
    rows = [
        (dataset_id, user_id, p, e, mtime)
        for p, e in zip(phone_vals, email_vals)
    ]

    try:
        from datetime import datetime
        with _get_index_conn() as conn:
            # Delete old rows for this dataset, then insert fresh ones
            conn.execute("DELETE FROM cross_rel_index WHERE dataset_id = ?", (dataset_id,))
            conn.execute("DELETE FROM cross_rel_meta  WHERE dataset_id = ?", (dataset_id,))
            if rows:
                conn.executemany(
                    "INSERT INTO cross_rel_index(dataset_id, user_id, phone_norm, email_norm, mtime) "
                    "VALUES (?,?,?,?,?)",
                    rows
                )
            conn.execute(
                "INSERT INTO cross_rel_meta(dataset_id, mtime, indexed_at) VALUES (?,?,?)",
                (dataset_id, mtime, datetime.utcnow().isoformat())
            )
            conn.commit()
        _log.info("_index_dataset: indexed dataset %d (%d rows)", dataset_id, len(rows))
    except Exception as exc:
        _log.error("_index_dataset: DB write failed for dataset %d: %s", dataset_id, exc)
    finally:
        with _rebuild_lock:
            _REBUILDING.pop(dataset_id, None)


def _ensure_datasets_indexed(datasets, db: Session):
    """
    Check each dataset's mtime. If stale or missing, kick off a background
    thread to re-index it. Returns True if ALL datasets are already indexed
    and current (page can load immediately), False if any are still building.
    """
    all_ready = True
    for ds in datasets:
        raw_path     = _resolve_path(ds.file_path)
        current_mtime = _file_mtime(raw_path)
        indexed_mtime = _get_indexed_mtime(ds.id)

        if current_mtime == indexed_mtime:
            continue  # already up to date

        all_ready = False
        with _rebuild_lock:
            if _REBUILDING.get(ds.id):
                continue  # already rebuilding
            _REBUILDING[ds.id] = True

        t = threading.Thread(
            target=_index_dataset,
            args=(ds.id, ds.user_id, ds.file_path, current_mtime),
            daemon=True,
        )
        t.start()

    return all_ready


# ════════════════════════════════════════════════════════════
#  FAST SQL GROUP QUERIES
# ════════════════════════════════════════════════════════════

def _query_groups(dataset_ids: list[int], user_ids_filter: "set | None" = None) -> dict:
    """
    Run the three GROUP BY queries against the index and return
    combined/phone/email group lists in the same format as _build_cross_groups().

    This replaces the entire pandas scan — runs in milliseconds.
    """
    if not dataset_ids:
        return {"combined": [], "phone": [], "email": [], "file_colors": {}}

    placeholders = ",".join("?" * len(dataset_ids))

    combined_groups = []
    phone_groups    = []
    email_groups    = []
    combined_keys   = set()

    with _get_index_conn() as conn:

        # ── Combined: phone+email both match across 2+ datasets ──────────────
        rows = conn.execute(f"""
            SELECT phone_norm, email_norm,
                   GROUP_CONCAT(DISTINCT dataset_id) AS ds_ids,
                   GROUP_CONCAT(DISTINCT user_id)    AS u_ids,
                   COUNT(*)                           AS total_records
            FROM cross_rel_index
            WHERE dataset_id IN ({placeholders})
              AND phone_norm IS NOT NULL
              AND email_norm IS NOT NULL
            GROUP BY phone_norm, email_norm
            HAVING COUNT(DISTINCT dataset_id) >= 2
            ORDER BY total_records DESC
        """, dataset_ids).fetchall()

        for phone, email, ds_ids_str, u_ids_str, total in rows:
            ds_ids = [int(x) for x in ds_ids_str.split(",")]
            u_ids  = [int(x) for x in u_ids_str.split(",")]
            if user_ids_filter and not any(u in user_ids_filter for u in u_ids):
                continue
            combined_keys.add((phone, email))
            combined_groups.append({
                "phone":         phone,
                "email":         email,
                "mode":          "combined",
                "total_records": total,
                "file_ids":      ds_ids,
                "user_ids":      u_ids,
                "file_data":     [],   # loaded lazily via AJAX drill-down
            })

        # ── Phone-only: phone matches across 2+ datasets, not in combined ────
        rows = conn.execute(f"""
            SELECT phone_norm,
                   GROUP_CONCAT(DISTINCT dataset_id) AS ds_ids,
                   GROUP_CONCAT(DISTINCT user_id)    AS u_ids,
                   COUNT(*)                           AS total_records
            FROM cross_rel_index
            WHERE dataset_id IN ({placeholders})
              AND phone_norm IS NOT NULL
            GROUP BY phone_norm
            HAVING COUNT(DISTINCT dataset_id) >= 2
            ORDER BY total_records DESC
        """, dataset_ids).fetchall()

        for phone, ds_ids_str, u_ids_str, total in rows:
            # Skip if this phone is already fully represented in combined
            if phone in {k[0] for k in combined_keys}:
                continue
            ds_ids = [int(x) for x in ds_ids_str.split(",")]
            u_ids  = [int(x) for x in u_ids_str.split(",")]
            if user_ids_filter and not any(u in user_ids_filter for u in u_ids):
                continue
            phone_groups.append({
                "phone":         phone,
                "email":         None,
                "mode":          "phone",
                "total_records": total,
                "file_ids":      ds_ids,
                "user_ids":      u_ids,
                "file_data":     [],
            })

        # ── Email-only ────────────────────────────────────────────────────────
        rows = conn.execute(f"""
            SELECT email_norm,
                   GROUP_CONCAT(DISTINCT dataset_id) AS ds_ids,
                   GROUP_CONCAT(DISTINCT user_id)    AS u_ids,
                   COUNT(*)                           AS total_records
            FROM cross_rel_index
            WHERE dataset_id IN ({placeholders})
              AND email_norm IS NOT NULL
            GROUP BY email_norm
            HAVING COUNT(DISTINCT dataset_id) >= 2
            ORDER BY total_records DESC
        """, dataset_ids).fetchall()

        for email, ds_ids_str, u_ids_str, total in rows:
            if email in {k[1] for k in combined_keys}:
                continue
            ds_ids = [int(x) for x in ds_ids_str.split(",")]
            u_ids  = [int(x) for x in u_ids_str.split(",")]
            if user_ids_filter and not any(u in user_ids_filter for u in u_ids):
                continue
            email_groups.append({
                "phone":         None,
                "email":         email,
                "mode":          "email",
                "total_records": total,
                "file_ids":      ds_ids,
                "user_ids":      u_ids,
                "file_data":     [],
            })

    return {
        "combined": combined_groups,
        "phone":    phone_groups,
        "email":    email_groups,
        "file_colors": {},   # filled in by caller
    }


# ════════════════════════════════════════════════════════════
#  HELPERS
# ════════════════════════════════════════════════════════════

def _resolve_path(file_path: str) -> str:
    base = r"D:\Vnnovate Data\project"
    if os.path.isabs(file_path):
        return file_path
    return os.path.join(base, file_path)


def _color_for_index(i: int) -> str:
    return FILE_COLORS[i % len(FILE_COLORS)]


def _file_mtime(path: str) -> float:
    try:
        return os.path.getmtime(path)
    except OSError:
        return 0.0


def _load_file_df(dataset: Dataset) -> "pd.DataFrame | None":
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


def _paginate(items: list, page: int, per_page: int = 10):
    total       = len(items)
    total_pages = max(1, (total + per_page - 1) // per_page)
    page        = max(1, min(page, total_pages))
    start       = (page - 1) * per_page
    return items[start:start + per_page], page, total_pages


def _page_range(page: int, total_pages: int) -> list:
    pages = []
    for p in range(1, total_pages + 1):
        if p == 1 or p == total_pages or abs(p - page) <= 2:
            pages.append(p)
        elif pages and pages[-1] != '...':
            pages.append('...')
    return pages


# ── Init schema on module load ───────────────────────────────────────────────
try:
    _ensure_index_schema()
except Exception as _e:
    _log.error("cross_relation: failed to init index schema: %s", _e)


# ════════════════════════════════════════════════════════════
#  STATUS POLL ENDPOINT
# ════════════════════════════════════════════════════════════

@router.get("/cross-relations/status")
def cross_relations_status(
    request:  Request,
    db:       Session = Depends(get_db),
):
    """
    AJAX endpoint polled every 2 seconds while spinner is showing.
    Returns {"ready": true, "pending": 0} when all files are indexed.
    """
    current_user = get_current_user(request)
    if not current_user:
        return JSONResponse({"ready": False, "pending": 0})

    user_role  = current_user.get("role") if isinstance(current_user, dict) else current_user.role
    user_id    = current_user.get("id")   if isinstance(current_user, dict) else current_user.id
    admin_mode = user_role == "admin"

    if admin_mode:
        datasets = db.query(Dataset).all()
    else:
        effective_user = get_effective_user(request, db)
        eff_id = effective_user.get("id") if isinstance(effective_user, dict) else effective_user.id
        datasets = db.query(Dataset).filter_by(user_id=eff_id).all()

    with _rebuild_lock:
        pending = sum(1 for ds in datasets if _REBUILDING.get(ds.id, False))

    # Also count stale (not yet started)
    stale = 0
    for ds in datasets:
        raw_path = _resolve_path(ds.file_path)
        if _file_mtime(raw_path) != _get_indexed_mtime(ds.id):
            with _rebuild_lock:
                if not _REBUILDING.get(ds.id, False):
                    stale += 1

    total_pending = pending + stale
    return JSONResponse({"ready": total_pending == 0, "pending": total_pending})


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

    # ── Ensure index is up to date (kicks off background threads if needed) ──
    all_ready = _ensure_datasets_indexed(active_datasets, db)

    # ── Build UI data (no file I/O — instant) ────────────────────────────────
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

    if not all_ready:
        # Some files still indexing — show spinner, browser will poll /status
        return templates.TemplateResponse("cross_relation_view.html", {
            "request":           request,
            "user":              current_user,
            "admin_mode":        admin_mode,
            "admin_users":       None,
            "viewing_user":      None,
            "current_mode":      mode,
            "groups":            [],
            "page":              1,
            "total_pages":       1,
            "page_range":        [1],
            "combined_count":    0,
            "phone_count":       0,
            "email_count":       0,
            "combined_pct":      0,
            "phone_pct":         0,
            "email_pct":         0,
            "total_files":       len(all_datasets),
            "total_users":       len(all_users_db) if admin_mode else 0,
            "all_users":         all_users_view,
            "all_files":         all_files_view,
            "file_colors":       {},
            "show_header":       True,
            "categories":        [],
            "category_counts":   {},
            "total_datasets":    len(all_datasets),
            "selected_file_ids": selected_file_ids,
            "cross_user":        cross_user,
            "is_computing":      True,
        })

    # ── Query groups from index (FAST — pure SQL, milliseconds) ──────────────
    ds_ids = [ds.id for ds in active_datasets]

    # cross_user filter: only show groups spanning multiple users
    user_ids_filter = None
    if cross_user and admin_mode:
        # We want groups where user_ids has 2+ distinct values — handled in SQL HAVING
        # Pass a sentinel so _query_groups knows to apply cross-user HAVING
        user_ids_filter = "cross_user"

    result = _query_groups(ds_ids)

    combined_groups = result["combined"]
    phone_groups    = result["phone"]
    email_groups    = result["email"]

    # Apply cross-user filter post-query
    if cross_user and admin_mode:
        combined_groups = [g for g in combined_groups if len(set(g["user_ids"])) > 1]
        phone_groups    = [g for g in phone_groups    if len(set(g["user_ids"])) > 1]
        email_groups    = [g for g in email_groups    if len(set(g["user_ids"])) > 1]

    file_colors = color_map  # dataset_id -> color

    # ── Mode fallback + paginate ──────────────────────────────────────────────
    mode_map = {"combined": combined_groups, "phone": phone_groups, "email": email_groups}
    if not mode_map.get(mode):
        for fallback in ("combined", "phone", "email"):
            if mode_map.get(fallback):
                mode = fallback
                break
    mode_groups = mode_map.get(mode, combined_groups)
    paged_groups, page, total_pages = _paginate(mode_groups, page)

    total_all = len(combined_groups) + len(phone_groups) + len(email_groups) or 1
    def pct(n): return round(n / total_all * 100)

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
        "is_computing":      False,
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

    file_id_list = []
    if file_ids:
        try:
            file_id_list = [int(fid.strip()) for fid in file_ids.split(",") if fid.strip()]
        except ValueError:
            return JSONResponse({"file_groups": []})

    if not file_id_list:
        return JSONResponse({"file_groups": []})

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

            if phone_col:
                df["__phone_norm__"] = df[phone_col].apply(normalize_phone)
            if email_col:
                df["__email_norm__"] = df[email_col].apply(normalize_email)

            mask = pd.Series([True] * len(df), index=df.index)
            if phone and phone_col:
                mask = mask & (df["__phone_norm__"] == phone)
            if email and email_col:
                mask = mask & (df["__email_norm__"] == email)

            matched = df[mask]
            if matched.empty:
                continue

            display_cols = [c for c in matched.columns if not c.startswith("__")]
            records = [
                {k: (None if (isinstance(v, float) and v != v)
                      else v.item() if hasattr(v, 'item')
                      else str(v) if hasattr(v, 'isoformat')
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
            _log.exception("cross_relation_records: failed dataset %s: %s", ds.id, exc)
            continue

    return JSONResponse({"file_groups": file_groups})

# ════════════════════════════════════════════════════════════════
#  AJAX — CARD DETAIL (lazy-load with 10-row preview per file)
# ════════════════════════════════════════════════════════════════

@router.get("/cross-relations/card-detail", response_class=HTMLResponse)
def crf_card_detail(
    request:  Request,
    db:       Session       = Depends(get_db),
    phone:    Optional[str] = Query(None),
    email:    Optional[str] = Query(None),
    file_ids: Optional[str] = Query(None),
    mode:     str           = Query("combined"),
):
    """
    Returns an HTML partial for a group card's expanded body.
    Uses the same logic as /cross-relations/records but renders
    a Jinja2 template with PREVIEW_LIMIT rows visible per file
    and the rest hidden — revealed by the JS "Show All" button.
    """
    current_user = get_current_user(request)
    if not current_user:
        return HTMLResponse("", status_code=401)

    user_role  = current_user.get("role") if isinstance(current_user, dict) else current_user.role
    admin_mode = user_role == "admin"

    # ── Parse file_ids ───────────────────────────────────────────
    file_id_list = []
    if file_ids:
        try:
            file_id_list = [int(fid.strip()) for fid in file_ids.split(",") if fid.strip()]
        except ValueError:
            return HTMLResponse("", status_code=400)

    if not file_id_list:
        return HTMLResponse("", status_code=400)

    # ── Fetch datasets (with ownership check for non-admins) ─────
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

    # ── Build per-file record groups (same logic as /records) ────
    file_groups = []
    for ds in datasets:
        try:
            df = _load_file_df(ds)
            if df is None:
                continue

            phone_col, email_col = _detect_cols(df)

            if phone_col:
                df["__phone_norm__"] = df[phone_col].apply(normalize_phone)
            if email_col:
                df["__email_norm__"] = df[email_col].apply(normalize_email)

            mask = pd.Series([True] * len(df), index=df.index)
            if phone and phone_col:
                mask = mask & (df["__phone_norm__"] == phone)
            if email and email_col:
                mask = mask & (df["__email_norm__"] == email)

            matched = df[mask]
            if matched.empty:
                continue

            display_cols = [c for c in matched.columns if not c.startswith("__")]
            records = [
                [
                    (None if (isinstance(v, float) and v != v)
                     else v.item() if hasattr(v, "item")
                     else str(v) if hasattr(v, "isoformat")
                     else v)
                    for v in row
                ]
                for row in matched[display_cols].fillna("").itertuples(index=False, name=None)
            ]

            owner = db.query(User).filter_by(id=ds.user_id).first()

            file_groups.append({
                "dataset_id": ds.id,
                "file_name":  ds.file_name,
                "user_name":  (owner.full_name or owner.username) if (owner and admin_mode) else None,
                "color":      color_map.get(ds.id, "#334155"),
                "columns":    display_cols,
                "records":    records,       # full list — template slices to PREVIEW_LIMIT
            })

        except Exception as exc:
            _log.exception("crf_card_detail: failed dataset %s: %s", ds.id, exc)
            continue

    return templates.TemplateResponse(
        "partials/crf_card_detail_partial.html",
        {
            "request":       request,
            "file_groups":   file_groups,
            "PREVIEW_LIMIT": CARD_PREVIEW_LIMIT,
            "admin_mode":    admin_mode,
        },
    )