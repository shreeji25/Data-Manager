# modules/shared.py
import pandas as pd
import re
from pathlib import Path
import os

# ==================================================
# DIRECTORIES
# ==================================================

BASE_DIR = Path(__file__).resolve().parent.parent

UPLOAD_DIR = BASE_DIR / "uploads"
TEMP_DIR   = BASE_DIR / "temp_uploads"
CLEAN_DIR  = BASE_DIR / "cleaned"

UPLOAD_DIR.mkdir(exist_ok=True)
TEMP_DIR.mkdir(exist_ok=True)
CLEAN_DIR.mkdir(exist_ok=True)


# ==================================================
# SINGLE UNIFIED CACHE
# ==================================================
# FIX: The original file had THREE separate cache dicts
# (DATASET_CACHE, _DATAFRAME_CACHE, _dataframe_cache) that did the same
# job and caused silent cache misses.  One dict only.

_DATAFRAME_CACHE: dict = {}
DUPLICATE_CACHE:  dict = {}   # kept for relation.py compatibility


def get_cached_df(key: str):
    return _DATAFRAME_CACHE.get(key)


def set_cached_df(key: str, df):
    _DATAFRAME_CACHE[key] = df


# Legacy aliases so any existing callers still work
def cache_dataframe(key, df):
    _DATAFRAME_CACHE[key] = df


def get_cached_dataframe(key):
    return _DATAFRAME_CACHE.get(key)


def cache_dataframe_v2(key, df):
    _DATAFRAME_CACHE[key] = df


def get_cached_dataframe_v2(key):
    if key in _DATAFRAME_CACHE:
        return _DATAFRAME_CACHE[key]

    # Fall back to disk
    for base in (UPLOAD_DIR / key, UPLOAD_DIR / f"cleaned_{key}"):
        if base.exists():
            try:
                df = read_file(str(base))
                _DATAFRAME_CACHE[key] = df
                return df
            except Exception:
                pass

    return None


def get_cached_df_by_path(path: str):
    if path in _DATAFRAME_CACHE:
        return _DATAFRAME_CACHE[path]
    try:
        df = pd.read_csv(path)
    except Exception:
        df = pd.read_excel(path)
    _DATAFRAME_CACHE[path] = df
    return df


# ==================================================
# HEADER DETECTION HELPERS
# ==================================================

# FIX: The original _is_fake_header() flagged legitimate column names like
# "Service Type", "Account No", "Extension" because it matched substrings
# 'SERVICE', 'ACCOUNT', 'EXT', 'IST' against the entire column name.
#
# These patterns were meant to catch DATA VALUES appearing in the header row
# (e.g. a phone number, a full sentence, or an email address).  They should
# NOT match ordinary words that happen to contain those letter sequences.
#
# The fix removes the fragile keyword list entirely and keeps only the five
# structural rules that are universally reliable:
#   1. Empty / Unnamed / nan
#   2. Contains an @ (email address)
#   3. Contains 8+ consecutive digits (phone / account number)
#   4. More than 60 characters long
#   5. Pure number
#
# The threshold is raised from 20 % to 40 % to further reduce false positives.

_FAKE_HEADER_THRESHOLD = 40   # percent of columns that must look bad


def _col_looks_like_data(col: str) -> bool:
    """
    Return True if a single column name looks like it is actually a data
    value rather than a proper column header.
    """
    col = str(col).strip()

    # 1. Empty / Unnamed / nan
    if not col or col.lower().startswith("unnamed") or col.lower() == "nan":
        return True

    # 2. Contains an email address
    if "@" in col:
        return True

    # 3. Contains 8 or more consecutive digits (phone / long account number)
    if re.search(r"\d{8,}", col):
        return True

    # 4. Very long free-text (>60 chars — likely an address or description)
    if len(col) > 60:
        return True

    # 5. Pure number (e.g. "1", "42", "3.14")
    if re.match(r"^\d+\.?\d*$", col):
        return True

    return False


def _is_fake_header(columns) -> bool:
    """
    Return True if the column names look like a data row, not real headers.
    Uses only structural rules — no keyword matching.
    """
    total = len(columns)
    if total == 0:
        return False

    bad = sum(1 for c in columns if _col_looks_like_data(str(c)))
    bad_pct = (bad / total) * 100

    return bad_pct >= _FAKE_HEADER_THRESHOLD


def _analyze_first_rows(df: pd.DataFrame, num_rows: int = 3) -> bool:
    """
    Backup check: look at actual data rows for data-like patterns.

    FIX: The original flagged any row containing an @ symbol as "looks like
    data". This is wrong — email columns are the most common use case and
    their values will always contain @. Checking for @ in data rows is
    useless as a header-detection signal.

    This backup check now only triggers on structural signs that the HEADER
    ROW itself was skipped (e.g. a phone number or very long sentence
    appearing where a column name should be).  Since _is_fake_header() already
    handles those cases, _analyze_first_rows() is now a very conservative
    last-resort check that only fires when ALL of the first rows look like
    they contain nothing but raw data values with no plausible header above.

    In practice: disabled for normal use by returning False always.
    The primary check (_is_fake_header) is sufficient.
    """
    # Disabled: causes false positives on any file with email data.
    # _is_fake_header() is sufficient for header detection.
    return False


# ==================================================
# MAIN FILE READER
# ==================================================

def read_file(file_path: str) -> pd.DataFrame:
    """
    Universal file reader.
    Supports: CSV / XLS / XLSX / TXT
    
    FIX: The original tried pd.read_csv() FIRST on every file regardless of
    extension.  pd.read_csv() does NOT raise an exception on .xlsx files — it
    partially parses the raw XML binary and returns garbage column names like
    '##0_);', '##0\\)□□!', '##0.00_);[Red]\\(\"$\"#', etc.  These names look
    valid to _is_fake_header(), so the DataFrame is used as-is with dozens of
    garbage columns instead of the real 3 columns, causing:
      • The correction page to appear for perfectly valid files
      • The correction page to show meaningless column names
      • All subsequent duplicate detection to fail

    The fix: choose the PRIMARY engine based on file extension first.
    Only fall back to other engines if the primary engine raises an exception.
    """
    file_path = str(file_path)
    ext = os.path.splitext(file_path)[1].lower()

    df = None

    # ── Extension-first engine selection ──────────────────────────────────
    if ext in (".xlsx", ".xls"):
        # Primary: correct Excel engine
        engine = "openpyxl" if ext == ".xlsx" else "xlrd"
        try:
            df = pd.read_excel(file_path, engine=engine)
        except Exception:
            pass

        # Fallback: try the other Excel engine
        if df is None:
            fallback = "xlrd" if engine == "openpyxl" else "openpyxl"
            try:
                df = pd.read_excel(file_path, engine=fallback)
            except Exception:
                pass

        # Last resort: maybe it's actually a CSV with wrong extension
        if df is None:
            try:
                df = pd.read_csv(file_path, encoding="utf-8", low_memory=False)
            except Exception:
                try:
                    df = pd.read_csv(file_path, encoding="latin1", low_memory=False)
                except Exception:
                    pass

    else:
        # Primary: CSV (for .csv, .txt, and unknown extensions)
        try:
            df = pd.read_csv(file_path, encoding="utf-8", low_memory=False)
        except UnicodeDecodeError:
            try:
                df = pd.read_csv(file_path, encoding="latin1", low_memory=False)
            except Exception:
                pass
        except Exception:
            pass

        # Fallback: maybe it's actually an Excel file with wrong extension
        if df is None:
            for engine in ("openpyxl", "xlrd"):
                try:
                    df = pd.read_excel(file_path, engine=engine)
                    break
                except Exception:
                    pass

        # Final fallback
        if df is None:
            try:
                df = pd.read_csv(
                    file_path, encoding="utf-8", encoding_errors="ignore"
                )
            except Exception:
                pass

    if df is None:
        raise Exception(f"Unsupported or corrupt file: {os.path.basename(file_path)}")

    # ── Clean raw column names ─────────────────────────────────────────────
    df.columns = [
        str(col).strip().replace("\n", " ").replace("\t", " ")
        for col in df.columns
    ]

    # ── Detect and fix fake headers ────────────────────────────────────────
    primary_check = _is_fake_header(df.columns)
    backup_check  = False

    if not primary_check:
        backup_check = _analyze_first_rows(df)

    if primary_check or backup_check:
        try:
            if ext == ".csv":
                df = pd.read_csv(
                    file_path, header=None,
                    encoding="utf-8", encoding_errors="ignore",
                )
            else:
                df = pd.read_excel(file_path, header=None)
        except Exception as e:
            raise Exception(f"Re-read without header failed: {e}")

        df.columns = [f"Column_{i + 1}" for i in range(len(df.columns))]

    # ── Fix unnamed columns ────────────────────────────────────────────────
    df.columns = [
        f"Column_{i + 1}"
        if (str(c).strip().lower().startswith("unnamed") or str(c).strip() in ("", "nan"))
        else c
        for i, c in enumerate(df.columns)
    ]

    # ── Drop fully empty rows and reset index ─────────────────────────────
    df = df.dropna(how="all").reset_index(drop=True)

    return df


# ==================================================
# COLUMN ANALYSIS
# ==================================================

def check_required_columns(df: pd.DataFrame):
    """
    Detect name / email / phone columns and flag header problems.
    Returns (has_required: bool, detected: dict, info: dict)
    """
    info = {
        "has_header_problem": False,
        "problem_reason": None,
        "problematic_headers": [],
    }
    detected = {}
    generated_columns = []

    for col in df.columns:
        col_str = str(col).strip().lower()

        # Auto-generated sentinel
        if re.match(r"^column_\d+$", col_str):
            generated_columns.append(col)
            continue

        # Structural header problems
        if not col_str or col_str == "nan":
            info["has_header_problem"] = True
            info["problem_reason"] = "null_header"
            info["problematic_headers"].append(col)
            continue

        if "@" in col_str:
            info["has_header_problem"] = True
            info["problem_reason"] = "email_in_header"
            info["problematic_headers"].append(col)
            continue

        if re.search(r"\d{8,}", col_str):
            info["has_header_problem"] = True
            info["problem_reason"] = "phone_in_header"
            info["problematic_headers"].append(col)
            continue

        # Column mapping detection
        if "name" in col_str and "file" not in col_str:
            detected["name"] = col
        if "mail" in col_str or "email" in col_str:
            detected["email"] = col
        if any(k in col_str for k in ("phone", "mobile", "contact")):
            detected["phone"] = col

    if generated_columns:
        info["has_header_problem"] = True
        info["problem_reason"] = "no_header_detected"
        info["problematic_headers"] = generated_columns

    has_required = bool(detected) and not info["has_header_problem"]
    return has_required, detected, info


def analyze_file_columns(df: pd.DataFrame):
    """Prepare data for the column-correction page."""
    columns = list(df.columns)
    preview = {col: df[col].astype(str).head(10).tolist() for col in columns}
    _, detected, _ = check_required_columns(df)

    return {
        "columns": columns,
        "preview": preview,
        "detected_mapping": detected,
        "header_problems": {},
    }


# ==================================================
# COLUMN MAPPING
# ==================================================

def apply_column_mapping(df: pd.DataFrame, mapping: dict) -> pd.DataFrame:
    """Rename columns using a user-supplied mapping dict."""
    if not mapping:
        return df
    return df.rename(columns=mapping)


# ==================================================
# NORMALIZERS
# ==================================================

def normalize_phone(phone) -> str:
    """
    Normalize a phone number.

    FIX: The original used phone[-10:] (last 10 digits) which works for
    Indian numbers with country code 91 but silently corrupts other formats.
    The fix strips the Indian country code (91) explicitly when present,
    matching the logic in duplicate_detector.py, so both modules behave
    identically.
    """
    if not phone:
        return None

    phone = str(phone).strip()

    # Remove .0 suffix from Excel float BEFORE stripping non-digits
    if phone.endswith(".0"):
        phone = phone[:-2]

    phone = re.sub(r"\D", "", phone)

    # Strip Indian country code
    if phone.startswith("91") and len(phone) > 10:
        phone = phone[2:]

    # Accept 7–12 digit numbers (matches duplicate_detector.py)
    if 7 <= len(phone) <= 12:
        return phone

    return None


def normalize_email(email: str) -> str:
    """Clean and lowercase an email address."""
    if not email:
        return None
    val = str(email).strip().lower()

    EMPTY_VALUES = {'', 'nan', 'none', 'null', 'n/a', 'na', '-', 'nil'}
    if val in EMPTY_VALUES:
      return None          # ✅ catches "nan" strings from pandas
    return val
  
# ==================================================
# DUPLICATE DETECTION  (upload-time marking)
# ==================================================

# ==================================================
# SHARED COLUMN DETECTION CONSTANTS
# Same lists used by shared.py AND duplicate_detector.py
# so both pages always detect the same columns.
# ==================================================

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


def _detect_phone_email_cols(df: pd.DataFrame):
    """
    Shared helper — returns (phone_col, email_col) after:
      1. Strict keyword matching with exclude list
      2. Merging multiple phone columns (contact_no + phone_no) into one
    df must already have lowercase stripped column names.
    """
    phone_cols = [
        c for c in df.columns
        if any(k in c for k in PHONE_KEYWORDS)
        and not any(b in c for b in PHONE_EXCLUDES)
    ]
    email_cols = [
        c for c in df.columns
        if any(k in c for k in EMAIL_KEYWORDS)
        and not any(b in c for b in EMAIL_EXCLUDES)
    ]

    # Merge multiple phone columns — first non-null value per row wins
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

    email_col = email_cols[0] if email_cols else None
    return phone_col, email_col


def detect_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Mark duplicate rows in a DataFrame.

    Rules:
      __dup_combined__ — phone+email BOTH match, both fields non-empty
      __dup_phone__    — phone matches, NOT already combined
      __dup_email__    — email matches, NOT already combined

    Each row belongs to exactly ONE duplicate category.
    NaN/empty values are never matched against each other.
    Uses same column detection as duplicate_detector.py so view page
    and relation page always show the same counts.
    """
    df = df.copy()
    df.columns = df.columns.str.lower().str.strip()

    phone_col, email_col = _detect_phone_email_cols(df)

    if phone_col:
        df[phone_col] = df[phone_col].apply(normalize_phone)
    if email_col:
        df[email_col] = df[email_col].apply(normalize_email)

    df["__dup_combined__"] = False
    df["__dup_phone__"]    = False
    df["__dup_email__"]    = False

    # Combined — both fields must have real non-empty values
    if phone_col and email_col:
        has_both = (
            df[phone_col].notna() & (df[phone_col] != "") &
            df[email_col].notna() & (df[email_col] != "")
        )
        df["__dup_combined__"] = (
            df.duplicated(subset=[phone_col, email_col], keep=False) & has_both
        )

    # Phone only — must have real phone value AND not already combined
    if phone_col:
        has_phone = df[phone_col].notna() & (df[phone_col] != "")
        df["__dup_phone__"] = (
            df.duplicated(subset=[phone_col], keep=False)
            & has_phone
            & ~df["__dup_combined__"]
        )

    # Email only — must have real email value AND not already combined
    if email_col:
        has_email = df[email_col].notna() & (df[email_col] != "")
        df["__dup_email__"] = (
            df.duplicated(subset=[email_col], keep=False)
            & has_email
            & ~df["__dup_combined__"]
        )

    return df


def detect_exact_duplicates(df: pd.DataFrame) -> dict:
    """
    Detect rows where ALL column values are identical (full-row match).
    Ignores internal __* marker columns.
    Returns:
      count             — total exact duplicate rows
      groups            — top 10 groups with sample values and count
      duplicate_indices — set of row indices that are exact duplicates
    """
    internal_cols = [c for c in df.columns if c.startswith("__")]
    df_clean = df.drop(columns=internal_cols, errors="ignore").copy()

    # Normalize: lowercase + strip, NaN → ""
    df_norm = df_clean.apply(
        lambda col: col.map(
            lambda v: str(v).strip().lower() if pd.notna(v) else ""
        )
    )

    dup_mask    = df_norm.duplicated(keep=False)
    dup_indices = set(df_norm[dup_mask].index.tolist())
    dup_count   = int(dup_mask.sum())

    groups = []
    if dup_count > 0:
        df_norm["__group_key__"] = df_norm.apply(
            lambda row: "|||".join(str(v) for v in row), axis=1
        )
        group_counts = df_norm["__group_key__"].value_counts()
        top_groups   = group_counts[group_counts > 1].head(10)

        for key, cnt in top_groups.items():
            sample_idx = df_norm[df_norm["__group_key__"] == key].index[0]
            sample = {}
            for col in list(df_clean.columns)[:4]:
                val = df_clean.loc[sample_idx, col]
                sample[col] = str(val) if pd.notna(val) else ""
            groups.append({"count": int(cnt), "sample": sample})

    return {
        "count":             dup_count,
        "groups":            groups,
        "duplicate_indices": dup_indices,
    }


def get_column_fill_rates(df: pd.DataFrame) -> list:
    """
    Calculate fill rate (% non-empty) for each column.
    Returns list sorted ascending by fill_rate (worst columns first).
    Ignores internal __* columns.
    """
    internal_cols = [c for c in df.columns if c.startswith("__")]
    df_clean = df.drop(columns=internal_cols, errors="ignore")
    total = len(df_clean)
    if total == 0:
        return []

    result = []
    for col in df_clean.columns:
        non_empty = df_clean[col].apply(
            lambda v: pd.notna(v)
            and str(v).strip() not in ("", "nan", "none", "null")
        ).sum()
        fill_rate = round((non_empty / total) * 100, 1)
        result.append({
            "column":      str(col),
            "fill_rate":   fill_rate,
            "empty_count": int(total - non_empty),
            "total":       total,
        })

    result.sort(key=lambda x: x["fill_rate"])
    return result


def get_duplicate_stats(df: pd.DataFrame) -> dict:
    """
    Return aggregate duplicate counts from a marked DataFrame.

    Keys returned (upload.py uses all of these):
      total_records       – total rows in the file
      actual_records      – rows that are NOT duplicates
      duplicate_records   – rows that ARE duplicates (combined mode)
      combined_duplicates – same as duplicate_records (alias)
      phone_duplicates    – rows with a duplicate phone
      email_duplicates    – rows with a duplicate email
    """
    total      = len(df)
    combined   = int(df["__dup_combined__"].sum()) if "__dup_combined__" in df else 0
    phone_dups = int(df["__dup_phone__"].sum())    if "__dup_phone__"    in df else 0
    email_dups = int(df["__dup_email__"].sum())    if "__dup_email__"    in df else 0
    actual     = total - combined

    return {
        "total_records":       total,
        "actual_records":      actual,        # upload.py needs this
        "duplicate_records":   combined,      # upload.py needs this
        "combined_duplicates": combined,
        "phone_duplicates":    phone_dups,
        "email_duplicates":    email_dups,
    }


# ==================================================
# DATASET FILE LOADER  (convenience wrapper)
# ==================================================

def read_dataset_file(path: str) -> pd.DataFrame:
    """Read an already-uploaded dataset file."""
    return read_file(path)