import pandas as pd
import zipfile
import os
import tempfile
import re
import numpy as np
from modules import shared
# ===============================
# GLOBAL NORMALIZERS
# ===============================

def clean_phone_global(x):
    """Clean and normalize phone numbers"""
    if pd.isna(x):
        return None

    x = str(x).strip()

    if x.endswith(".0"):
        x = x[:-2]

    x = re.sub(r"[^\d]", "", x)

    if x.startswith("91") and len(x) > 10:
        x = x[2:]

    if 7 <= len(x) <= 12:
        return x

    return None


def clean_email_global(x):
    """Clean and normalize email addresses"""
    if pd.isna(x):
        return None

    x = str(x).strip().lower()

    pattern = r"^[\w\.-]+@[\w\.-]+\.\w+$"

    if re.match(pattern, x):
        return x

    return None


# =========================
# FILE READER FUNCTION
# =========================

   
def read_single_file(path: str, ext: str):
    """Read a single file and return DataFrame"""
    try:
        df = shared.read_file(path)
        df.columns = df.columns.astype(str).str.strip().str.lower()
        return df
    except Exception:
        return None

# =========================
# PROCESS ONE DATAFRAME
# =========================

def process_dataframe(df: pd.DataFrame):
    """Process a DataFrame to find duplicate contacts"""

    if df is None or df.empty:
        return {"combined": [], "phone": [], "email": []}

    df = df.copy()
    df.columns = df.columns.str.lower().str.strip()

    # ── Strict column detection (same as shared.py) ──────────────────────
    PHONE_KEYWORDS = [
        "phone_no", "contact_no", "phone", "mobile", "cell",
        "tel", "mob", "phoneno", "mobileno", "contactno"
    ]
    PHONE_EXCLUDES = [
        "zip", "pin", "postal", "code", "index", "sr",
        "ref", "two", "four", "wheeler", "company", "fax"
    ]
    EMAIL_KEYWORDS = ["email", "e-mail", "emailid", "emailaddress", "mail"]
    EMAIL_EXCLUDES = ["name", "username", "filename"]

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

    name_cols = [
        c for c in df.columns
        if any(k in c for k in ["name", "user", "customer", "client", "person", "candidate", "applicant"])
        and not any(b in c for b in ["file", "user_name", "username"])
    ]

    # ── Merge multiple phone columns into one ─────────────────────────────
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

    # ── Name handling ────────────────────────────────────────────────────────
    if name_cols:
        df["__name__"] = (
            df[name_cols]
            .fillna("")
            .astype(str)
            .agg(
                lambda x: " ".join(
                    v.strip() for v in x
                    if v.strip() and v.strip().lower() not in ["nan", "none", "null"]
                ),
                axis=1,
            )
        )
    else:
        df["__name__"] = ""

    name_col = "__name__"
    # Blank names become empty string (NOT "UNKNOWN" — see BUG FIX note below)
    df[name_col] = df[name_col].astype(str).str.strip()
    df[name_col] = df[name_col].replace(["nan", "none", "null"], "")

    # ── Clean phone ──────────────────────────────────────────────────────────
    def clean_phone(x):
        if pd.isna(x):
            return None
        x = str(x).strip()
        if x.endswith(".0"):
            x = x[:-2]
        x = re.sub(r"[^\d]", "", x)
        if x.startswith("91") and len(x) > 10:
            x = x[2:]
        if 7 <= len(x) <= 12:
            return x
        return None

    # ── Clean email ──────────────────────────────────────────────────────────
    def clean_email(x):
        if pd.isna(x):
            return None
        x = str(x).strip().lower()
        if re.match(r"^[\w\.-]+@[\w\.-]+\.\w+$", x):
            return x
        return None

    if phone_col:
        df[phone_col] = df[phone_col].apply(clean_phone)
    if email_col:
        df[email_col] = df[email_col].apply(clean_email)

    # ── Keep rows that have at least one contact field ────────────────────────
    if phone_col and email_col:
        df_filtered = df.dropna(subset=[phone_col, email_col], how="all")
    elif phone_col:
        df_filtered = df.dropna(subset=[phone_col])
    elif email_col:
        df_filtered = df.dropna(subset=[email_col])
    else:
        df_filtered = df

    results = {"combined": [], "phone": [], "email": []}

    # ── COMBINED (phone AND email both match) ────────────────────────────────
    if phone_col and email_col:
        temp = df_filtered.dropna(subset=[phone_col, email_col])

        if not temp.empty:
            grouped = (
                temp
                .groupby([phone_col, email_col])
                .agg(
                    user_names=(name_col, lambda x: list(x)),
                    row_count=(name_col, "size"),      # real row count
                )
                .reset_index()
            )
            grouped = grouped[grouped["row_count"] > 1]

            for _, r in grouped.iterrows():
                results["combined"].append({
                    "phone":      r[phone_col],
                    "email":      r[email_col],
                    "user_names": ", ".join(dict.fromkeys(           # preserve order, dedupe
                        n for n in r["user_names"] if n.strip()
                    )),
                    "user_count": int(r["row_count"]),               # real row count
                })

    # ── PHONE (phone appears more than once) ─────────────────────────────────
    # Also collect the set of emails each phone group contains
    # so _build_strict_modes can correctly classify combined vs phone-only
    if phone_col:
        phone_data = df_filtered.dropna(subset=[phone_col])

        if not phone_data.empty:
            if email_col:
                grouped = (
                    phone_data
                    .groupby(phone_col)
                    .agg(
                        user_names=(name_col, lambda x: list(x)),
                        row_count=(name_col, "size"),
                        emails=(email_col, lambda x: list(x)),
                    )
                    .reset_index()
                )
            else:
                grouped = (
                    phone_data
                    .groupby(phone_col)
                    .agg(
                        user_names=(name_col, lambda x: list(x)),
                        row_count=(name_col, "size"),
                    )
                    .reset_index()
                )
                grouped["emails"] = [[] for _ in range(len(grouped))]

            grouped = grouped[grouped["row_count"] > 1]

            for _, r in grouped.iterrows():
                # Collect unique non-null emails in this phone group
                emails_in_group = list(dict.fromkeys(
                    e for e in r["emails"]
                    if e and str(e).strip() not in ("", "nan", "none", "null")
                ))
                results["phone"].append({
                    "phone":      r[phone_col],
                    "email":      emails_in_group[0] if len(emails_in_group) == 1 else None,
                    "emails":     emails_in_group,   # full list for _build_strict_modes
                    "user_names": ", ".join(dict.fromkeys(
                        n for n in r["user_names"] if n.strip()
                    )),
                    "user_count": int(r["row_count"]),
                })

    # ── EMAIL (email appears more than once) ─────────────────────────────────
    # Also collect the set of phones each email group contains
    if email_col:
        email_data = df_filtered.dropna(subset=[email_col])

        if not email_data.empty:
            if phone_col:
                grouped = (
                    email_data
                    .groupby(email_col)
                    .agg(
                        user_names=(name_col, lambda x: list(x)),
                        row_count=(name_col, "size"),
                        phones=(phone_col, lambda x: list(x)),
                    )
                    .reset_index()
                )
            else:
                grouped = (
                    email_data
                    .groupby(email_col)
                    .agg(
                        user_names=(name_col, lambda x: list(x)),
                        row_count=(name_col, "size"),
                    )
                    .reset_index()
                )
                grouped["phones"] = [[] for _ in range(len(grouped))]

            grouped = grouped[grouped["row_count"] > 1]

            for _, r in grouped.iterrows():
                phones_in_group = list(dict.fromkeys(
                    p for p in r["phones"]
                    if p and str(p).strip() not in ("", "nan", "none", "null")
                ))
                results["email"].append({
                    "phone":      phones_in_group[0] if len(phones_in_group) == 1 else None,
                    "phones":     phones_in_group,   # full list for _build_strict_modes
                    "email":      r[email_col],
                    "user_names": ", ".join(dict.fromkeys(
                        n for n in r["user_names"] if n.strip()
                    )),
                    "user_count": int(r["row_count"]),
                })

    return results


# =========================
# MAIN FUNCTION
# =========================

def extract_duplicate_contacts(file_path: str, file_ext: str):
    """Extract duplicate contacts from a file"""

    all_results = {"combined": [], "phone": [], "email": []}

    # ── ZIP support ──────────────────────────────────────────────────────────
    if file_ext == "zip":
        with zipfile.ZipFile(file_path, "r") as z:
            supported = [
                f for f in z.namelist()
                if f.lower().endswith((".csv", ".xls", ".xlsx", ".txt"))
            ]

            if not supported:
                raise ValueError("ZIP does not contain any supported data file")

            temp_dir = tempfile.gettempdir()

            for data_file in supported:
                extracted_path = z.extract(data_file, temp_dir)
                ext = data_file.split(".")[-1].lower()
                try:
                   df = shared.read_file(extracted_path)
                except Exception:
                     df = None

                if df is not None:
                    result = process_dataframe(df)
                    all_results["combined"].extend(result["combined"])
                    all_results["phone"].extend(result["phone"])
                    all_results["email"].extend(result["email"])

    # ── Normal single file ───────────────────────────────────────────────────
    else:
        df = shared.read_file(file_path)

        if df is None:
            raise ValueError("Unsupported file format or corrupted file")

        all_results = process_dataframe(df)

    # ── Merge duplicates across files (ZIP only meaningfully uses this) ──────
    all_results["combined"] = _merge_mode(all_results["combined"])
    all_results["phone"]    = _merge_mode(all_results["phone"])
    all_results["email"]    = _merge_mode(all_results["email"])

    return all_results


def _merge_mode(mode_data: list) -> list:
    """
    Merge records that share the same (phone, email) key across multiple
    files in a ZIP.

    BUG FIXES vs original merge_mode():
      1. Records are NO LONGER dropped when all names are blank/UNKNOWN.
         A duplicate is real even if the name column is empty.
         user_names falls back to "—" for display in that case.

      2. user_count now tracks the TOTAL ROW COUNT (sum of occurrences),
         not the number of distinct names. Two rows with the same name
         still count as 2 duplicate rows.
    """
    merged = {}

    for r in mode_data:
        key = (r["phone"], r["email"])

        if key not in merged:
            merged[key] = {
                "phone":      r["phone"],
                "email":      r["email"],
                "user_names": [],
                "row_count":  r["user_count"],   # start with this file's row count
            }
        else:
            merged[key]["row_count"] += r["user_count"]  # accumulate across files

        # Collect non-empty names (preserve order, dedupe)
        for n in str(r["user_names"]).split(","):
            n = n.strip()
            if n and n.upper() not in ("UNKNOWN", "NAN", "NONE", "NULL", ""):
                if n not in merged[key]["user_names"]:
                    merged[key]["user_names"].append(n)

    final = []
    for v in merged.values():
        # FIX 1: Always include the record — even with no names
        display_names = ", ".join(v["user_names"]) if v["user_names"] else "—"

        final.append({
            "phone":      v["phone"],
            "email":      v["email"],
            "user_names": display_names,
            "user_count": v["row_count"],   # FIX 2: real row count, not name count
        })

    return final


# ===============================
# EXPORT NORMALIZERS
# ===============================

def normalize_phone_public(x):
    """Public function to normalize phone"""
    if x is None or x == "":
        return None
    return clean_phone_global(x)


def normalize_email_public(x):
    """Public function to normalize email"""
    if x is None or x == "":
        return None
    return clean_email_global(x)


# ===============================
# MARK DUPLICATES (for upload preview)
# ===============================

def mark_duplicates(df: pd.DataFrame):
    """Mark duplicate records in DataFrame — same logic as detect_duplicates in shared.py"""

    if df is None or df.empty:
        return df

    df = df.copy()
    df.columns = df.columns.str.lower().str.strip()

    PHONE_KEYWORDS = [
        "phone_no", "contact_no", "phone", "mobile", "cell",
        "tel", "mob", "phoneno", "mobileno", "contactno"
    ]
    PHONE_EXCLUDES = [
        "zip", "pin", "postal", "code", "index", "sr",
        "ref", "two", "four", "wheeler", "company", "fax"
    ]
    EMAIL_KEYWORDS = ["email", "e-mail", "emailid", "emailaddress", "mail"]
    EMAIL_EXCLUDES = ["name", "username", "filename"]

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

    email_col = email_cols[0] if email_cols else None

    df["__is_duplicate__"] = False

    if not phone_col and not email_col:
        return df

    def clean_phone_local(x):
        if pd.isna(x): return None
        x = str(x).strip()
        if x.endswith(".0"): x = x[:-2]
        x = re.sub(r"[^\d]", "", x)
        if x.startswith("91") and len(x) > 10: x = x[2:]
        if 7 <= len(x) <= 12: return x
        return None

    def clean_email_local(x):
        if pd.isna(x): return None
        x = str(x).strip().lower()
        if re.match(r"^[\w\.-]+@[\w\.-]+\.\w+$", x): return x
        return None

    if phone_col:
        df["_clean_phone_"] = df[phone_col].apply(clean_phone_local)
    if email_col:
        df["_clean_email_"] = df[email_col].apply(clean_email_local)

    if phone_col:
        phone_counts = df["_clean_phone_"].value_counts()
        dup_phones = phone_counts[phone_counts > 1].index
        df.loc[df["_clean_phone_"].isin(dup_phones), "__is_duplicate__"] = True

    if email_col:
        email_counts = df["_clean_email_"].value_counts()
        dup_emails = email_counts[email_counts > 1].index
        df.loc[df["_clean_email_"].isin(dup_emails), "__is_duplicate__"] = True

    df.drop(
        columns=[c for c in ["_clean_phone_", "_clean_email_"] if c in df.columns],
        inplace=True,
    )

    return df