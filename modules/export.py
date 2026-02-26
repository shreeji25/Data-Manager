"""
Data export routes (CSV, Excel, PDF)
"""
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import FileResponse, RedirectResponse
from sqlalchemy.orm import Session
import os
import pandas as pd
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors

from auth import get_current_user
from database import get_db
from models import Dataset, DuplicateRelation
from modules.shared import (
    CLEAN_DIR, DUPLICATE_CACHE, read_dataset_file,
    get_cached_df, set_cached_df, read_file, normalize_phone, normalize_email
)
from utils.permissions import get_effective_user
from utils.duplicate_detector import extract_duplicate_contacts

router = APIRouter(prefix="/export", tags=["export"])


@router.get("/csv/{dataset_id}")
def export_clean_csv(dataset_id: int, request: Request, db: Session = Depends(get_db)):
    """Export clean data as CSV (duplicates removed)"""
    # ✅ Check authentication
    user = get_current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=302)
    
    # Get effective user
    effective_user = get_effective_user(request, db)
    if not effective_user:
        raise HTTPException(status_code=403, detail="Select a user first")
    
    # Fetch dataset scoped to effective user
    dataset = db.query(Dataset).filter(
        Dataset.id == dataset_id,
        Dataset.user_id == effective_user.id
    ).first()
    
    if not dataset:
        raise HTTPException(status_code=404)
    
    # Load dataframe from cache or file
    df = get_cached_df(dataset.file_name)
    
    if df is None:
        # Cache miss - reload from file
        if not os.path.exists(dataset.file_path):
            raise HTTPException(
                status_code=404,
                detail=f"Dataset file not found: {dataset.file_path}"
            )
        
        try:
            from modules.shared import read_file
            df = read_file(dataset.file_path)
            set_cached_df(dataset.file_name, df)
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to read dataset file: {str(e)}"
            )
    
    # Remove duplicates - KEEP FIRST OCCURRENCE
    df_copy = df.copy()
    
    # Find phone and email columns
    phone_col = None
    email_col = None
    
    for col in df_copy.columns:
        col_lower = str(col).lower().strip()
        if not phone_col and any(k in col_lower for k in ["phone", "mobile", "contact", "tel", "cell"]):
            phone_col = col
        if not email_col and any(k in col_lower for k in ["email", "mail", "e-mail"]):
            email_col = col
    
    # Build subset for duplicate detection
    subset = []
    if phone_col:
        subset.append(phone_col)
    if email_col:
        subset.append(email_col)
    
    # Drop duplicates keeping first occurrence
    if subset:
        df_clean = df_copy.drop_duplicates(subset=subset, keep='first')
    else:
        # If no phone/email columns found, just return original data
        df_clean = df_copy
    
    # Remove internal duplicate marker columns if they exist
    dup_cols = [c for c in df_clean.columns if c.startswith("__dup_") or c == "__is_duplicate__"]
    if dup_cols:
        df_clean = df_clean.drop(columns=dup_cols)
    
    path = os.path.join(CLEAN_DIR, f"CLEAN_{dataset.file_name}.csv")
    df_clean.to_csv(path, index=False)
    
    return FileResponse(path, filename=os.path.basename(path))


@router.get("/excel/{dataset_id}")
def export_clean_excel(dataset_id: int, request: Request, db: Session = Depends(get_db)):
    """Export clean data as Excel (duplicates removed)"""
    # ✅ Check authentication
    user = get_current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=302)
    
    # Get effective user
    effective_user = get_effective_user(request, db)
    if not effective_user:
        raise HTTPException(status_code=403, detail="Select a user first")
    
    # Fetch dataset scoped to effective user
    dataset = db.query(Dataset).filter(
        Dataset.id == dataset_id,
        Dataset.user_id == effective_user.id
    ).first()
    
    if not dataset:
        raise HTTPException(status_code=404)
    
    # Load dataframe from cache or file
    df = get_cached_df(dataset.file_name)
    
    if df is None:
        # Cache miss - reload from file
        if not os.path.exists(dataset.file_path):
            raise HTTPException(
                status_code=404,
                detail=f"Dataset file not found: {dataset.file_path}"
            )
        
        try:
            from modules.shared import read_file
            df = read_file(dataset.file_path)
            set_cached_df(dataset.file_name, df)
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to read dataset file: {str(e)}"
            )
    
    # Remove duplicates - KEEP FIRST OCCURRENCE
    df_copy = df.copy()
    
    # Find phone and email columns
    phone_col = None
    email_col = None
    
    for col in df_copy.columns:
        col_lower = str(col).lower().strip()
        if not phone_col and any(k in col_lower for k in ["phone", "mobile", "contact", "tel", "cell"]):
            phone_col = col
        if not email_col and any(k in col_lower for k in ["email", "mail", "e-mail"]):
            email_col = col
    
    # Build subset for duplicate detection
    subset = []
    if phone_col:
        subset.append(phone_col)
    if email_col:
        subset.append(email_col)
    
    # Drop duplicates keeping first occurrence
    if subset:
        df_clean = df_copy.drop_duplicates(subset=subset, keep='first')
    else:
        # If no phone/email columns found, just return original data
        df_clean = df_copy
    
    # Remove internal duplicate marker columns if they exist
    dup_cols = [c for c in df_clean.columns if c.startswith("__dup_") or c == "__is_duplicate__"]
    if dup_cols:
        df_clean = df_clean.drop(columns=dup_cols)
    
    path = os.path.join(CLEAN_DIR, f"CLEAN_{dataset.file_name}.xlsx")
    df_clean.to_excel(path, index=False)
    
    return FileResponse(path, filename=os.path.basename(path))


@router.get("/pdf/{dataset_id}")
def export_clean_pdf(dataset_id: int, request: Request, db: Session = Depends(get_db)):
    """Export clean data as PDF (duplicates removed)"""
    # ✅ Check authentication
    user = get_current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=302)
    
    # Get effective user
    effective_user = get_effective_user(request, db)
    if not effective_user:
        raise HTTPException(status_code=403, detail="Select a user first")
    
    # Fetch dataset scoped to effective user
    dataset = db.query(Dataset).filter(
        Dataset.id == dataset_id,
        Dataset.user_id == effective_user.id
    ).first()
    
    if not dataset:
        raise HTTPException(status_code=404)
    
    # Load dataframe from cache or file
    df = get_cached_df(dataset.file_name)
    
    if df is None:
        # Cache miss - reload from file
        if not os.path.exists(dataset.file_path):
            raise HTTPException(
                status_code=404,
                detail=f"Dataset file not found: {dataset.file_path}"
            )
        
        try:
            from modules.shared import read_file
            df = read_file(dataset.file_path)
            set_cached_df(dataset.file_name, df)
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to read dataset file: {str(e)}"
            )
    
    # Remove duplicates - KEEP FIRST OCCURRENCE
    df_copy = df.copy()
    
    # Find phone and email columns
    phone_col = None
    email_col = None
    
    for col in df_copy.columns:
        col_lower = str(col).lower().strip()
        if not phone_col and any(k in col_lower for k in ["phone", "mobile", "contact", "tel", "cell"]):
            phone_col = col
        if not email_col and any(k in col_lower for k in ["email", "mail", "e-mail"]):
            email_col = col
    
    # Build subset for duplicate detection
    subset = []
    if phone_col:
        subset.append(phone_col)
    if email_col:
        subset.append(email_col)
    
    # Drop duplicates keeping first occurrence
    if subset:
        df_clean = df_copy.drop_duplicates(subset=subset, keep='first')
    else:
        # If no phone/email columns found, just return original data
        df_clean = df_copy
    
    # Remove internal duplicate marker columns if they exist
    dup_cols = [c for c in df_clean.columns if c.startswith("__dup_") or c == "__is_duplicate__"]
    if dup_cols:
        df_clean = df_clean.drop(columns=dup_cols)
    
    pdf_path = os.path.join(CLEAN_DIR, f"CLEAN_{dataset.file_name}.pdf")
    
    doc = SimpleDocTemplate(pdf_path, pagesize=A4)
    table = Table([df_clean.columns.tolist()] + df_clean.values.tolist(), repeatRows=1)
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
        ("FONTSIZE", (0, 0), (-1, -1), 8),
    ]))
    
    doc.build([table])
    
    return FileResponse(pdf_path, filename=os.path.basename(pdf_path))


@router.get("/relations/csv/{dataset_id}")
def export_duplicate_csv(dataset_id: int, request: Request, db: Session = Depends(get_db)):
    """Export duplicate relations as CSV"""
    # ✅ Check authentication
    user = get_current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=302)
    
    # Get effective user
    effective_user = get_effective_user(request, db)
    if not effective_user:
        raise HTTPException(status_code=403, detail="Select a user first")
    
    # Fetch dataset scoped to effective user
    dataset = db.query(Dataset).filter(
        Dataset.id == dataset_id,
        Dataset.user_id == effective_user.id
    ).first()
    
    if not dataset:
        raise HTTPException(status_code=404)
    
    relations = db.query(DuplicateRelation).filter(
        DuplicateRelation.dataset_id == dataset_id
    ).all()
    
    df = pd.DataFrame([
        {
            "Phone": r.phone,
            "Email": r.email,
            "User Names": r.user_names,
            "User Count": r.user_count
        }
        for r in relations
    ])
    
    path = f"exports/duplicate_relations_{dataset_id}.csv"
    os.makedirs("exports", exist_ok=True)
    df.to_csv(path, index=False)
    
    return FileResponse(path, filename=f"duplicate_relations_{dataset_id}.csv")


@router.get("/clean-relations/{dataset_id}")
def export_clean_using_relations(dataset_id: int, request: Request, db: Session = Depends(get_db)):
    """Export clean data using duplicate relations (keeps only first occurrence)"""
    # ✅ Check authentication
    user = get_current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=302)
    
    # Get effective user
    effective_user = get_effective_user(request, db)
    if not effective_user:
        raise HTTPException(status_code=403, detail="Select a user first")
    
    # Fetch dataset scoped to effective user
    dataset = db.query(Dataset).filter(
        Dataset.id == dataset_id,
        Dataset.user_id == effective_user.id
    ).first()
    
    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")
    
    # Build duplicate cache if missing
    if dataset_id not in DUPLICATE_CACHE:
        file_ext = os.path.splitext(dataset.file_path)[1].lower().replace(".", "")
        DUPLICATE_CACHE[dataset_id] = extract_duplicate_contacts(
            dataset.file_path,
            file_ext
        )
    
    relations = DUPLICATE_CACHE.get(dataset_id)
    
    if not relations:
        raise HTTPException(
            status_code=400,
            detail="Duplicate relations not found"
        )
    
    # Load original file
    df = read_dataset_file(dataset.file_path)
    df.columns = df.columns.str.lower().str.strip()
    
    # Find phone / email columns
    phone_col = None
    email_col = None
    
    for c in df.columns:
        lc = c.lower()
        
        if not phone_col and any(
            k in lc for k in ["phone", "mobile", "contact", "cell", "tel", "contactno", "phoneno"]
        ):
            phone_col = c
        
        if not email_col and any(
            k in lc for k in ["email", "mail", "emailid", "e-mail", "emailaddress"]
        ):
            email_col = c
    
    # Normalize data
    if phone_col:
        df[phone_col] = df[phone_col].apply(normalize_phone)
    
    if email_col:
        df[email_col] = df[email_col].apply(normalize_email)
    
    # Build duplicate keys
    duplicate_keys = set()
    
    for mode in ["combined", "phone", "email"]:
        for r in relations.get(mode, []):
            phone = r.get("phone") or ""
            email = r.get("email") or ""
            duplicate_keys.add((phone, email))
    
    # Remove extra duplicates
    seen = set()
    clean_rows = []
    
    for _, row in df.iterrows():
        phone = row.get(phone_col) if phone_col else ""
        email = row.get(email_col) if email_col else ""
        
        key = (phone, email)
        
        # Keep first occurrence only
        if key not in seen:
            seen.add(key)
            clean_rows.append(row)
    
    clean_df = pd.DataFrame(clean_rows)
    
    # Save file
    filename = f"CLEAN_RELATION_{dataset.file_name}.xlsx"
    path = os.path.join(CLEAN_DIR, filename)
    clean_df.to_excel(path, index=False)
    
    return FileResponse(
        path,
        filename=filename,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )