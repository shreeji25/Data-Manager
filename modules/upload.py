"""
modules/upload.py

Multi-tenant upload:
- Files stored in uploads/{user_id}/filename  (prevents cross-user collision)
- Dataset always saved with user_id = current_user.id
- Category lookup scoped to current_user.id
- Large file support: async chunked streaming + background processing
"""

from pathlib import Path

from fastapi import APIRouter, UploadFile, File, Form, Request, HTTPException, Depends, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
from datetime import datetime
import os
import shutil
import asyncio

from auth import get_current_user
from database import get_db, SessionLocal
from models import Dataset, Category, UploadLog, User
from modules import shared

router = APIRouter()
templates = Jinja2Templates(directory="templates")

TEMP_UPLOAD_DIR = Path("temp_uploads")
TEMP_UPLOAD_DIR.mkdir(exist_ok=True)

# 1 MB chunks — streams large files without loading into RAM
CHUNK_SIZE = 1024 * 1024


def get_user_upload_dir(user_id: int) -> Path:
    """Returns and creates per-user upload directory: uploads/{user_id}/"""
    user_dir = Path("uploads") / str(user_id)
    user_dir.mkdir(parents=True, exist_ok=True)
    return user_dir


async def save_upload_chunked(upload_file: UploadFile, dest: Path) -> int:
    """
    Stream UploadFile to disk in CHUNK_SIZE chunks.
    Non-blocking — never loads the whole file into RAM.
    Returns total bytes written.
    """
    total_bytes = 0
    with open(dest, "wb") as f:
        while True:
            chunk = await upload_file.read(CHUNK_SIZE)
            if not chunk:
                break
            f.write(chunk)
            total_bytes += len(chunk)
    return total_bytes


def process_large_file_background(dataset_id: int, file_path: str, safe_filename: str, user_upload_dir: str):
    """
    Background task: runs AFTER HTTP response is returned to user.
    Handles slow pandas processing for large files (100k+ rows).
    Updates the dataset row_count, actual_records, duplicate_records once done.
    """
    db = SessionLocal()
    try:
        df = shared.read_file(file_path)
        if df is None or df.empty:
            return

        shared.set_cached_df(safe_filename, df)
        df_marked = shared.detect_duplicates(df)
        stats = shared.get_duplicate_stats(df_marked)

        # Save cleaned CSV for reference
        cleaned_path = Path(user_upload_dir) / f"cleaned_{safe_filename}"
        df_marked.to_csv(cleaned_path, index=False)

        # Update DB record with real counts
        dataset = db.query(Dataset).filter(Dataset.id == dataset_id).first()
        if dataset:
            dataset.row_count        = stats["total_records"]
            dataset.actual_records   = stats["actual_records"]
            dataset.duplicate_records = stats["duplicate_records"]
            db.commit()

    except Exception as e:
        print(f"❌ Background processing error for dataset {dataset_id}: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()


@router.get("/upload", response_class=HTMLResponse)
async def upload_page(
    request: Request,
    db: Session = Depends(get_db),
):
    user = get_current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=302)

    user_id = user["id"]

    categories = (
        db.query(Category)
        .filter(Category.user_id == user_id)
        .order_by(Category.name)
        .all()
    )

    return templates.TemplateResponse("upload.html", {
        "request": request,
        "user": user,
        "categories": categories,
    })


@router.post("/upload")
async def upload_file(
    request: Request,
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    category_id: int = Form(...),
    description: str = Form(""),
    db: Session = Depends(get_db),
):
    user = get_current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=302)

    user_id = user["id"]
    temp_file_path = None

    try:
        # ── Category must belong to this user ────────────────────────────
        category = db.query(Category).filter(
            Category.id == category_id,
            Category.user_id == user_id,
        ).first()

        if not category:
            return JSONResponse(
                {"status": "error", "message": "Invalid category selected"},
                status_code=400,
            )

        # ── File type validation ─────────────────────────────────────────
        allowed_extensions = [".csv", ".xlsx", ".xls"]
        file_ext = os.path.splitext(file.filename)[1].lower()

        if file_ext not in allowed_extensions:
            return JSONResponse(
                {"status": "error", "message": f"File type {file_ext} not supported. Use CSV or Excel."},
                status_code=400,
            )

        # ── Stream file to temp dir (async, chunked — no RAM limit) ─────
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_filename = f"{timestamp}_{file.filename}"
        temp_file_path = TEMP_UPLOAD_DIR / safe_filename

        await save_upload_chunked(file, temp_file_path)

        # ── Check file size after saving ─────────────────────────────────
        file_size_mb = temp_file_path.stat().st_size / (1024 * 1024)
        print(f"📁 Uploaded: {file.filename} ({file_size_mb:.1f} MB)")

        # ── For very large files (>20MB) use background processing ───────
        # Saves immediately to DB with row_count=0, updates after processing
        LARGE_FILE_THRESHOLD_MB = 20

        if file_size_mb > LARGE_FILE_THRESHOLD_MB:
            user_upload_dir = get_user_upload_dir(user_id)
            final_file_path = user_upload_dir / safe_filename
            shutil.move(str(temp_file_path), str(final_file_path))

            # Save DB record immediately so user sees it on dashboard
            dataset = Dataset(
                file_name=file.filename,
                file_path=str(final_file_path),
                department=category.name,
                category_id=category.id,
                user_id=user_id,
                description=description,
                row_count=0,           # will be updated by background task
                actual_records=0,
                duplicate_records=0,
            )
            db.add(dataset)
            db.commit()
            db.refresh(dataset)

            # Log
            log = UploadLog(
                file_name=file.filename,
                status="processing",
                message=f"Large file ({file_size_mb:.1f} MB) — processing in background"
            )
            db.add(log)
            db.commit()

            # Process duplicates/stats after response is sent
            background_tasks.add_task(
                process_large_file_background,
                dataset.id,
                str(final_file_path),
                safe_filename,
                str(user_upload_dir),
            )

            return RedirectResponse("/dashboard", status_code=303)

        # ── Normal path (files ≤20MB) — process inline ──────────────────
        try:
            df = shared.read_file(str(temp_file_path))
            shared.set_cached_df(safe_filename, df)
        except Exception as e:
            if temp_file_path and temp_file_path.exists():
                os.remove(temp_file_path)
            return JSONResponse(
                {"status": "error", "message": f"Error reading file: {str(e)}"},
                status_code=400,
            )

        if df.empty:
            os.remove(temp_file_path)
            return JSONResponse(
                {"status": "error", "message": "File is empty or contains no valid data"},
                status_code=400,
            )

        # ── Header validation ────────────────────────────────────────────
        has_required, detected_mapping, info = shared.check_required_columns(df)

        if info["has_header_problem"]:
            reason = info["problem_reason"]
            problem_count = len(info["problematic_headers"])
            if reason == "no_header_detected":
                log_msg = f"No header row detected - {problem_count} auto-generated columns"
            elif reason == "email_in_header":
                log_msg = f"Email address in header ({problem_count} columns)"
            elif reason == "phone_in_header":
                log_msg = f"Phone number in header ({problem_count} columns)"
            elif reason == "null_header":
                log_msg = f"Null/empty headers ({problem_count} columns)"
            else:
                log_msg = f"Header problem: {reason}"
        else:
            log_msg = f"Headers valid. Detected: {', '.join(detected_mapping.keys())}"

        log = UploadLog(file_name=file.filename, status="analyzing", message=log_msg)
        db.add(log)
        db.commit()

        user_upload_dir = get_user_upload_dir(user_id)

        if has_required:
            # ── AUTO-UPLOAD PATH ─────────────────────────────────────────
            df_marked = shared.detect_duplicates(df)
            stats = shared.get_duplicate_stats(df_marked)

            final_file_path = user_upload_dir / safe_filename
            shutil.move(str(temp_file_path), str(final_file_path))

            cleaned_file_path = user_upload_dir / f"cleaned_{safe_filename}"
            df_marked.to_csv(cleaned_file_path, index=False)

            dataset = Dataset(
                file_name=file.filename,
                file_path=str(final_file_path),
                department=category.name,
                category_id=category.id,
                user_id=user_id,
                description=description,
                row_count=stats["total_records"],
                actual_records=stats["actual_records"],
                duplicate_records=stats["duplicate_records"],
            )

            db.add(dataset)
            db.commit()

            return RedirectResponse("/dashboard", status_code=303)

        else:
            # ── CORRECTION PAGE PATH ─────────────────────────────────────
            preview_data = df.head(10).to_dict(orient="list")

            return templates.TemplateResponse("correction.html", {
                "request": request,
                "user": user,
                "temp_id": safe_filename,
                "columns": list(df.columns),
                "preview": preview_data,
                "category_id": category_id,
                "description": description,
                "original_filename": file.filename,
                "problem_reason": info["problem_reason"]
            })

    except Exception as e:
        if temp_file_path and Path(temp_file_path).exists():
            os.remove(temp_file_path)
        return JSONResponse(
            {"status": "error", "message": f"Upload failed: {str(e)}"},
            status_code=500,
        )


@router.post("/upload/fix")
async def fix_column_mapping(
    request: Request,
    db: Session = Depends(get_db),
):
    user = get_current_user(request)
    if not user:
        return RedirectResponse("/login", status_code=302)

    user_id = user["id"]

    try:
        form = await request.form()

        temp_filename = form.get("temp_id")
        category_id   = form.get("category_id") or form.get("department")
        description   = form.get("description", "")

        # ── Build column mapping from form ───────────────────────────────
        mapping = {}
        for key, value in form.items():
            if key.startswith("map_") and value:
                original_col = key[4:]  # strip "map_" prefix safely
                new_name = value.strip().lower().replace(" ", "_")
                mapping[original_col] = new_name

        if not temp_filename or not mapping:
            raise HTTPException(400, "Missing mapping data")

        temp_path = TEMP_UPLOAD_DIR / temp_filename
        if not temp_path.exists():
            raise HTTPException(404, "Temp file not found. Please re-upload.")

        df = shared.read_file(str(temp_path))
        if df is None or df.empty:
            raise HTTPException(400, "Failed to read file")

        # ── Restore the first data row that was lost as header ───────────
        # shared.read_file() uses row 0 as column names (header=0), so when
        # the original file had no valid header, row 0 was actual data that
        # got consumed as column names and dropped from the dataframe.
        # We rebuild it from df.columns and insert it back as row 0.
        import pandas as pd
        problem_reason = form.get("problem_reason", "")
        if problem_reason == "no_header_detected":
            first_row = pd.DataFrame([df.columns.tolist()], columns=df.columns)
            df = pd.concat([first_row, df], ignore_index=True)

        df = shared.apply_column_mapping(df, mapping)
        df_marked = shared.detect_duplicates(df)
        stats = shared.get_duplicate_stats(df_marked)

        user_upload_dir = get_user_upload_dir(user_id)

        final_path = user_upload_dir / temp_filename
        shutil.move(str(temp_path), str(final_path))

        temp_path_obj = Path(temp_filename)
        cleaned_name = f"cleaned_{temp_path_obj.stem}.csv"
        cleaned = user_upload_dir / cleaned_name
        df_marked.to_csv(cleaned, index=False)

        shared.cache_dataframe(cleaned_name, df_marked)

        # ── Category must belong to this user ────────────────────────────
        category = db.query(Category).filter(
            Category.id == category_id,
            Category.user_id == user_id,
        ).first()

        dataset = Dataset(
            file_name=temp_filename,
            file_path=str(final_path),
            department=category.name if category else str(category_id),
            category_id=category.id if category else None,
            user_id=user_id,
            description=description,
            row_count=stats["total_records"],
            actual_records=stats["actual_records"],
            duplicate_records=stats["duplicate_records"],
        )

        db.add(dataset)
        db.commit()
        db.refresh(dataset)

        return RedirectResponse(url=f"/view/{dataset.id}", status_code=303)

    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(500, f"Header correction failed: {str(e)}")


@router.post("/upload/auto-detect")
async def auto_detect_columns(request: Request):
    try:
        data = await request.json()
        temp_filename = data.get("temp_filename")

        if not temp_filename:
            return JSONResponse({"status": "error", "message": "Missing filename"}, status_code=400)

        temp_file_path = TEMP_UPLOAD_DIR / temp_filename
        if not temp_file_path.exists():
            return JSONResponse({"status": "error", "message": "File not found"}, status_code=404)

        df = shared.read_file(str(temp_file_path))
        column_analysis = shared.analyze_file_columns(df)

        message = "Auto-detection complete"
        if not column_analysis["detected_mapping"]:
            message = "Could not auto-detect columns. Please map manually."

        return JSONResponse({
            "status": "success",
            "message": message,
            "detected_mapping": column_analysis["detected_mapping"],
        })

    except Exception as e:
        return JSONResponse(
            {"status": "error", "message": f"Auto-detection failed: {str(e)}"},
            status_code=500,
        )