"""
run.py — Start server with settings for large file uploads.
Use this instead of running uvicorn directly.

Usage:
    python run.py
"""
import uvicorn

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=False,

        # ── Keep connection alive during large uploads ────────────
        # Default is 5s — kills connection mid-upload for large files
        timeout_keep_alive=600,          # 10 minutes

        # ── No HTTP body size limit at the parser level ──────────
        # h11 (HTTP/1.1 parser) defaults to rejecting large bodies
        h11_max_incomplete_event_size=0, # 0 = unlimited
    )