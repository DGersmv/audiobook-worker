"""
HTTPS endpoint on VPS: multipart upload for audiobook jobs.
Env (same machine as worker): AUDIOBOOK_UPLOAD_SECRET, AUDIOBOOK_LOCAL_INPUT_DIR,
  SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY.
Optional: AUDIOBOOK_CORS_ORIGINS=comma-separated (default *).

Run: uvicorn upload_server:app --host 127.0.0.1 --port 8001
nginx: client_max_body_size 200m; proxy_pass http://127.0.0.1:8001;
"""

from __future__ import annotations

import hashlib
import hmac
import os
import re
import time
from pathlib import Path

import requests
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware

AUDIOBOOK_UPLOAD_SECRET = os.getenv('AUDIOBOOK_UPLOAD_SECRET', '')
AUDIOBOOK_LOCAL_INPUT_DIR = os.getenv('AUDIOBOOK_LOCAL_INPUT_DIR', '/opt/227-audiobook/incoming')
SUPABASE_URL = os.getenv('SUPABASE_URL', '').rstrip('/')
SUPABASE_SERVICE_ROLE_KEY = os.getenv('SUPABASE_SERVICE_ROLE_KEY', '')
MAX_BYTES = int(os.getenv('AUDIOBOOK_UPLOAD_MAX_BYTES', str(200 * 1024 * 1024)))

EXT_OK = {'.epub', '.fb2', '.txt', '.docx', '.html', '.htm'}
EXT_TO_FORMAT = {
    '.epub': 'epub',
    '.fb2': 'fb2',
    '.txt': 'txt',
    '.docx': 'docx',
    '.html': 'html',
    '.htm': 'html',
}

app = FastAPI(title='227 audiobook upload')

_origins = os.getenv('AUDIOBOOK_CORS_ORIGINS', '*')
_cors = [o.strip() for o in _origins.split(',') if o.strip()] if _origins.strip() else ['*']
app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors,
    allow_credentials=False,
    allow_methods=['POST', 'OPTIONS'],
    allow_headers=['*'],
)


def headers_json():
    return {
        'apikey': SUPABASE_SERVICE_ROLE_KEY,
        'Authorization': f'Bearer {SUPABASE_SERVICE_ROLE_KEY}',
        'Content-Type': 'application/json',
    }


def verify_token(secret: str, job_id: str, exp: int, token: str) -> bool:
    if not secret or len(token) < 32:
        return False
    try:
        exp_i = int(exp)
    except (TypeError, ValueError):
        return False
    if int(time.time()) > exp_i:
        return False
    msg = f'{job_id}|{exp_i}'.encode()
    expected = hmac.new(secret.encode(), msg, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, token)


def rest_url(path: str) -> str:
    return f'{SUPABASE_URL}/rest/v1/{path}'


@app.get('/audiobook/health')
def health():
    return {'ok': True}


@app.post('/audiobook/upload')
async def upload(
    job_id: str = Form(...),
    upload_token: str = Form(...),
    upload_expires_at: int = Form(...),
    file: UploadFile = File(...),
):
    if not AUDIOBOOK_UPLOAD_SECRET or len(AUDIOBOOK_UPLOAD_SECRET) < 16:
        raise HTTPException(500, 'AUDIOBOOK_UPLOAD_SECRET not configured')
    if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
        raise HTTPException(500, 'Supabase env not configured')

    jid = (job_id or '').strip()
    if not re.match(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', jid, re.I):
        raise HTTPException(400, 'invalid job_id')

    if not verify_token(AUDIOBOOK_UPLOAD_SECRET, jid, upload_expires_at, upload_token):
        raise HTTPException(403, 'invalid or expired upload_token')

    r = requests.get(
        rest_url('audiobook_jobs'),
        headers=headers_json(),
        params={
            'id': f'eq.{jid}',
            'select': 'id,email,status,source_filename',
        },
        timeout=30,
    )
    r.raise_for_status()
    rows = r.json()
    if not rows:
        raise HTTPException(404, 'job_not_found')
    job = rows[0]
    if job.get('status') != 'pending':
        raise HTTPException(400, 'job_not_pending')
    if not job.get('source_filename'):
        raise HTTPException(400, 'invalid_job')

    name = (file.filename or '').strip()
    lower = name.lower()
    dot = lower.rfind('.')
    ext = lower[dot:] if dot >= 0 else ''
    if ext not in EXT_OK:
        raise HTTPException(400, 'unsupported file extension')

    src_name = (job.get('source_filename') or '').strip().lower()
    src_dot = src_name.rfind('.')
    src_ext = src_name[src_dot:] if src_dot >= 0 else ''
    if src_ext and src_ext != ext:
        raise HTTPException(400, 'filename does not match job')

    fmt = EXT_TO_FORMAT.get(ext)
    if not fmt:
        raise HTTPException(400, 'unsupported format')

    input_path = f'jobs/{jid}/input{ext}'
    dest_dir = Path(AUDIOBOOK_LOCAL_INPUT_DIR) / 'jobs' / jid
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_file = dest_dir / f'input{ext}'
    tmp_file = dest_dir / f'input{ext}.part'

    total = 0
    try:
        with open(tmp_file, 'wb') as out:
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                total += len(chunk)
                if total > MAX_BYTES:
                    raise HTTPException(413, 'file too large')
                out.write(chunk)
        if total == 0:
            raise HTTPException(400, 'empty file')
        tmp_file.replace(dest_file)
    except HTTPException:
        try:
            tmp_file.unlink(missing_ok=True)
        except OSError:
            pass
        try:
            dest_file.unlink(missing_ok=True)
        except OSError:
            pass
        raise

    patch = requests.patch(
        rest_url('audiobook_jobs'),
        headers={**headers_json(), 'Prefer': 'return=minimal'},
        params={'id': f'eq.{jid}'},
        json={'input_path': input_path, 'source_format': fmt},
        timeout=30,
    )
    if not patch.ok:
        try:
            dest_file.unlink(missing_ok=True)
        except OSError:
            pass
        raise HTTPException(500, f'db_update_failed: {patch.text[:200]}')

    return {'ok': True, 'jobId': jid, 'inputPath': input_path, 'sourceFormat': fmt}
