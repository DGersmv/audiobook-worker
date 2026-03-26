import os
import re
import time
import zipfile
import tempfile
import shutil
import asyncio
import subprocess
from datetime import datetime, timezone
import boto3
from botocore.config import Config
import requests
from bs4 import BeautifulSoup
import edge_tts
from lxml import etree
import xml.etree.ElementTree as ET

try:
    from docx import Document  # type: ignore
except Exception:
    Document = None

S3_ENDPOINT = os.getenv('S3_ENDPOINT', 'https://s3.regru.cloud')
S3_BUCKET = os.getenv('S3_BUCKET', 'book-storage')
# If set (same path layout as upload_server: .../jobs/<uuid>/input.<ext>), input is copied from disk instead of S3 download.
AUDIOBOOK_LOCAL_INPUT_DIR = os.getenv('AUDIOBOOK_LOCAL_INPUT_DIR', '').strip()
S3_ACCESS_KEY_ID = os.getenv('S3_ACCESS_KEY_ID')
S3_SECRET_ACCESS_KEY = os.getenv('S3_SECRET_ACCESS_KEY')
S3_REGION = os.getenv('S3_REGION', 'us-east-1')

VOICE = os.getenv('AUDIOBOOK_VOICE', 'ru-RU-DmitryNeural')
MAX_LEN = int(os.getenv('AUDIOBOOK_MAX_LEN', '3000'))
CHAPTERS_MIN = int(os.getenv('AUDIOBOOK_CHAPTERS_MIN', '2'))
SIGNED_EXPIRES_SECONDS = int(os.getenv('AUDIOBOOK_SIGNED_EXPIRES_SECONDS', str(7 * 24 * 60 * 60)))
POLL_SLEEP_SECONDS = int(os.getenv('AUDIOBOOK_POLL_SLEEP_SECONDS', '8'))

SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_SERVICE_ROLE_KEY = os.getenv('SUPABASE_SERVICE_ROLE_KEY')
RESEND_API_KEY = os.getenv('RESEND_API_KEY')
RESEND_FROM_EMAIL = os.getenv('RESEND_FROM_EMAIL', '227.info <noreply@227.info>')

if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
    raise RuntimeError('Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY env vars')
if not S3_ACCESS_KEY_ID or not S3_SECRET_ACCESS_KEY:
    raise RuntimeError('Missing S3_ACCESS_KEY_ID or S3_SECRET_ACCESS_KEY (Reg.ru S3)')


def s3_client():
    return boto3.client(
        's3',
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY_ID,
        aws_secret_access_key=S3_SECRET_ACCESS_KEY,
        region_name=S3_REGION,
        config=Config(signature_version='s3v4', s3={'addressing_style': 'path'}),
    )


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def headers_json():
    return {
        'apikey': SUPABASE_SERVICE_ROLE_KEY,
        'Authorization': f'Bearer {SUPABASE_SERVICE_ROLE_KEY}',
        'Content-Type': 'application/json',
    }


def rest_url(path: str) -> str:
    return f'{SUPABASE_URL}/rest/v1/{path}'


def sanitize_filename(s: str, max_len: int = 90) -> str:
    s = (s or '').strip()
    if not s:
        s = 'book'
    # Заменяем всё небуквенно-цифровое на underscore. Юникод оставим (Windows обычно ок), но ограничим спецсимволы.
    s = re.sub(r'[^0-9A-Za-zА-Яа-яЁё_\-]+', '_', s)
    s = re.sub(r'_+', '_', s).strip('_')
    if len(s) > max_len:
        s = s[:max_len].rstrip('_')
    return s or 'book'


def clean_text(text: str) -> str:
    text = text.replace('\xa0', ' ')
    text = text.replace('\r', '\n')
    text = re.sub(r'\n\n\n+', '\n\n', text)
    return text.strip()


def split_text(text: str, max_len: int = 3000):
    parts = []
    text = (text or '').strip()
    if not text:
        return parts

    while len(text) > max_len:
        chunk = text[:max_len]
        split_pos = max(
            chunk.rfind('. '),
            chunk.rfind('! '),
            chunk.rfind('? '),
            chunk.rfind('\n'),
        )
        if split_pos < 500:
            split_pos = max_len
        else:
            split_pos = split_pos + 1

        part = text[:split_pos].strip()
        if part:
            parts.append(part)
        text = text[split_pos:].strip()

    if text:
        parts.append(text)
    return parts


def looks_like_title(line: str) -> bool:
    line = (line or '').strip()
    if not line:
        return False
    if len(line) > 60:
        return False
    if line.count('.') > 1:
        return False
    words = [w for w in line.split() if w]
    if len(words) > 8:
        return False
    return True


def find_chapter_splits(text: str):
    patterns = [
        r'(?im)^\s*(глава\s+\d+)\s*$',
        r'(?im)^\s*(глава\s+[ivxlcdm]+)\s*$',
        r'(?im)^\s*(глава\s+[а-яё]+)\s*$',
        r'(?im)^\s*(часть\s+\d+)\s*$',
        r'(?im)^\s*(часть\s+[ivxlcdm]+)\s*$',
        r'(?im)^\s*(chapter\s+\d+)\s*$',
        r'(?im)^\s*(part\s+\d+)\s*$',
    ]

    matches = []
    for pat in patterns:
        for m in re.finditer(pat, text):
            matches.append((m.start(), m.group(1).strip()))

    # unique by start pos
    matches = sorted({(pos, title) for pos, title in matches}, key=lambda x: x[0])

    if len(matches) >= CHAPTERS_MIN:
        return matches

    # Fallback: попробуем найти короткие заголовки по линиям
    lines = text.splitlines(keepends=True)
    pos = 0
    heur = []
    for line in lines:
        stripped = line.strip()
        if looks_like_title(stripped):
            # часто заголовки — в верхнем регистре, либо начинаются с ключевого слова
            low = stripped.lower()
            is_kw = low.startswith(('глава', 'часть', 'chapter', 'part'))
            is_caps = stripped == stripped.upper() and any(ch.isalpha() for ch in stripped)
            if is_kw or is_caps:
                heur.append((pos, stripped))
        pos += len(line)

    heur = sorted({(p, t) for p, t in heur}, key=lambda x: x[0])
    # если слишком много ложных, урежем
    return heur[:200] if len(heur) >= 2 else matches


def split_by_chapters(full_text: str):
    text = clean_text(full_text)
    if not text:
        return [('Книга', '')]

    matches = find_chapter_splits(text)
    if not matches:
        return [('Книга', text)]

    chapters = []
    first_pos = matches[0][0]
    intro = text[:first_pos].strip()
    if intro and len(intro) > 300:
        chapters.append(('Вступление', intro))

    for i, (start, title) in enumerate(matches):
        end = matches[i + 1][0] if i + 1 < len(matches) else len(text)
        chunk = text[start:end].strip()
        if not chunk:
            continue
        # убираем строку с самим заголовком, если она совпала
        chunk_lines = chunk.splitlines()
        if chunk_lines and chunk_lines[0].strip() == title.strip():
            chunk = '\n'.join(chunk_lines[1:]).strip()
        chapters.append((title, chunk))

    if not chapters:
        return [('Книга', text)]
    return chapters


def extract_title_from_epub(extract_root: str):
    for root, _dirs, files in os.walk(extract_root):
        for f in files:
            if f.lower() == 'content.opf':
                opf_path = os.path.join(root, f)
                try:
                    tree = ET.parse(opf_path)
                    elem = tree.find('.//{http://purl.org/dc/elements/1.1/}title')
                    if elem is not None and elem.text:
                        return elem.text.strip()
                except Exception:
                    pass
    return None


def extract_full_text_epub(epub_path: str, work_dir: str):
    extract_dir = os.path.join(work_dir, 'epub')
    os.makedirs(extract_dir, exist_ok=True)

    with zipfile.ZipFile(epub_path, 'r') as z:
        z.extractall(extract_dir)

    html_files = []
    for root, _dirs, files in os.walk(extract_dir):
        for f in files:
            if f.lower().endswith(('.html', '.xhtml', '.htm')):
                html_files.append(os.path.join(root, f))

    html_files.sort()

    all_text = []
    for fp in html_files:
        try:
            with open(fp, 'rb') as f:
                soup = BeautifulSoup(f.read(), 'html.parser')
            text = soup.get_text('\n')
            text = clean_text(text)
            if len(text) > 300:
                all_text.append(text)
        except Exception:
            continue

    title = extract_title_from_epub(extract_dir) or os.path.splitext(os.path.basename(epub_path))[0]
    return title, '\n\n'.join(all_text)


def extract_full_text_fb2(fb2_path: str):
    title = os.path.splitext(os.path.basename(fb2_path))[0]
    parts = []
    try:
        tree = ET.parse(fb2_path)
        root = tree.getroot()

        # book title
        for elem in root.iter():
            tag = elem.tag.split('}')[-1]
            if tag == 'book-title' and elem.text and elem.text.strip():
                title = elem.text.strip()
                break

        buf = []
        for elem in root.iter():
            tag = elem.tag.split('}')[-1]
            if tag == 'p':
                if elem.text and elem.text.strip():
                    buf.append(elem.text.strip())
            elif tag == 'title':
                if elem.text and elem.text.strip():
                    buf.append(elem.text.strip())

        parts = buf
    except Exception as e:
        raise RuntimeError(f'FB2 parse failed: {e}')

    text = clean_text('\n'.join(parts))
    return title, text


def extract_full_text_txt(txt_path: str):
    raw = None
    for enc in ('utf-8', 'cp1251', 'latin-1'):
        try:
            with open(txt_path, 'r', encoding=enc) as f:
                raw = f.read()
            break
        except Exception:
            continue
    raw = raw or ''
    title = os.path.splitext(os.path.basename(txt_path))[0]
    return title, clean_text(raw)


def extract_full_text_docx(docx_path: str):
    if Document is None:
        raise RuntimeError('python-docx is not installed on worker machine')

    doc = Document(docx_path)
    title = os.path.splitext(os.path.basename(docx_path))[0]

    lines = []
    for p in doc.paragraphs:
        t = (p.text or '').strip()
        if not t:
            continue
        # Иногда heading стилизован — но для простоты просто берём текст.
        lines.append(t)

    return title, clean_text('\n'.join(lines))


def extract_full_text_html(html_path: str):
    with open(html_path, 'rb') as f:
        soup = BeautifulSoup(f.read(), 'html.parser')
    title = os.path.splitext(os.path.basename(html_path))[0]
    text = clean_text(soup.get_text('\n'))
    return title, text


def download_file(object_key: str, dest_path: str):
    if AUDIOBOOK_LOCAL_INPUT_DIR:
        src = os.path.join(AUDIOBOOK_LOCAL_INPUT_DIR, object_key)
        if os.path.isfile(src):
            shutil.copy2(src, dest_path)
            return
    s3_client().download_file(S3_BUCKET, object_key, dest_path)


def upload_file(object_key: str, src_path: str, content_type: str):
    s3_client().upload_file(
        src_path,
        S3_BUCKET,
        object_key,
        ExtraArgs={'ContentType': content_type},
    )


def signed_url(object_key: str, expires_in: int = SIGNED_EXPIRES_SECONDS):
    return s3_client().generate_presigned_url(
        'get_object',
        Params={'Bucket': S3_BUCKET, 'Key': object_key},
        ExpiresIn=expires_in,
    )


def rest_get_pending(limit: int = 1):
    params = {
        'select': 'id,email,input_path,source_format,source_filename,book_title_sanitized,status',
        'status': 'eq.pending',
        'limit': str(limit),
        'order': 'created_at.asc',
    }
    r = requests.get(rest_url('audiobook_jobs'), headers=headers_json(), params=params, timeout=60)
    r.raise_for_status()
    rows = r.json()
    # input_path может быть null, отфильтруем
    return [row for row in rows if row.get('input_path')]


def rest_claim_job(job_id: str):
    # атомарно: только если status=pending
    params = {
        'id': f'eq.{job_id}',
        'status': 'eq.pending',
        'select': 'id',
    }
    r = requests.patch(
        rest_url('audiobook_jobs'),
        headers={**headers_json(), 'Prefer': 'return=representation'},
        params=params,
        json={'status': 'processing'},
        timeout=60,
    )
    # В некоторых конфигурациях updated_at может быть auto, поэтому не ругаемся.
    if r.status_code == 406:
        return False
    r.raise_for_status()
    out = r.json()
    return bool(out)


def rest_update_job(job_id: str, *, status: str, output_zip_path: str | None = None, error_message: str | None = None):
    payload = {'status': status}
    if output_zip_path is not None:
        payload['output_zip_path'] = output_zip_path
    if error_message is not None:
        payload['error_message'] = error_message
    if status in ('completed', 'failed'):
        payload['completed_at'] = now_iso()
    r = requests.patch(
        rest_url('audiobook_jobs'),
        headers={**headers_json(), 'Prefer': 'return=representation'},
        params={'id': f'eq.{job_id}'},
        json=payload,
        timeout=60,
    )
    r.raise_for_status()


def rest_set_failed(job_id: str, message: str):
    rest_update_job(job_id, status='failed', error_message=message)


def merge_mp3_files(files: list[str], output_file: str):
    # ffmpeg concat demuxer требует список с абсолютными путями.
    list_file = os.path.join(os.path.dirname(output_file), 'concat_list.txt')
    with open(list_file, 'w', encoding='utf-8') as f:
        for fp in files:
            abs_path = os.path.abspath(fp).replace('\\', '/')
            f.write(f"file '{abs_path}'\n")

    subprocess.run(
        ['ffmpeg', '-y', '-f', 'concat', '-safe', '0', '-i', list_file, '-c', 'copy', output_file],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


async def tts_to_file(text: str, filename: str):
    communicate = edge_tts.Communicate(text=text, voice=VOICE)
    await communicate.save(filename)


async def synthesize_chapter_mp3(chapter_text: str, out_mp3: str, work_dir: str):
    parts = split_text(chapter_text, max_len=MAX_LEN)
    if not parts:
        # на случай пустых глав
        open(out_mp3, 'wb').close()
        return

    temp_dir = os.path.join(work_dir, 'parts')
    os.makedirs(temp_dir, exist_ok=True)

    part_paths = []
    for i, part in enumerate(parts, start=1):
        part_path = os.path.join(temp_dir, f'part_{i:04d}.mp3')
        await tts_to_file(part, part_path)
        part_paths.append(part_path)

    merge_mp3_files(part_paths, out_mp3)

    # чистим части
    for p in part_paths:
        try:
            os.remove(p)
        except Exception:
            pass


async def process_job(job: dict):
    job_id = job['id']
    email = job['email']
    input_path = job['input_path']
    source_format = job.get('source_format')
    source_filename = job.get('source_filename') or ''
    book_title = job.get('book_title_sanitized') or source_filename

    if not source_format:
        # fallback по расширению
        source_format = os.path.splitext(source_filename)[1].lstrip('.').lower()

    work_dir = os.path.join(tempfile.gettempdir(), f'audiobook_{job_id}')
    if os.path.exists(work_dir):
        shutil.rmtree(work_dir, ignore_errors=True)
    os.makedirs(work_dir, exist_ok=True)

    local_input = os.path.join(work_dir, f'input.{source_format}')
    output_zip_local = os.path.join(work_dir, f'output_{job_id}.zip')

    try:
        download_file(input_path, local_input)

        # извлечение текста
        if source_format == 'epub':
            title, full_text = extract_full_text_epub(local_input, work_dir)
        elif source_format == 'fb2':
            title, full_text = extract_full_text_fb2(local_input)
        elif source_format == 'txt':
            title, full_text = extract_full_text_txt(local_input)
        elif source_format == 'docx':
            title, full_text = extract_full_text_docx(local_input)
        elif source_format in ('html',):
            title, full_text = extract_full_text_html(local_input)
        else:
            raise RuntimeError(f'Unsupported source_format: {source_format}')

        book_title = title
        book_slug = sanitize_filename(book_title)

        chapters = split_by_chapters(full_text)
        # ограничение, чтобы не генерить 10000 файлов
        chapters = chapters[:200] if len(chapters) > 200 else chapters

        mp3_dir = os.path.join(work_dir, 'mp3')
        os.makedirs(mp3_dir, exist_ok=True)

        chapter_mp3_files = []
        for idx, (ch_title, ch_text) in enumerate(chapters, start=1):
            chapter_num = f'{idx:02d}'
            mark = sanitize_filename(ch_title, max_len=30)[:30]
            out_mp3 = os.path.join(mp3_dir, f'{book_slug}_{chapter_num}_{mark}.mp3')
            await synthesize_chapter_mp3(ch_text, out_mp3, work_dir)
            chapter_mp3_files.append(out_mp3)

        # zip
        manifest_path = os.path.join(work_dir, 'manifest.txt')
        with open(manifest_path, 'w', encoding='utf-8') as mf:
            for i, (ch_title, _ch_text) in enumerate(chapters, start=1):
                mf.write(f'{i:02d} {ch_title}\n')

        with zipfile.ZipFile(output_zip_local, 'w', compression=zipfile.ZIP_DEFLATED) as z:
            z.write(manifest_path, arcname='manifest.txt')
            for mp3 in chapter_mp3_files:
                z.write(mp3, arcname=os.path.basename(mp3))

        output_zip_path = f'jobs/{job_id}/output.zip'
        upload_file(output_zip_path, output_zip_local, 'application/zip')

        # signed URL + email
        if RESEND_API_KEY:
            zip_url = signed_url(output_zip_path)
            subject = f'Аудиокнига: {book_title}'
            html = f"""
            <p>Здравствуйте!</p>
            <p>Готова аудиокнига <strong>{book_title}</strong>.</p>
            <p>Скачивание:</p>
            <p><a href="{zip_url}">Скачать ZIP с MP3</a></p>
            <p>Если загрузка не началась — откройте ссылку в браузере.</p>
            <hr/>
            <p style="font-size:12px;color:#666">227.info</p>
            """

            requests.post(
                'https://api.resend.com/emails',
                headers={
                    'Authorization': f'Bearer {RESEND_API_KEY}',
                    'Content-Type': 'application/json',
                },
                json={
                    'from': RESEND_FROM_EMAIL,
                    'to': [email],
                    'subject': subject,
                    'html': html,
                },
                timeout=60,
            ).raise_for_status()

        # update job
        rest_update_job(job_id, status='completed', output_zip_path=output_zip_path, error_message=None)

    except Exception as e:
        err = str(e)
        try:
            rest_set_failed(job_id, err[:2000])
        except Exception:
            pass
        raise
    finally:
        try:
            shutil.rmtree(work_dir, ignore_errors=True)
        except Exception:
            pass


async def main_loop():
    while True:
        try:
            pending_jobs = rest_get_pending(limit=5)
            if not pending_jobs:
                time.sleep(POLL_SLEEP_SECONDS)
                continue

            for job in pending_jobs:
                job_id = job['id']
                claimed = rest_claim_job(job_id)
                if not claimed:
                    continue

                # обрабатываем синхронно, чтобы не утонуть в TTS
                await process_job(job)

        except Exception as e:
            print('Worker loop error:', e)
            time.sleep(5)


if __name__ == '__main__':
    asyncio.run(main_loop())
