import mimetypes
import os
import uuid
import stat
from werkzeug.utils import secure_filename
from flask import current_app

# Keep reasonable file size limit for uploads
MAX_FILE_SIZE = 10 * 1024 * 1024  # 10MB per new requirement

# Allowed MIME types for document files (for validation) - PDF only
ALLOWED_MIMETYPES = {
    'application/pdf',
}


def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in current_app.config.get('ALLOWED_EXTENSIONS', set())


def _guess_mimetype_by_magic(file_path):
    try:
        import magic  # python-magic (optional)
        m = magic.Magic(mime=True)
        return m.from_file(file_path)
    except Exception:
        return None


def validate_file_mimetype(file_path: str, original_filename: str) -> bool:
    """Validate file mimetype matches extension to prevent spoofed files.

    Prefer using libmagic if available, otherwise fall back to mimetypes.guess_type
    and the provided upload MIME from the client.
    """
    # Try libmagic first (more accurate)
    detected = _guess_mimetype_by_magic(file_path)
    if detected:
        return detected in ALLOWED_MIMETYPES

    # Fallback: guess from extension
    guessed_type = mimetypes.guess_type(original_filename)[0]
    if guessed_type and guessed_type in ALLOWED_MIMETYPES:
        return True

    return False


def scan_file_for_viruses(file_path: str) -> bool:
    """Placeholder virus scanner. Integrate with a real scanner in production.

    For now perform basic sanity checks; return False if file is suspicious.
    """
    try:
        if not os.path.exists(file_path) or not os.access(file_path, os.R_OK):
            return False
        size = os.path.getsize(file_path)
        if size == 0 or size > MAX_FILE_SIZE:
            return False
        # Could add heuristic checks here (scanning for ZIP/PDF anomalies etc.)
        return True
    except Exception:
        return False


def save_uploaded_file(file, applicant_id):
    if not file or not file.filename:
        return None, 'No file provided.'

    original_filename = secure_filename(file.filename)
    if not allowed_file(original_filename):
        return None, 'Unsupported file type. Please upload a PDF, Word, text, or other document file.'

    # Read stream size safely
    file.stream.seek(0, os.SEEK_END)
    file_size = file.stream.tell()
    file.stream.seek(0)

    if file_size > MAX_FILE_SIZE:
        return None, 'File exceeds the 5MB size limit.'

    if file_size == 0:
        return None, 'File is empty. Please upload a valid file.'

    file_ext = original_filename.rsplit('.', 1)[1].lower()
    unique_filename = f"{applicant_id}_{uuid.uuid4().hex}.{file_ext}"

    # Store uploads inside the Flask instance folder (not in the code/static tree)
    upload_folder = os.path.join(current_app.instance_path, current_app.config.get('UPLOAD_FOLDER', 'uploads/resumes'))
    os.makedirs(upload_folder, exist_ok=True)

    file_path = os.path.join(upload_folder, unique_filename)
    try:
        file.save(file_path)
    except Exception as e:
        return None, f'Failed to save file: {str(e)}'

    # Set restrictive file permissions (owner read/write only) where supported
    try:
        os.chmod(file_path, stat.S_IRUSR | stat.S_IWUSR)
    except Exception:
        # Not critical if chmod fails on some platforms (e.g., Windows ACLs)
        pass

    # Validate MIME type to prevent file spoofing
    if not validate_file_mimetype(file_path, original_filename):
        try:
            os.remove(file_path)
        except OSError:
            pass
        return None, 'File type validation failed. The file does not match its extension.'

    if not scan_file_for_viruses(file_path):
        try:
            os.remove(file_path)
        except OSError:
            pass
        return None, 'The uploaded file did not pass the security scan.'

    # Return path relative to instance path so storage is not directly inside app root
    relative_path = os.path.relpath(file_path, start=current_app.instance_path).replace('\\', '/')
    mimetype = file.mimetype or mimetypes.guess_type(original_filename)[0] or 'application/octet-stream'

    return (
        {
            'original_filename': original_filename,
            'stored_filename': unique_filename,
            'storage_path': relative_path,
            'file_size': file_size,
            'mime_type': mimetype,
        },
        None,
    )
