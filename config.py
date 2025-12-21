import os
from datetime import timedelta


class Config:
    # SECRET_KEY should be set via environment variable in production.
    # A benign development default is kept here for local/dev convenience.
    SECRET_KEY = os.environ.get("SECRET_KEY") or os.environ.get("FLASK_SECRET_KEY") or "dev-secret-key-change-me"

    # MySQL configuration (use environment variables in production)
    MYSQL_HOST = os.environ.get("MYSQL_HOST") or "localhost"
    MYSQL_USER = os.environ.get("MYSQL_USER") or "root"
    MYSQL_PASSWORD = os.environ.get("MYSQL_PASSWORD") or ""  # EMPTY is common for local XAMPP
    MYSQL_DB = os.environ.get("MYSQL_DB") or "recruitment_system"

    PERMANENT_SESSION_LIFETIME = timedelta(days=1)
    # Use the instance directory for uploaded resumes by default (safer)
    UPLOAD_FOLDER = os.environ.get("UPLOAD_FOLDER") or os.path.join(os.getcwd(), "instance", "uploads", "resumes")
    MAX_CONTENT_LENGTH = int(os.environ.get("MAX_CONTENT_LENGTH") or 10 * 1024 * 1024)
    ALLOWED_EXTENSIONS = set(x.strip().lower() for x in os.environ.get("ALLOWED_EXTENSIONS", "pdf,doc,docx,jpg,png").split(","))

    # SMTP configuration (defaults set for Gmail App Password usage)
    SMTP_SERVER = os.environ.get("SMTP_SERVER", "smtp.gmail.com")
    SMTP_PORT = int(os.environ.get("SMTP_PORT", 587))
    # Gmail credentials - can be overridden via environment variables
    SMTP_USERNAME = os.environ.get("SMTP_USERNAME", "bogieabacial@gmail.com")
    SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD", "gzau bqed hlmm poke").replace(
        " ", ""
    )  # Gmail App Password (remove spaces)
    SMTP_USE_TLS = os.environ.get("SMTP_USE_TLS", "true").lower() in ("1", "true", "yes")
    SMTP_FROM_ADDRESS = os.environ.get("SMTP_FROM_ADDRESS", "bogieabacial@gmail.com") or SMTP_USERNAME
    SMTP_FROM_NAME = os.environ.get("SMTP_FROM_NAME", "J&T Express Recruitment")
