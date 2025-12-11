import os
from datetime import timedelta

class Config:
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'whitehat88-recruitment-secret-key-2025'
    
    # MySQL Configuration for XAMPP - EMPTY PASSWORD
    MYSQL_HOST = os.environ.get('MYSQL_HOST') or 'localhost'
    MYSQL_USER = os.environ.get('MYSQL_USER') or 'root'
    MYSQL_PASSWORD = os.environ.get('MYSQL_PASSWORD') or ''  # EMPTY for XAMPP
    MYSQL_DB = os.environ.get('MYSQL_DB') or 'recruitment_system'
    
    PERMANENT_SESSION_LIFETIME = timedelta(days=1)
    UPLOAD_FOLDER = 'uploads/resumes'  # Private folder, not in static
    MAX_CONTENT_LENGTH = 10 * 1024 * 1024
    # Only allow PDF files for resumes
    ALLOWED_EXTENSIONS = {'pdf'}

    # SMTP configuration (defaults set for Gmail App Password usage)
    SMTP_SERVER = os.environ.get('SMTP_SERVER', 'smtp.gmail.com')
    SMTP_PORT = int(os.environ.get('SMTP_PORT', 587))
    # Gmail credentials - can be overridden via environment variables
    SMTP_USERNAME = os.environ.get('SMTP_USERNAME', 'bogieabacial@gmail.com')
    SMTP_PASSWORD = os.environ.get('SMTP_PASSWORD', 'gzau bqed hlmm poke').replace(' ', '')  # Gmail App Password (remove spaces)
    SMTP_USE_TLS = os.environ.get('SMTP_USE_TLS', 'true').lower() in ('1', 'true', 'yes')
    SMTP_FROM_ADDRESS = os.environ.get('SMTP_FROM_ADDRESS', 'bogieabacial@gmail.com') or SMTP_USERNAME
    SMTP_FROM_NAME = os.environ.get('SMTP_FROM_NAME', 'J&T Express Recruitment')