import smtplib
import ssl
import textwrap
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formataddr

from config import Config


def _log_email(recipient: str, subject: str, body: str) -> None:
    """Fallback debug logger when SMTP is not configured."""
    message = textwrap.dedent(
        f"""
        ================= EMAIL DEBUG OUTPUT =================
        To: {recipient}
        Subject: {subject}

        {body.strip()}
        ======================================================
        """
    ).strip()
    print(message)


def send_email(recipient: str, subject: str, body: str, html_body: str = None) -> None:
    """Send an email using the configured SMTP settings (Gmail by default).
    
    Args:
        recipient: Email address of the recipient
        subject: Email subject line
        body: Plain text email body
        html_body: Optional HTML email body (if provided, email will be sent as multipart)
    """
    smtp_user = Config.SMTP_USERNAME
    smtp_pass = Config.SMTP_PASSWORD
    smtp_server = Config.SMTP_SERVER
    smtp_port = Config.SMTP_PORT
    from_address = Config.SMTP_FROM_ADDRESS or smtp_user

    if not (smtp_user and smtp_pass and from_address):
        _log_email(recipient, subject, body)
        return

    message = MIMEMultipart()
    from_name = Config.SMTP_FROM_NAME or 'HR Manager'
    message['From'] = formataddr((from_name, from_address))
    message['To'] = recipient
    message['Subject'] = subject
    
    # Add plain text body
    message.attach(MIMEText(body, 'plain', 'utf-8'))
    
    # Add HTML body if provided
    if html_body:
        message.attach(MIMEText(html_body, 'html', 'utf-8'))

    try:
        context = ssl.create_default_context()
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            if Config.SMTP_USE_TLS:
                server.starttls(context=context)
            server.login(smtp_user, smtp_pass)
            server.send_message(message)
    except Exception as exc:
        print(f"⚠️ SMTP send failed: {exc}. Falling back to console log.")
        _log_email(recipient, subject, body)
