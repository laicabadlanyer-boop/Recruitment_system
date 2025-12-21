Run the app locally

1) Activate virtualenv and install deps

```powershell
& "C:/xampp/htdocs/Recruitment System/.venv/Scripts/Activate.ps1"
pip install -r requirements.txt
```

2) Copy `.env.example` to `.env` and edit DB credentials if needed

```powershell
copy .env.example .env
notepad .env
```

3) (Optional) Initialize DB (interactive)

```powershell
python init_database.py
```

4) Quick environment checks

```powershell
python check_env.py
```

5) Run the app using the helper runner

```powershell
$env:FLASK_DEBUG='True'
python run_local.py
```

6) Creating a default admin (production / provisioning)

- Recommended: set environment vars in your provisioning scripts or CI:

```powershell
setx DEFAULT_ADMIN_EMAIL "admin@example.com"
setx DEFAULT_ADMIN_PASSWORD "YourStrongPasswordHere"
```

- Alternatively, use the management CLI to create an admin explicitly (interactive password prompt):

```powershell
python manage.py create_admin --email admin@example.com
```

- For scripting (non-interactive), provide `--password` (avoid storing in plain text):

```powershell
python manage.py create_admin --email admin@example.com --password "S3cureP@ss!"
```

- For automated provisioning, you can let the CLI generate a secure one-time password and write it to a file for secure retrieval (recommended for provisioning scripts):

```powershell
python manage.py create_admin --email admin@example.com --generate-password --otp-file "instance/default_admin_20251220.txt"
```

- To list admins:

```powershell
python manage.py list_admins
```

- To rotate an admin password interactively or via generation:

```powershell
python manage.py rotate_admin_password --email admin@example.com
python manage.py rotate_admin_password --email admin@example.com --generate-password --otp-file "instance/rotated_admin_20251220.txt"
```

- To rotate passwords for all admins (generate OTPs for each):

```powershell
# Write OTPs to files in instance/
python manage.py rotate_all_admins --generate-password --otp-dir "instance/admin_otps"

# Generate OTPs and email them to admin addresses (requires SMTP configured in config.py)
python manage.py rotate_all_admins --generate-password --email-otp
```

Note: Ensure your SMTP environment variables are configured in `config.py` or via environment for `utils.mailer.send_email`. OTP files are created with restricted perms where supported; prefer a secrets manager/products for production workflows.
Note: For production, we recommend using a secrets manager rather than passing passwords on the command line or storing them in plain files. The OTP file is created with restricted permissions where supported.

If the process still exits immediately, capture logs to a file and paste them here:

```powershell
$env:FLASK_DEBUG='True'
& ".venv/Scripts/python.exe" run_local.py *> app_run.log 2>&1
notepad app_run.log
```
