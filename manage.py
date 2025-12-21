#!/usr/bin/env python3
"""Simple management CLI for administrative tasks.

Usage:
  python manage.py create_admin --email admin@example.com --password secret
  python manage.py create_admin --email admin@example.com    (will prompt for password)
"""
import argparse
import getpass
import sys
import re
import traceback

from utils.auth import hash_password
from utils.database import get_db
from utils.mailer import send_email

# Import ensure_schema_compatibility from app if available (best-effort)
try:
    from app import ensure_schema_compatibility
except Exception:

    def ensure_schema_compatibility():
        return True


def create_parser():
    parser = argparse.ArgumentParser(prog="manage.py", description="Management CLI for recruitment system")
    subparsers = parser.add_subparsers(dest="command")

    # create_admin subcommand
    create_admin = subparsers.add_parser("create_admin", help="Create or update an administrator account")
    create_admin.add_argument("--email", required=True, help="Email address for the admin account")
    create_admin.add_argument(
        "--password",
        required=False,
        help="Password for the admin account. If omitted, you will be prompted unless --generate-password is used",
    )
    create_admin.add_argument(
        "--generate-password",
        action="store_true",
        help="Generate a secure one-time password instead of prompting or using --password",
    )
    create_admin.add_argument(
        "--otp-file",
        required=False,
        help="If provided with --generate-password, write the generated password to this file (securely)",
    )
    create_admin.add_argument("--role", choices=["admin", "hr"], default="admin", help="Account role (admin or hr)")
    create_admin.add_argument("--force", action="store_true", help="Force update if account already exists")

    _list_admins = subparsers.add_parser("list_admins", help="List admin accounts")

    rotate = subparsers.add_parser("rotate_admin_password", help="Rotate/update an admin password")
    rotate.add_argument("--email", required=True, help="Email address for the admin account")
    rotate.add_argument(
        "--password", required=False, help="Password to set (if omitted, prompt or use --generate-password)"
    )
    rotate.add_argument("--generate-password", action="store_true", help="Generate a secure one-time password")
    rotate.add_argument(
        "--otp-file",
        required=False,
        help="If provided with --generate-password, write the generated password to this file (securely)",
    )
    rotate.add_argument("--force", action="store_true", help="Force update even if checks fail")

    rotate_all = subparsers.add_parser("rotate_all_admins", help="Rotate passwords for all admin accounts")
    rotate_all.add_argument(
        "--generate-password", action="store_true", help="Generate secure one-time passwords for each admin"
    )
    rotate_all.add_argument(
        "--otp-dir",
        required=False,
        help="If provided with --generate-password, write one-time passwords into this directory (files named by email)",
    )
    rotate_all.add_argument(
        "--email-otp",
        action="store_true",
        help="If provided with --generate-password, send one-time passwords to admin emails via SMTP",
    )
    rotate_all.add_argument("--force", action="store_true", help="Force update passwords even if checks fail")

    return parser


def is_valid_email(value):
    # Simple validation
    return bool(re.match(r"[^@\s]+@[^@\s]+\.[^@\s]+", value))


def prompt_password():
    pw = getpass.getpass("Enter password: ")
    pw2 = getpass.getpass("Confirm password: ")
    if pw != pw2:
        print("Passwords do not match. Aborting.")
        return None
    if not pw:
        print("Empty password not allowed. Aborting.")
        return None
    return pw


def generate_password(length=24):
    import secrets

    # Use URL-safe token and trim to desired length
    return secrets.token_urlsafe(32)[:length]


def write_otp_to_file(path, password):
    import os

    try:
        # Ensure parent directory exists
        os.makedirs(os.path.dirname(path), exist_ok=True)
        # Write file with restricted permissions where possible
        flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
        try:
            fd = os.open(path, flags, 0o600)
            with os.fdopen(fd, "w") as f:
                f.write(password + "\n")
        except Exception:
            # Fallback: normal write
            with open(path, "w") as f:
                f.write(password + "\n")
        # Make file read-only where possible
        try:
            os.chmod(path, 0o600)
        except Exception:
            pass
        print(f"✅ Generated password written to {path} (set permissions to owner-read/write where supported)")
        return True
    except Exception as e:
        print(f"❌ Failed to write OTP to file {path}: {e}")
        return False


def create_admin_account(email, password, role="admin", force=False):
    if not is_valid_email(email):
        print("Invalid email address")
        return False

    db = get_db()
    if not db:
        print("❌ Database connection failed. Ensure database is available.")
        return False

    ensure_schema_compatibility()

    cursor = db.cursor(dictionary=True)
    try:
        cursor.execute("SELECT user_id FROM users WHERE email = %s LIMIT 1", (email,))
        existing = cursor.fetchone()

        if existing:
            user_id = existing["user_id"]
            if not force and not password:
                print("User already exists. Use --force or provide --password to update password.")
                return False

            # Update existing user
            updates = []
            params = []
            updates.append("user_type = 'super_admin'" if role == "admin" else "user_type = 'hr'")
            updates.append("email_verified = 1")
            if password:
                try:
                    password_hash = hash_password(password)
                except Exception as e:
                    print(f"❌ Failed to hash password: {e}")
                    return False
                updates.append("password_hash = %s")
                params.append(password_hash)

            if updates:
                sql = "UPDATE users SET " + ", ".join(updates) + " WHERE user_id = %s"
                params.append(user_id)
                cursor.execute(sql, tuple(params))
                # Update admins table password_hash as well if present
                if password:
                    try:
                        cursor.execute(
                            "UPDATE admins SET password_hash = %s WHERE user_id = %s", (password_hash, user_id)
                        )
                    except Exception:
                        pass
                db.commit()
                print(f"✅ Updated existing user {email}")
            else:
                print("No changes to apply.")
        else:
            # Create new user
            try:
                password_hash = hash_password(password)
            except Exception as e:
                print(f"❌ Failed to hash password: {e}")
                return False

            cursor.execute(
                "INSERT INTO users (email, password_hash, user_type, is_active, email_verified) VALUES (%s, %s, %s, %s, 1)",
                (email, password_hash, "super_admin" if role == "admin" else "hr", True),
            )
            user_id = cursor.lastrowid
            # Insert admins table row
            cursor.execute(
                "INSERT INTO admins (user_id, full_name, email, password_hash) VALUES (%s, %s, %s, %s)",
                (user_id, "System Administrator" if role == "admin" else "HR User", email, password_hash),
            )
            db.commit()
            print(f"✅ Created {role} account: {email}")

        return True
    except Exception as exc:
        try:
            db.rollback()
        except Exception:
            pass
        print("❌ Failed to create/update admin:", exc)
        print(traceback.format_exc())
        return False
    finally:
        try:
            cursor.close()
        except Exception:
            pass


def main(argv=None):
    parser = create_parser()
    args = parser.parse_args(argv)

    if args.command == "create_admin":
        email = args.email
        pw = args.password

        # Handle generated password flow
        if args.generate_password:
            pw = generate_password()
            if args.otp_file:
                ok = write_otp_to_file(args.otp_file, pw)
                if not ok:
                    return 3
            else:
                print("✅ Generated one-time password (keep it safe):")
                print(pw)

        # If not generating and no password provided, prompt interactively
        if not args.generate_password and not pw:
            pw = prompt_password()
            if not pw:
                return 1

        return 0 if create_admin_account(email, pw, role=args.role, force=args.force) else 2

    if args.command == "list_admins":
        db = get_db()
        if not db:
            print("❌ Database connection failed.")
            return 2
        cursor = db.cursor()
        try:
            cursor.execute(
                "SELECT a.admin_id, a.email, u.user_type FROM admins a JOIN users u ON u.user_id = a.user_id"
            )
            rows = cursor.fetchall()
            if not rows:
                print("No admin accounts found.")
                return 0
            for r in rows:
                # r may be dict-like or tuple depending on DB wrapper
                if isinstance(r, dict):
                    print(f"- {r.get('email')} (admin_id={r.get('admin_id')}, role={r.get('user_type')})")
                else:
                    print(f"- {r[1]} (admin_id={r[0]}, role={r[2]})")
            return 0
        finally:
            try:
                cursor.close()
            except Exception:
                pass

    if args.command == "rotate_admin_password":
        email = args.email
        pw = args.password

        if args.generate_password:
            pw = generate_password()
            if args.otp_file:
                ok = write_otp_to_file(args.otp_file, pw)
                if not ok:
                    return 3
            else:
                print("✅ Generated one-time password (keep it safe):")
                print(pw)

        if not pw:
            pw = prompt_password()
            if not pw:
                return 1

        # Perform rotation
        db = get_db()
        if not db:
            print("❌ Database connection failed.")
            return 2
        cursor = db.cursor()
        try:
            cursor.execute("SELECT user_id FROM users WHERE email = %s LIMIT 1", (email,))
            user = cursor.fetchone()
            if not user:
                print("User not found.")
                return 2
            user_id = user["user_id"] if isinstance(user, dict) else user[0]
            try:
                password_hash = hash_password(pw)
            except Exception as e:
                print(f"❌ Failed to hash password: {e}")
                return 2
            cursor.execute("UPDATE users SET password_hash = %s WHERE user_id = %s", (password_hash, user_id))
            # Update admins table too if exists
            try:
                cursor.execute("UPDATE admins SET password_hash = %s WHERE user_id = %s", (password_hash, user_id))
            except Exception:
                pass
            db.commit()
            print("✅ Password rotated successfully.")
            return 0
        except Exception as exc:
            try:
                db.rollback()
            except Exception:
                pass
            print("❌ Failed to rotate password:", exc)
            return 2
        finally:
            try:
                cursor.close()
            except Exception:
                pass

    if args.command == "rotate_all_admins":
        # Rotate passwords for all admins
        db = get_db()
        if not db:
            print("❌ Database connection failed.")
            return 2

        otp_dir = args.otp_dir
        gen = args.generate_password
        email_otp = args.email_otp
        force = args.force

        cursor = db.cursor()
        try:
            cursor.execute("SELECT admin_id, user_id, email FROM admins")
            admins = cursor.fetchall() or []
            if not admins:
                print("No admins found to rotate.")
                return 0

            for row in admins:
                # row may be dict or tuple
                admin_id = row["admin_id"] if isinstance(row, dict) else row[0]
                user_id = row["user_id"] if isinstance(row, dict) else row[1]
                email = row["email"] if isinstance(row, dict) else row[2]

                # Determine password
                if gen:
                    pw = generate_password()
                else:
                    # If not generating, cannot batch prompt - skip unless force
                    if not force:
                        print(f"Skipping {email}: no generated password provided (use --generate-password or --force)")
                        continue
                    pw = None

                if pw:
                    try:
                        password_hash = hash_password(pw)
                    except Exception as e:
                        print(f"Failed to hash password for {email}: {e}")
                        continue
                    try:
                        cursor.execute(
                            "UPDATE users SET password_hash = %s WHERE user_id = %s", (password_hash, user_id)
                        )
                        try:
                            cursor.execute(
                                "UPDATE admins SET password_hash = %s WHERE user_id = %s", (password_hash, user_id)
                            )
                        except Exception:
                            pass
                        db.commit()
                    except Exception as exc:
                        print(f"Failed to update password for {email}: {exc}")
                        try:
                            db.rollback()
                        except Exception:
                            pass
                        continue

                    # OTP file
                    if otp_dir:
                        import os

                        safe_name = email.replace("@", "_at_").replace(".", "_")
                        path = os.path.join(otp_dir, f"{safe_name}.txt")
                        write_otp_to_file(path, pw)

                    # Email OTP
                    if email_otp and email:
                        subject = "Your account password has been rotated"
                        body = f"Your account password has been rotated. Use the following one-time password to log in: {pw}\nPlease change it on first login."
                        try:
                            send_email(email, subject, body)
                            print(f"✅ Sent OTP email to {email}")
                        except Exception as e:
                            print(f"⚠️ Failed to send OTP to {email}: {e}")

            print("✅ Completed rotation for admins")
            return 0
        finally:
            try:
                cursor.close()
            except Exception:
                pass

    parser.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())
