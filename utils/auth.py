import bcrypt
import logging
from flask import session
from utils.database import get_db

log = logging.getLogger(__name__)


def _update_last_timestamp(db, table, pk_column, pk_value, column):
    """Safely update last_login/last_logout columns if they exist."""
    if not db or pk_value is None:
        return

    # Whitelist allowed table and column names to prevent SQL injection
    ALLOWED_TABLES = {"users", "admins", "applicants"}
    ALLOWED_COLUMNS = {"last_login", "last_logout"}
    ALLOWED_PK_COLUMNS = {"user_id", "admin_id", "applicant_id"}

    # Validate inputs against whitelist
    if table not in ALLOWED_TABLES:
        print(f"⚠️ Invalid table name for timestamp update: {table}")
        return
    if column not in ALLOWED_COLUMNS:
        print(f"⚠️ Invalid column name for timestamp update: {column}")
        return
    if pk_column not in ALLOWED_PK_COLUMNS:
        print(f"⚠️ Invalid primary key column name: {pk_column}")
        return

    cursor = None
    try:
        cursor = db.cursor()
        # Use parameterized query with table name validation
        cursor.execute("SHOW COLUMNS FROM `{}` LIKE %s".format(table), (column,))
        if not cursor.fetchone():
            return
        cursor.execute(
            "UPDATE `{}` SET `{}` = NOW() WHERE `{}` = %s".format(table, column, pk_column),
            (pk_value,),
        )
        db.commit()
    except Exception as exc:
        try:
            db.rollback()
        except Exception:
            pass
        log.warning("⚠️ Unable to update %s.%s: %s", table, column, exc)
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass


def hash_password(password):
    """Hash a password using bcrypt. Raises ValueError for invalid input."""
    if not password:
        raise ValueError("Password must be a non-empty string")
    try:
        return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")
    except Exception as exc:
        log.exception("❌ Failed to hash password: %s", exc)
        raise


def check_password(hashed_password, user_password):
    try:
        return bcrypt.checkpw(user_password.encode("utf-8"), hashed_password.encode("utf-8"))
    except Exception:
        return False


def login_user(account_id, role, email, full_name="", auth_user_id=None):
    import uuid
    import json

    session["user_id"] = account_id
    session["auth_user_id"] = auth_user_id or account_id
    # Normalize role to lowercase for consistent comparisons across the app
    session["user_role"] = (role or "").lower()
    session["user_email"] = email
    session["user_name"] = full_name
    session["logged_in"] = True

    # Generate a unique session_id
    session_id = str(uuid.uuid4())
    session["auth_session_id"] = session_id

    db = None
    cursor = None
    try:
        db = get_db()
        if db:
            cursor = db.cursor()
            try:
                # Check which columns exist in auth_sessions table
                cursor.execute("SHOW COLUMNS FROM auth_sessions")
                columns_raw = cursor.fetchall()

                # Create session data
                session_data = json.dumps(
                    {
                        "user_id": session.get("auth_user_id"),
                        "role": role,
                        "email": email,
                    }
                )

                # Build INSERT statement - session_id is required (it's the PRIMARY KEY)
                fields = ["session_id", "user_id", "session_data", "expires_at"]
                values = ["%s", "%s", "%s", "DATE_ADD(NOW(), INTERVAL 24 HOUR)"]
                params = [session_id, session.get("auth_user_id"), session_data]

                sql = f"INSERT INTO auth_sessions ({', '.join(fields)}) VALUES ({', '.join(values)})"
                cursor.execute(sql, tuple(params))
                db.commit()
                _update_last_timestamp(db, "users", "user_id", session.get("auth_user_id"), "last_login")
                if role in {"admin", "hr"}:
                    _update_last_timestamp(db, "admins", "admin_id", account_id, "last_login")
                elif role == "applicant":
                    _update_last_timestamp(db, "applicants", "applicant_id", account_id, "last_login")
            except Exception as e:
                log.exception("Error logging session: %s", e)
                if db:
                    try:
                        db.rollback()
                    except Exception:
                        pass
            finally:
                if cursor:
                    try:
                        cursor.close()
                    except Exception:
                        pass
    except Exception as outer_e:
        log.exception("Error in login_user database connection: %s", outer_e)


def logout_user():
    auth_user_id = session.get("auth_user_id")
    account_id = session.get("user_id")
    role = session.get("user_role")

    db = get_db()
    if db and auth_user_id:
        cursor = db.cursor()
        try:
            auth_session_id = session.get("auth_session_id")
            if auth_session_id:
                cursor.execute("SHOW COLUMNS FROM auth_sessions")
                columns = {row[0] if isinstance(row, tuple) else row.get("Field") for row in (cursor.fetchall() or [])}
                fields = []
                if "is_active" in columns:
                    fields.append("is_active = 0")
                if "last_activity" in columns:
                    fields.append("last_activity = NOW()")
                if "logout_time" in columns:
                    fields.append("logout_time = NOW()")
                set_clause = ", ".join(fields) if fields else "logout_time = NOW()"
                cursor.execute(f"UPDATE auth_sessions SET {set_clause} WHERE session_id = %s", (auth_session_id,))
            else:
                cursor.execute("SHOW COLUMNS FROM auth_sessions")
                columns = {row[0] if isinstance(row, tuple) else row.get("Field") for row in (cursor.fetchall() or [])}
                fields = []
                if "is_active" in columns:
                    fields.append("is_active = 0")
                if "last_activity" in columns:
                    fields.append("last_activity = NOW()")
                if "logout_time" in columns:
                    fields.append("logout_time = NOW()")
                set_clause = ", ".join(fields) if fields else "logout_time = NOW()"
                cursor.execute(
                    f"UPDATE auth_sessions SET {set_clause} WHERE user_id = %s AND ({'is_active = 1' if 'is_active' in columns else '1=1'})",
                    (session["auth_user_id"],),
                )
            db.commit()
            _update_last_timestamp(db, "users", "user_id", auth_user_id, "last_logout")
            if role in {"admin", "hr"}:
                _update_last_timestamp(db, "admins", "admin_id", account_id, "last_logout")
            elif role == "applicant":
                _update_last_timestamp(db, "applicants", "applicant_id", account_id, "last_logout")
        except Exception as e:
            print(f"Error updating logout time: {e}")
            db.rollback()
        finally:
            cursor.close()

    session.pop("auth_session_id", None)
    session.clear()


def get_current_user():
    if not session.get("logged_in"):
        return None

    auth_user_id = session.get("auth_user_id")
    if not auth_user_id:
        return None

    # Try to get database connection, but don't block if it fails
    try:
        db = get_db()
        if not db:
            # Return session-based data if database unavailable
            return {
                "id": session.get("user_id"),
                "role": session.get("user_role"),
                "email": session.get("user_email"),
                "name": session.get("user_name", "User"),
                "branch_id": session.get("branch_id"),
                "branch_name": session.get("branch_name"),
            }
    except Exception:
        # Database connection failed, return session data
        return {
            "id": session.get("user_id"),
            "role": session.get("user_role"),
            "email": session.get("user_email"),
            "name": session.get("user_name", "User"),
            "branch_id": session.get("branch_id"),
            "branch_name": session.get("branch_name"),
        }

    cursor = None
    try:
        cursor = db.cursor(dictionary=True)
        cursor.execute(
            """
            SELECT user_id, email, user_type, is_active
            FROM users
            WHERE user_id = %s
            LIMIT 1
            """,
            (auth_user_id,),
        )
        user_record = cursor.fetchone()

        if not user_record or not user_record.get("is_active"):
            logout_user()
            return None

        user_type = user_record["user_type"]
        role = session.get("user_role")
        account_id = session.get("user_id")
        display_name = session.get("user_name", "User")
        branch_id = None
        branch_name = None

        if user_type in {"super_admin", "hr"}:
            cursor.execute(
                """
                SELECT a.admin_id,
                       a.full_name,
                       a.branch_id,
                       b.branch_name
                FROM admins a
                LEFT JOIN branches b ON a.branch_id = b.branch_id
                WHERE a.user_id = %s
                LIMIT 1
                """,
                (auth_user_id,),
            )
            admin_record = cursor.fetchone()
            if admin_record:
                account_id = admin_record["admin_id"]
                display_name = admin_record.get("full_name") or display_name
                # Get branch_id and branch_name from admins table
                branch_id = admin_record.get("branch_id")
                branch_name = admin_record.get("branch_name")
                role = "admin" if user_type == "super_admin" else "hr"
            else:
                role = "admin" if user_type == "super_admin" else "hr"
                branch_id = None
                branch_name = None
        elif user_type == "applicant":
            cursor.execute(
                """
                SELECT applicant_id, full_name
                FROM applicants
                WHERE user_id = %s
                LIMIT 1
                """,
                (auth_user_id,),
            )
            applicant_record = cursor.fetchone()
            if applicant_record:
                account_id = applicant_record["applicant_id"]
                display_name = applicant_record.get("full_name") or display_name
                role = "applicant"
            else:
                role = "applicant"
        else:
            role = "applicant"

        # Sync session with canonical values (normalize to lowercase)
        session["user_role"] = (role or "").lower()
        session["user_id"] = account_id
        session["user_name"] = display_name
        session["user_email"] = user_record["email"]

        if branch_id:
            session["branch_id"] = branch_id
            session["branch_name"] = branch_name
        else:
            session.pop("branch_id", None)
            session.pop("branch_name", None)

        return {
            "id": account_id,
            "role": role,
            "email": user_record["email"],
            "name": display_name,
            "branch_id": branch_id,
            "branch_name": branch_name,
            "user_type": user_type,
        }
    except Exception as exc:
        print(f"⚠️ current_user lookup failed: {exc}")
        return {
            "id": session.get("user_id"),
            "role": session.get("user_role"),
            "email": session.get("user_email"),
            "name": session.get("user_name", "User"),
            "branch_id": session.get("branch_id"),
            "branch_name": session.get("branch_name"),
        }
    finally:
        if cursor:
            try:
                cursor.close()
            except Exception:
                pass


def is_logged_in():
    return "logged_in" in session and session["logged_in"]
