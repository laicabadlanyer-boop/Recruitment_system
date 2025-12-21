import sqlite3
import tempfile
import pytest

import manage


class FakeCursor:
    def __init__(self, conn):
        self._cur = conn.cursor()
        self._last = None

    def execute(self, sql, params=()):
        # Translate MySQL-style %s placeholders to SQLite ? placeholders for tests
        try:
            safe_sql = sql.replace("%s", "?")
        except Exception:
            safe_sql = sql
        self._cur.execute(safe_sql, params)
        self._last = self._cur

    def fetchone(self):
        row = self._last.fetchone()
        if row is None:
            return None
        cols = [d[0] for d in self._last.description]
        return {cols[i]: row[i] for i in range(len(cols))}

    def fetchall(self):
        rows = self._last.fetchall()
        cols = [d[0] for d in self._last.description]
        return [{cols[i]: r[i] for i in range(len(cols))} for r in rows]

    @property
    def lastrowid(self):
        return self._cur.lastrowid

    def close(self):
        try:
            self._cur.close()
        except Exception:
            pass


class FakeDB:
    def __init__(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.isolation_level = None
        self.setup_schema()

    def setup_schema(self):
        c = self.conn.cursor()
        c.execute(
            """
            CREATE TABLE users (
                user_id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT UNIQUE,
                password_hash TEXT,
                user_type TEXT,
                is_active INTEGER,
                email_verified INTEGER
            )
        """
        )
        c.execute(
            """
            CREATE TABLE admins (
                admin_id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                full_name TEXT,
                email TEXT,
                password_hash TEXT
            )
        """
        )
        self.conn.commit()

    def cursor(self, dictionary=True):
        return FakeCursor(self.conn)

    def commit(self):
        try:
            self.conn.commit()
        except Exception:
            pass

    def rollback(self):
        try:
            self.conn.rollback()
        except Exception:
            pass

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass


@pytest.fixture
def fake_db(monkeypatch):
    db = FakeDB()
    import utils.database as dbmod
    import manage as mg

    monkeypatch.setattr(dbmod, "get_db", lambda: db)
    # Also patch the get_db used directly in manage module
    monkeypatch.setattr(mg, "get_db", lambda: db)
    # Avoid schema compatibility checks that expect Flask app context
    monkeypatch.setattr(mg, "ensure_schema_compatibility", lambda: True)
    return db


def test_create_admin_creates_user_and_admin(fake_db):
    email = "int-admin@example.com"
    rc = manage.create_admin_account(email, "TestPass123!", role="admin", force=False)
    assert rc is True

    cur = fake_db.conn.cursor()
    cur.execute("SELECT email, user_type, email_verified FROM users WHERE email = ?", (email,))
    row = cur.fetchone()
    assert row is not None
    assert row[0] == email
    assert row[1] == "super_admin"
    assert row[2] == 1

    cur.execute("SELECT email FROM admins WHERE email = ?", (email,))
    admin_row = cur.fetchone()
    assert admin_row is not None


def test_create_admin_with_generated_password_and_otp_file(tmp_path, monkeypatch, fake_db):
    email = "otp-admin@example.com"
    otp_file = tmp_path / "otp.txt"
    # Call CLI create path with generate flag
    args = ["create_admin", "--email", email, "--generate-password", "--otp-file", str(otp_file)]
    # Simulate running main
    rc = manage.main(args)
    assert rc == 0
    # Ensure otp file exists and contains a password
    assert otp_file.exists()
    content = otp_file.read_text().strip()
    assert len(content) >= 8

    # Verify user was created
    cur = fake_db.conn.cursor()
    cur.execute("SELECT email FROM users WHERE email = ?", (email,))
    row = cur.fetchone()
    assert row is not None
