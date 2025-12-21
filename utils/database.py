import mysql.connector
from mysql.connector import Error
from flask import g, current_app


def get_db():
    if "db" not in g:
        try:
            # Use connection pooling and faster timeout
            g.db = mysql.connector.connect(
                host=current_app.config["MYSQL_HOST"],
                user=current_app.config["MYSQL_USER"],
                password=current_app.config["MYSQL_PASSWORD"],
                database=current_app.config["MYSQL_DB"],
                autocommit=False,
                connect_timeout=3,  # Fast timeout - fail quickly if MySQL not running
                raise_on_warnings=False,
                use_unicode=True,
                charset="utf8mb4",
                connection_timeout=3,
                buffered=True,
            )
            # Test connection immediately
            g.db.ping(reconnect=False, attempts=1, delay=0)
        except Error as e:
            print(f"⚠️ Database connection error: {e}")
            return None
        except Exception as e:
            print(f"⚠️ Unexpected database error: {e}")
            return None
    return g.db


def close_db(e=None):
    db = g.pop("db", None)
    if db is not None:
        db.close()


def execute_query(query, params=None, fetch_one=False, fetch_all=False):
    db = get_db()
    if not db:
        return None

    cursor = db.cursor(dictionary=True)
    try:
        cursor.execute(query, params or ())

        if fetch_one:
            result = cursor.fetchone()
        elif fetch_all:
            result = cursor.fetchall()
        else:
            db.commit()
            result = cursor.lastrowid

        return result
    except Error as e:
        print(f"Query error: {e}")
        db.rollback()
        return None
    finally:
        cursor.close()
