import mysql.connector
from mysql.connector import Error
from flask import g, current_app

def get_db():
    if 'db' not in g:
        try:
            g.db = mysql.connector.connect(
                host=current_app.config['MYSQL_HOST'],
                user=current_app.config['MYSQL_USER'],
                password=current_app.config['MYSQL_PASSWORD'],
                database=current_app.config['MYSQL_DB'],
                autocommit=False,
                connect_timeout=10
            )
        except Error as e:
            return None
    return g.db

def close_db(e=None):
    db = g.pop('db', None)
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