import sys
from contextlib import contextmanager
from psycopg import connect
from config.settings import settings

def get_connection():
    return connect(
        host=settings.DB_HOST,
        port=settings.DB_PORT,
        dbname=settings.DB_NAME,
        user=settings.DB_USER,
        password=settings.DB_PASSWORD,
    )


@contextmanager
def safe_connection():
    if settings.ENVIRONMENT.upper() == "PROD":
        confirm = input("Running against PROD. Type 'yes' to continue: ")
        if confirm.lower() != "yes":
            print("Aborted.")
            sys.exit(1)

    conn = get_connection()
    try:
        yield conn
    finally:
        conn.close()
        conn.close()
