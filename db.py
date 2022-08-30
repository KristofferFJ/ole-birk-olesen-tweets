import sqlite3
from sqlite3 import Error, Connection

db_name = "twitter.db"


def open_connection() -> Connection:
    """ create a database connection to a SQLite database """
    try:
        return sqlite3.connect(db_name)
    except Error as e:
        print(e)


def get_latest_pagination_token() -> str:
    return open_connection().cursor().execute(
        "select PAGINATION_TOKEN from PAGINATION_TOKEN ORDER BY ROWID DESC LIMIT 1"
    ).fetchone()[0]
