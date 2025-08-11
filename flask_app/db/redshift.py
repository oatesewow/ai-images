import os
import psycopg2
from contextlib import contextmanager
from ..config import AppConfig

CFG = AppConfig()

@contextmanager
def redshift_conn():
    conn = psycopg2.connect(host=CFG.redshift.host, port=CFG.redshift.port, dbname=CFG.redshift.dbname, user=CFG.redshift.user, password=CFG.redshift.password)
    conn.autocommit = False
    try:
        yield conn
    finally:
        conn.close()
