"""Conexión a la base transaccional PostgreSQL."""

import time

import psycopg2

from app import config


def connect(retries: int = 30, delay: float = 2.0):
    """Conecta a PostgreSQL reintentando hasta que el contenedor esté sano."""
    last_error = None
    for _ in range(retries):
        try:
            return psycopg2.connect(
                host=config.DB_HOST,
                port=config.DB_PORT,
                dbname=config.DB_NAME,
                user=config.DB_USER,
                password=config.DB_PASSWORD,
            )
        except psycopg2.OperationalError as exc:
            last_error = exc
            time.sleep(delay)
    raise RuntimeError(f"No se pudo conectar a PostgreSQL: {last_error}")


def fetch_all(sql: str, params: tuple = (), retries: int = 30) -> list[dict]:
    """Ejecuta una SELECT y devuelve todas las filas como diccionarios."""
    conn = connect(retries=retries)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        conn.close()
