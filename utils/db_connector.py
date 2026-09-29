"""
Script Name: db_connector.py
Author: Yurani Duque
Fecha: 2026-09-29
Fase ETL: Soporte (utilizado por todas las fases)
Descripcion: crea la conexion hacia la base de datos Postgres a partir de variables
    de entorno, sin exponer credenciales en el codigo fuente. Incluye una carga
    masiva con COPY para tablas grandes.
Entradas: variables de entorno DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
Salidas: engine de sqlalchemy listo para usar
Notas: requiere un archivo .env en la raiz del proyecto (ver .env.example)
"""

import io
import os

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

load_dotenv()


def get_engine():
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT")
    name = os.getenv("DB_NAME")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")

    if not all([host, port, name, user, password]):
        raise RuntimeError("faltan variables de entorno de conexion, revisa el .env")

    url = URL.create("postgresql+psycopg2", user, password, host, int(port), name)
    return create_engine(url, pool_pre_ping=True)


def copy_dataframe(conn, df: pd.DataFrame, table: str, schema: str | None = None) -> None:
    """Carga un dataframe con COPY usando una conexion de sqlalchemy abierta."""
    target = f'"{schema}"."{table}"' if schema else f'"{table}"'
    columns = ", ".join(f'"{c}"' for c in df.columns)
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False, na_rep="")
    buffer.seek(0)

    raw = conn.connection
    with raw.cursor() as cur:
        cur.copy_expert(f"COPY {target} ({columns}) FROM STDIN WITH (FORMAT csv, NULL '')", buffer)
