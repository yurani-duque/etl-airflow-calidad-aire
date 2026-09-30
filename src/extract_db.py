"""
Script Name: extract_db.py
Author: Valentina Velasco
Fecha: 2026-09-30
Fase ETL: Extraccion
Descripcion: consulta con SQL la tabla de emisiones (source.owid_co2) en la base de
    datos fuente y la deja en staging.
Entradas: tabla source.owid_co2
Salidas: staging/<run_id>/owid.parquet
Notas: lee por bloques para no cargar toda la tabla de una sola vez
"""

from pathlib import Path

import pandas as pd
from sqlalchemy import text

from utils.config_loader import load_config, staging_dir
from utils.db_connector import get_engine
from utils.logger import get_logger

logger = get_logger("extract_db")

COLUMNAS = [
    "country", "iso_code", "year", "population", "gdp", "co2", "co2_per_capita",
    "total_ghg", "energy_per_capita",
]


def extraer_owid(run_id: str) -> str:
    cfg = load_config()
    schema = cfg["pipeline"]["source_schema"]
    table = cfg["sources"]["owid_table"]
    consulta = text(f'select {", ".join(COLUMNAS)} from "{schema}"."{table}"')

    bloques = []
    with get_engine().connect() as conn:
        for bloque in pd.read_sql(consulta, conn, chunksize=20000):
            bloques.append(bloque)

    df = pd.concat(bloques, ignore_index=True)
    if df.empty:
        raise ValueError(f"{schema}.{table} esta vacia, ejecute seed_source_db primero")

    salida = Path(staging_dir(run_id)) / "owid.parquet"
    df.to_parquet(salida, index=False)
    logger.info(f"filas extraidas de la base de datos: {len(df)}")
    return str(salida)
