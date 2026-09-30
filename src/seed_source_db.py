"""
Script Name: seed_source_db.py
Author: Valentina Velasco
Fecha: 2026-09-30
Fase ETL: Preparacion de la fuente (base de datos)
Descripcion: guarda el dataset de emisiones de CO2 de Our World in Data en la base de
    datos fuente (esquema source), de donde luego lo extrae Airflow.
Entradas: data/raw/owid-co2-data.csv.gz (o la URL oficial con --url)
Salidas: tabla source.owid_co2
Notas: es idempotente; si la tabla ya tiene datos no la vuelve a cargar salvo con --force
"""

import argparse

import pandas as pd
from sqlalchemy import text

from utils.config_loader import load_config, resolve
from utils.db_connector import copy_dataframe, get_engine
from utils.logger import get_logger

logger = get_logger("seed_source_db")


def leer_owid(desde_url: bool) -> pd.DataFrame:
    fuentes = load_config()["sources"]
    if desde_url:
        logger.info(f"descargando {fuentes['owid_url']}")
        return pd.read_csv(fuentes["owid_url"])
    return pd.read_csv(resolve(fuentes["owid_csv"]))


def cargar_owid(force: bool = False, desde_url: bool = False) -> int:
    cfg = load_config()
    schema = cfg["pipeline"]["source_schema"]
    table = cfg["sources"]["owid_table"]
    engine = get_engine()

    with engine.begin() as conn:
        conn.execute(text(f'create schema if not exists "{schema}"'))
        existe = conn.execute(text("select to_regclass(:t)"), {"t": f"{schema}.{table}"}).scalar()
        if existe and not force:
            filas = conn.execute(text(f'select count(*) from "{schema}"."{table}"')).scalar()
            if filas:
                logger.info(f"{schema}.{table} ya tiene {filas} filas, no se recarga")
                return filas

    df = leer_owid(desde_url)
    df.head(0).to_sql(table, engine, schema=schema, if_exists="replace", index=False)
    with engine.begin() as conn:
        copy_dataframe(conn, df, table, schema)
    logger.info(f"{schema}.{table} cargada con {len(df)} filas")
    return len(df)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Carga el dataset OWID en la base de datos fuente")
    parser.add_argument("--force", action="store_true", help="recargar aunque ya existan datos")
    parser.add_argument("--url", action="store_true", help="descargar desde la URL oficial")
    args = parser.parse_args()
    cargar_owid(force=args.force, desde_url=args.url)
