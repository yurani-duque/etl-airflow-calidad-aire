"""
Script Name: load_dw.py
Author: Yurani Duque
Fecha: 2026-10-03
Fase ETL: Carga
Descripcion: carga el dataset validado en el Data Warehouse (esquema dw): primero las
    dimensiones y despues la tabla de hechos, todo en una sola transaccion.
Entradas: staging/<run_id>/enriched.parquet, country_indicators.parquet, dwh_version/001_create_dw_schema.sql
Salidas: tablas dw.dim_*, dw.fact_air_quality y dw.etl_run_log
Notas: las dimensiones se actualizan (upsert); los hechos del CSV no se reescriben y los de la API se actualizan
"""

from pathlib import Path

import pandas as pd
from sqlalchemy import text

from utils.config_loader import load_config, resolve, staging_dir
from utils.db_connector import copy_dataframe, get_engine
from utils.logger import get_logger

logger = get_logger("load_dw")

SOURCE_IDS = {"kaggle_csv": 1, "open_meteo_api": 2}
INDICADORES = ["population", "gdp", "co2_per_capita", "total_ghg", "energy_per_capita"]
MEDIDAS = [
    "temperature_celsius", "humidity", "wind_kph", "pressure_mb", "precip_mm", "cloud", "uv_index",
    "pm2_5", "pm10", "co", "no2", "so2", "ozone", "us_epa_index", "aqi_us", "pm25_who_ratio", "pm25_exceeds_who",
]


def ejecutar_ddl(conn) -> None:
    ddl = resolve("dwh_version/001_create_dw_schema.sql").read_text(encoding="utf-8")
    conn.execute(text(ddl))


def upsert(conn, df: pd.DataFrame, schema: str, table: str, conflicto: list[str], actualizar: bool) -> None:
    columnas = ", ".join(f'"{c}"' for c in df.columns)
    tmp = f"tmp_{table}"
    conn.execute(text(f'create temp table {tmp} on commit drop as select {columnas} from "{schema}"."{table}" where false'))
    copy_dataframe(conn, df, tmp)

    if actualizar:
        cambios = ", ".join(f'"{c}" = excluded."{c}"' for c in df.columns if c not in conflicto)
        accion = f"do update set {cambios}"
    else:
        accion = "do nothing"
    conn.execute(text(
        f'insert into "{schema}"."{table}" ({columnas}) select {columnas} from {tmp} '
        f'on conflict ({", ".join(conflicto)}) {accion}'
    ))
    conn.execute(text(f"drop table {tmp}"))


def preparar_dim_location(enriquecido: pd.DataFrame) -> pd.DataFrame:
    columnas = ["location_key", "iso3", "country", "location_name", "latitude", "longitude", "timezone"]
    return enriquecido[columnas].drop_duplicates("location_key").reset_index(drop=True)


def preparar_dim_date(enriquecido: pd.DataFrame) -> pd.DataFrame:
    fechas = pd.DataFrame({"full_date": pd.date_range(enriquecido["date"].min(), enriquecido["date"].max())})
    fechas["date_id"] = fechas["full_date"].dt.strftime("%Y%m%d").astype(int)
    fechas["year"] = fechas["full_date"].dt.year
    fechas["quarter"] = fechas["full_date"].dt.quarter
    fechas["month"] = fechas["full_date"].dt.month
    fechas["month_name"] = fechas["full_date"].dt.month_name()
    fechas["day"] = fechas["full_date"].dt.day
    fechas["day_of_week"] = fechas["full_date"].dt.day_name()
    fechas["is_weekend"] = fechas["full_date"].dt.dayofweek >= 5
    return fechas[["date_id", "full_date", "year", "quarter", "month", "month_name", "day", "day_of_week", "is_weekend"]]


def preparar_dim_indicadores(indicadores: pd.DataFrame) -> pd.DataFrame:
    df = indicadores.rename(columns={"country_owid": "country"}).copy()
    df["population"] = df["population"].round().astype("Int64")
    columnas = ["iso3", "country"]
    for indicador in INDICADORES:
        columnas += [indicador, f"{indicador}_year"]
    return df[columnas]


def preparar_hechos(enriquecido: pd.DataFrame, ids_ubicacion: dict, ids_pais: dict) -> pd.DataFrame:
    hechos = pd.DataFrame({
        "location_id": enriquecido["location_key"].map(ids_ubicacion),
        "date_id": enriquecido["date"].dt.strftime("%Y%m%d").astype(int),
        "source_id": enriquecido["source"].map(SOURCE_IDS),
        "country_indicator_id": enriquecido["iso3"].map(ids_pais).astype("Int64"),
    })
    return pd.concat([hechos, enriquecido[MEDIDAS]], axis=1)


def cargar_dw(run_id: str) -> int:
    schema = load_config()["pipeline"]["dw_schema"]
    carpeta = Path(staging_dir(run_id))
    enriquecido = pd.read_parquet(carpeta / "enriched.parquet")
    indicadores = pd.read_parquet(carpeta / "country_indicators.parquet")

    with get_engine().begin() as conn:
        ejecutar_ddl(conn)
        upsert(conn, preparar_dim_location(enriquecido), schema, "dim_location", ["location_key"], True)
        upsert(conn, preparar_dim_date(enriquecido), schema, "dim_date", ["date_id"], False)
        upsert(conn, preparar_dim_indicadores(indicadores), schema, "dim_country_indicator", ["iso3"], True)

        ids_ubicacion = dict(conn.execute(text(f"select location_key, location_id from {schema}.dim_location")).all())
        ids_pais = dict(conn.execute(text(f"select iso3, country_indicator_id from {schema}.dim_country_indicator")).all())
        hechos = preparar_hechos(enriquecido, ids_ubicacion, ids_pais)

        clave = ["location_id", "date_id", "source_id"]
        upsert(conn, hechos[hechos["source_id"] == SOURCE_IDS["kaggle_csv"]], schema, "fact_air_quality", clave, False)
        upsert(conn, hechos[hechos["source_id"] == SOURCE_IDS["open_meteo_api"]], schema, "fact_air_quality", clave, True)

    logger.info(f"carga completa en {schema}: {len(hechos)} filas de hechos procesadas")
    return len(hechos)


def registrar_ejecucion(run_id: str, estado: str, filas: int | None, detalle: str) -> None:
    schema = load_config()["pipeline"]["dw_schema"]
    with get_engine().begin() as conn:
        ejecutar_ddl(conn)
        conn.execute(text(
            f"insert into {schema}.etl_run_log (run_id, status, fact_rows_loaded, detail) "
            "values (:run_id, :estado, :filas, :detalle) "
            "on conflict (run_id) do update set status = excluded.status, "
            "fact_rows_loaded = excluded.fact_rows_loaded, detail = excluded.detail, finished_at = now()"
        ), {"run_id": run_id, "estado": estado, "filas": filas, "detalle": detalle})
