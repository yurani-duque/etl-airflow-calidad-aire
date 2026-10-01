"""
Script Name: transform_csv.py
Author: Valentina Velasco
Fecha: 2026-10-01
Fase ETL: Transformacion
Descripcion: limpia y normaliza el CSV historico: renombra columnas,
    convierte fechas, asigna ISO3 a los paises, unifica ubicaciones duplicadas,
    conserva una lectura por ciudad y dia y descarta valores fisicamente invalidos.
Entradas: staging/<run_id>/weather_csv.parquet
Salidas: staging/<run_id>/csv_clean.parquet
Notas: si una ciudad tiene dos lecturas el mismo dia se conserva la mas reciente
"""

from pathlib import Path

import pandas as pd

from src.locations import asignar_pais, cargar_mapa_paises, construir_ubicaciones
from utils.config_loader import staging_dir
from utils.logger import get_logger

logger = get_logger("transform_csv")

RENOMBRAR = {
    "air_quality_PM2.5": "pm2_5",
    "air_quality_PM10": "pm10",
    "air_quality_Carbon_Monoxide": "co",
    "air_quality_Nitrogen_dioxide": "no2",
    "air_quality_Sulphur_dioxide": "so2",
    "air_quality_Ozone": "ozone",
    "air_quality_us-epa-index": "us_epa_index",
}

COLUMNAS_FINALES = [
    "location_key", "iso3", "country", "location_name", "latitude", "longitude", "timezone",
    "date", "source", "temperature_celsius", "humidity", "wind_kph", "pressure_mb",
    "precip_mm", "cloud", "uv_index", "pm2_5", "pm10", "co", "no2", "so2", "ozone",
    "us_epa_index", "aqi_us",
]

NO_NEGATIVAS = ["pm2_5", "pm10", "co", "no2", "so2", "ozone", "precip_mm", "wind_kph", "pressure_mb", "uv_index"]


def limpiar_csv(df: pd.DataFrame, mapa_paises: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns=RENOMBRAR)
    for columna in ["country", "location_name", "timezone"]:
        df[columna] = df[columna].astype("string").str.strip()

    df["last_updated"] = pd.to_datetime(df["last_updated"], errors="coerce")
    df = df.dropna(subset=["last_updated", "country", "location_name"])
    df["date"] = df["last_updated"].dt.normalize()

    df = asignar_pais(df, mapa_paises)
    ubicaciones, mapa = construir_ubicaciones(df)
    df = df.drop(columns=["country", "latitude", "longitude", "timezone"])
    df = df.merge(mapa, on=["iso3", "location_name"], how="left").drop(columns=["location_name", "iso3"])
    df = df.merge(ubicaciones, on="location_key", how="left")

    antes = len(df)
    df = df.sort_values("last_updated_epoch").drop_duplicates(["location_key", "date"], keep="last")
    logger.info(f"lecturas duplicadas por ciudad y dia eliminadas: {antes - len(df)}")

    antes = len(df)
    df = df[df["humidity"].between(0, 100) & df["cloud"].between(0, 100)]
    df = df[df["temperature_celsius"].between(-90, 60)]
    for columna in NO_NEGATIVAS:
        df = df[df[columna].isna() | (df[columna] >= 0)]
    df = df[df["us_epa_index"].between(1, 6)]
    logger.info(f"filas con valores fisicamente invalidos eliminadas: {antes - len(df)}")

    df["source"] = "kaggle_csv"
    df["aqi_us"] = float("nan")
    return df[COLUMNAS_FINALES].reset_index(drop=True)


def transformar_csv(run_id: str) -> str:
    carpeta = Path(staging_dir(run_id))
    df = pd.read_parquet(carpeta / "weather_csv.parquet")
    limpio = limpiar_csv(df, cargar_mapa_paises())
    salida = carpeta / "csv_clean.parquet"
    limpio.to_parquet(salida, index=False)
    logger.info(f"filas transformadas: {len(limpio)}")
    return str(salida)
