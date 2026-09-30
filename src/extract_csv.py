"""
Script Name: extract_csv.py
Author: Valentina Velasco
Fecha: 2026-09-30
Fase ETL: Extraccion
Descripcion: lee el CSV historico (World Weather Repository) y lo deja
    en staging en formato parquet.
Entradas: data/raw/GlobalWeatherRepository.csv.gz
Salidas: staging/<run_id>/weather_csv.parquet
Notas: valida que existan las columnas que usa la transformacion
"""

from pathlib import Path

import pandas as pd

from utils.config_loader import load_config, resolve, staging_dir
from utils.logger import get_logger

logger = get_logger("extract_csv")

COLUMNAS_ESPERADAS = {
    "country", "location_name", "latitude", "longitude", "timezone",
    "last_updated", "last_updated_epoch", "temperature_celsius", "humidity",
    "wind_kph", "pressure_mb", "precip_mm", "cloud", "uv_index",
    "air_quality_Carbon_Monoxide", "air_quality_Ozone", "air_quality_Nitrogen_dioxide",
    "air_quality_Sulphur_dioxide", "air_quality_PM2.5", "air_quality_PM10",
    "air_quality_us-epa-index",
}


def extraer_csv(run_id: str) -> str:
    ruta = resolve(load_config()["sources"]["weather_csv"])
    logger.info(f"leyendo {ruta}")
    df = pd.read_csv(ruta, encoding="utf-8")

    faltantes = COLUMNAS_ESPERADAS - set(df.columns)
    if faltantes:
        raise ValueError(f"columnas faltantes en el csv: {sorted(faltantes)}")

    salida = Path(staging_dir(run_id)) / "weather_csv.parquet"
    df.to_parquet(salida, index=False)
    logger.info(f"filas extraidas: {len(df)}")
    return str(salida)
