"""
Script Name: transform_api.py
Author: Valentina Velasco
Fecha: 2026-10-02
Fase ETL: Transformacion
Descripcion: convierte las lecturas horarias de calidad del aire a valores diarios,
    las une con el clima diario y las deja con el mismo esquema que el CSV.
Entradas: staging/<run_id>/api_air_quality.parquet, api_weather.parquet, ubicaciones.parquet
Salidas: staging/<run_id>/api_clean.parquet
Notas: solo se conservan dias completos; el indice EPA (1 a 6) se deriva del AQI de EE.UU.
"""

from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from src.transform_csv import COLUMNAS_FINALES
from utils.config_loader import load_config, staging_dir
from utils.logger import get_logger

logger = get_logger("transform_api")

CORTES_AQI = [-1, 50, 100, 150, 200, 300, float("inf")]


def aqi_a_indice_epa(aqi: pd.Series) -> pd.Series:
    return pd.cut(aqi, bins=CORTES_AQI, labels=[1, 2, 3, 4, 5, 6]).astype("Int64")


def fecha_local_de_hoy(offset_segundos: int) -> pd.Timestamp:
    ahora = datetime.now(timezone.utc)
    return (pd.Timestamp(ahora).tz_localize(None) + pd.Timedelta(seconds=offset_segundos)).normalize()


def agregar_aire_diario(aire: pd.DataFrame, min_horas: int) -> pd.DataFrame:
    aire = aire.copy()
    aire["date"] = pd.to_datetime(aire["time"]).dt.normalize()
    grupos = aire.groupby(["location_key", "date"])
    diario = grupos.agg(
        pm2_5=("pm2_5", "mean"), pm10=("pm10", "mean"), co=("carbon_monoxide", "mean"),
        no2=("nitrogen_dioxide", "mean"), so2=("sulphur_dioxide", "mean"), ozone=("ozone", "mean"),
        aqi_us=("us_aqi", "max"), horas=("pm2_5", "count"), offset=("utc_offset_seconds", "first"),
    ).reset_index()
    diario = diario[diario["horas"] >= min_horas]
    hoy = diario["offset"].map(fecha_local_de_hoy)
    return diario[diario["date"] < hoy].drop(columns=["horas", "offset"])


def preparar_clima_diario(clima: pd.DataFrame) -> pd.DataFrame:
    clima = clima.rename(columns={
        "temperature_2m_mean": "temperature_celsius", "relative_humidity_2m_mean": "humidity",
        "precipitation_sum": "precip_mm", "wind_speed_10m_max": "wind_kph",
        "uv_index_max": "uv_index", "surface_pressure_mean": "pressure_mb", "cloud_cover_mean": "cloud",
    })
    clima["date"] = pd.to_datetime(clima["time"]).dt.normalize()
    columnas = ["location_key", "date", "temperature_celsius", "humidity", "wind_kph",
                "pressure_mb", "precip_mm", "cloud", "uv_index"]
    return clima[columnas]


def construir_api(aire: pd.DataFrame, clima: pd.DataFrame, ubicaciones: pd.DataFrame, min_horas: int) -> pd.DataFrame:
    diario = agregar_aire_diario(aire, min_horas)
    unido = diario.merge(preparar_clima_diario(clima), on=["location_key", "date"], how="left")
    unido = unido.merge(ubicaciones, on="location_key", how="left")
    unido["us_epa_index"] = aqi_a_indice_epa(unido["aqi_us"])
    unido["source"] = "open_meteo_api"
    return unido[COLUMNAS_FINALES].reset_index(drop=True)


def transformar_api(run_id: str) -> str:
    carpeta = Path(staging_dir(run_id))
    resultado = construir_api(
        pd.read_parquet(carpeta / "api_air_quality.parquet"),
        pd.read_parquet(carpeta / "api_weather.parquet"),
        pd.read_parquet(carpeta / "ubicaciones.parquet"),
        load_config()["api"]["min_hours_per_day"],
    )
    salida = carpeta / "api_clean.parquet"
    resultado.to_parquet(salida, index=False)
    logger.info(f"filas diarias de la api: {len(resultado)}")
    return str(salida)
