"""
Script Name: extract_api.py
Author: Yurani Duque
Fecha: 2026-10-01
Fase ETL: Extraccion
Descripcion: consulta la API publica Open-Meteo (calidad del aire y clima) para cada
    ciudad del dataset historico y deja los datos crudos en staging. La extraccion es
    incremental: solo pide los dias posteriores al ultimo dia ya cargado en el Data Warehouse.
Entradas: API Open-Meteo (sin llave), lista de ubicaciones derivada del CSV, dw.fact_air_quality
Salidas: staging/<run_id>/ubicaciones.parquet, api_air_quality.parquet, api_weather.parquet
Notas: las ciudades se consultan por bloques; ante errores 429/5xx se reintenta con espera exponencial
"""

import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
from sqlalchemy import text
from sqlalchemy.exc import ProgrammingError

from src.locations import asignar_pais, cargar_mapa_paises, construir_ubicaciones
from utils.config_loader import load_config, resolve, staging_dir
from utils.db_connector import get_engine
from utils.logger import get_logger

logger = get_logger("extract_api")

AIRE = "pm2_5,pm10,carbon_monoxide,nitrogen_dioxide,sulphur_dioxide,ozone,us_aqi"
CLIMA = (
    "temperature_2m_mean,relative_humidity_2m_mean,precipitation_sum,"
    "wind_speed_10m_max,uv_index_max,surface_pressure_mean,cloud_cover_mean"
)
ESTADOS_REINTENTO = (429, 500, 502, 503, 504)
ID_FUENTE_API = 2


def obtener_ubicaciones() -> tuple[pd.DataFrame, pd.Timestamp]:
    """Devuelve las ubicaciones canonicas y el ultimo dia disponible en el CSV."""
    ruta = resolve(load_config()["sources"]["weather_csv"])
    columnas = ["country", "location_name", "latitude", "longitude", "timezone", "last_updated_epoch", "last_updated"]
    df = pd.read_csv(ruta, usecols=columnas, encoding="utf-8")
    ultimo_dia_csv = pd.to_datetime(df["last_updated"]).max().normalize()
    ubicaciones, _ = construir_ubicaciones(asignar_pais(df, cargar_mapa_paises()))
    return ubicaciones, ultimo_dia_csv


def ultimo_dia_api_cargado():
    """Ultimo dia de la API que ya esta en el Data Warehouse, o None si aun no hay datos."""
    schema = load_config()["pipeline"]["dw_schema"]
    consulta = text(
        f"select max(d.full_date) from {schema}.fact_air_quality f "
        f"join {schema}.dim_date d using (date_id) where f.source_id = :fuente"
    )
    try:
        with get_engine().connect() as conn:
            fecha = conn.execute(consulta, {"fuente": ID_FUENTE_API}).scalar()
    except ProgrammingError:
        return None
    return pd.Timestamp(fecha) if fecha else None


def calcular_dias_pasados(ultimo_cubierto: pd.Timestamp, hoy: pd.Timestamp, minimo: int, maximo: int) -> int:
    """Dias hacia atras que hay que pedir para cubrir el hueco, con un traslape de 2 dias."""
    return max(minimo, min(maximo, (hoy - ultimo_cubierto).days + 2))


def consultar(url: str, params: dict, api_cfg: dict):
    espera = 1
    for intento in range(api_cfg["max_retries"] + 1):
        try:
            resp = requests.get(url, params=params, timeout=api_cfg["timeout_seconds"])
            if resp.status_code in ESTADOS_REINTENTO:
                raise requests.HTTPError(f"estado {resp.status_code}")
            resp.raise_for_status()
            datos = resp.json()
            return datos if isinstance(datos, list) else [datos]
        except requests.RequestException as error:
            if intento == api_cfg["max_retries"]:
                raise
            logger.warning(f"intento {intento + 1} fallido ({error}), reintento en {espera}s")
            time.sleep(espera)
            espera *= 2


def a_tabla(respuestas: list, bloque: pd.DataFrame, clave: str) -> pd.DataFrame:
    tablas = []
    for ubicacion, (_, fila) in zip(respuestas, bloque.iterrows()):
        tabla = pd.DataFrame(ubicacion[clave])
        tabla["location_key"] = fila["location_key"]
        tabla["utc_offset_seconds"] = ubicacion["utc_offset_seconds"]
        tablas.append(tabla)
    return pd.concat(tablas, ignore_index=True)


def extraer_api(run_id: str) -> str:
    api_cfg = load_config()["api"]
    ubicaciones, ultimo_dia_csv = obtener_ubicaciones()
    ultimo_cubierto = ultimo_dia_api_cargado() or ultimo_dia_csv
    hoy = pd.Timestamp(datetime.now(timezone.utc)).tz_localize(None).normalize()
    dias = calcular_dias_pasados(ultimo_cubierto, hoy, api_cfg["min_past_days"], api_cfg["max_past_days"])
    logger.info(f"ultimo dia cubierto: {ultimo_cubierto.date()}, se consultan {dias} dias hacia atras")

    salida = Path(staging_dir(run_id))
    ubicaciones.to_parquet(salida / "ubicaciones.parquet", index=False)

    aire, clima = [], []
    for inicio in range(0, len(ubicaciones), api_cfg["chunk_size"]):
        bloque = ubicaciones.iloc[inicio:inicio + api_cfg["chunk_size"]]
        comunes = {
            "latitude": ",".join(bloque["latitude"].astype(str)),
            "longitude": ",".join(bloque["longitude"].astype(str)),
            "past_days": dias,
            "forecast_days": 1,
            "timezone": "auto",
        }
        resp_aire = consultar(api_cfg["air_quality_url"], {**comunes, "hourly": AIRE}, api_cfg)
        resp_clima = consultar(api_cfg["weather_url"], {**comunes, "daily": CLIMA}, api_cfg)
        aire.append(a_tabla(resp_aire, bloque, "hourly"))
        clima.append(a_tabla(resp_clima, bloque, "daily"))
        logger.info(f"ciudades consultadas: {min(inicio + api_cfg['chunk_size'], len(ubicaciones))}/{len(ubicaciones)}")
        time.sleep(0.5)

    pd.concat(aire, ignore_index=True).to_parquet(salida / "api_air_quality.parquet", index=False)
    pd.concat(clima, ignore_index=True).to_parquet(salida / "api_weather.parquet", index=False)
    return str(salida)
