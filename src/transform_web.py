"""
Script Name: transform_web.py
Author: Valentina Velasco
Fecha: 2026-10-10
Fase ETL: Transformacion
Descripcion: limpia la tabla extraida de la web: quita notas al pie, convierte el texto a
    numeros, asigna el codigo ISO3 de cada pais y descarta territorios sin codigo.
Entradas: staging/<run_id>/web_raw.parquet, config/paises_iso3.csv
Salidas: staging/<run_id>/web_clean.parquet con iso3, country_web, pm25_annual_web y life_years_lost_web
Notas: los anos de vida perdidos se guardan como magnitud positiva (la pagina los publica con signo negativo)
"""

from pathlib import Path

import pandas as pd

from src.locations import cargar_mapa_paises
from utils.config_loader import staging_dir
from utils.logger import get_logger

logger = get_logger("transform_web")

# nombres de la pagina que no estan en el mapa de paises del CSV
ALIAS_ISO3 = {
    "democratic republic of the congo": "COD", "republic of the congo": "COG", "republic of china": "TWN",
    "south sudan": "SSD", "palestine": "PSE", "moldova": "MDA", "ivory coast": "CIV",
    "são tomé and príncipe": "STP", "cayman islands": "CYM", "gibraltar": "GIB", "french guiana": "GUF",
    "western sahara": "ESH", "curaçao": "CUW", "aruba": "ABW", "bermuda": "BMU", "réunion": "REU",
    "martinique": "MTQ", "faroe islands": "FRO", "falklands": "FLK", "puerto rico": "PRI",
    "us virgin islands": "VIR", "british virgin islands": "VGB", "french polynesia": "PYF",
    "american samoa": "ASM", "guam": "GUM", "nauru": "NRU", "cook islands": "COK", "greenland": "GRL",
    "federated states of micronesia": "FSM", "mayotte": "MYT",
}


def a_numero(serie: pd.Series) -> pd.Series:
    """Convierte texto como '−4.82' (signo tipografico) a numero."""
    limpio = serie.str.replace("−", "-", regex=False).str.replace(r"[^\d.\-]", "", regex=True)
    return pd.to_numeric(limpio, errors="coerce")


def asignar_iso3(paises: pd.Series, mapa: pd.DataFrame) -> pd.Series:
    """ISO3 por nombre de origen, luego por nombre estandar y por ultimo por alias."""
    por_origen = dict(zip(mapa["nombre_origen"].str.lower(), mapa["iso3"]))
    por_estandar = dict(zip(mapa["pais_estandar"].str.lower(), mapa["iso3"]))
    nombres = paises.str.lower()
    iso3 = nombres.map(por_origen)
    iso3 = iso3.fillna(nombres.map(por_estandar)).fillna(nombres.map(ALIAS_ISO3))
    logger.info(f"paises por nombre de origen: {nombres.isin(por_origen).sum()}, "
                f"por nombre estandar o alias: {iso3.notna().sum() - nombres.isin(por_origen).sum()}")
    return iso3


def limpiar_web(crudo: pd.DataFrame, mapa: pd.DataFrame) -> pd.DataFrame:
    df = crudo.copy()
    df["country_web"] = df["country"].str.replace(r"\[.*?\]", "", regex=True).str.strip()
    df["pm25_annual_web"] = a_numero(df["pm25"])
    df["life_years_lost_web"] = a_numero(df["life_loss"]).abs()
    df["iso3"] = asignar_iso3(df["country_web"], mapa)

    sin_iso3 = df["iso3"].isna().sum()
    df = df.dropna(subset=["iso3", "pm25_annual_web"]).drop_duplicates("iso3")
    logger.info(f"territorios sin codigo ISO3 descartados: {sin_iso3}")
    columnas = ["iso3", "country_web", "pm25_annual_web", "life_years_lost_web"]
    return df[columnas].reset_index(drop=True)


def transformar_web(run_id: str) -> str:
    carpeta = Path(staging_dir(run_id))
    limpio = limpiar_web(pd.read_parquet(carpeta / "web_raw.parquet"), cargar_mapa_paises())
    salida = carpeta / "web_clean.parquet"
    limpio.to_parquet(salida, index=False)
    logger.info(f"paises con PM2.5 anual de la web: {len(limpio)}")
    return str(salida)
