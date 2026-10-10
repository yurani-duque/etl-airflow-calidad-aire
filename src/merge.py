"""
Script Name: merge.py
Author: Yurani Duque
Fecha: 2026-10-02
Fase ETL: Transformacion
Descripcion: une las cuatro fuentes en un unico dataset enriquecido: lecturas del CSV y de
    la API por ciudad y dia, mas los indicadores de emisiones del pais de cada ciudad y el
    PM2.5 anual nacional obtenido de la web.
Entradas: staging/<run_id>/csv_clean.parquet, api_clean.parquet, country_indicators.parquet, web_clean.parquet
Salidas: staging/<run_id>/enriched.parquet
Notas: la API solo aporta dias posteriores al ultimo dia del CSV para no duplicar lecturas;
    pm25_vs_national es la lectura diaria de la ciudad menos el promedio anual de su pais
"""

from pathlib import Path

import pandas as pd

from utils.config_loader import load_config, staging_dir
from utils.logger import get_logger

logger = get_logger("merge")

COLUMNAS_INDICADORES = [
    "population", "population_year", "gdp", "gdp_year", "co2_per_capita",
    "co2_per_capita_year", "total_ghg", "total_ghg_year", "energy_per_capita", "energy_per_capita_year",
]


def unir_fuentes(csv: pd.DataFrame, api: pd.DataFrame, indicadores: pd.DataFrame, umbral_pm25: float,
                 web: pd.DataFrame | None = None) -> pd.DataFrame:
    ultimo_dia_csv = csv["date"].max()
    api_nueva = api[api["date"] > ultimo_dia_csv]
    hechos = pd.concat([csv, api_nueva], ignore_index=True)

    paises = indicadores[["iso3"] + COLUMNAS_INDICADORES].copy()
    paises = paises.rename(columns={c: f"ind_{c}" for c in COLUMNAS_INDICADORES})
    enriquecido = hechos.merge(paises, on="iso3", how="left")

    if web is None:
        web = pd.DataFrame({
            "iso3": pd.Series([], dtype="object"),
            "pm25_annual_web": pd.Series([], dtype="float64"),
            "life_years_lost_web": pd.Series([], dtype="float64"),
        })
    enriquecido = enriquecido.merge(web[["iso3", "pm25_annual_web", "life_years_lost_web"]], on="iso3", how="left")
    enriquecido["pm25_vs_national"] = enriquecido["pm2_5"] - enriquecido["pm25_annual_web"]

    enriquecido["pm25_who_ratio"] = enriquecido["pm2_5"] / umbral_pm25
    excede = (enriquecido["pm2_5"] > umbral_pm25).astype("boolean")
    enriquecido["pm25_exceeds_who"] = excede.mask(enriquecido["pm2_5"].isna())
    return enriquecido.sort_values(["date", "location_key"]).reset_index(drop=True)


def inyectar_datos_invalidos(df: pd.DataFrame) -> pd.DataFrame:
    """Solo para demostrar el bloqueo de carga: agrega humedades imposibles y filas repetidas."""
    invalido = df.head(300).copy()
    invalido["humidity"] = 150.0
    return pd.concat([df, invalido], ignore_index=True)


def unir(run_id: str, simular_falla: bool = False) -> str:
    carpeta = Path(staging_dir(run_id))
    enriquecido = unir_fuentes(
        pd.read_parquet(carpeta / "csv_clean.parquet"),
        pd.read_parquet(carpeta / "api_clean.parquet"),
        pd.read_parquet(carpeta / "country_indicators.parquet"),
        load_config()["quality"]["who_pm25_24h"],
        pd.read_parquet(carpeta / "web_clean.parquet"),
    )
    if simular_falla:
        enriquecido = inyectar_datos_invalidos(enriquecido)
        logger.warning("simulacion activa: se inyectaron filas invalidas para probar la validacion")
    salida = carpeta / "enriched.parquet"
    enriquecido.to_parquet(salida, index=False)
    por_fuente = enriquecido["source"].value_counts().to_dict()
    logger.info(f"dataset enriquecido: {len(enriquecido)} filas {por_fuente}")
    return str(salida)
