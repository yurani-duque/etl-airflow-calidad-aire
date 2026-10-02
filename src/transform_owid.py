"""
Script Name: transform_owid.py
Author: Valentina Velasco
Fecha: 2026-10-02
Fase ETL: Transformacion
Descripcion: reduce el dataset de emisiones (pais-ano) a un valor vigente por pais e
    indicador, descartando agregados regionales como "World" o "Africa".
Entradas: staging/<run_id>/owid.parquet
Salidas: staging/<run_id>/country_indicators.parquet
Notas: cada indicador conserva el ultimo ano con dato disponible, porque las series
    no terminan el mismo ano (por ejemplo el PIB no tiene datos recientes)
"""

from pathlib import Path

import pandas as pd

from utils.config_loader import staging_dir
from utils.logger import get_logger

logger = get_logger("transform_owid")

INDICADORES = ["population", "gdp", "co2_per_capita", "total_ghg", "energy_per_capita"]


def ultimo_valor_por_pais(df: pd.DataFrame) -> pd.DataFrame:
    df = df.dropna(subset=["iso_code"])
    df = df[~df["iso_code"].str.startswith("OWID_")]

    resultado = df.drop_duplicates("iso_code")[["iso_code", "country"]].set_index("iso_code")
    for indicador in INDICADORES:
        ultimo = (
            df.dropna(subset=[indicador]).sort_values("year")
            .groupby("iso_code")[["year", indicador]].last()
            .rename(columns={"year": f"{indicador}_year"})
        )
        resultado = resultado.join(ultimo)

    resultado = resultado.reset_index().rename(columns={"iso_code": "iso3", "country": "country_owid"})
    for indicador in INDICADORES:
        resultado[f"{indicador}_year"] = resultado[f"{indicador}_year"].astype("Int16")
    return resultado


def transformar_owid(run_id: str) -> str:
    carpeta = Path(staging_dir(run_id))
    df = pd.read_parquet(carpeta / "owid.parquet")
    indicadores = ultimo_valor_por_pais(df)
    salida = carpeta / "country_indicators.parquet"
    indicadores.to_parquet(salida, index=False)
    logger.info(f"paises con indicadores: {len(indicadores)}")
    return str(salida)
