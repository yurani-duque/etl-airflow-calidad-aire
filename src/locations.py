"""
Script Name: locations.py
Author: Valentina Velasco
Fecha: 2026-10-01
Fase ETL: Transformacion (soporte)
Descripcion: normaliza los paises a codigo ISO3 y construye la lista canonica de
    ubicaciones (una fila por ciudad real, sin duplicados por nombre, idioma o
    cambios de coordenadas en el origen).
Entradas: config/paises_iso3.csv, dataframe con iso3, country, location_name,
    latitude, longitude, timezone y last_updated_epoch
Salidas: tabla de ubicaciones con location_key y mapa (iso3, location_name) -> location_key
Notas: location_key = iso3|nombre_canonico; las coordenadas son las de la lectura mas reciente
"""

import pandas as pd

from utils.config_loader import load_config, resolve


def cargar_mapa_paises() -> pd.DataFrame:
    ruta = resolve(load_config()["sources"]["countries_map"])
    return pd.read_csv(ruta, encoding="utf-8")


def asignar_pais(df: pd.DataFrame, mapa: pd.DataFrame) -> pd.DataFrame:
    """Reemplaza el nombre de pais original por iso3 y nombre estandar en ingles."""
    unido = df.merge(mapa, how="left", left_on="country", right_on="nombre_origen")
    unido = unido.drop(columns=["country", "nombre_origen"])
    return unido.rename(columns={"pais_estandar": "country"})


def construir_ubicaciones(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Devuelve (ubicaciones, mapa). Nombres distintos con las mismas coordenadas
    aproximadas (0.1 grados) dentro de un pais se consideran la misma ciudad."""
    por_nombre = (
        df.sort_values("last_updated_epoch")
        .groupby(["iso3", "location_name"])
        .agg(
            country=("country", "last"), latitude=("latitude", "last"),
            longitude=("longitude", "last"), timezone=("timezone", "last"),
            lecturas=("latitude", "size"),
        )
        .reset_index()
    )
    por_nombre["grupo"] = (
        por_nombre["iso3"] + "|" + por_nombre["latitude"].round(1).astype(str)
        + "|" + por_nombre["longitude"].round(1).astype(str)
    )
    canonicas = (
        por_nombre.sort_values(["lecturas", "location_name"], ascending=[False, True])
        .drop_duplicates("grupo")
    )
    canonicas["location_key"] = canonicas["iso3"] + "|" + canonicas["location_name"].str.lower()

    mapa = por_nombre[["iso3", "location_name", "grupo"]].merge(
        canonicas[["grupo", "location_key"]], on="grupo"
    )[["iso3", "location_name", "location_key"]]

    columnas = ["location_key", "iso3", "country", "location_name", "latitude", "longitude", "timezone"]
    ubicaciones = canonicas[columnas].copy()
    ubicaciones["latitude"] = ubicaciones["latitude"].round(4)
    ubicaciones["longitude"] = ubicaciones["longitude"].round(4)
    return ubicaciones.reset_index(drop=True), mapa.reset_index(drop=True)
