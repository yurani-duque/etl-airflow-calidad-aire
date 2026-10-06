"""
Script Name: test_locations.py
Author: Valentina Velasco
Fecha: 2026-10-06
Fase ETL: Pruebas
Descripcion: valida el mapeo de paises a ISO3 y la construccion de ubicaciones canonicas.
Entradas: config/paises_iso3.csv y datos simulados
Salidas: resultado de pytest
Notas: correr con "uv run pytest test/"
"""

import pandas as pd

from src.locations import asignar_pais, cargar_mapa_paises, construir_ubicaciones
from utils.config_loader import load_config, resolve


def _lecturas(filas):
    columnas = ["iso3", "country", "location_name", "latitude", "longitude", "timezone", "last_updated_epoch"]
    return pd.DataFrame(filas, columns=columnas)


def test_mapa_cubre_todos_los_paises_del_csv():
    ruta = resolve(load_config()["sources"]["weather_csv"])
    paises = pd.read_csv(ruta, usecols=["country"])["country"].unique()
    mapa = cargar_mapa_paises()
    assert set(paises) <= set(mapa["nombre_origen"])


def test_nombres_en_otros_idiomas_terminan_en_el_mismo_iso3():
    mapa = cargar_mapa_paises()
    df = pd.DataFrame({"country": ["Polônia", "Польша", "Turkey", "火鸡"]})
    resultado = asignar_pais(df, mapa)
    assert resultado["iso3"].tolist() == ["POL", "POL", "TUR", "TUR"]


def test_ciudad_con_coordenadas_cambiantes_es_una_sola_ubicacion():
    df = _lecturas([
        ("AFG", "Afghanistan", "Kabul", 34.52, 69.18, "Asia/Kabul", 1),
        ("AFG", "Afghanistan", "Kabul", 34.5167, 69.1833, "Asia/Kabul", 2),
    ])
    ubicaciones, mapa = construir_ubicaciones(df)
    assert len(ubicaciones) == 1
    assert ubicaciones.loc[0, "latitude"] == 34.5167
    assert mapa["location_key"].nunique() == 1


def test_nombres_distintos_con_las_mismas_coordenadas_se_unifican():
    df = _lecturas([
        ("KWT", "Kuwait", "Kuwait City", 29.37, 47.97, "Asia/Kuwait", 1),
        ("KWT", "Kuwait", "Kuwait City", 29.37, 47.97, "Asia/Kuwait", 2),
        ("KWT", "Kuwait", "Kuwait", 29.37, 47.97, "Asia/Kuwait", 3),
    ])
    ubicaciones, mapa = construir_ubicaciones(df)
    assert len(ubicaciones) == 1
    assert ubicaciones.loc[0, "location_name"] == "Kuwait City"
    assert mapa["location_key"].nunique() == 1
