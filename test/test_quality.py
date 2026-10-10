"""
Script Name: test_quality.py
Author: Valentina Velasco
Fecha: 2026-10-06
Fase ETL: Pruebas
Descripcion: comprueba que las suites de Great Expectations aprueban datos validos y
    detectan datos corruptos (lo que bloquea la carga al Data Warehouse).
Entradas: datos simulados
Salidas: resultado de pytest
Notas: requiere great-expectations (Python 3.12), se ejecuta dentro del contenedor:
    docker compose run --rm --no-deps airflow-cli pytest test/test_quality.py test/test_dag.py
"""

from datetime import datetime

import pandas as pd
import pytest

pytest.importorskip("great_expectations")

from src.quality import evaluar_suites, obtener_contexto  # noqa: E402

HOY = datetime(2026, 10, 1)


def _dataset(n=60):
    fechas = pd.date_range("2026-09-25", periods=n // 2)
    filas = []
    for ciudad in ["COL|bogota", "CHL|santiago"]:
        for fecha in fechas:
            filas.append({
                "location_key": ciudad, "iso3": ciudad[:3], "country": "X", "location_name": ciudad[4:],
                "latitude": 4.6, "longitude": -74.0, "timezone": "UTC", "date": fecha, "source": "kaggle_csv",
                "temperature_celsius": 15.0, "humidity": 70.0, "wind_kph": 10.0, "pressure_mb": 1010.0,
                "cloud": 40.0, "pm2_5": 12.0, "pm10": 20.0, "us_epa_index": 1,
            })
    return pd.DataFrame(filas)


def _indicadores():
    n = 160
    return pd.DataFrame({
        "iso3": [f"A{chr(65 + i // 26)}{chr(65 + i % 26)}" for i in range(n)],
        "country_owid": "X", "population": 1_000_000.0, "co2_per_capita": 3.0, "gdp": 1e9,
    })


def _evaluar(datos):
    contexto = obtener_contexto(efimero=True)
    indicadores = _indicadores()
    datos = datos.assign(iso3=indicadores["iso3"].iloc[0])
    return evaluar_suites(contexto, "test", datos, indicadores, HOY, min_filas=10)[0]


def test_datos_validos_pasan_las_validaciones():
    assert _evaluar(_dataset())["aprobada"] is True


def test_humedad_imposible_bloquea_la_carga():
    datos = _dataset()
    datos.loc[:5, "humidity"] = 150.0
    resumen = _evaluar(datos)
    assert resumen["aprobada"] is False
    assert resumen["suites"]["suite_hechos_calidad_aire"]["fallos_criticos"] >= 1


def test_filas_repetidas_bloquean_la_carga():
    datos = _dataset()
    resumen = _evaluar(pd.concat([datos, datos.head(5)], ignore_index=True))
    assert resumen["aprobada"] is False


def _web(n=200):
    return pd.DataFrame({
        "iso3": [f"B{chr(65 + i // 26)}{chr(65 + i % 26)}" for i in range(n)],
        "pm25_annual_web": 12.0, "life_years_lost_web": 0.5,
    })


def _evaluar_con_web(web):
    contexto = obtener_contexto(efimero=True)
    indicadores = _indicadores()
    datos = _dataset().assign(iso3=indicadores["iso3"].iloc[0])
    return evaluar_suites(contexto, "test", datos, indicadores, HOY, min_filas=10, web=web)[0]


def test_datos_web_validos_pasan():
    resumen = _evaluar_con_web(_web())
    assert resumen["suites"]["suite_web_pm25"]["aprobada"] is True


def test_pm25_web_fuera_de_rango_bloquea_la_carga():
    web = _web()
    web.loc[0, "pm25_annual_web"] = 540.0
    assert _evaluar_con_web(web)["aprobada"] is False


def test_pocas_filas_en_la_web_bloquean_la_carga():
    assert _evaluar_con_web(_web(n=20))["aprobada"] is False
