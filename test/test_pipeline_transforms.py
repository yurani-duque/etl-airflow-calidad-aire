"""
Script Name: test_pipeline_transforms.py
Author: Valentina Velasco
Fecha: 2026-10-06
Fase ETL: Pruebas
Descripcion: valida las transformaciones de las tres fuentes y la union en el dataset enriquecido.
Entradas: datos simulados
Salidas: resultado de pytest
Notas: correr con "uv run pytest test/"
"""

import pandas as pd

import pytest

from src.extract_api import calcular_dias_pasados
from src.extract_web import parsear_tabla
from src.locations import cargar_mapa_paises
from src.merge import unir_fuentes
from src.transform_api import agregar_aire_diario, aqi_a_indice_epa
from src.transform_csv import COLUMNAS_FINALES, limpiar_csv
from src.transform_owid import ultimo_valor_por_pais
from src.transform_web import limpiar_web


def _fila_csv(**cambios):
    fila = {
        "country": "Colombia", "location_name": "Bogota", "latitude": 4.6, "longitude": -74.08,
        "timezone": "America/Bogota", "last_updated": "2026-01-10 10:00", "last_updated_epoch": 1,
        "temperature_celsius": 15.0, "humidity": 70, "wind_kph": 10.0, "pressure_mb": 1010.0,
        "precip_mm": 0.0, "cloud": 40, "uv_index": 3.0, "air_quality_Carbon_Monoxide": 200.0,
        "air_quality_Ozone": 30.0, "air_quality_Nitrogen_dioxide": 10.0, "air_quality_Sulphur_dioxide": 2.0,
        "air_quality_PM2.5": 12.0, "air_quality_PM10": 20.0, "air_quality_us-epa-index": 1,
    }
    fila.update(cambios)
    return fila


def test_csv_conserva_la_lectura_mas_reciente_del_dia():
    df = pd.DataFrame([
        _fila_csv(last_updated="2026-01-10 08:00", last_updated_epoch=1, temperature_celsius=10.0),
        _fila_csv(last_updated="2026-01-10 20:00", last_updated_epoch=2, temperature_celsius=18.0),
    ])
    limpio = limpiar_csv(df, cargar_mapa_paises())
    assert len(limpio) == 1
    assert limpio.loc[0, "temperature_celsius"] == 18.0


def test_csv_descarta_valores_fisicamente_invalidos():
    df = pd.DataFrame([
        _fila_csv(last_updated="2026-01-10 10:00", last_updated_epoch=1),
        _fila_csv(last_updated="2026-01-11 10:00", last_updated_epoch=2, humidity=140),
        _fila_csv(last_updated="2026-01-12 10:00", last_updated_epoch=3, **{"air_quality_PM2.5": -5.0}),
        _fila_csv(last_updated="2026-01-13 10:00", last_updated_epoch=4, temperature_celsius=79.3),
    ])
    limpio = limpiar_csv(df, cargar_mapa_paises())
    assert len(limpio) == 1
    assert list(limpio.columns) == COLUMNAS_FINALES


def test_dias_a_consultar_a_la_api_segun_lo_ya_cargado():
    hoy = pd.Timestamp("2026-10-13")
    assert calcular_dias_pasados(pd.Timestamp("2026-10-12"), hoy, minimo=3, maximo=92) == 3
    assert calcular_dias_pasados(pd.Timestamp("2026-09-08"), hoy, minimo=3, maximo=92) == 37
    assert calcular_dias_pasados(pd.Timestamp("2025-01-01"), hoy, minimo=3, maximo=92) == 92


def test_indice_epa_a_partir_del_aqi():
    aqi = pd.Series([0, 50, 51, 100, 150, 200, 300, 301])
    assert aqi_a_indice_epa(aqi).tolist() == [1, 1, 2, 2, 3, 4, 5, 6]


def test_api_descarta_dias_incompletos_y_el_dia_actual():
    horas = pd.date_range("2020-01-01", periods=48, freq="h")
    aire = pd.DataFrame({
        "time": horas.strftime("%Y-%m-%dT%H:%M"), "pm2_5": 10.0, "pm10": 20.0, "carbon_monoxide": 100.0,
        "nitrogen_dioxide": 5.0, "sulphur_dioxide": 1.0, "ozone": 40.0, "us_aqi": 40,
        "location_key": "COL|bogota", "utc_offset_seconds": 0,
    })
    aire = aire.iloc[:40]
    diario = agregar_aire_diario(aire, min_horas=18)
    assert len(diario) == 1
    assert str(diario.loc[0, "date"].date()) == "2020-01-01"


def test_owid_toma_el_ultimo_dato_disponible_y_excluye_agregados():
    df = pd.DataFrame({
        "country": ["Chile", "Chile", "Chile", "World"], "iso_code": ["CHL", "CHL", "CHL", "OWID_WRL"],
        "year": [2020, 2021, 2022, 2022], "population": [1.0, 2.0, 3.0, 9.0], "gdp": [5.0, 6.0, None, 7.0],
        "co2_per_capita": [1.0, 1.1, 1.2, 4.0], "total_ghg": [1.0, 1.0, 1.0, 1.0], "energy_per_capita": [1.0, 1.0, 1.0, 1.0],
    })
    resultado = ultimo_valor_por_pais(df)
    assert resultado["iso3"].tolist() == ["CHL"]
    assert resultado.loc[0, "gdp"] == 6.0 and resultado.loc[0, "gdp_year"] == 2021
    assert resultado.loc[0, "population_year"] == 2022


def test_union_no_duplica_dias_que_ya_estan_en_el_csv():
    base = {"iso3": "COL", "location_key": "COL|bogota", "source": "kaggle_csv", "pm2_5": 20.0}
    csv = pd.DataFrame([{**base, "date": pd.Timestamp("2026-09-08")}])
    api = pd.DataFrame([
        {**base, "source": "open_meteo_api", "date": pd.Timestamp("2026-09-08")},
        {**base, "source": "open_meteo_api", "date": pd.Timestamp("2026-09-09"), "pm2_5": 8.0},
    ])
    indicadores = pd.DataFrame({"iso3": ["COL"], "country_owid": ["Colombia"], **{
        c: [1] for c in ["population", "population_year", "gdp", "gdp_year", "co2_per_capita", "co2_per_capita_year",
                         "total_ghg", "total_ghg_year", "energy_per_capita", "energy_per_capita_year"]}})
    unido = unir_fuentes(csv, api, indicadores, umbral_pm25=15)
    assert len(unido) == 2
    assert unido["pm25_exceeds_who"].tolist() == [True, False]
    assert "ind_population" in unido.columns


HTML_WEB = """
<html><body>
<table class="wikitable"><tr><th>Otra</th><th>Tabla</th></tr><tr><td>1</td><td>2</td></tr></table>
<table class="wikitable">
<tr><th>Rank</th><th>Country</th><th>Particulate matter ( PM2.5 ) (in μg/m 3 ) WHO-Guideline=5</th>
<th>Loss of life expectancy (years)</th></tr>
<tr><td>1</td><td>Bangladesh</td><td>54.17</td><td>\u22124.82</td></tr>
<tr><td>2</td><td>Ivory Coast[12]</td><td>10.45</td><td>\u22120.53</td></tr>
<tr><td>3</td><td>Northern Cyprus</td><td>14.51</td><td>\u22120.93</td></tr>
</table></body></html>
"""


def test_web_localiza_la_tabla_de_paises_por_su_encabezado():
    tabla = parsear_tabla(HTML_WEB)
    assert tabla["country"].tolist() == ["Bangladesh", "Ivory Coast[12]", "Northern Cyprus"]


def test_web_falla_si_la_estructura_de_la_pagina_cambia():
    with pytest.raises(ValueError):
        parsear_tabla("<html><body><table class='wikitable'><tr><th>x</th></tr></table></body></html>")


def test_web_limpia_notas_signos_y_asigna_iso3():
    limpio = limpiar_web(parsear_tabla(HTML_WEB), cargar_mapa_paises())
    assert limpio["iso3"].tolist() == ["BGD", "CIV"]
    assert limpio.loc[0, "pm25_annual_web"] == 54.17
    assert limpio.loc[0, "life_years_lost_web"] == 4.82
    assert limpio.loc[1, "country_web"] == "Ivory Coast"


def test_union_calcula_la_diferencia_con_el_promedio_nacional():
    base = {"iso3": "COL", "location_key": "COL|bogota", "source": "kaggle_csv", "date": pd.Timestamp("2026-09-08")}
    csv = pd.DataFrame([{**base, "pm2_5": 20.0}])
    api = pd.DataFrame([{**base, "source": "open_meteo_api", "date": pd.Timestamp("2026-09-09"), "pm2_5": 8.0}])
    indicadores = pd.DataFrame({"iso3": ["COL"], **{
        c: [1] for c in ["population", "population_year", "gdp", "gdp_year", "co2_per_capita", "co2_per_capita_year",
                         "total_ghg", "total_ghg_year", "energy_per_capita", "energy_per_capita_year"]}})
    web = pd.DataFrame({"iso3": ["COL"], "pm25_annual_web": [12.0], "life_years_lost_web": [0.7]})
    unido = unir_fuentes(csv, api, indicadores, umbral_pm25=15, web=web)
    assert unido["pm25_vs_national"].tolist() == [8.0, -4.0]
