"""
Script Name: test_ml_streaming.py
Author: Valentina Velasco
Fecha: 2026-11-21
Fase ETL: Pruebas
Descripcion: verifica el contrato de eventos de Kafka (estructura, validacion y ausencia del objetivo), la
    construccion de eventos del productor, la division por fecha del entrenamiento y la inferencia del
    consumidor sobre un mensaje, sin necesidad de un broker.
Entradas: datos simulados
Salidas: resultado de pytest
Notas: requiere scikit-learn y confluent-kafka; se ejecuta con "docker compose run --rm streaming-tests"
"""

import json

import numpy as np
import pandas as pd
import pytest

pytest.importorskip("sklearn")
pytest.importorskip("confluent_kafka")

from src.kafka_consumer import ConsumidorPrediccion, predecir_evento  # noqa: E402
from src.kafka_producer import construir_evento  # noqa: E402
from src.ml_contract import CAMPOS_EVENTO, FEATURES, TARGET, validar_evento  # noqa: E402
from src.train_model import construir_pipeline, dividir_por_fecha  # noqa: E402


def _fila(**cambios):
    fila = {
        "location_key": "COL|bogota", "location_name": "Bogota", "country": "Colombia", "iso3": "COL",
        "latitude": 4.6, "longitude": -74.08, "feature_date": pd.Timestamp("2026-10-01"),
        "pm2_5": 12.0, "pm10": 20.0, "co": 200.0, "no2": 10.0, "so2": 2.0, "ozone": 30.0,
        "temperature_celsius": 15.0, "humidity": 70.0, "wind_kph": 10.0, "pressure_mb": 1010.0, "precip_mm": 0.0,
        "cloud": 40.0, "uv_index": 3.0, "pm25_lag1": 11.0, "pm25_lag2": 10.5, "pm25_lag3": 12.0, "pm25_lag7": 14.0, "pm25_mean_3d": 11.2, "pm25_mean_7d": 12.5,
        "pm25_std_7d": 2.1, "pm25_mean_30d": 13.0, "ratio_today_7d": 0.96, "delta_temp": 0.5, "delta_pressure": -1.0, "delta_wind": 2.0,
        "month": 10, "day_of_week": 4, "co2_per_capita": 1.8, "gdp_per_capita": 6500.0,
        "energy_per_capita": 14000.0, "pm25_annual_web": 18.0,
    }
    fila.update(cambios)
    return pd.Series(fila)


def test_el_evento_tiene_la_estructura_documentada_y_no_incluye_el_objetivo():
    evento = construir_evento(_fila())
    assert set(CAMPOS_EVENTO) == set(evento)
    assert TARGET not in evento
    assert evento["feature_date"] == "2026-10-01"
    json.dumps(evento)  # debe ser serializable


def test_los_nulos_viajan_como_null_y_el_evento_sigue_siendo_valido():
    evento = construir_evento(_fila(pm25_lag7=np.nan, gdp_per_capita=np.nan))
    assert evento["pm25_lag7"] is None
    assert validar_evento(evento) == (True, "")


@pytest.mark.parametrize("cambio, texto", [
    ({"humidity": 150.0}, "fuera de rango"),
    ({"pm2_5": None}, "obligatorios"),
    ({"pm2_5": "alto"}, "no es numerico"),
    ({"feature_date": "ayer"}, "fecha"),
])
def test_los_eventos_invalidos_se_rechazan_con_su_motivo(cambio, texto):
    evento = {**construir_evento(_fila()), **cambio}
    valido, motivo = validar_evento(evento)
    assert not valido and texto in motivo


def test_un_evento_que_incluye_el_objetivo_se_rechaza_por_fuga_de_datos():
    evento = {**construir_evento(_fila()), TARGET: 20.0}
    assert validar_evento(evento)[0] is False


def _dataset(n_dias=200, ciudades=("COL", "CHL", "IND")):
    generador = np.random.default_rng(0)
    filas = []
    for i, iso in enumerate(ciudades):
        nivel = 10 + 15 * i
        serie = nivel + 5 * np.sin(np.arange(n_dias) / 20) + generador.normal(0, 2, n_dias)
        for dia in range(n_dias - 1):
            fila = _fila(iso3=iso, location_key=f"{iso}|x", feature_date=pd.Timestamp("2026-01-01") + pd.Timedelta(days=dia),
                         pm2_5=serie[dia], pm25_lag1=serie[dia - 1] if dia else np.nan, pm25_mean_7d=serie[max(0, dia - 7):dia + 1].mean())
            fila[TARGET] = serie[dia + 1]
            filas.append(fila)
    return pd.DataFrame(filas)


def test_la_division_por_fecha_no_mezcla_entrenamiento_y_prueba():
    df = _dataset()
    entrenamiento, prueba = dividir_por_fecha(df, 30)
    assert entrenamiento["feature_date"].max() < prueba["feature_date"].min()
    # el objetivo de la ultima fila de entrenamiento (dia siguiente) no cae dentro de la prueba
    assert entrenamiento["feature_date"].max() + pd.Timedelta(days=1) < prueba["feature_date"].min()


def test_el_pipeline_entrena_y_predice_un_solo_evento():
    df = _dataset()
    entrenamiento, _ = dividir_por_fecha(df, 30)
    pipeline = construir_pipeline().fit(entrenamiento[FEATURES], entrenamiento[TARGET])
    evento = construir_evento(_fila(iso3="COL"))
    prediccion = predecir_evento(pipeline, evento)
    assert 0 < prediccion < 100


def test_un_pais_desconocido_no_rompe_la_inferencia():
    df = _dataset()
    entrenamiento, _ = dividir_por_fecha(df, 30)
    pipeline = construir_pipeline().fit(entrenamiento[FEATURES], entrenamiento[TARGET])
    assert np.isfinite(predecir_evento(pipeline, construir_evento(_fila(iso3="ZZZ"))))


def test_el_consumidor_procesa_validos_y_descarta_invalidos(monkeypatch, tmp_path):
    df = _dataset()
    entrenamiento, _ = dividir_por_fecha(df, 30)
    pipeline = construir_pipeline().fit(entrenamiento[FEATURES], entrenamiento[TARGET])
    monkeypatch.setattr("src.kafka_consumer.cargar_modelo", lambda: (pipeline, {"version": "prueba"}))
    descartes = []
    consumidor = ConsumidorPrediccion(al_descartar=descartes.append, guardar=False)

    bueno = consumidor.procesar(json.dumps(construir_evento(_fila())).encode())
    malo = consumidor.procesar(json.dumps({**construir_evento(_fila()), "humidity": 999}).encode())
    roto = consumidor.procesar(b"esto no es json")

    assert bueno["predicted"] > 0 and bueno["model_version"] == "prueba" and "alert" in bueno
    assert malo is None and roto is None
    assert consumidor.procesados == 1 and consumidor.descartados == 2 and len(descartes) == 2

