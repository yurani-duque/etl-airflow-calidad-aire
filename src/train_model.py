"""
Script Name: train_model.py
Author: Valentina Velasco
Fecha: 2026-11-19
Fase ETL: Machine learning (entrenamiento)
Descripcion: entrena un modelo que predice el PM2.5 del dia siguiente en cada ciudad a partir del dataset
    dw.v_prediction_features (modelo de cambio diario, ver src/ml_model.py). Compara con dos lineas base, evalua
    con una division por fecha y guarda el modelo completo (preprocesamiento incluido) y sus metadatos.
Entradas: vista dw.v_prediction_features del Data Warehouse
Salidas: models/pm25_next_day_model.pkl y models/model_metadata.json
Notas: la prueba son los ultimos 90 dias; el entrenamiento termina un dia antes del inicio de la prueba para
    que el objetivo de ninguna fila de entrenamiento caiga dentro del periodo de prueba
"""

import json
from datetime import datetime, timezone
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
import sklearn
from sklearn.metrics import f1_score, mean_absolute_error, mean_squared_error, precision_score, r2_score, recall_score
from sqlalchemy import text

from src.ml_contract import FEATURES, FEATURES_CATEGORICAS, TARGET
from src.ml_model import ModeloCambioDiario
from utils.config_loader import load_config, resolve
from utils.db_connector import get_engine
from utils.logger import get_logger

logger = get_logger("train_model")

MODELO = "cambio_diario_hgb_ensamble"


def cargar_dataset() -> pd.DataFrame:
    schema = load_config()["pipeline"]["dw_schema"]
    with get_engine().connect() as conn:
        df = pd.read_sql(text(f"select * from {schema}.v_prediction_features"), conn)
    df["feature_date"] = pd.to_datetime(df["feature_date"])
    return df


def dividir_por_fecha(df: pd.DataFrame, dias_prueba: int):
    """Entrenamiento con lo antiguo y prueba con lo reciente. Solo filas con objetivo conocido."""
    conocido = df[df[TARGET].notna() & df["pm2_5"].notna()].copy()
    ultimo = conocido["feature_date"].max()
    inicio_prueba = ultimo - pd.Timedelta(days=dias_prueba - 1)
    entrenamiento = conocido[conocido["feature_date"] < inicio_prueba - pd.Timedelta(days=1)]
    prueba = conocido[conocido["feature_date"] >= inicio_prueba]
    return entrenamiento, prueba


def construir_pipeline() -> ModeloCambioDiario:
    return ModeloCambioDiario()


def metricas(real: pd.Series, previsto: np.ndarray, umbral: float, margen_alerta: float | None = None) -> dict:
    """Error de la prediccion y calidad de la alerta. Excedencia real: PM2.5 > umbral de la OMS.
    Alerta: prediccion > margen_alerta (si no se indica, el mismo umbral)."""
    margen_alerta = umbral if margen_alerta is None else margen_alerta
    real_alerta, prev_alerta = np.asarray(real) > umbral, np.asarray(previsto) > margen_alerta
    return {
        "mae": round(float(mean_absolute_error(real, previsto)), 3),
        "rmse": round(float(np.sqrt(mean_squared_error(real, previsto))), 3),
        "r2": round(float(r2_score(real, previsto)), 4),
        "alerta_precision": round(float(precision_score(real_alerta, prev_alerta, zero_division=0)), 4),
        "alerta_recall": round(float(recall_score(real_alerta, prev_alerta, zero_division=0)), 4),
        "alerta_f1": round(float(f1_score(real_alerta, prev_alerta, zero_division=0)), 4),
    }


def entrenar(dias_prueba: int | None = None) -> dict:
    cfg = load_config()
    umbral = cfg["quality"]["who_pm25_24h"]
    margen = cfg["ml"]["alert_threshold"]
    dias_prueba = dias_prueba or cfg["ml"]["test_days"]

    df = cargar_dataset()
    entrenamiento, prueba = dividir_por_fecha(df, dias_prueba)
    logger.info(f"filas de entrenamiento: {len(entrenamiento):,} | filas de prueba: {len(prueba):,}")

    pipeline = construir_pipeline()
    pipeline.fit(entrenamiento[FEATURES], entrenamiento[TARGET])
    previsto = pipeline.predict(prueba[FEATURES])

    persistencia = prueba["pm2_5"].to_numpy()
    media_7d = prueba["pm25_mean_7d"].fillna(prueba["pm2_5"]).to_numpy()
    # la alerta se compara en igualdad de condiciones: todos alertan cuando su valor supera el margen
    resultados = {
        "modelo": metricas(prueba[TARGET], previsto, umbral, margen),
        "linea_base_persistencia": metricas(prueba[TARGET], persistencia, umbral, margen),
        "linea_base_media_7d": metricas(prueba[TARGET], media_7d, umbral, margen),
    }
    for nombre, valores in resultados.items():
        logger.info(f"{nombre}: {valores}")

    # error por ciudad en la prueba, para el informe
    por_ciudad = (
        prueba.assign(error=np.abs(prueba[TARGET].to_numpy() - previsto))
        .groupby("location_name")["error"].mean().sort_values(ascending=False).head(10).round(2).to_dict()
    )

    version = f"{MODELO}-{datetime.now(timezone.utc):%Y%m%d}"
    metadata = {
        "version": version,
        "modelo": MODELO,
        "objetivo": TARGET,
        "descripcion_objetivo": "PM2.5 del dia siguiente (ug/m3) en cada ciudad",
        "umbral_alerta_oms": umbral,
        "margen_alerta": margen,
        "regla_alerta": f"se alerta cuando el PM2.5 previsto supera {margen} ug/m3 (margen de precaucion bajo la guia de {umbral})",
        "features": FEATURES,
        "features_categoricas": FEATURES_CATEGORICAS,
        "entrenado_en": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "sklearn": sklearn.__version__,
        "filas_entrenamiento": int(len(entrenamiento)),
        "filas_prueba": int(len(prueba)),
        "entrenamiento_desde": str(entrenamiento["feature_date"].min().date()),
        "entrenamiento_hasta": str(entrenamiento["feature_date"].max().date()),
        "prueba_desde": str(prueba["feature_date"].min().date()),
        "prueba_hasta": str(prueba["feature_date"].max().date()),
        "metricas": resultados,
        "mayor_error_por_ciudad": por_ciudad,
    }

    ruta_modelo, ruta_meta = resolve(cfg["ml"]["model_path"]), resolve(cfg["ml"]["metadata_path"])
    ruta_modelo.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(pipeline, ruta_modelo)
    Path(ruta_meta).write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info(f"modelo guardado en {ruta_modelo}")
    return metadata


if __name__ == "__main__":
    entrenar()

