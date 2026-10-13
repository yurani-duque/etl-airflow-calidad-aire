"""
Script Name: quarantine.py
Author: Yurani Duque
Fecha: 2026-10-13
Fase ETL: Cuarentena de datos
Descripcion: cuando la validacion de calidad falla, identifica las filas invalidas del lote, las guarda
    en cuarentena con su motivo, registra el lote en dw.quarantine_log y emite una alerta. El lote no se carga.
Entradas: staging/<run_id>/enriched.parquet y los resultados de dw.dq_validation_log
Salidas: data/quarantine/<run_id>/filas_cuarentena.parquet, reporte.json y una fila en dw.quarantine_log
Notas: los limites fisicos (RANGOS) son los mismos de la suite de hechos de Great Expectations
"""

import json
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from utils.config_loader import load_config, resolve, staging_dir
from utils.db_connector import get_engine
from utils.logger import get_logger

logger = get_logger("quarantine")

RANGOS = {
    "humidity": (0, 100), "cloud": (0, 100), "temperature_celsius": (-90, 60), "wind_kph": (0, 500),
    "pressure_mb": (400, 1100), "pm2_5": (0, 1000), "pm10": (0, 10_000), "us_epa_index": (1, 6),
}
CLAVES_NO_NULAS = ["location_key", "iso3", "date"]
CLAVE_UNICA = ["location_key", "date", "source"]


def filas_invalidas(df: pd.DataFrame) -> pd.DataFrame:
    """Devuelve las filas que incumplen alguna regla critica a nivel de fila, con la columna `motivo`."""
    motivos = pd.Series("", index=df.index, dtype="object")

    def marcar(mascara: pd.Series, motivo: str) -> None:
        motivos.loc[mascara] = (motivos.loc[mascara] + "; " + motivo).str.lstrip("; ")

    for columna, (minimo, maximo) in RANGOS.items():
        if columna in df.columns:
            valor = df[columna]
            marcar(valor.notna() & ((valor < minimo) | (valor > maximo)), f"{columna} fuera de rango ({minimo} a {maximo})")
    for columna in CLAVES_NO_NULAS:
        if columna in df.columns:
            marcar(df[columna].isna(), f"{columna} nulo")
    if all(c in df.columns for c in CLAVE_UNICA):
        marcar(df.duplicated(CLAVE_UNICA, keep="first"), "fila repetida (ubicacion, fecha y fuente)")

    invalidas = df[motivos != ""].copy()
    invalidas["motivo"] = motivos[motivos != ""]
    return invalidas


def reglas_fallidas(run_id: str) -> list[str]:
    """Reglas criticas que fallaron en la validacion de la ejecucion, leidas de la tabla de auditoria."""
    schema = load_config()["pipeline"]["dw_schema"]
    consulta = text(
        f"select suite_name || ': ' || expectation_type || coalesce(' (' || column_name || ')', '') "
        f"from {schema}.dq_validation_log where run_id = :run_id and not success and severity = 'critical'"
    )
    try:
        with get_engine().connect() as conn:
            return [fila[0] for fila in conn.execute(consulta, {"run_id": run_id}).all()]
    except Exception as error:  # la cuarentena no debe fallar por no poder leer el log
        logger.warning(f"no se pudieron leer las reglas fallidas: {error}")
        return []


def poner_en_cuarentena(run_id: str) -> dict:
    carpeta = Path(staging_dir(run_id))
    lote = pd.read_parquet(carpeta / "enriched.parquet")
    invalidas = filas_invalidas(lote)
    reglas = reglas_fallidas(run_id)

    destino = resolve("data/quarantine") / "".join(c if c.isalnum() or c in "-_" else "_" for c in run_id)
    destino.mkdir(parents=True, exist_ok=True)
    invalidas.to_parquet(destino / "filas_cuarentena.parquet", index=False)

    por_motivo = invalidas["motivo"].value_counts().to_dict()
    reporte = {
        "run_id": run_id, "filas_del_lote": int(len(lote)), "filas_en_cuarentena": int(len(invalidas)),
        "motivos": por_motivo, "reglas_criticas_fallidas": reglas,
    }
    (destino / "reporte.json").write_text(json.dumps(reporte, ensure_ascii=False, indent=2), encoding="utf-8")

    schema = load_config()["pipeline"]["dw_schema"]
    detalle = f"lote en cuarentena: {len(invalidas)} filas invalidas de {len(lote)}; reglas: {'; '.join(reglas) or 'ver reporte'}"
    with get_engine().begin() as conn:
        from src.load_dw import ejecutar_ddl

        ejecutar_ddl(conn)
        conn.execute(text(
            f"insert into {schema}.quarantine_log (run_id, batch_rows, quarantined_rows, failed_rules, path) "
            "values (:run_id, :lote, :invalidas, :reglas, :ruta) "
            "on conflict (run_id) do update set batch_rows = excluded.batch_rows, "
            "quarantined_rows = excluded.quarantined_rows, failed_rules = excluded.failed_rules, "
            "path = excluded.path, created_at = now()"
        ), {"run_id": run_id, "lote": len(lote), "invalidas": len(invalidas), "reglas": "; ".join(reglas), "ruta": str(destino)})

    from src.load_dw import registrar_ejecucion

    registrar_ejecucion(run_id, "quarantined", None, detalle[:900])
    logger.error(f"ALERTA: {detalle}. Filas guardadas en {destino}")
    return reporte
