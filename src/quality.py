"""
Script Name: quality.py
Author: Valentina Velasco
Fecha: 2026-10-04
Fase ETL: Validacion de calidad
Descripcion: define y ejecuta con Great Expectations cuatro suites de validacion (hechos,
    ubicaciones, indicadores de pais y datos web) y registra el resultado de cada
    expectativa en dw.dq_validation_log.
Entradas: staging/<run_id>/enriched.parquet, country_indicators.parquet y web_clean.parquet
Salidas: resumen de validacion (aprobada, fallos criticos), tabla dw.dq_validation_log y Data Docs en gx/
Notas: las expectativas criticas bloquean la carga; las de advertencia solo se registran
"""

from datetime import datetime, timedelta, timezone
from pathlib import Path

import great_expectations as gx
import great_expectations.expectations as gxe
import pandas as pd
from sqlalchemy import text

from utils.config_loader import ROOT, load_config, staging_dir
from utils.db_connector import get_engine
from utils.logger import get_logger

logger = get_logger("quality")

CRITICA = {"severity": "critical"}
ADVERTENCIA = {"severity": "warning"}


def suite_hechos(hoy: datetime, min_filas: int = 100_000) -> list:
    return [
        gxe.ExpectTableRowCountToBeBetween(min_value=min_filas, max_value=5_000_000, meta=CRITICA),
        gxe.ExpectColumnValuesToNotBeNull(column="location_key", meta=CRITICA),
        gxe.ExpectColumnValuesToNotBeNull(column="iso3", meta=CRITICA),
        gxe.ExpectColumnValuesToNotBeNull(column="date", meta=CRITICA),
        gxe.ExpectColumnValuesToBeInSet(column="source", value_set=["kaggle_csv", "open_meteo_api"], meta=CRITICA),
        gxe.ExpectCompoundColumnsToBeUnique(column_list=["location_key", "date", "source"], meta=CRITICA),
        gxe.ExpectColumnValuesToBeBetween(column="humidity", min_value=0, max_value=100, meta=CRITICA),
        gxe.ExpectColumnValuesToBeBetween(column="cloud", min_value=0, max_value=100, meta=CRITICA),
        gxe.ExpectColumnValuesToBeBetween(column="temperature_celsius", min_value=-90, max_value=60, meta=CRITICA),
        gxe.ExpectColumnValuesToBeBetween(column="wind_kph", min_value=0, max_value=500, mostly=0.999, meta=CRITICA),
        gxe.ExpectColumnValuesToBeBetween(column="pressure_mb", min_value=400, max_value=1100, mostly=0.999, meta=CRITICA),
        gxe.ExpectColumnValuesToBeBetween(column="pm2_5", min_value=0, max_value=1000, mostly=0.999, meta=CRITICA),
        gxe.ExpectColumnValuesToBeBetween(column="pm10", min_value=0, max_value=10_000, mostly=0.9999, meta=CRITICA),
        gxe.ExpectColumnValuesToBeBetween(column="us_epa_index", min_value=1, max_value=6, mostly=0.99, meta=CRITICA),
        gxe.ExpectColumnValuesToNotBeNull(column="pm2_5", mostly=0.95, meta=ADVERTENCIA),
        gxe.ExpectColumnValuesToNotBeNull(column="temperature_celsius", mostly=0.99, meta=ADVERTENCIA),
        gxe.ExpectColumnMaxToBeBetween(
            column="date", min_value=hoy - timedelta(days=4), max_value=hoy + timedelta(days=1), meta=ADVERTENCIA
        ),
    ]


def suite_ubicaciones(iso3_con_indicadores: list) -> list:
    return [
        gxe.ExpectColumnValuesToBeUnique(column="location_key", meta=CRITICA),
        gxe.ExpectColumnValuesToNotBeNull(column="location_name", meta=CRITICA),
        gxe.ExpectColumnValuesToBeBetween(column="latitude", min_value=-90, max_value=90, meta=CRITICA),
        gxe.ExpectColumnValuesToBeBetween(column="longitude", min_value=-180, max_value=180, meta=CRITICA),
        gxe.ExpectColumnValuesToMatchRegex(column="iso3", regex=r"^[A-Z]{3}$", meta=CRITICA),
        gxe.ExpectColumnValuesToBeInSet(column="iso3", value_set=iso3_con_indicadores, mostly=0.99, meta=ADVERTENCIA),
    ]


def suite_indicadores() -> list:
    return [
        gxe.ExpectTableRowCountToBeBetween(min_value=150, max_value=260, meta=CRITICA),
        gxe.ExpectColumnValuesToBeUnique(column="iso3", meta=CRITICA),
        gxe.ExpectColumnValuesToNotBeNull(column="iso3", meta=CRITICA),
        gxe.ExpectColumnValuesToBeBetween(column="population", min_value=1, mostly=0.99, meta=CRITICA),
        gxe.ExpectColumnValuesToBeBetween(column="co2_per_capita", min_value=0, max_value=100, mostly=0.98, meta=ADVERTENCIA),
        gxe.ExpectColumnValuesToNotBeNull(column="gdp", mostly=0.7, meta=ADVERTENCIA),
    ]


def suite_web() -> list:
    return [
        gxe.ExpectTableRowCountToBeBetween(min_value=150, max_value=260, meta=CRITICA),
        gxe.ExpectColumnValuesToNotBeNull(column="iso3", meta=CRITICA),
        gxe.ExpectColumnValuesToBeUnique(column="iso3", meta=CRITICA),
        gxe.ExpectColumnValuesToMatchRegex(column="iso3", regex=r"^[A-Z]{3}$", meta=CRITICA),
        gxe.ExpectColumnValuesToBeBetween(column="pm25_annual_web", min_value=0, max_value=100, meta=CRITICA),
        gxe.ExpectColumnValuesToBeBetween(column="life_years_lost_web", min_value=0, max_value=10, mostly=0.99, meta=ADVERTENCIA),
    ]


def obtener_contexto(efimero: bool = False):
    if efimero:
        return gx.get_context(mode="ephemeral")
    return gx.get_context(mode="file", project_root_dir=str(ROOT))


def ejecutar_suite(contexto, nombre: str, expectativas: list, datos: pd.DataFrame):
    origen = contexto.data_sources.add_or_update_pandas(name="staging")
    try:
        activo = origen.get_asset(nombre)
    except LookupError:
        activo = origen.add_dataframe_asset(name=nombre)
    try:
        lote = activo.get_batch_definition("dataset_completo")
    except KeyError:
        lote = activo.add_batch_definition_whole_dataframe("dataset_completo")

    suite = contexto.suites.add_or_update(gx.ExpectationSuite(name=nombre, expectations=expectativas))
    definicion = contexto.validation_definitions.add_or_update(
        gx.ValidationDefinition(name=f"validacion_{nombre}", data=lote, suite=suite)
    )
    return definicion.run(batch_parameters={"dataframe": datos})


def filas_de_log(run_id: str, nombre: str, resultado) -> list[dict]:
    filas = []
    for r in resultado.results:
        config = r.expectation_config
        detalle = r.result or {}
        filas.append({
            "run_id": run_id,
            "suite_name": nombre,
            "expectation_type": config.type,
            "column_name": config.kwargs.get("column") or ",".join(config.kwargs.get("column_list", [])) or None,
            "severity": (config.meta or {}).get("severity", "critical"),
            "success": bool(r.success),
            "element_count": detalle.get("element_count"),
            "unexpected_percent": detalle.get("unexpected_percent"),
            "observed_value": str(detalle.get("observed_value"))[:500] if "observed_value" in detalle else None,
        })
    return filas


def guardar_log(filas: list[dict]) -> None:
    schema = load_config()["pipeline"]["dw_schema"]
    sentencia = text(
        f"insert into {schema}.dq_validation_log (run_id, suite_name, expectation_type, column_name, severity, "
        "success, element_count, unexpected_percent, observed_value) values (:run_id, :suite_name, "
        ":expectation_type, :column_name, :severity, :success, :element_count, :unexpected_percent, :observed_value)"
    )
    with get_engine().begin() as conn:
        conn.execute(text((Path(ROOT) / "dwh_version" / "001_create_dw_schema.sql").read_text(encoding="utf-8")))
        conn.execute(sentencia, filas)


def evaluar_suites(contexto, run_id: str, enriquecido: pd.DataFrame, indicadores: pd.DataFrame,
                   hoy: datetime, min_filas: int = 100_000, web: pd.DataFrame | None = None):
    """Ejecuta las suites (cuatro si hay datos web) y devuelve (resumen, filas_de_log)."""
    ubicaciones = enriquecido.drop_duplicates("location_key")
    suites = [
        ("suite_hechos_calidad_aire", suite_hechos(hoy, min_filas), enriquecido),
        ("suite_ubicaciones", suite_ubicaciones(sorted(indicadores["iso3"].unique())), ubicaciones),
        ("suite_indicadores_pais", suite_indicadores(), indicadores.rename(columns={"country_owid": "country"})),
    ]
    if web is not None:
        suites.append(("suite_web_pm25", suite_web(), web))

    log, suites_resumen = [], {}
    for nombre, expectativas, datos in suites:
        resultado = ejecutar_suite(contexto, nombre, expectativas, datos)
        filas = filas_de_log(run_id, nombre, resultado)
        log += filas
        criticos = [f for f in filas if not f["success"] and f["severity"] == "critical"]
        avisos = [f for f in filas if not f["success"] and f["severity"] != "critical"]
        suites_resumen[nombre] = {"aprobada": not criticos, "fallos_criticos": len(criticos), "advertencias": len(avisos)}
        logger.info(f"{nombre}: {len(filas)} expectativas, {len(criticos)} criticas fallidas, {len(avisos)} advertencias")

    total_criticos = sum(s["fallos_criticos"] for s in suites_resumen.values())
    return {"aprobada": total_criticos == 0, "fallos_criticos": total_criticos, "suites": suites_resumen}, log


def validar_calidad(run_id: str) -> dict:
    carpeta = Path(staging_dir(run_id))
    enriquecido = pd.read_parquet(carpeta / "enriched.parquet")
    indicadores = pd.read_parquet(carpeta / "country_indicators.parquet")
    web = pd.read_parquet(carpeta / "web_clean.parquet")

    contexto = obtener_contexto()
    resumen, log = evaluar_suites(
        contexto, run_id, enriquecido, indicadores, datetime.now(timezone.utc).replace(tzinfo=None), web=web
    )
    guardar_log(log)
    contexto.build_data_docs()
    return resumen
