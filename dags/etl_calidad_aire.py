"""
Script Name: etl_calidad_aire.py
Author: Yurani Duque
Fecha: 2026-10-05
Fase ETL: Orquestacion (Extraccion + Transformacion + Validacion + Carga)
Descripcion: DAG de Airflow que extrae datos de cuatro fuentes (CSV historico, base de datos
    de emisiones, API Open-Meteo y tabla web de contaminacion por pais), los transforma, los
    une, valida su calidad con Great Expectations y solo si pasan las validaciones los carga
    al Data Warehouse.
Entradas: CSV historico, tabla source.owid_co2, API Open-Meteo, pagina web (scraping)
Salidas: tablas del esquema dw en PostgreSQL
Notas: programado diariamente; si la validacion falla, la tarea load_dw no se ejecuta
"""

from datetime import datetime, timedelta

from airflow.sdk import Param, dag, get_current_context, task
from airflow.sdk.exceptions import AirflowFailException

ARGS = {"owner": "equipo-etl", "retries": 2, "retry_delay": timedelta(minutes=2)}


def registrar_fallo(context) -> None:
    from src.load_dw import registrar_ejecucion

    tarea = context["ti"].task_id
    registrar_ejecucion(context["run_id"], "failed", None, f"fallo en la tarea {tarea}: {context.get('exception')}")


def run_id_actual() -> str:
    return get_current_context()["run_id"]


@dag(
    dag_id="etl_calidad_aire",
    description="ETL de clima y calidad del aire con validacion de calidad antes de la carga",
    schedule="@daily",
    start_date=datetime(2026, 9, 29),
    catchup=False,
    max_active_runs=1,
    default_args={**ARGS, "on_failure_callback": registrar_fallo},
    tags=["etl", "ods13", "calidad-del-aire"],
    params={
        "prueba_falla_calidad": Param(
            False, type="boolean",
            description="Activar para inyectar datos invalidos y comprobar que la validacion bloquea la carga",
        )
    },
)
def etl_calidad_aire():
    @task
    def extract_csv() -> str:
        from src.extract_csv import extraer_csv
        return extraer_csv(run_id_actual())

    @task
    def extract_db() -> str:
        from src.extract_db import extraer_owid
        return extraer_owid(run_id_actual())

    @task
    def extract_api() -> str:
        from src.extract_api import extraer_api
        return extraer_api(run_id_actual())

    @task
    def extract_web() -> str:
        from src.extract_web import extraer_web
        return extraer_web(run_id_actual())

    @task
    def transform_csv() -> str:
        from src.transform_csv import transformar_csv
        return transformar_csv(run_id_actual())

    @task
    def transform_owid() -> str:
        from src.transform_owid import transformar_owid
        return transformar_owid(run_id_actual())

    @task
    def transform_api() -> str:
        from src.transform_api import transformar_api
        return transformar_api(run_id_actual())

    @task
    def transform_web() -> str:
        from src.transform_web import transformar_web
        return transformar_web(run_id_actual())

    @task
    def merge_sources() -> str:
        from src.merge import unir

        contexto = get_current_context()
        return unir(contexto["run_id"], bool(contexto["params"].get("prueba_falla_calidad")))

    @task
    def validate_quality() -> dict:
        from src.quality import validar_calidad

        resumen = validar_calidad(run_id_actual())
        if not resumen["aprobada"]:
            raise AirflowFailException(f"validacion fallida, fallos criticos: {resumen['fallos_criticos']}")
        return resumen

    @task
    def load_dw() -> int:
        from src.load_dw import cargar_dw, registrar_ejecucion

        run_id = run_id_actual()
        filas = cargar_dw(run_id)
        registrar_ejecucion(run_id, "success", filas, "validaciones aprobadas y carga completa")
        return filas

    csv_crudo, db_crudo, api_crudo, web_crudo = extract_csv(), extract_db(), extract_api(), extract_web()
    csv_limpio, owid_limpio, api_limpio, web_limpio = transform_csv(), transform_owid(), transform_api(), transform_web()
    unido, validado, cargado = merge_sources(), validate_quality(), load_dw()

    csv_crudo >> csv_limpio
    db_crudo >> owid_limpio
    api_crudo >> api_limpio
    web_crudo >> web_limpio
    [csv_limpio, owid_limpio, api_limpio, web_limpio] >> unido >> validado >> cargado


etl_calidad_aire()
