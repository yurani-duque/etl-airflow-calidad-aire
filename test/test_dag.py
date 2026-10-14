"""
Script Name: test_dag.py
Author: Yurani Duque
Fecha: 2026-10-06
Fase ETL: Pruebas
Descripcion: verifica que el DAG de Airflow carga sin errores y que el orden de las tareas
    respeta la logica del pipeline (cuatro extracciones, union, validacion y carga o cuarentena).
Entradas: dags/etl_calidad_aire.py
Salidas: resultado de pytest
Notas: requiere Airflow, se ejecuta dentro del contenedor
"""

from pathlib import Path

import pytest

pytest.importorskip("airflow")

from airflow.dag_processing.dagbag import DagBag  # noqa: E402

DAGS = Path(__file__).resolve().parents[1] / "dags"


@pytest.fixture(scope="module")
def dag():
    bolsa = DagBag(dag_folder=str(DAGS), bundle_path=DAGS, bundle_name="dags-folder")
    assert bolsa.import_errors == {}
    return bolsa.dags["etl_calidad_aire"]


def test_hay_cuatro_tareas_de_extraccion_independientes(dag):
    for tarea in ["extract_csv", "extract_db", "extract_api", "extract_web"]:
        assert dag.get_task(tarea).upstream_task_ids == set()


def test_la_union_espera_las_cuatro_transformaciones(dag):
    esperadas = {"transform_csv", "transform_owid", "transform_api", "transform_web"}
    assert dag.get_task("merge_sources").upstream_task_ids == esperadas


def test_la_carga_solo_ocurre_despues_de_validar(dag):
    assert dag.get_task("validate_quality").upstream_task_ids == {"merge_sources"}
    assert dag.get_task("load_dw").upstream_task_ids == {"validate_quality"}


def test_la_calidad_mala_envia_el_lote_a_cuarentena(dag):
    assert dag.get_task("validate_quality").downstream_task_ids == {"load_dw", "quarantine_data"}
    assert dag.get_task("quarantine_data").upstream_task_ids == {"validate_quality"}


def test_programacion_diaria(dag):
    assert dag.schedule == "@daily"


