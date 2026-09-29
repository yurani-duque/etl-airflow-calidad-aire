"""
Script Name: config_loader.py
Author: Yurani Duque
Fecha: 2026-09-29
Fase ETL: Soporte
Descripcion: carga config/config.yaml y resuelve rutas relativas a la raiz del proyecto.
Entradas: config/config.yaml, variable de entorno PROJECT_ROOT (opcional)
Salidas: diccionario de configuracion y rutas absolutas
Notas: dentro de Docker la raiz es /opt/airflow/app
"""

import os
from pathlib import Path

import yaml

ROOT = Path(os.getenv("PROJECT_ROOT", Path(__file__).resolve().parents[1]))


def load_config() -> dict:
    with open(ROOT / "config" / "config.yaml", "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def resolve(relative_path: str) -> Path:
    return ROOT / relative_path


def staging_dir(run_id: str) -> Path:
    safe_id = "".join(c if c.isalnum() or c in "-_" else "_" for c in run_id)
    path = resolve(load_config()["pipeline"]["staging_dir"]) / safe_id
    path.mkdir(parents=True, exist_ok=True)
    return path
