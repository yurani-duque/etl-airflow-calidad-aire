"""
Script Name: extract_web.py
Author: Yurani Duque
Fecha: 2026-10-10
Fase ETL: Extraccion
Descripcion: descarga la pagina publica de Wikipedia "List of countries by air pollution" y
    extrae su tabla de paises (PM2.5 anual y anos de vida perdidos) con requests y BeautifulSoup.
Entradas: pagina web configurada en config.yaml (web.url)
Salidas: staging/<run_id>/web_raw.parquet con las columnas rank, country, pm25 y life_loss en texto
Notas: no limpia nada; si la pagina no responde o cambia de estructura la tarea falla y Airflow la reintenta
"""

from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

from utils.config_loader import load_config, staging_dir
from utils.logger import get_logger

logger = get_logger("extract_web")

COLUMNAS = ["rank", "country", "pm25", "life_loss"]


def parsear_tabla(html: str) -> pd.DataFrame:
    """Localiza la tabla de paises por su encabezado y devuelve sus filas como texto."""
    sopa = BeautifulSoup(html, "lxml")
    for tabla in sopa.select("table.wikitable"):
        filas = tabla.select("tr")
        if not filas:
            continue
        encabezado = " ".join(c.get_text(" ", strip=True) for c in filas[0].find_all(["th", "td"]))
        if "Country" in encabezado and "PM2.5" in encabezado:
            registros = []
            for fila in filas[1:]:
                celdas = [c.get_text(" ", strip=True) for c in fila.find_all(["td", "th"])]
                if len(celdas) >= len(COLUMNAS):
                    registros.append(celdas[:len(COLUMNAS)])
            return pd.DataFrame(registros, columns=COLUMNAS)
    raise ValueError("no se encontro la tabla de contaminacion por pais: la estructura de la pagina cambio")


def extraer_web(run_id: str) -> str:
    cfg = load_config()["web"]
    respuesta = requests.get(cfg["url"], headers={"User-Agent": cfg["user_agent"]}, timeout=cfg["timeout_seconds"])
    respuesta.raise_for_status()
    tabla = parsear_tabla(respuesta.text)
    salida = Path(staging_dir(run_id)) / "web_raw.parquet"
    tabla.to_parquet(salida, index=False)
    logger.info(f"filas extraidas de la pagina web: {len(tabla)}")
    return str(salida)
