"""
Script Name: logger.py
Author: Yurani Duque
Fecha: 2026-09-29
Fase ETL: Soporte (utilizado por todas las fases)
Descripcion: configura un logger comun para las fases del pipeline, con salida a
    consola y a archivo con marca de tiempo.
Entradas: nombre del modulo que solicita el logger, variable de entorno LOG_DIR (opcional)
Salidas: objeto logging.Logger configurado
Notas: los logs quedan en logs/etl.log; si la carpeta no es escribible solo se usa la consola
"""

import logging
import os

LOG_DIR = os.getenv("LOG_DIR", "logs")
LOG_FILE = os.path.join(LOG_DIR, "etl.log")


def get_logger(name):
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(fmt)
    logger.addHandler(console_handler)

    try:
        os.makedirs(LOG_DIR, exist_ok=True)
        file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
        file_handler.setFormatter(fmt)
        logger.addHandler(file_handler)
    except OSError:
        logger.warning("no se pudo escribir en %s, se usa solo la consola", LOG_DIR)
    return logger
