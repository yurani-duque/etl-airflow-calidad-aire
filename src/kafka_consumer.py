"""
Script Name: kafka_consumer.py
Author: Yurani Duque
Fecha: 2026-11-19
Fase ETL: Streaming (consumidor e inferencia)
Descripcion: consume los eventos del topico de Kafka, descarta los invalidos, genera una prediccion del PM2.5 del
    dia siguiente para cada evento valido con el pipeline serializado (cargado una sola vez) y guarda el resultado
    en dw.predictions. Un callback permite que otra aplicacion (el dashboard) reciba cada prediccion al instante.
Entradas: topico de Kafka, models/pm25_next_day_model.pkl y models/model_metadata.json
Salidas: tabla dw.predictions y llamadas al callback `al_predecir`
Notas: los mensajes se procesan de uno en uno (una prediccion por mensaje); el modelo no se reentrena
"""

import argparse
import json
import os
import threading
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

import joblib
import pandas as pd
from confluent_kafka import Consumer, KafkaException
from sqlalchemy import text

from src.ml_contract import FEATURES, validar_evento
from utils.config_loader import load_config, resolve
from utils.db_connector import get_engine
from utils.logger import get_logger

logger = get_logger("kafka_consumer")


def servidores() -> str:
    return os.getenv("KAFKA_BOOTSTRAP_SERVERS") or load_config()["kafka"]["bootstrap_servers"]


def cargar_modelo() -> tuple[object, dict]:
    cfg = load_config()["ml"]
    ruta_modelo, ruta_meta = resolve(cfg["model_path"]), resolve(cfg["metadata_path"])
    if not Path(ruta_modelo).exists():
        raise FileNotFoundError(f"no existe {ruta_modelo}; entrena primero con: docker compose run --rm trainer")
    return joblib.load(ruta_modelo), json.loads(Path(ruta_meta).read_text(encoding="utf-8"))


def predecir_evento(modelo, evento: dict) -> float:
    """Aplica el pipeline ajustado (preprocesamiento + modelo) a un solo evento."""
    fila = pd.DataFrame([{campo: evento.get(campo) for campo in FEATURES}])
    return float(modelo.predict(fila)[0])


def guardar_predicciones(registros: list[dict]) -> None:
    if not registros:
        return
    schema = load_config()["pipeline"]["dw_schema"]
    sentencia = text(
        f"insert into {schema}.predictions (event_id, event_ts, consumed_at, location_key, feature_date, "
        "predicted_pm25_next_day, alert_who, model_version, latency_ms, features) values "
        "(:event_id, :event_ts, :consumed_at, :location_key, :feature_date, :predicted, :alert, :model_version, "
        ":latency_ms, cast(:features as jsonb)) on conflict (event_id) do nothing"
    )
    with get_engine().begin() as conn:
        conn.execute(sentencia, [
            {
                "event_id": r["event_id"], "event_ts": r["event_ts"], "consumed_at": r["consumed_at"],
                "location_key": r["location_key"], "feature_date": r["feature_date"], "predicted": r["predicted"],
                "alert": r["alert"], "model_version": r["model_version"], "latency_ms": r["latency_ms"],
                "features": json.dumps(r["features"]),
            }
            for r in registros
        ])


class ConsumidorPrediccion:
    """Consume eventos, predice y reporta. Se puede ejecutar en un hilo (dashboard) o desde la terminal."""

    def __init__(self, al_predecir=None, al_descartar=None, guardar=True, grupo: str | None = None):
        cfg = load_config()
        self.topico = cfg["kafka"]["topic"]
        self.grupo = grupo or cfg["kafka"]["consumer_group"]
        self.umbral_oms = cfg["quality"]["who_pm25_24h"]
        self.margen = cfg["ml"]["alert_threshold"]
        self.modelo, self.metadatos = cargar_modelo()
        self.version = self.metadatos["version"]
        self.al_predecir, self.al_descartar, self.guardar = al_predecir, al_descartar, guardar
        self.detener = threading.Event()
        self.procesados = 0
        self.descartados = 0

    def _consumidor(self) -> Consumer:
        return Consumer({
            "bootstrap.servers": servidores(), "group.id": self.grupo, "auto.offset.reset": "earliest",
            "enable.auto.commit": True, "auto.commit.interval.ms": 2000,
        })

    def procesar(self, bruto: bytes) -> dict | None:
        """Valida y predice un mensaje. Devuelve el registro de prediccion o None si el mensaje es invalido."""
        inicio = time.perf_counter()
        try:
            evento = json.loads(bruto)
        except (ValueError, TypeError):
            self._descartar(bruto, "el mensaje no es JSON valido")
            return None
        valido, motivo = validar_evento(evento)
        if not valido:
            self._descartar(evento, motivo)
            return None
        prediccion = predecir_evento(self.modelo, evento)
        ahora = datetime.now(timezone.utc)
        registro = {
            "event_id": evento["event_id"], "event_ts": evento.get("event_ts") or ahora.isoformat(),
            "consumed_at": ahora.isoformat(), "location_key": evento["location_key"],
            "location_name": evento["location_name"], "country": evento["country"],
            "latitude": evento["latitude"], "longitude": evento["longitude"],
            "feature_date": str(evento["feature_date"])[:10], "pm25_today": evento["pm2_5"],
            "predicted": round(prediccion, 3), "alert": prediccion > self.margen, "model_version": self.version,
            "latency_ms": round((time.perf_counter() - inicio) * 1000, 2),
            "features": {campo: evento.get(campo) for campo in FEATURES},
        }
        self.procesados += 1
        return registro

    def _descartar(self, contenido, motivo: str) -> None:
        self.descartados += 1
        logger.warning(f"mensaje descartado: {motivo}")
        if self.al_descartar:
            self.al_descartar({"motivo": motivo, "contenido": str(contenido)[:200], "momento": datetime.now(timezone.utc).isoformat()})

    def ejecutar(self, max_mensajes: int | None = None, inactividad: float | None = None) -> None:
        consumidor = self._consumidor()
        consumidor.subscribe([self.topico])
        pendientes: list[dict] = []
        ultimo_guardado, ultimo_mensaje = time.monotonic(), time.monotonic()
        logger.info(f"consumidor activo en {self.topico} (grupo {self.grupo}, modelo {self.version})")
        try:
            while not self.detener.is_set():
                mensaje = consumidor.poll(0.5)
                if mensaje is not None and not mensaje.error():
                    ultimo_mensaje = time.monotonic()
                    registro = self.procesar(mensaje.value())
                    if registro:
                        pendientes.append(registro)
                        if self.al_predecir:
                            self.al_predecir(registro)
                elif mensaje is not None and mensaje.error():
                    raise KafkaException(mensaje.error())
                if pendientes and (len(pendientes) >= 50 or time.monotonic() - ultimo_guardado > 2):
                    if self.guardar:
                        guardar_predicciones(pendientes)
                    pendientes, ultimo_guardado = [], time.monotonic()
                if max_mensajes and self.procesados + self.descartados >= max_mensajes:
                    break
                if inactividad and time.monotonic() - ultimo_mensaje > inactividad:
                    break
        finally:
            if pendientes and self.guardar:
                guardar_predicciones(pendientes)
            consumidor.close()
            logger.info(f"consumidor detenido: {self.procesados} predicciones, {self.descartados} descartados")


class EstadoEnVivo:
    """Estado en memoria que el dashboard lee. Lo alimenta el consumidor, no la base de datos."""

    def __init__(self, capacidad: int = 5000):
        self.bloqueo = threading.Lock()
        self.registros: deque = deque(maxlen=capacidad)
        self.descartes: deque = deque(maxlen=200)
        self.inicio = time.monotonic()
        self.total = 0

    def agregar(self, registro: dict) -> None:
        with self.bloqueo:
            self.registros.append(registro)
            self.total += 1

    def descartar(self, descarte: dict) -> None:
        with self.bloqueo:
            self.descartes.append(descarte)

    def copia(self) -> tuple[list[dict], list[dict], int]:
        with self.bloqueo:
            return list(self.registros), list(self.descartes), self.total


if __name__ == "__main__":
    analizador = argparse.ArgumentParser(description="Consume eventos de Kafka y genera predicciones")
    analizador.add_argument("--max-mensajes", type=int, default=None)
    analizador.add_argument("--inactividad", type=float, default=None, help="termina tras estos segundos sin mensajes")
    analizador.add_argument("--grupo", default=None)
    argumentos = analizador.parse_args()
    ConsumidorPrediccion(grupo=argumentos.grupo).ejecutar(argumentos.max_mensajes, argumentos.inactividad)

