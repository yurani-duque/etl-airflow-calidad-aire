"""
Script Name: kafka_producer.py
Author: Yurani Duque
Fecha: 2026-11-19
Fase ETL: Streaming (productor)
Descripcion: simula un flujo en tiempo real: lee de dw.v_prediction_features (derivada de la tabla de hechos) las
    filas de los ultimos dias disponibles y publica un evento JSON por ciudad y dia en un topico de Kafka, con una
    pausa corta entre mensajes. Cada evento trae las variables de entrada del modelo y un identificador.
Entradas: vista dw.v_prediction_features, broker de Kafka (KAFKA_BOOTSTRAP_SERVERS o config.yaml)
Salidas: eventos en el topico configurado (por defecto air_quality_features)
Notas: el evento NO incluye el objetivo (PM2.5 del dia siguiente); la clave del mensaje es location_key, por lo que
    todos los eventos de una ciudad llegan en orden a la misma particion
"""

import argparse
import json
import os
import time
import uuid

import pandas as pd
from confluent_kafka import KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from sqlalchemy import text

from src.ml_contract import CAMPOS_EVENTO, COLUMNAS_VISTA, ahora_iso
from utils.config_loader import load_config
from utils.db_connector import get_engine
from utils.logger import get_logger

logger = get_logger("kafka_producer")


def servidores() -> str:
    return os.getenv("KAFKA_BOOTSTRAP_SERVERS") or load_config()["kafka"]["bootstrap_servers"]


def asegurar_topico(topico: str, particiones: int) -> None:
    admin = AdminClient({"bootstrap.servers": servidores()})
    if topico in admin.list_topics(timeout=15).topics:
        return
    for futuro in admin.create_topics([NewTopic(topico, num_partitions=particiones, replication_factor=1)]).values():
        try:
            futuro.result()
        except KafkaException as error:
            if "TOPIC_ALREADY_EXISTS" not in str(error):
                raise
    logger.info(f"topico creado: {topico} ({particiones} particiones)")


def leer_filas(dias: int, ciudades: int | None = None) -> pd.DataFrame:
    """Los ultimos `dias` dias disponibles de cada ciudad (no todas las ciudades tienen el mismo ultimo dia).
    Solo se leen las columnas de entrada, nunca el objetivo."""
    schema = load_config()["pipeline"]["dw_schema"]
    columnas = ", ".join(COLUMNAS_VISTA)
    consulta = text(
        f"select {columnas} from ("
        f"  select v.*, max(v.feature_date) over (partition by v.location_key) as ultimo_dia "
        f"  from {schema}.v_prediction_features v where v.pm2_5 is not null"
        f") ultimos where feature_date > ultimo_dia - :dias order by feature_date, location_key"
    )
    with get_engine().connect() as conn:
        df = pd.read_sql(consulta, conn, params={"dias": dias})
    if ciudades:
        df = df[df["location_key"].isin(df["location_key"].drop_duplicates().head(ciudades))]
    return df


def construir_evento(fila: pd.Series) -> dict:
    """Convierte una fila de la vista en un evento. Los nulos viajan como null y las fechas como texto ISO."""
    evento = {"event_id": str(uuid.uuid4()), "event_ts": ahora_iso()}
    for campo in CAMPOS_EVENTO:
        if campo in evento:
            continue
        valor = fila[campo]
        if campo == "feature_date":
            valor = str(pd.Timestamp(valor).date())
        elif pd.isna(valor):
            valor = None
        elif hasattr(valor, "item"):
            valor = valor.item()
        evento[campo] = valor
    return evento


def publicar(filas: pd.DataFrame, retraso: float | None = None, topico: str | None = None) -> dict:
    cfg = load_config()["kafka"]
    topico = topico or cfg["topic"]
    retraso = cfg["producer_delay_seconds"] if retraso is None else retraso
    asegurar_topico(topico, cfg["partitions"])

    errores = []
    productor = Producer({"bootstrap.servers": servidores(), "linger.ms": 5, "acks": "all"})

    def al_entregar(error, mensaje):
        if error is not None:
            errores.append(str(error))

    enviados = 0
    for _, fila in filas.iterrows():
        evento = construir_evento(fila)
        productor.produce(topico, key=evento["location_key"], value=json.dumps(evento, ensure_ascii=False), callback=al_entregar)
        productor.poll(0)
        enviados += 1
        if retraso:
            time.sleep(retraso)
    productor.flush(30)
    if errores:
        raise RuntimeError(f"{len(errores)} mensajes no se entregaron; primer error: {errores[0]}")
    logger.info(f"eventos publicados en {topico}: {enviados}")
    return {"topico": topico, "eventos": enviados}


def publicar_ultimos_dias(dias: int = 1, retraso: float | None = None, ciudades: int | None = None) -> dict:
    filas = leer_filas(dias, ciudades)
    if filas.empty:
        raise RuntimeError("no hay filas para publicar: la tabla de hechos esta vacia")
    logger.info(f"se publicaran {len(filas)} eventos de {filas['feature_date'].min()} a {filas['feature_date'].max()}")
    return publicar(filas, retraso)


if __name__ == "__main__":
    analizador = argparse.ArgumentParser(description="Publica en Kafka las variables de entrada del modelo")
    analizador.add_argument("--dias", type=int, default=1, help="cuantos dias recientes publicar")
    analizador.add_argument("--retraso", type=float, default=None, help="segundos entre mensajes")
    analizador.add_argument("--ciudades", type=int, default=None, help="limitar el numero de ciudades (pruebas)")
    argumentos = analizador.parse_args()
    publicar_ultimos_dias(argumentos.dias, argumentos.retraso, argumentos.ciudades)
