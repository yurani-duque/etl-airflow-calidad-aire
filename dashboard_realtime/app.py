"""
Script Name: app.py
Author: Valentina Velasco
Fecha: 2026-11-20
Fase ETL: Visualizacion en tiempo real
Descripcion: dashboard de Streamlit que muestra las predicciones del PM2.5 del dia siguiente a medida que el
    consumidor de Kafka las genera. Un hilo consume el topico, aplica el modelo serializado a cada evento y
    alimenta un estado en memoria; la pantalla se refresca cada segundo leyendo ese estado (no consulta la base).
Entradas: topico de Kafka y models/pm25_next_day_model.pkl
Salidas: aplicacion web en el puerto 8502 (las predicciones tambien quedan en dw.predictions)
Notas: ejecutar con "docker compose up -d realtime"; el productor se lanza desde el DAG o con src.kafka_producer
"""

import threading
import time

import pandas as pd
import plotly.express as px
import streamlit as st

from src.kafka_consumer import ConsumidorPrediccion, EstadoEnVivo

st.set_page_config(page_title="Real-time PM2.5 forecast", layout="wide")


@st.cache_resource
def iniciar_consumo():
    """Arranca una sola vez el consumidor en un hilo. Si Kafka aun no esta listo, reintenta."""
    estado = EstadoEnVivo()
    estado.error = None
    consumidor = ConsumidorPrediccion(al_predecir=estado.agregar, al_descartar=estado.descartar)

    def correr():
        while True:
            try:
                estado.error = None
                consumidor.ejecutar()
            except Exception as error:  # noqa: BLE001 - se muestra en pantalla y se reintenta
                estado.error = str(error)
                time.sleep(5)

    threading.Thread(target=correr, daemon=True, name="kafka-consumer").start()
    return estado, consumidor


estado, consumidor = iniciar_consumo()
meta = consumidor.metadatos
umbral, margen = consumidor.umbral_oms, consumidor.margen

st.title("Real-time PM2.5 forecast for capital cities")
st.caption(
    f"Each Kafka event carries today's air quality, weather and country indicators; the consumer predicts tomorrow's PM2.5 "
    f"with the serialized model. An alert is raised when the forecast exceeds {margen} µg/m³, a precautionary margin below the WHO 24 h guideline ({umbral} µg/m³)."
)

with st.sidebar:
    st.header("Stream and model")
    st.write(f"**Topic:** `{consumidor.topico}`")
    st.write(f"**Consumer group:** `{consumidor.grupo}`")
    st.write(f"**Model:** `{meta['version']}`")
    mejor, base = meta["metricas"]["modelo"], meta["metricas"]["linea_base_persistencia"]
    st.write(f"**Test MAE:** {mejor['mae']} µg/m³ (persistence baseline: {base['mae']})")
    st.write(f"**Test period:** {meta['prueba_desde']} to {meta['prueba_hasta']}")
    st.caption("The dashboard reads messages through the Kafka consumer; it does not query the database.")


@st.fragment(run_every=1)
def panel():
    registros, descartes, total = estado.copia()
    if estado.error:
        st.error(f"Waiting for Kafka: {estado.error}")
    if not registros:
        st.info("Waiting for events. Publish some with the DAG task or `python -m src.kafka_producer`.")
        return

    df = pd.DataFrame(registros)
    df["consumed_at"] = pd.to_datetime(df["consumed_at"])
    ultimos = df.sort_values("consumed_at").groupby("location_key").tail(1)
    reciente = df[df["consumed_at"] > df["consumed_at"].max() - pd.Timedelta(seconds=10)]
    ritmo = len(reciente) / 10

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Predictions made", f"{total:,}")
    c2.metric("Invalid messages dropped", f"{len(descartes):,}")
    c3.metric("Cities with alert", f"{int(ultimos['alert'].sum())} of {len(ultimos)}")
    c4.metric("Messages per second", f"{ritmo:.1f}")
    c5.metric("Avg. inference latency", f"{df['latency_ms'].mean():.1f} ms")

    izq, der = st.columns([1, 1])
    top = ultimos.nlargest(15, "predicted").sort_values("predicted")
    top["city"] = top["location_name"] + " (" + top["country"] + ")"
    grafica = px.bar(top, x="predicted", y="city", orientation="h", color="alert",
                     color_discrete_map={True: "#c0392b", False: "#2e86c1"},
                     labels={"predicted": f"Predicted PM2.5 for tomorrow (µg/m³)<br>dotted: alert ({margen}) · dashed: WHO ({umbral})", "city": "", "alert": "Alert"})
    grafica.add_vline(x=margen, line_dash="dot")
    grafica.add_vline(x=umbral, line_dash="dash")
    grafica.update_layout(height=420, margin=dict(l=0, r=0, t=10, b=0), showlegend=False)
    izq.plotly_chart(grafica, width="stretch", key=f"top-{total}")

    mapa = px.scatter_geo(ultimos, lat="latitude", lon="longitude", color="predicted", size="predicted",
                          hover_name="location_name", color_continuous_scale="YlOrRd", range_color=(0, 80),
                          labels={"predicted": "Predicted PM2.5"})
    mapa.update_layout(height=420, margin=dict(l=0, r=0, t=10, b=0))
    der.plotly_chart(mapa, width="stretch", key=f"map-{total}")

    izq2, der2 = st.columns([1, 1])
    dispersion = px.scatter(ultimos, x="pm25_today", y="predicted", hover_name="location_name", opacity=0.7,
                            labels={"pm25_today": "PM2.5 today (µg/m³)", "predicted": "Predicted PM2.5 tomorrow (µg/m³)"})
    tope = float(max(ultimos["pm25_today"].max(), ultimos["predicted"].max()))
    dispersion.add_scatter(x=[0, tope], y=[0, tope], mode="lines", name="tomorrow = today", line=dict(dash="dash"))
    dispersion.update_layout(height=340, margin=dict(l=0, r=0, t=10, b=0))
    izq2.plotly_chart(dispersion, width="stretch", key=f"scatter-{total}")

    acumulado = df.set_index("consumed_at").resample("5s").size().cumsum().rename("predictions").reset_index()
    flujo = px.area(acumulado, x="consumed_at", y="predictions", labels={"consumed_at": "", "predictions": "Cumulative predictions"})
    flujo.update_layout(height=340, margin=dict(l=0, r=0, t=10, b=0))
    der2.plotly_chart(flujo, width="stretch", key=f"flow-{total}")

    st.subheader("Latest predictions")
    tabla = df.sort_values("consumed_at", ascending=False).head(15)[
        ["consumed_at", "location_name", "country", "feature_date", "pm25_today", "predicted", "alert", "latency_ms"]
    ]
    tabla["consumed_at"] = tabla["consumed_at"].dt.strftime("%H:%M:%S")
    st.dataframe(tabla.rename(columns={
        "consumed_at": "received", "location_name": "city", "feature_date": "data date",
        "pm25_today": "PM2.5 today", "predicted": "PM2.5 tomorrow (pred.)", "alert": "alert", "latency_ms": "latency (ms)",
    }), width="stretch", hide_index=True)

    if descartes:
        with st.expander(f"Dropped messages ({len(descartes)})"):
            st.dataframe(pd.DataFrame(descartes).tail(20), width="stretch", hide_index=True)


panel()




