"""
Script Name: app.py
Author: Valentina Velasco
Fecha: 2026-10-08
Fase ETL: Visualizacion
Descripcion: dashboard de Streamlit sobre el Data Warehouse validado. Responde que ciudades
    superan la guia de la OMS para PM2.5, como se comparan con el promedio nacional, como se
    relaciona el aire con las emisiones y el ingreso de cada pais, como evoluciona en el tiempo
    y en que estado esta la calidad de los datos.
Entradas: tablas del esquema dw (solo datos ya validados y cargados por el DAG)
Salidas: aplicacion web en el puerto 8501
Notas: ejecutar con "docker compose up -d dashboard" o "uv run streamlit run dashboard/app.py"
"""

import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import bindparam, text

from utils.config_loader import load_config
from utils.db_connector import get_engine

CFG = load_config()
SCHEMA = CFG["pipeline"]["dw_schema"]
UMBRAL_OMS = CFG["quality"]["who_pm25_24h"]

st.set_page_config(page_title="Calidad del aire y emisiones", layout="wide")


@st.cache_resource
def engine():
    return get_engine()


@st.cache_data(ttl=300)
def consulta(sql: str, **params) -> pd.DataFrame:
    sentencia = text(sql)
    for nombre, valor in params.items():
        if isinstance(valor, (list, tuple)):
            sentencia = sentencia.bindparams(bindparam(nombre, expanding=True))
    with engine().connect() as conn:
        return pd.read_sql(sentencia, conn, params=params)


rango = consulta(f"select min(full_date) as ini, max(full_date) as fin from {SCHEMA}.dim_date")
fuentes = consulta(f"select source_id, source_name from {SCHEMA}.dim_source order by 1")

st.title("Calidad del aire y emisiones en ciudades capitales")
st.caption("ODS 11.6 (calidad del aire urbano) y ODS 13 (acción por el clima). Datos validados con Great Expectations antes de cargarse.")

with st.sidebar:
    st.header("Filtros")
    ini, fin = st.date_input(
        "Rango de fechas", value=(rango.loc[0, "ini"], rango.loc[0, "fin"]),
        min_value=rango.loc[0, "ini"], max_value=rango.loc[0, "fin"],
    )
    elegidas = st.multiselect("Fuentes", fuentes["source_name"].tolist(), default=fuentes["source_name"].tolist())
    top_n = st.slider("Ciudades en el ranking", 5, 30, 15)

ids_fuente = fuentes.loc[fuentes["source_name"].isin(elegidas), "source_id"].tolist() or [0]
FILTRO = "d.full_date between :ini and :fin and f.source_id in :fuentes"
PARAMS = {"ini": ini, "fin": fin, "fuentes": ids_fuente}

kpi = consulta(f"""
    select count(distinct f.location_id) as ciudades, count(*) as registros, max(d.full_date) as ultima,
           100 * avg(f.pm25_exceeds_who::int) as pct_excede
    from {SCHEMA}.fact_air_quality f join {SCHEMA}.dim_date d using (date_id)
    where {FILTRO}
""", **PARAMS).iloc[0]
ultimo_run = consulta(f"select run_id, status, finished_at from {SCHEMA}.etl_run_log order by finished_at desc limit 1")

c1, c2, c3, c4 = st.columns(4)
c1.metric("Ciudades monitoreadas", f"{int(kpi['ciudades']):,}")
c2.metric("Registros ciudad-día", f"{int(kpi['registros']):,}")
c3.metric(f"Días sobre la guía OMS ({UMBRAL_OMS} µg/m³)", f"{kpi['pct_excede']:.1f}%")
c4.metric("Último dato disponible", str(kpi["ultima"]))

tab_ciudades, tab_emisiones, tab_tiempo, tab_calidad = st.tabs(
    ["Ciudades", "Aire vs emisiones e ingreso", "Evolución", "Calidad de los datos"]
)

with tab_ciudades:
    ciudades = consulta(f"""
        select l.location_name, l.country, l.latitude, l.longitude, avg(f.pm2_5) as pm25,
               100 * avg(f.pm25_exceeds_who::int) as pct_excede, count(*) as dias
        from {SCHEMA}.fact_air_quality f join {SCHEMA}.dim_location l using (location_id)
             join {SCHEMA}.dim_date d using (date_id)
        where {FILTRO} and f.pm2_5 is not null
        group by 1, 2, 3, 4 having count(*) >= 30
    """, **PARAMS)
    ranking = ciudades.nlargest(top_n, "pm25").sort_values("pm25")
    ranking["ciudad"] = ranking["location_name"] + " (" + ranking["country"] + ")"

    izq, der = st.columns([1, 1])
    fig = px.bar(ranking, x="pm25", y="ciudad", orientation="h", color="pct_excede",
                 color_continuous_scale="Reds", labels={"pm25": "PM2.5 promedio (µg/m³)", "pct_excede": "% días sobre OMS", "ciudad": ""})
    fig.add_vline(x=UMBRAL_OMS, line_dash="dash", annotation_text="guía OMS 24 h")
    izq.plotly_chart(fig, width="stretch")
    mapa = px.scatter_geo(ciudades, lat="latitude", lon="longitude", color="pm25", size="pm25",
                          hover_name="location_name", color_continuous_scale="YlOrRd", range_color=(0, 80),
                          labels={"pm25": "PM2.5 promedio"})
    der.plotly_chart(mapa, width="stretch")

    sobre_oms = int((ciudades["pct_excede"] > 50).sum())
    st.info(f"{sobre_oms} de {len(ciudades)} ciudades superan la guía diaria de la OMS en más de la mitad de los días del periodo seleccionado.")

    st.subheader("Ciudad frente al promedio nacional")
    nacional = consulta(f"""
        select l.location_name, l.country, avg(f.pm2_5) as pm25_ciudad, max(c.pm25_annual_web) as pm25_nacional,
               max(c.life_years_lost_web) as anios_perdidos
        from {SCHEMA}.fact_air_quality f join {SCHEMA}.dim_location l using (location_id)
             join {SCHEMA}.dim_country_indicator c using (country_indicator_id)
             join {SCHEMA}.dim_date d using (date_id)
        where {FILTRO} and f.pm2_5 is not null and c.pm25_annual_web is not null
        group by 1, 2 having count(*) >= 30
    """, **PARAMS)
    if nacional.empty:
        st.warning("Aún no hay datos del promedio nacional (fuente web).")
    else:
        fig = px.scatter(nacional, x="pm25_nacional", y="pm25_ciudad", hover_name="location_name", color="anios_perdidos",
                         color_continuous_scale="YlOrRd",
                         labels={"pm25_nacional": "PM2.5 anual del país, fuente web (µg/m³)",
                                 "pm25_ciudad": "PM2.5 promedio de la ciudad (µg/m³)", "anios_perdidos": "años de vida perdidos"})
        tope = float(max(nacional["pm25_nacional"].max(), nacional["pm25_ciudad"].max()))
        fig.add_scatter(x=[0, tope], y=[0, tope], mode="lines", name="ciudad = país", line=dict(dash="dash"))
        st.plotly_chart(fig, width="stretch")
        encima = int((nacional["pm25_ciudad"] > nacional["pm25_nacional"]).sum())
        st.info(f"{encima} de {len(nacional)} ciudades tienen un PM2.5 promedio mayor que el promedio anual de su país.")

with tab_emisiones:
    paises = consulta(f"""
        select c.iso3, c.country, c.population, c.gdp, c.co2_per_capita, c.energy_per_capita, avg(f.pm2_5) as pm25
        from {SCHEMA}.fact_air_quality f join {SCHEMA}.dim_country_indicator c using (country_indicator_id)
             join {SCHEMA}.dim_date d using (date_id)
        where {FILTRO} and f.pm2_5 is not null
        group by 1, 2, 3, 4, 5, 6
    """, **PARAMS)
    paises["gdp_per_capita"] = paises["gdp"] / paises["population"]
    opciones = {"CO₂ per cápita (t)": "co2_per_capita", "PIB per cápita (int-$)": "gdp_per_capita", "Energía per cápita (kWh)": "energy_per_capita"}
    etiqueta = st.radio("Indicador del país", list(opciones), horizontal=True)
    x = opciones[etiqueta]
    datos = paises.dropna(subset=[x, "pm25"])
    datos = datos[datos[x] > 0]

    fig = px.scatter(datos, x=x, y="pm25", size="population", hover_name="country", log_x=True,
                     labels={x: etiqueta, "pm25": "PM2.5 promedio (µg/m³)"}, opacity=0.7)
    pendiente, origen = np.polyfit(np.log10(datos[x]), datos["pm25"], 1)
    xs = np.linspace(datos[x].min(), datos[x].max(), 100)
    fig.add_scatter(x=xs, y=pendiente * np.log10(xs) + origen, mode="lines", name="tendencia", line=dict(dash="dash"))
    st.plotly_chart(fig, width="stretch")

    rho = datos[x].rank().corr(datos["pm25"].rank())
    st.info(f"Correlación de Spearman entre {etiqueta} y PM2.5 en {len(datos)} países: {rho:.2f}.")

with tab_tiempo:
    serie = consulta(f"""
        select date_trunc('month', d.full_date)::date as mes, s.source_name as fuente, avg(f.pm2_5) as pm25
        from {SCHEMA}.fact_air_quality f join {SCHEMA}.dim_date d using (date_id)
             join {SCHEMA}.dim_source s using (source_id)
        where {FILTRO} and f.pm2_5 is not null
        group by 1, 2 order by 1
    """, **PARAMS)
    fig = px.line(serie, x="mes", y="pm25", color="fuente", markers=True,
                  labels={"mes": "", "pm25": "PM2.5 promedio mensual (µg/m³)", "fuente": "fuente"})
    fig.add_hline(y=UMBRAL_OMS, line_dash="dash", annotation_text="guía OMS 24 h")
    st.plotly_chart(fig, width="stretch")
    st.caption("La serie de la API continúa el histórico del CSV: cada ejecución diaria del DAG agrega los días más recientes.")

    disponibles = consulta(f"select distinct location_name from {SCHEMA}.dim_location order by 1")["location_name"].tolist()
    elegidas_ciudades = st.multiselect("Comparar ciudades", disponibles, default=[c for c in ["Bogota", "Santiago", "New Delhi"] if c in disponibles])
    if elegidas_ciudades:
        diario = consulta(f"""
            select d.full_date as fecha, l.location_name as ciudad, f.pm2_5 as pm25
            from {SCHEMA}.fact_air_quality f join {SCHEMA}.dim_date d using (date_id)
                 join {SCHEMA}.dim_location l using (location_id)
            where {FILTRO} and l.location_name in :ciudades and f.pm2_5 is not null
            order by 1
        """, ciudades=elegidas_ciudades, **PARAMS)
        st.plotly_chart(px.line(diario, x="fecha", y="pm25", color="ciudad", labels={"pm25": "PM2.5 (µg/m³)", "fecha": ""}), width="stretch")

with tab_calidad:
    if ultimo_run.empty:
        st.warning("Aún no hay ejecuciones registradas.")
    else:
        estado = ultimo_run.loc[0, "status"]
        (st.success if estado == "success" else st.error)(f"Última ejecución del DAG: {ultimo_run.loc[0, 'run_id']} ({estado})")

    resumen = consulta(f"""
        select suite_name as suite, severity as severidad, count(*) as expectativas,
               count(*) filter (where success) as aprobadas
        from {SCHEMA}.dq_validation_log
        where run_id = (select run_id from {SCHEMA}.etl_run_log where status = 'success' order by finished_at desc limit 1)
        group by 1, 2 order by 1, 2
    """)
    st.subheader("Última validación aprobada (Great Expectations)")
    st.dataframe(resumen, width="stretch", hide_index=True)

    historial = consulta(f"select run_id, status, fact_rows_loaded, finished_at, detail from {SCHEMA}.etl_run_log order by finished_at desc limit 15")
    st.subheader("Historial de ejecuciones")
    st.dataframe(historial, width="stretch", hide_index=True)
