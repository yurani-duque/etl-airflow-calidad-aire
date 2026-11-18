"""
Script Name: generate_report.py
Author: Valentina Velasco
Fecha: 2026-11-20
Fase ETL: Reporte analitico estatico
Descripcion: genera un informe HTML autocontenido a partir de consultas al modelo dimensional validado (esquema dw).
    Responde preguntas del proyecto con graficos e interpretaciones calculadas de los datos y resume el desempeno
    del modelo de prediccion. Es una foto en un momento dado: no se actualiza solo ni muestra mensajes de Kafka.
Entradas: tablas dw.* y models/model_metadata.json
Salidas: reports/analytical_report.html
Notas: regenerar con "docker compose run --rm report" o "python -m reports.generate_report"
"""

import base64
import html
import io
import json
from datetime import datetime, timezone
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sqlalchemy import text

from utils.config_loader import ROOT, load_config, resolve
from utils.db_connector import get_engine

SCHEMA = load_config()["pipeline"]["dw_schema"]
UMBRAL = load_config()["quality"]["who_pm25_24h"]
SALIDA = Path(ROOT) / "reports" / "analytical_report.html"
ROJO, AZUL, GRIS, VERDE = "#c0392b", "#2e86c1", "#7f8c8d", "#1e8449"
plt.rcParams.update({"font.size": 10, "axes.spines.top": False, "axes.spines.right": False, "figure.dpi": 110})


def consulta(sql: str, **params) -> pd.DataFrame:
    with get_engine().connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)


def imagen(fig) -> str:
    buffer = io.BytesIO()
    fig.savefig(buffer, format="png", bbox_inches="tight")
    plt.close(fig)
    return f'<img alt="chart" src="data:image/png;base64,{base64.b64encode(buffer.getvalue()).decode()}">'


def spearman(a: pd.Series, b: pd.Series) -> float:
    return float(a.rank().corr(b.rank()))


def seccion(titulo: str, pregunta: str, grafico: str, hallazgo: str) -> str:
    return (f"<section><h2>{html.escape(titulo)}</h2><p class='q'><b>Question.</b> {html.escape(pregunta)}</p>"
            f"{grafico}<p class='finding'><b>Finding.</b> {hallazgo}</p></section>")


def cobertura() -> dict:
    fila = consulta(f"""
        select count(*) as hechos, count(distinct f.location_id) as ciudades, min(d.full_date) as desde, max(d.full_date) as hasta
        from {SCHEMA}.fact_air_quality f join {SCHEMA}.dim_date d using (date_id)""").iloc[0]
    fuentes = consulta(f"select s.source_name, count(*) as filas from {SCHEMA}.fact_air_quality f "
                       f"join {SCHEMA}.dim_source s using (source_id) group by 1 order by 1")
    return {"fila": fila, "fuentes": fuentes}


def q_ranking() -> str:
    df = consulta(f"""
        select l.location_name, l.country, avg(f.pm2_5) as pm25, 100 * avg(f.pm25_exceeds_who::int) as pct_excede
        from {SCHEMA}.fact_air_quality f join {SCHEMA}.dim_location l using (location_id)
        where f.pm2_5 is not null group by 1, 2 having count(*) >= 30""")
    top = df.nlargest(15, "pm25").sort_values("pm25")
    fig, ax = plt.subplots(figsize=(8.5, 5))
    ax.barh(top["location_name"] + " (" + top["country"] + ")", top["pm25"], color=ROJO)
    ax.axvline(UMBRAL, ls="--", color="k")
    ax.set_title(f"Dashed line: WHO 24 h guideline ({UMBRAL} µg/m³)", fontsize=9, loc="left")
    ax.set_xlabel("Average PM2.5 (µg/m³)")
    mayoria = int((df["pct_excede"] > 50).sum())
    hallazgo = (f"{mayoria} of {len(df)} capitals exceed the WHO daily guideline ({UMBRAL} µg/m³) on more than half of their days. "
                f"{', '.join(top['location_name'].iloc[::-1].head(5))} lead the ranking, with averages between "
                f"{top['pm25'].min():.0f} and {top['pm25'].max():.0f} µg/m³.")
    return seccion("1. Which capitals have the worst air?", "Which cities exceed the WHO guideline and how often?", imagen(fig), hallazgo)


def q_emisiones() -> str:
    df = consulta(f"""
        select c.iso3, c.country, c.population, c.gdp, c.co2_per_capita, avg(f.pm2_5) as pm25
        from {SCHEMA}.fact_air_quality f join {SCHEMA}.dim_country_indicator c using (country_indicator_id)
        where f.pm2_5 is not null group by 1, 2, 3, 4, 5""").dropna()
    df["gdp_pc"] = df["gdp"].astype(float) / df["population"].astype(float)
    df = df[(df["co2_per_capita"] > 0) & (df["gdp_pc"] > 0)]
    fig, ejes = plt.subplots(1, 2, figsize=(10, 4))
    for ax, columna, etiqueta in [(ejes[0], "co2_per_capita", "CO₂ per capita (t)"), (ejes[1], "gdp_pc", "GDP per capita (int-$)")]:
        ax.scatter(df[columna], df["pm25"], s=18, alpha=0.6, color=AZUL)
        ax.set_xscale("log")
        ax.set_xlabel(etiqueta)
        ax.set_ylabel("Average PM2.5 of the country's capital (µg/m³)")
        ax.set_title(f"Spearman ρ = {spearman(df[columna], df['pm25']):.2f}")
    rho_co2, rho_pib = spearman(df["co2_per_capita"], df["pm25"]), spearman(df["gdp_pc"], df["pm25"])
    hallazgo = (f"Across {len(df)} countries, the rank correlation between a capital's PM2.5 and its country's CO₂ per capita is "
                f"{rho_co2:.2f} and with GDP per capita {rho_pib:.2f}. Urban air pollution is almost unrelated to national emissions per person "
                "and only weakly associated with lower income: local sources, geography and dust matter more than national averages. "
                "This is a correlation between countries, not causation.")
    return seccion("2. Is air quality related to emissions and income?", "Do countries with higher CO₂ or income have cleaner or dirtier capitals?", imagen(fig), hallazgo)


def q_nacional() -> str:
    df = consulta(f"""
        select l.location_name, avg(f.pm2_5) as ciudad, max(c.pm25_annual_web) as pais
        from {SCHEMA}.fact_air_quality f join {SCHEMA}.dim_location l using (location_id)
        join {SCHEMA}.dim_country_indicator c using (country_indicator_id)
        where f.pm2_5 is not null and c.pm25_annual_web is not null group by 1 having count(*) >= 30""")
    fig, ax = plt.subplots(figsize=(5.8, 5))
    ax.scatter(df["pais"], df["ciudad"], s=16, alpha=0.6, color=ROJO)
    tope = float(max(df["pais"].max(), df["ciudad"].max()))
    ax.plot([0, tope], [0, tope], ls="--", color="k")
    ax.set_xlabel("Country annual PM2.5, scraped table (µg/m³)")
    ax.set_ylabel("Capital average PM2.5 (µg/m³)")
    encima = int((df["ciudad"] > df["pais"]).sum())
    hallazgo = (f"{encima} of {len(df)} capitals have an average PM2.5 above their country's annual average "
                f"({100 * encima / len(df):.0f} %). Capitals concentrate more pollution than the rest of the country, "
                "which supports prioritising urban air-quality policy (SDG 11.6). The country figure comes from the scraped web source.")
    return seccion("3. How do capitals compare with their country?", "Is the capital's air worse than the national average?", imagen(fig), hallazgo)


def q_estacionalidad() -> str:
    df = consulta(f"""
        select date_trunc('month', d.full_date)::date as mes, avg(f.pm2_5) as pm25
        from {SCHEMA}.fact_air_quality f join {SCHEMA}.dim_date d using (date_id)
        where f.pm2_5 is not null group by 1 order by 1""")
    fig, ax = plt.subplots(figsize=(8.5, 3.6))
    ax.plot(pd.to_datetime(df["mes"]), df["pm25"], marker="o", color=AZUL)
    ax.axhline(UMBRAL, ls="--", color="k")
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b %Y"))
    ax.set_ylabel("Monthly average PM2.5 (µg/m³)")
    pico = df.loc[df["pm25"].idxmax()]
    hallazgo = (f"The highest monthly average ({pico['pm25']:.1f} µg/m³) occurred in {pd.Timestamp(pico['mes']):%B %Y}; "
                f"the series stays above the {UMBRAL} µg/m³ guideline in most months, so exceedance is persistent rather than occasional. "
                "Seasonal patterns motivate the month and the 7-day history used as model features.")
    return seccion("4. How does PM2.5 evolve over time?", "Is there seasonality or a trend?", imagen(fig), hallazgo)


def q_modelo() -> str:
    from src.kafka_consumer import cargar_modelo
    from src.ml_contract import FEATURES, TARGET
    from src.train_model import cargar_dataset, dividir_por_fecha

    modelo, meta = cargar_modelo()
    _, prueba = dividir_por_fecha(cargar_dataset(), load_config()["ml"]["test_days"])
    previsto = modelo.predict(prueba[FEATURES])
    real = prueba[TARGET].to_numpy()
    fig, ejes = plt.subplots(1, 2, figsize=(10, 4.2))
    ejes[0].hexbin(real, previsto, gridsize=45, bins="log", mincnt=1, cmap="Blues", extent=(0, 150, 0, 150))
    ejes[0].plot([0, 150], [0, 150], ls="--", color="k")
    ejes[0].set_xlabel("Actual PM2.5 tomorrow (µg/m³)")
    ejes[0].set_ylabel("Predicted PM2.5 tomorrow (µg/m³)")
    ejes[0].set_title("Test period: predicted vs actual")
    ciudades = pd.DataFrame({"city": prueba["location_name"].to_numpy(), "error": np.abs(real - previsto)})
    peor = ciudades.groupby("city")["error"].mean().nlargest(10).sort_values()
    ejes[1].barh(peor.index, peor.values, color=ROJO)
    ejes[1].set_xlabel("Mean absolute error (µg/m³)")
    ejes[1].set_title("Hardest cities to forecast")
    m = meta["metricas"]
    filas = "".join(
        f"<tr><td>{nombre}</td><td>{v['mae']}</td><td>{v['rmse']}</td><td>{v['r2']}</td><td>{v['alerta_precision']}</td><td>{v['alerta_recall']}</td></tr>"
        for nombre, v in [("Daily-change gradient boosting ensemble", m["modelo"]), ("Baseline: tomorrow = today", m["linea_base_persistencia"]),
                          ("Baseline: 7-day mean", m["linea_base_media_7d"])]
    )
    tabla = ("<table><tr><th>Model</th><th>MAE</th><th>RMSE</th><th>R²</th><th>Alert precision</th><th>Alert recall</th></tr>" + filas + "</table>")
    mejora = 100 * (1 - m["modelo"]["mae"] / m["linea_base_persistencia"]["mae"])
    hallazgo = (f"Trained on {meta['entrenamiento_desde']} to {meta['entrenamiento_hasta']} and evaluated on {meta['prueba_desde']} to {meta['prueba_hasta']} "
                f"({meta['filas_prueba']:,} city-days, split by date to avoid leakage). The model's MAE is {m['modelo']['mae']} µg/m³, "
                f"{mejora:.0f} % lower than the persistence baseline ({m['linea_base_persistencia']['mae']}). "
                f"Alert rule: warn when the forecast exceeds {meta['margen_alerta']} µg/m³ (a precautionary margin); an alert is correct when the actual value exceeds the WHO guideline of {UMBRAL}. "
                f"With that rule the model reaches precision {m['modelo']['alerta_precision']} and recall {m['modelo']['alerta_recall']} (F1 {m['modelo']['alerta_f1']}, baseline {m['linea_base_persistencia']['alerta_f1']}). "
                "Dust-storm cities such as those in the right-hand chart have spiky series and remain the main limitation.")
    return seccion("5. Can we forecast tomorrow's PM2.5?", "Does the model beat naive baselines on unseen recent days?", tabla + imagen(fig), hallazgo)


def q_calidad_y_streaming() -> str:
    corridas = consulta(f"select status, count(*) as n from {SCHEMA}.etl_run_log group by 1 order by 1")
    reglas = consulta(f"""
        select suite_name, severity, count(*) as reglas, count(*) filter (where success) as cumplidas
        from {SCHEMA}.dq_validation_log
        where run_id = (select run_id from {SCHEMA}.etl_run_log where status = 'success' order by finished_at desc limit 1)
        group by 1, 2 order by 1, 2""")
    cuarentena = consulta(f"select count(*) as lotes, coalesce(sum(quarantined_rows), 0) as filas from {SCHEMA}.quarantine_log").iloc[0]
    try:
        pred = consulta(f"select count(*) as n, coalesce(sum(alert_who::int), 0) as alertas, avg(latency_ms) as lat from {SCHEMA}.predictions").iloc[0]
        streaming = (f"The Kafka consumer has stored {int(pred['n']):,} predictions in <code>dw.predictions</code> "
                     f"({int(pred['alertas']):,} above the WHO guideline) with an average inference latency of {float(pred['lat'] or 0):.1f} ms.")
    except Exception:  # noqa: BLE001
        streaming = "No predictions have been stored yet."
    estado = ", ".join(f"{fila.status}: {fila.n}" for fila in corridas.itertuples())
    tabla = ("<table><tr><th>Suite</th><th>Severity</th><th>Rules</th><th>Passed</th></tr>" +
             "".join(f"<tr><td>{r.suite_name}</td><td>{r.severity}</td><td>{r.reglas}</td><td>{r.cumplidas}</td></tr>" for r in reglas.itertuples()) +
             "</table>")
    hallazgo = (f"Pipeline runs by status: {estado}. {int(cuarentena['lotes'])} batch(es) were sent to quarantine "
                f"({int(cuarentena['filas']):,} invalid rows set aside) instead of reaching the fact table. {streaming}")
    return seccion("6. Can we trust the data, and what does the streaming path add?", "Were critical checks passed, and is the operational path running?", tabla, hallazgo)


def generar() -> Path:
    cob = cobertura()
    fila = cob["fila"]
    meta = json.loads(Path(resolve(load_config()["ml"]["metadata_path"])).read_text(encoding="utf-8"))
    fuentes = ", ".join(f"{r.source_name} ({r.filas:,})" for r in cob["fuentes"].itertuples())
    secciones = [q_ranking(), q_emisiones(), q_nacional(), q_estacionalidad(), q_modelo(), q_calidad_y_streaming()]
    pagina = f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<title>Air quality, emissions and forecast: analytical report</title>
<style>
body{{font-family:Segoe UI,Arial,sans-serif;max-width:980px;margin:32px auto;padding:0 20px;color:#1f2937;line-height:1.5}}
h1{{margin-bottom:4px}} h2{{margin-top:36px;border-bottom:2px solid #e5e7eb;padding-bottom:4px}}
.meta{{color:#6b7280;font-size:14px}} img{{max-width:100%;display:block;margin:12px auto}}
.q{{color:#374151}} .finding{{background:#f1f5f9;padding:12px 14px;border-radius:8px}}
table{{border-collapse:collapse;margin:12px 0;font-size:14px}} td,th{{border:1px solid #d1d5db;padding:5px 10px;text-align:left}}
th{{background:#e5e7eb}} code{{background:#f3f4f6;padding:1px 4px;border-radius:3px}}
.cards{{display:flex;gap:12px;flex-wrap:wrap;margin:16px 0}} .card{{flex:1;min-width:150px;background:#f1f5f9;border-radius:8px;padding:10px 14px}}
.card b{{font-size:22px;display:block}}
</style></head><body>
<h1>Air quality, emissions and next-day forecast</h1>
<p class="meta">Static analytical report generated {datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC from the validated dimensional model (schema <code>dw</code>).
Point-in-time output: regenerate with <code>docker compose run --rm report</code>.</p>
<div class="cards">
<div class="card"><b>{int(fila['hechos']):,}</b>city-day records</div>
<div class="card"><b>{int(fila['ciudades'])}</b>capital cities</div>
<div class="card"><b>{fila['desde']}</b>first day</div>
<div class="card"><b>{fila['hasta']}</b>last day</div>
</div>
<p class="meta">Sources in the fact table: {fuentes}. Country attributes come from Our World in Data (database) and a scraped web table.
Forecast model: <code>{meta['version']}</code>.</p>
{''.join(secciones)}
<section><h2>Method and limitations</h2>
<p>All figures query <code>dw.fact_air_quality</code> and its dimensions, loaded only after the Great Expectations checks passed. Country-level
comparisons use the latest available value of each indicator. CSV rows are point readings and API rows are daily means; the <code>source</code> column keeps them apart.
Correlations are across countries and do not imply causation. The forecast uses information up to day t to predict day t+1 and is evaluated on a later, unseen period.</p></section>
</body></html>"""
    SALIDA.write_text(pagina, encoding="utf-8")
    return SALIDA


if __name__ == "__main__":
    print("report written to", generar())

