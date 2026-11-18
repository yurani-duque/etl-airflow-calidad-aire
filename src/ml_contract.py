"""
Script Name: ml_contract.py
Author: Valentina Velasco
Fecha: 2026-11-19
Fase ETL: Machine learning (contrato de datos)
Descripcion: define las columnas que usan por igual el entrenamiento, el productor y el consumidor de Kafka:
    las variables de entrada del modelo, el objetivo y la estructura del evento publicado.
Entradas: ninguna (constantes)
Salidas: listas de columnas y la funcion de validacion de un evento
Notas: el objetivo (PM2.5 del dia siguiente) NO forma parte del evento para evitar fuga de datos
"""

from datetime import date, datetime, timezone

FEATURES_NUMERICAS = [
    "pm2_5", "pm10", "co", "no2", "so2", "ozone",
    "temperature_celsius", "humidity", "wind_kph", "pressure_mb", "precip_mm", "cloud", "uv_index",
    "pm25_lag1", "pm25_lag2", "pm25_lag3", "pm25_lag7", "pm25_mean_3d", "pm25_mean_7d", "pm25_std_7d", "pm25_mean_30d",
    "ratio_today_7d", "delta_temp", "delta_pressure", "delta_wind",
    "month", "day_of_week", "latitude",
    "co2_per_capita", "gdp_per_capita", "energy_per_capita", "pm25_annual_web",
]
FEATURES_CATEGORICAS = ["iso3"]
FEATURES = FEATURES_NUMERICAS + FEATURES_CATEGORICAS
TARGET = "target_pm25_next_day"

# identificacion del evento (no entran al modelo)
CAMPOS_IDENTIFICACION = ["event_id", "event_ts", "location_key", "location_name", "country", "longitude", "feature_date"]
CAMPOS_EVENTO = CAMPOS_IDENTIFICACION + FEATURES

# columnas de la vista dw.v_prediction_features que el productor necesita (sin el objetivo)
COLUMNAS_VISTA = ["location_key", "location_name", "country", "longitude", "feature_date"] + FEATURES

# rangos fisicos usados para descartar mensajes invalidos en el consumidor
RANGOS_VALIDOS = {
    "pm2_5": (0, 1000), "pm10": (0, 10_000), "humidity": (0, 100), "cloud": (0, 100),
    "temperature_celsius": (-90, 60), "wind_kph": (0, 500), "pressure_mb": (400, 1100),
    "latitude": (-90, 90), "longitude": (-180, 180), "month": (1, 12), "day_of_week": (1, 7),
}
# el evento no tiene sentido sin la medicion del dia: PM2.5, ubicacion y fecha son obligatorias
OBLIGATORIOS = ["event_id", "location_key", "feature_date", "pm2_5", "latitude", "iso3"]


def validar_evento(evento: dict) -> tuple[bool, str]:
    """Devuelve (valido, motivo). Un evento es valido si tiene la estructura y los rangos esperados."""
    if not isinstance(evento, dict):
        return False, "el mensaje no es un objeto JSON"
    faltantes = [c for c in CAMPOS_EVENTO if c not in evento]
    if faltantes:
        return False, f"campos ausentes: {', '.join(faltantes[:4])}"
    nulos = [c for c in OBLIGATORIOS if evento.get(c) in (None, "")]
    if nulos:
        return False, f"campos obligatorios nulos: {', '.join(nulos)}"
    for campo in FEATURES_NUMERICAS + ["longitude"]:
        valor = evento.get(campo)
        if valor is not None and (isinstance(valor, bool) or not isinstance(valor, (int, float))):
            return False, f"{campo} no es numerico"
    for campo, (minimo, maximo) in RANGOS_VALIDOS.items():
        valor = evento.get(campo)
        if valor is not None and not (minimo <= valor <= maximo):
            return False, f"{campo}={valor} fuera de rango ({minimo} a {maximo})"
    try:
        date.fromisoformat(str(evento["feature_date"])[:10])
    except ValueError:
        return False, "feature_date no es una fecha ISO"
    if "target_pm25_next_day" in evento:
        return False, "el evento incluye el objetivo (fuga de datos)"
    return True, ""


def ahora_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")
