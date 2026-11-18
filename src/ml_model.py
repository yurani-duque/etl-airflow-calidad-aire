"""
Script Name: ml_model.py
Author: Valentina Velasco
Fecha: 2026-11-19
Fase ETL: Machine learning (modelo)
Descripcion: modelo que predice el PM2.5 de manana como el PM2.5 de hoy mas un cambio diario estimado. Promedia dos
    regresores de arboles potenciados: uno con perdida de error absoluto (robusto a picos) y otro con perdida
    cuadratica (penaliza los errores grandes). Incluye su propio preprocesamiento, por lo que es un solo objeto serializable.
Entradas: dataframe con las columnas FEATURES de src.ml_contract
Salidas: prediccion del PM2.5 del dia siguiente en ug/m3 (nunca negativa)
Notas: predecir el cambio en vez del nivel aprovecha que el aire de manana se parece mucho al de hoy; el modelo
    solo debe aportar la correccion. El consumidor de Kafka carga este mismo objeto con joblib
"""

import numpy as np
from sklearn.base import BaseEstimator, RegressorMixin
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OrdinalEncoder

from src.ml_contract import FEATURES_CATEGORICAS, FEATURES_NUMERICAS


class ModeloCambioDiario(BaseEstimator, RegressorMixin):
    def __init__(self, columna_hoy: str = "pm2_5", semilla: int = 42):
        self.columna_hoy = columna_hoy
        self.semilla = semilla

    def _regresor(self, perdida: str) -> Pipeline:
        preprocesamiento = ColumnTransformer([
            # los numericos pasan tal cual: el modelo maneja los nulos de forma nativa
            ("numericas", "passthrough", FEATURES_NUMERICAS),
            # el pais es categorico; un pais nunca visto recibe -1 y no rompe la inferencia
            ("categoricas", OrdinalEncoder(handle_unknown="use_encoded_value", unknown_value=-1), FEATURES_CATEGORICAS),
        ])
        regresor = HistGradientBoostingRegressor(
            loss=perdida, learning_rate=0.04, max_iter=700, max_leaf_nodes=31, min_samples_leaf=40,
            l2_regularization=1.0, early_stopping=True, validation_fraction=0.1, n_iter_no_change=25,
            random_state=self.semilla, categorical_features=[len(FEATURES_NUMERICAS)],
        )
        return Pipeline([("preprocesamiento", preprocesamiento), ("modelo", regresor)])

    def fit(self, X, y):
        cambio = np.asarray(y, dtype=float) - X[self.columna_hoy].to_numpy(dtype=float)
        self.regresores_ = [self._regresor("absolute_error").fit(X, cambio), self._regresor("squared_error").fit(X, cambio)]
        return self

    def predict(self, X):
        cambio = np.mean([regresor.predict(X) for regresor in self.regresores_], axis=0)
        return np.clip(X[self.columna_hoy].to_numpy(dtype=float) + cambio, 0, None)
