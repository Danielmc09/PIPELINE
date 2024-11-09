# tests/test_base_transformer.py

import pytest
import pandas as pd
from src.transform.base_transformer import BaseTransformer

# Crear un esquema general que usarán las pruebas de base
GENERAL_SCHEMA = {
    "day": "datetime64[ns]",
    "user_id": "int64",
    "event_data.position": "int64",
    "event_data.value_prop": "object"
}

class GeneralTransformer(BaseTransformer):
    """
    Transformador general para pruebas basado en BaseTransformer.
    No contiene métodos específicos, solo para probar métodos generales.
    """
    def __init__(self):
        super().__init__(GENERAL_SCHEMA, list(GENERAL_SCHEMA.keys()), ["event_data.value_prop"])

def test_remove_critical_missing():
    data = {
        "day": ["2020-11-01", None],
        "user_id": [98702, 98703],
        "event_data.position": [0, None],
        "event_data.value_prop": ["cellphone_recharge", None]
    }
    df = pd.DataFrame(data)

    transformer = GeneralTransformer()
    transformer.cleaner.critical_threshold = 1

    result_df = transformer.cleaner.remove_critical_missing(df)

    assert len(result_df) == 1, "Debe eliminar las filas con datos críticos faltantes."
    assert result_df["user_id"].iloc[0] == 98702, "Debe mantener la fila con datos completos."

def test_impute_non_critical_missing():
    data = {
        "day": ["2020-11-01", "2020-11-02"],
        "user_id": [98702, 98703],
        "event_data.position": [0, 1],
        "event_data.value_prop": ["cellphone_recharge", None]
    }
    df = pd.DataFrame(data)

    transformer = GeneralTransformer()

    result_df = transformer.cleaner.impute_non_critical_missing(df)

    assert result_df["event_data.value_prop"].iloc[1] == "desconocido", \
        "Debe imputar los valores nulos en columnas no críticas con 'desconocido'."

def test_enforce_schema():
    data = {
        "day": ["2020-11-01", "2020-11-02"],
        "user_id": [98702, 98703],
        "event_data.position": ["0", 1],
        "event_data.value_prop": ["cellphone_recharge", "prepaid"]
    }
    df = pd.DataFrame(data)

    transformer = GeneralTransformer()

    result_df = transformer.schema_validator.enforce_schema(df)

    assert result_df["day"].dtype == 'datetime64[ns]', "El tipo de 'day' debe ser datetime64[ns]."
    assert result_df["user_id"].dtype == 'int64', "El tipo de 'user_id' debe ser int64."
    assert result_df["event_data.position"].dtype == 'int64', "El tipo de 'event_data.position' debe ser int64."
    assert result_df["event_data.value_prop"].dtype == 'object', "El tipo de 'event_data.value_prop' debe ser object."
