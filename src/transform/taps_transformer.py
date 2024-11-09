# src/transform/taps_transformer.py

import pandas as pd
from src.transform.base_transformer import BaseTransformer
from src.utils.logger import logger

TAPS_SCHEMA = {
    "day": "datetime64[ns]",
    "user_id": "int64",
    "position": "int64",
    "value_prop": "object"
}

class TapsTransformer(BaseTransformer):
    def __init__(self, schema=None):
        critical_columns = list(TAPS_SCHEMA.keys())
        non_critical_columns = ["event_data.value_prop"]
        super().__init__(schema or TAPS_SCHEMA, critical_columns, non_critical_columns, date_columns=["day"])

    def process_data(self, df):
        logger.info("Iniciando el procesamiento completo de datos de taps.")
        df = self.expand_event_data(df)
        df = super().process_data(df)
        logger.info(f"Columnas en df después de expandir y procesar: {df.columns.tolist()}")
        return df
