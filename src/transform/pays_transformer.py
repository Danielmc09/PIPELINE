# PaysTransformer
import pandas as pd
from src.transform.base_transformer import BaseTransformer

PAYS_SCHEMA = {
    "pay_date": "datetime64[ns]",
    "total": "float64",
    "user_id": "int64",
    "value_prop": "object"
}

class PaysTransformer(BaseTransformer):
    def __init__(self, schema=None):
        critical_columns = list(PAYS_SCHEMA.keys())
        non_critical_columns = []
        super().__init__(schema or PAYS_SCHEMA, critical_columns, non_critical_columns, date_columns=["pay_date"])

