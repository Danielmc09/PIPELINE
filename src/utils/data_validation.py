# src/utils/data_validation.py

import pandas as pd
from src.utils.logger import logger

class SchemaValidator:
    def __init__(self, schema):
        """
        Inicializa el validador de esquema con un esquema específico.

        Args:
            schema (dict): Diccionario que define las columnas y tipos de datos esperados.
        """
        self.schema = schema

    def enforce_schema(self, df):
        """
        Aplica el esquema esperado para cualquier DataFrame.

        Args:
            df (pd.DataFrame): El DataFrame a validar.

        Returns:
            pd.DataFrame: DataFrame con el esquema aplicado.
        """
        try:
            logger.info("Iniciando la validación del esquema.")
            for column, dtype in self.schema.items():
                if column in df.columns:
                    df[column] = df[column].astype(dtype)
                    logger.debug(f"Columna '{column}' convertida a {dtype}.")
                else:
                    logger.warning(f"Columna '{column}' esperada no encontrada.")
                    raise KeyError(f"Columna '{column}' esperada no encontrada.")
            logger.info("Esquema validado exitosamente.")
            return df
        except (KeyError, ValueError) as e:
            logger.error(f"Error en el esquema de datos: {e}")
            raise
