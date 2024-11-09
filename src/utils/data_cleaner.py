# src/utils/data_cleaner.py

import pandas as pd
from src.utils.logger import logger

class DataCleaner:
    def __init__(self, critical_columns, non_critical_columns, critical_threshold=100):
        """
        Inicializa el limpiador de datos con las columnas críticas, no críticas y el umbral de eliminación.

        Args:
            critical_columns (list): Lista de columnas críticas.
            non_critical_columns (list): Lista de columnas no críticas.
            critical_threshold (int): Umbral para la eliminación de filas con datos faltantes críticos.
        """
        self.critical_columns = critical_columns
        self.non_critical_columns = non_critical_columns
        self.critical_threshold = critical_threshold

    def remove_critical_missing(self, df):
        """
        Elimina filas con valores nulos en columnas críticas si superan el umbral.

        Args:
            df (pd.DataFrame): El DataFrame a procesar.

        Returns:
            pd.DataFrame: DataFrame sin filas con valores nulos en columnas críticas.

        Raises:
            KeyError: Si faltan columnas críticas en el DataFrame.
        """
        # Verificar que todas las columnas críticas existen en el DataFrame
        missing_columns = [col for col in self.critical_columns if col not in df.columns]
        if missing_columns:
            logger.error(f"Columnas críticas faltantes en el DataFrame: {missing_columns}")
            raise KeyError(f"Columnas críticas faltantes: {missing_columns}")

        try:
            # Identificar filas con valores nulos en columnas críticas
            missing_critical = df[self.critical_columns].isnull().any(axis=1)
            num_missing_critical = missing_critical.sum()

            if num_missing_critical > 0:
                logger.warning(f"Se encontraron {num_missing_critical} registros con datos críticos faltantes.")
                if num_missing_critical > self.critical_threshold:
                    logger.critical(f"Eliminación de {num_missing_critical} filas por datos críticos faltantes.")
                df = df.dropna(subset=self.critical_columns)
            return df
        except Exception as e:
            logger.error(f"Error al eliminar filas con datos críticos faltantes: {e}")
            raise

    def impute_non_critical_missing(self, df):
        """
        Imputa valores faltantes en columnas no críticas.

        Args:
            df (pd.DataFrame): El DataFrame a procesar.

        Returns:
            pd.DataFrame: DataFrame con valores imputados en columnas no críticas.
        """
        try:
            for column in self.non_critical_columns:
                if column in df.columns:
                    df[column] = df[column].fillna("desconocido")
                    logger.info(f"Valores nulos en la columna '{column}' imputados con 'desconocido'.")
            return df
        except Exception as e:
            logger.error(f"Error al imputar datos faltantes en columnas no críticas: {e}")
            raise
