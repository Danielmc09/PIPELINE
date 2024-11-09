import pandas as pd
from datetime import timedelta
from src.utils.logger import logger

class LastWeekFilter:
    def __init__(self, date_column):
        self.date_column = date_column

    def filter(self, df):
        """
        Filtra los datos para conservar solo las filas de la última semana.

        Args:
            df (pd.DataFrame): El DataFrame de entrada.

        Returns:
            pd.DataFrame: DataFrame filtrado.
        """
        try:
            logger.info(f"Filtrando el DataFrame para conservar solo las filas de la última semana usando la columna '{self.date_column}'.")

            # Comprobamos si la columna existe en el DataFrame
            if self.date_column not in df.columns:
                logger.error(f"La columna '{self.date_column}' no se encuentra en el DataFrame.")
                raise KeyError(f"La columna '{self.date_column}' no se encuentra en el DataFrame.")

            max_date = pd.to_datetime(df[self.date_column]).max()
            if not isinstance(max_date, pd.Timestamp):
                raise TypeError(f"max_date debería ser pd.Timestamp, pero es {type(max_date)}")

            last_week_date = max_date - pd.Timedelta(weeks=1)
            logger.info(f"Fecha máxima encontrada: {max_date}. Filtrando desde: {last_week_date}.")

            df[self.date_column] = pd.to_datetime(df[self.date_column], errors='coerce')

            # Filtramos el DataFrame
            filtered_df = df[df[self.date_column] >= last_week_date]
            logger.info(f"Número de registros después del filtrado: {filtered_df.shape[0]}.")

            return filtered_df
        except Exception as e:
            logger.error(f"Error al filtrar el DataFrame en LastWeekFilter: {e}")
            raise
