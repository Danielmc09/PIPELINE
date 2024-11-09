import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from utils.logger import logger


class DatasetOptimizer:
    def __init__(self, dataframe):
        self.dataframe = dataframe
        self.available_columns = set(dataframe.columns)

    def validate_columns(self):
        """Valida y registra las columnas disponibles en el DataFrame inicial."""
        logger.info(f"Columnas disponibles al inicio: {self.available_columns}")

    def consolidate_date_columns(self):
        """Consolidar columnas de fecha redundantes."""
        if 'day_taps' in self.available_columns and self.dataframe['day_prints'].equals(self.dataframe['day_taps']):
            self.dataframe = self.dataframe.drop(columns=['day_taps'])
            logger.info("Columna 'day_taps' eliminada por redundancia con 'day_prints'.")

    def rename_payment_columns(self):
        """Renombrar columnas relacionadas con pagos para mayor claridad, si están presentes."""
        columns_to_rename = {}
        if 'total' in self.available_columns:
            columns_to_rename['total'] = 'monto_ultimo_pago'
        if 'cumulative_payment_amount' in self.available_columns:
            columns_to_rename['cumulative_payment_amount'] = 'monto_total_pagos'

        if columns_to_rename:
            self.dataframe = self.dataframe.rename(columns=columns_to_rename)
            logger.info(f"Columnas renombradas: {columns_to_rename}")
            self.available_columns.update(columns_to_rename.values())  # Actualizar con nombres nuevos
        else:
            logger.warning("No se encontraron las columnas 'total' o 'cumulative_payment_amount' para renombrar.")

    def convert_clicked_to_binary(self):
        """Convertir la columna 'clicked' a valores binarios si existe."""
        if 'clicked' in self.available_columns:
            self.dataframe['clicked'] = self.dataframe['clicked'].apply(lambda x: 1 if x == "Sí" else 0)
            logger.info("Columna 'clicked' convertida a formato binario.")
        else:
            logger.warning("La columna 'clicked' no se encontró en el DataFrame.")

    def fill_missing_payment_values(self):
        """Rellenar valores nulos en columnas de pagos si están presentes."""
        if 'monto_total_pagos' in self.available_columns:
            self.dataframe['monto_total_pagos'] = self.dataframe['monto_total_pagos'].fillna(0.0)
        if 'monto_ultimo_pago' in self.available_columns:
            self.dataframe['monto_ultimo_pago'] = self.dataframe['monto_ultimo_pago'].fillna(0.0)
        logger.info("Valores nulos en columnas de pagos rellenados con 0.")

    def fill_missing_values(self):
        """Rellenar todos los valores NaN en el DataFrame con 0."""
        self.dataframe = self.dataframe.fillna(0)
        logger.info("Todos los valores NaN en el DataFrame han sido reemplazados con 0.")

    def filter_irrelevant_rows(self):
        """Eliminar filas con valores irrelevantes si las columnas requeridas están presentes."""
        if 'monto_total_pagos' in self.dataframe.columns and 'clicked' in self.dataframe.columns:
            self.dataframe = self.dataframe[
                ~((self.dataframe['monto_total_pagos'] == 0) & (self.dataframe['clicked'] == 0))]
            logger.info("Filas con 'monto_total_pagos' y 'clicked' ambos en cero eliminadas.")
        else:
            logger.warning("Las columnas 'monto_total_pagos' o 'clicked' no están presentes para aplicar el filtrado.")

    def optimize(self):
        """Ejecutar todas las optimizaciones en secuencia para pasos críticos y en paralelo para pasos independientes."""
        self.validate_columns()  # Validar y registrar las columnas iniciales

        # Ejecución secuencial de renombrado y consolidación, garantizando que las columnas necesarias existen
        self.rename_payment_columns()  # Renombrar columnas de pagos si están presentes
        self.consolidate_date_columns()  # Consolidar columnas de fecha redundantes si es posible

        # Ejecución en paralelo para los pasos restantes
        with ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(self.convert_clicked_to_binary),  # Convertir 'clicked' a binario
                executor.submit(self.fill_missing_payment_values),  # Rellenar valores nulos en columnas de pagos
                executor.submit(self.fill_missing_values),  # Rellenar todos los NaN con 0
            ]

            # Manejar excepciones en tareas paralelas
            for future in futures:
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Error en una de las tareas de optimización: {e}")

        # Filtrar filas irrelevantes después de las otras optimizaciones
        self.filter_irrelevant_rows()
        return self.dataframe
