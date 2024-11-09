import pandas as pd
from concurrent.futures import ThreadPoolExecutor
from src.utils.logger import logger
from src.utils.data_cleaner import DataCleaner
from src.utils.data_validation import SchemaValidator

class BaseTransformer:
    def __init__(self, schema, critical_columns, non_critical_columns, date_columns=None):
        self.schema = schema
        self.cleaner = DataCleaner(
            critical_columns=critical_columns,
            non_critical_columns=non_critical_columns
        )
        self.schema_validator = SchemaValidator(self.schema)
        self.date_columns = date_columns or []

    def expand_event_data(self, df, column_name="event_data", chunk_size=10000):
        """
        Expande la columna 'event_data' de manera eficiente en chunks.
        """
        try:
            logger.info("Iniciando expansión de 'event_data'.")

            if column_name in df.columns:
                # Expansión en chunks para mejorar el rendimiento
                expanded_chunks = []
                for i in range(0, len(df), chunk_size):
                    chunk = df.iloc[i:i + chunk_size]
                    event_data_expanded = pd.json_normalize(chunk[column_name])
                    expanded_chunk = chunk.drop(columns=[column_name]).reset_index(drop=True).join(event_data_expanded)
                    expanded_chunks.append(expanded_chunk)

                # Concatenar los chunks expandidos en un solo DataFrame
                df = pd.concat(expanded_chunks, ignore_index=True)
                logger.info("Expansión de 'event_data' completada.")
            else:
                logger.warning(f"La columna '{column_name}' no está en el DataFrame.")

            logger.info(f"Columnas en df después de la expansión de 'event_data': {df.columns.tolist()}")
            return df
        except (KeyError, ValueError) as e:
            logger.error(f"Error al expandir 'event_data': {e}")
            raise

    def convert_dates_parallel(self, df):
        """
        Convierte las columnas especificadas a tipo datetime en el DataFrame de manera paralela.
        """
        def convert_column(col):
            try:
                df[col] = pd.to_datetime(df[col], errors='coerce')
                if df[col].isna().sum() > 0:
                    logger.warning(f"Algunos valores en la columna '{col}' no se pudieron convertir a datetime.")
            except Exception as e:
                logger.error(f"Error al convertir columna '{col}' a datetime: {e}")
            return df

        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(convert_column, col) for col in self.date_columns]
            for future in futures:
                future.result()

        return df

    def process_data(self, df):
        """
        Procesa el DataFrame aplicando limpieza, expansión de 'event_data',
        conversión de fechas y validación de esquema.
        """
        logger.info("Iniciando el procesamiento completo de datos.")
        try:
            # Expande la columna 'event_data' en chunks
            df = self.expand_event_data(df)

            # Convertir las columnas de fecha especificadas de manera paralela
            df = self.convert_dates_parallel(df)

            # Limpia los datos con el limpiador DataCleaner
            df = self.cleaner.remove_critical_missing(df)
            df = self.cleaner.impute_non_critical_missing(df)

            # Valida el esquema del DataFrame usando SchemaValidator
            df = self.schema_validator.enforce_schema(df)
            logger.info("Procesamiento de datos completado.")
            return df
        except Exception as e:
            logger.error(f"Error durante el procesamiento de datos: {e}")
            raise
