import pandas as pd
from src.utils.data_loader import DataLoader
from src.utils.logger import logger


class BaseLoader:
    def __init__(self, file_path, transformer_class):
        """
        Inicializa el cargador base con la ruta del archivo y la clase del transformador.

        Args:
            file_path (str): Ruta del archivo de datos.
            transformer_class (class): Clase del transformador a utilizar.
        """
        self.file_path = file_path
        self.data_loader = DataLoader(self.file_path)
        self.transformer = transformer_class()
        logger.info(f"Inicializando {self.__class__.__name__} con archivo: {file_path}")

    def load_data(self):
        """
        Carga los datos desde el archivo.

        Returns:
            pd.DataFrame: DataFrame cargado desde el archivo.
        """
        try:
            if self.file_path.endswith(".json"):
                df = self.data_loader.load_json()
            elif self.file_path.endswith(".csv"):
                df = self.data_loader.load_csv()
            else:
                raise ValueError("Formato de archivo no soportado. Solo se admiten JSON y CSV.")

            logger.info(f"Archivo cargado exitosamente desde {self.file_path}")
            return df
        except (ValueError, FileNotFoundError) as e:
            logger.error(f"Error al cargar los datos desde {self.file_path}: {e}")
            raise

    def transform_data(self, df):
        """
        Aplica la transformación a los datos cargados.

        Args:
            df (pd.DataFrame): DataFrame cargado a transformar.

        Returns:
            pd.DataFrame: DataFrame transformado.
        """
        try:
            transformed_df = self.transformer.process_data(df)
            logger.info("Transformación de datos completada con éxito.")
            return transformed_df
        except KeyError as e:
            logger.error(f"Error al transformar los datos: {e}")
            raise

    def load_and_transform(self):
        """
        Ejecuta el proceso completo de carga y transformación de datos.

        Returns:
            pd.DataFrame: DataFrame procesado.
        """
        df = self.load_data()
        return self.transform_data(df)
