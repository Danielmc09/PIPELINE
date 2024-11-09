import pandas as pd
from src.utils.logger import logger  # Asegúrate de que tienes una clase de logger configurada

class DataLoader:
    def __init__(self, file_path):
        """
        Inicializa el cargador de datos con la ruta del archivo.

        Args:
            file_path (str): Ruta del archivo de datos.
        """
        self.file_path = file_path

    def load_json(self):
        """
        Carga un archivo JSON y devuelve un DataFrame.

        Returns:
            pd.DataFrame: DataFrame con los datos cargados desde JSON lines.
        """
        try:
            df = pd.read_json(self.file_path, lines=True)
            logger.info(f"Archivo JSON cargado exitosamente desde {self.file_path}")
            return df
        except ValueError as e:
            logger.error(
                f"Error: No se pudo cargar el archivo JSON en '{self.file_path}'. Asegúrese de que el formato sea correcto y que el archivo no esté vacío.")
            raise ValueError("Error de formato en el archivo JSON.") from e
        except FileNotFoundError as e:
            logger.error(f"Error: No se encontró el archivo '{self.file_path}'. Verifique que la ruta del archivo sea correcta.")
            raise FileNotFoundError("Archivo JSON no encontrado.") from e

    def load_csv(self):
        """
        Carga un archivo CSV y devuelve un DataFrame.

        Returns:
            pd.DataFrame: DataFrame con los datos cargados desde CSV.
        """
        try:
            df = pd.read_csv(self.file_path)
            logger.info(f"Archivo CSV cargado exitosamente desde {self.file_path}")
            return df
        except pd.errors.EmptyDataError as e:
            logger.error(f"Error: El archivo CSV en '{self.file_path}' está vacío. Asegúrese de que contenga datos.")
            raise ValueError("Archivo CSV vacío.") from e
        except FileNotFoundError as e:
            logger.error(f"Error: No se encontró el archivo '{self.file_path}'. Verifique que la ruta del archivo sea correcta.")
            raise FileNotFoundError("Archivo CSV no encontrado.") from e
