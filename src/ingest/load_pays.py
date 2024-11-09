# src/ingest/load_pays.py

from src.ingest.base_loader import BaseLoader
from src.transform.pays_transformer import PaysTransformer

class PaysLoader(BaseLoader):
    def __init__(self, file_path):
        """
        Inicializa el cargador de datos de pagos con la ruta del archivo.

        Args:
            file_path (str): Ruta del archivo de pagos CSV.
        """
        # Llama a BaseLoader pasando la clase transformadora específica de pagos
        super().__init__(file_path, PaysTransformer)
