# src/ingest/load_taps.py

from src.ingest.base_loader import BaseLoader
from src.transform.taps_transformer import TapsTransformer


class TapsLoader(BaseLoader):
    def __init__(self, file_path):
        """
        Inicializa el cargador de datos de taps con la ruta del archivo.

        Args:
            file_path (str): Ruta del archivo de datos de taps.
        """
        super().__init__(file_path, TapsTransformer)
