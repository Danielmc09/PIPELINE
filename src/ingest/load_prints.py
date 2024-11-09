# src/ingest/load_prints.py

from src.ingest.base_loader import BaseLoader
from src.transform.prints_transformer import PrintsTransformer

class PrintsLoader(BaseLoader):
    def __init__(self, file_path):
        """
        Inicializa el cargador de datos de prints con la ruta del archivo.

        Args:
            file_path (str): Ruta del archivo de datos de prints.
        """
        super().__init__(file_path, PrintsTransformer)
