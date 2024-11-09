from utils.logger import logger
import pandas as pd

class DatasetExporter:
    def export(self, dataframe, file_path, format="csv"):
        """
        Exporta el DataFrame optimizado al formato especificado.

        Args:
            dataframe (pd.DataFrame): El DataFrame a exportar.
            file_path (str): Ruta completa y nombre del archivo de destino.
            format (str): Formato de exportación ("csv", "json", "excel", "parquet", "feather"). Por defecto es "csv".
        """
        try:
            if format == "csv":
                self._export_to_csv(dataframe, file_path)
            elif format == "json":
                self._export_to_json(dataframe, file_path)
            elif format == "parquet":
                self._export_to_parquet(dataframe, file_path)
            elif format == "feather":
                self._export_to_feather(dataframe, file_path)
            else:
                logger.error(f"Formato de exportación '{format}' no soportado.")
                raise ValueError(f"Formato de exportación '{format}' no soportado.")
        except Exception as e:
            logger.error(f"Error al exportar el dataset en formato {format}: {e}")
            raise

    def _export_to_csv(self, dataframe, file_path):
        dataframe.to_csv(file_path, index=False)
        logger.info(f"Dataset exportado exitosamente en formato CSV a {file_path}")

    def _export_to_json(self, dataframe, file_path):
        dataframe.to_json(file_path, orient="records", lines=True)
        logger.info(f"Dataset exportado exitosamente en formato JSON a {file_path}")

    def _export_to_parquet(self, dataframe, file_path):
        dataframe.to_parquet(file_path, index=False)
        logger.info(f"Dataset exportado exitosamente en formato Parquet a {file_path}")

    def _export_to_feather(self, dataframe, file_path):
        dataframe.to_feather(file_path)
        logger.info(f"Dataset exportado exitosamente en formato Feather a {file_path}")
