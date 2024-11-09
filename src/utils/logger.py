import logging
import logging.config
import yaml
import os

# Ruta al archivo de configuración
LOGGING_CONFIG_PATH = "config/logging_config.yaml"
LOGS_DIR = "logs"
LOG_FILE_PATH = f"{LOGS_DIR}/pipeline.log"


def setup_logging():
    # Crear la carpeta de logs si no existe
    if not os.path.exists(LOGS_DIR):
        os.makedirs(LOGS_DIR)

    if os.path.exists(LOGGING_CONFIG_PATH):
        with open(LOGGING_CONFIG_PATH, "r") as f:
            config = yaml.safe_load(f.read())
            # Actualizar el path del archivo de logs en la configuración
            config['handlers']['file_handler']['filename'] = LOG_FILE_PATH
            logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=logging.INFO)
        print(f"Advertencia: No se encontró el archivo de configuración de logging en {LOGGING_CONFIG_PATH}.")


# Llamar a esta función en el punto de entrada de tu pipeline para configurar los logs
setup_logging()

# Crear un logger específico
logger = logging.getLogger("pipeline_logger")
