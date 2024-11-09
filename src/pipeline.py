import yaml
import time
from utils.logger import setup_logging, logger
from ingest.load_prints import PrintsLoader
from ingest.load_taps import TapsLoader
from ingest.load_pays import PaysLoader
from preprocess.filter_last_week import LastWeekFilter
from preprocess.aggregate_metrics import MetricsAggregator
from transform.join_data import DataJoiner
from load.export_dataset import DatasetExporter
from transform.optimize_data import DatasetOptimizer
import concurrent.futures

def load_config(config_path="config/config.yaml"):
    with open(config_path, "r") as file:
        return yaml.safe_load(file)


def load_data_parallel(loaders):
    """
    Carga los datos en paralelo desde múltiples fuentes.
    """
    data_frames = {}
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Crear una lista de tareas para cargar y transformar cada archivo
        future_to_loader = {executor.submit(loader.load_and_transform): name for name, loader in loaders.items()}

        # Recoger los resultados conforme se vayan completando
        for future in concurrent.futures.as_completed(future_to_loader):
            name = future_to_loader[future]
            try:
                # Almacenamos el DataFrame cargado y transformado en el diccionario
                data_frames[name] = future.result()
                logger.info(f"{name.capitalize()} cargado y transformado: {data_frames[name].shape} registros")
            except Exception as exc:
                logger.error(f"{name} falló con la excepción: {exc}")
    return data_frames


def load_data(loaders):
    data_frames = {}
    for name, loader in loaders.items():
        start_time = time.time()
        df = loader.load_and_transform()
        logger.info(f"{name.capitalize()} cargado y transformado: {df.shape} registros")
        data_frames[name] = df
        logger.info(f"{name.capitalize()} carga completada en {time.time() - start_time:.2f}s")
    return data_frames


def join_data(prints_df, taps_df, pays_df):
    data_joiner = DataJoiner()
    master_df = data_joiner.join_data(prints_df, taps_df, pays_df)
    logger.info(f"DataFrame maestro unido: {master_df.shape} registros, columnas: {master_df.columns.tolist()}")
    return master_df


def filter_data_last_week(master_df, date_column="day_prints"):
    last_week_filter = LastWeekFilter(date_column=date_column)
    filtered_df = last_week_filter.filter(master_df)
    logger.info(f"DataFrame filtrado por la última semana: {filtered_df.shape}")
    return filtered_df


def calculate_metrics_parallel(master_df, prints_last_week, pays_df):
    metrics_aggregator = MetricsAggregator()
    final_df = prints_last_week[['user_id', 'value_prop', 'day_prints']].copy()

    # Métricas de clics en paralelo
    click_indicator = metrics_aggregator.calculate_click_indicator(master_df, prints_last_week)
    final_df = final_df.merge(click_indicator, on=['user_id', 'value_prop', 'day_prints'], how='left')
    logger.info("Indicador de clics agregado en paralelo.")

    # Cantidad de vistas en paralelo
    view_counts = metrics_aggregator.calculate_view_counts(master_df, prints_last_week, 3)
    final_df = final_df.merge(view_counts, on=['user_id', 'value_prop', 'day_prints'], how='left')
    logger.info("Cantidad de vistas agregada en paralelo.")

    # Cantidad de clics en paralelo
    click_counts = metrics_aggregator.calculate_click_counts_per_value_prop(master_df, prints_last_week, 3)
    final_df = final_df.merge(click_counts, on=['user_id', 'value_prop', 'day_prints'], how='left')
    logger.info("Cantidad de clics agregada en paralelo.")

    # Importe acumulado de pagos en paralelo
    cumulative_payment_amounts = metrics_aggregator.calculate_cumulative_payment_amounts(master_df, pays_df, 3)
    final_df = final_df.merge(cumulative_payment_amounts, on=['user_id', 'value_prop', 'day_prints'], how='left')
    logger.info("Importe acumulado agregado en paralelo.")

    return final_df


def optimize_and_export(final_df, config):
    # Optimizar el dataset
    optimizer = DatasetOptimizer(final_df)
    final_df = optimizer.optimize()
    logger.info("Optimización del dataset final completada.")

    # Configurar la ruta y el formato de exportación
    export_format = config["output_paths"].get("export_format", "csv").lower()
    file_path = f"{config['output_paths']['final']}.{export_format}"

    # Exportar el dataset en el formato especificado
    exporter = DatasetExporter()
    exporter.export(final_df, file_path, format=export_format)
    logger.info(f"Dataset exportado exitosamente a {file_path} en formato {export_format}.")


def main():
    setup_logging()
    logger.info("Iniciando el pipeline de procesamiento de datos.")

    # Cargar configuración
    config = load_config()

    # Cargar datos
    loaders = {
        "prints": PrintsLoader(config["data_paths"]["prints"]),
        "taps": TapsLoader(config["data_paths"]["taps"]),
        "pays": PaysLoader(config["data_paths"]["pays"])
    }
    #data = load_data(loaders)
    data = load_data_parallel(loaders)

    # Unir datos en el DataFrame maestro
    master_df = join_data(data["prints"], data["taps"], data["pays"])

    # Filtrar últimos registros de la semana
    prints_last_week = filter_data_last_week(master_df)

    # Calcular métricas y unirlas al DataFrame final
    final_df = calculate_metrics_parallel(master_df, prints_last_week, data["pays"])

    # Optimizar y exportar el dataset final
    optimize_and_export(final_df, config)

    logger.info("Pipeline de procesamiento completado exitosamente.")


if __name__ == "__main__":
    main()
