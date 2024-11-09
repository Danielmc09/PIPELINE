import pandas as pd
from src.preprocess.aggregate_metrics import MetricsAggregator

def test_metrics_aggregate():
    # DataFrame de prueba para prints_df
    data = {
        "day_prints": ["2020-11-01", "2020-11-02"],
        "user_id": [98702, 98702],
        "position_prints": [0, 1],
        "value_prop": ["cellphone_recharge", "prepaid"]
    }
    prints_df = pd.DataFrame(data)
    prints_df["day_prints"] = pd.to_datetime(prints_df["day_prints"])

    # DataFrame de prueba para taps_df, incluyendo ambas columnas: 'day_taps' y 'day_prints'
    taps_data = {
        "day_taps": ["2020-11-01", "2020-11-01", "2020-11-02"],
        "user_id": [98702, 98702, 98702],
        "position_taps": [0, 1, 0],
        "value_prop": ["cellphone_recharge", "prepaid", "cellphone_recharge"]
    }
    taps_df = pd.DataFrame(taps_data)
    taps_df["day_taps"] = pd.to_datetime(taps_df["day_taps"])
    taps_df["day_prints"] = taps_df["day_taps"]  # Duplicamos 'day_taps' como 'day_prints'

    # Inicializar el MetricsAggregator
    metrics_aggregator = MetricsAggregator()

    # Llamada al método sin cambiar el orden de los argumentos
    click_indicator_df = metrics_aggregator.calculate_click_indicator(taps_df, prints_df)

    # Verificación de los resultados
    assert not click_indicator_df.empty, "El resultado de click indicator debería tener datos."
    expected_columns = {metrics_aggregator.user_id_col, metrics_aggregator.value_prop_col, "day_prints", "clicked"}
    assert set(click_indicator_df.columns) == expected_columns, f"Las columnas del resultado no son las esperadas: {click_indicator_df.columns}"
