# test/test_taps_loader.py

import pytest
import pandas as pd
from src.ingest.load_taps import TapsLoader
from src.transform.taps_transformer import TapsTransformer

def test_taps_transformation(tmp_path):
    # Datos de prueba con "event_data" ya expandidos para TapsLoader
    json_data = [
        '{"day":"2020-11-01","position":1,"value_prop":"prepaid","user_id":98702}'
    ]
    json_file = tmp_path / "taps.json"
    json_file.write_text("\n".join(json_data))

    loader = TapsLoader(file_path=str(json_file))
    df = loader.load_data()

    transformer = TapsTransformer()
    result_df = transformer.process_data(df)

    # Verificar que las columnas esperadas están presentes sin prefijo
    assert "position" in result_df.columns  # Ajustado para el nombre sin prefijo
    assert "value_prop" in result_df.columns  # Ajustado para el nombre sin prefijo
    assert result_df["position"].iloc[0] == 1
    assert result_df["value_prop"].iloc[0] == "prepaid"
