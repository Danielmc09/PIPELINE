# test/test_prints_loader.py

import pytest
import pandas as pd
from src.ingest.load_prints import PrintsLoader
from src.transform.prints_transformer import PrintsTransformer

def test_prints_expansion(tmp_path):
    # Datos de prueba con "event_data" anidado
    json_data = [
        '{"day":"2020-11-01","event_data":{"position":0,"value_prop":"cellphone_recharge"},"user_id":98702}'
    ]
    json_file = tmp_path / "prints.json"
    json_file.write_text("\n".join(json_data))

    loader = PrintsLoader(file_path=str(json_file))
    df = loader.load_data()

    transformer = PrintsTransformer()
    result_df = transformer.process_data(df)

    # Verificar que la expansión haya ocurrido correctamente
    assert "position" in result_df.columns  # Ajustado para el nombre sin prefijo
    assert "value_prop" in result_df.columns  # Ajustado para el nombre sin prefijo
    assert result_df["position"].iloc[0] == 0
    assert result_df["value_prop"].iloc[0] == "cellphone_recharge"
