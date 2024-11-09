# test/test_data_loader.py

import pytest
import pandas as pd
from src.utils.data_loader import DataLoader

def test_load_json_successful(tmp_path):
    # Crear un archivo JSON temporal
    json_data = [
        '{"day":"2020-11-01","event_data":{"position":0,"value_prop":"cellphone_recharge"},"user_id":98702}',
        '{"day":"2020-11-01","event_data":{"position":1,"value_prop":"prepaid"},"user_id":98702}'
    ]
    json_file = tmp_path / "test_prints.json"
    json_file.write_text("\n".join(json_data))

    # Instanciar DataLoader y cargar el archivo
    loader = DataLoader(file_path=str(json_file))
    df = loader.load_json()

    # Verificar que el DataFrame se cargue correctamente
    assert not df.empty, "El DataFrame no debe estar vacío"
    assert "day" in df.columns, "El DataFrame debe contener la columna 'day'"
    assert "event_data" in df.columns, "El DataFrame debe contener la columna 'event_data'"
    assert "user_id" in df.columns, "El DataFrame debe contener la columna 'user_id'"

def test_load_json_file_not_found():
    # Prueba para un archivo que no existe
    loader = DataLoader("ruta_incorrecta.json")
    with pytest.raises(FileNotFoundError, match="Archivo JSON no encontrado"):
        loader.load_json()

def test_load_json_invalid_format(tmp_path):
    # Crear un archivo JSON con formato inválido
    invalid_json = tmp_path / "invalid.json"
    invalid_json.write_text("Esto no es un JSON válido")

    # Instanciar DataLoader y tratar de cargar el archivo
    loader = DataLoader(str(invalid_json))
    with pytest.raises(ValueError, match="Error de formato en el archivo JSON"):
        loader.load_json()

def test_load_csv_successful(tmp_path):
    # Crear un archivo CSV temporal
    csv_data = "day,user_id,event_data.position,event_data.value_prop\n2020-11-01,98702,0,cellphone_recharge\n2020-11-01,98702,1,prepaid"
    csv_file = tmp_path / "test_pays.csv"
    csv_file.write_text(csv_data)

    # Instanciar DataLoader y cargar el archivo
    loader = DataLoader(file_path=str(csv_file))
    df = loader.load_csv()

    # Verificar que el DataFrame se cargue correctamente
    assert not df.empty, "El DataFrame no debe estar vacío"
    assert "day" in df.columns, "El DataFrame debe contener la columna 'day'"
    assert "user_id" in df.columns, "El DataFrame debe contener la columna 'user_id'"
    assert "event_data.position" in df.columns, "El DataFrame debe contener la columna 'event_data.position'"
    assert "event_data.value_prop" in df.columns, "El DataFrame debe contener la columna 'event_data.value_prop'"

def test_load_csv_file_not_found():
    # Prueba para un archivo CSV que no existe
    loader = DataLoader("ruta_incorrecta.csv")
    with pytest.raises(FileNotFoundError, match="Archivo CSV no encontrado"):
        loader.load_csv()

def test_load_csv_empty_file(tmp_path):
    # Crear un archivo CSV vacío
    empty_csv = tmp_path / "empty.csv"
    empty_csv.write_text("")

    # Instanciar DataLoader y tratar de cargar el archivo
    loader = DataLoader(str(empty_csv))
    with pytest.raises(ValueError, match="Archivo CSV vacío"):
        loader.load_csv()
