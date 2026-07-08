from pathlib import Path

import pandas as pd

from gold.split_cold_storage import melt_to_sensor_data, write_cold_storage


def test_melt_to_sensor_data_produces_long_format_and_drops_na() -> None:
    df = pd.DataFrame(
        {
            "timestamp": ["2024-08-01 00:00:00", "2024-08-01 00:00:30"],
            "machine_id": ["MCH-001", "MCH-002"],
            "iot_vitesse_rotation": [3900.0, None],
            "iot_courant_moteur": [18.9, 19.1],
        }
    )

    long = melt_to_sensor_data(df, ["iot_vitesse_rotation", "iot_courant_moteur"])

    assert set(long.columns) == {"sensor_timestamp", "id_machine", "sensor", "value"}
    # une valeur manquante (MCH-002/iot_vitesse_rotation) ne doit pas produire de ligne
    assert len(long) == 3
    assert not long["value"].isna().any()


def test_melt_to_sensor_data_ignores_columns_absent_from_the_frame() -> None:
    df = pd.DataFrame(
        {
            "timestamp": ["2024-08-01 00:00:00"],
            "machine_id": ["MCH-001"],
            "iot_vitesse_rotation": [3900.0],
        }
    )

    long = melt_to_sensor_data(df, ["iot_vitesse_rotation", "iot_courant_moteur"])

    assert list(long["sensor"]) == ["iot_vitesse_rotation"]


def test_write_cold_storage_writes_one_parquet_file_per_calendar_day(tmp_path: Path) -> None:
    df_iot = pd.DataFrame(
        {
            "timestamp": ["2024-08-01 23:00:00", "2024-08-02 01:00:00"],
            "machine_id": ["MCH-001", "MCH-001"],
            "iot_vitesse_rotation": [3900.0, 3800.0],
        }
    )
    df_plc = pd.DataFrame(
        {
            "timestamp": ["2024-08-01 23:00:00", "2024-08-02 01:00:00"],
            "machine_id": ["MCH-001", "MCH-001"],
            "id_type_metal": [1, 1],
        }
    )

    nb_fichiers = write_cold_storage(df_iot, df_plc, tmp_path)

    assert nb_fichiers == 2
    fichiers = sorted(tmp_path.glob("sensor_data_*.parquet"))
    assert len(fichiers) == 2

    relu = pd.concat(pd.read_parquet(f) for f in fichiers)
    assert set(relu.columns) == {"sensor_timestamp", "id_machine", "sensor", "value", "time"}
    assert set(relu["sensor"]) == {"iot_vitesse_rotation", "id_type_metal"}
    assert len(relu) == 4


def test_write_cold_storage_returns_zero_when_nothing_to_write(tmp_path: Path) -> None:
    vide = pd.DataFrame(columns=["timestamp", "machine_id"])

    assert write_cold_storage(vide, vide, tmp_path) == 0
    assert list(tmp_path.glob("*.parquet")) == []
