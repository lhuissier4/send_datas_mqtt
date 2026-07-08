import json
from pathlib import Path

import pandas as pd
import pytest

from gold.utils import (
    PLC_COLUMNS,
    SENSOR_COLUMNS,
    record_future_send_in_jsonl,
)


RESSOURCES = Path(__file__).parent / "ressources"
IOT_CSV = RESSOURCES / "df_iot_test.csv"
PLC_CSV = RESSOURCES / "df_plc_test.csv"


def _load_iot_head() -> pd.DataFrame:
    """lignes IoT (colonnes timestamp, machine_id, iot_*)."""
    return pd.DataFrame(pd.read_csv(IOT_CSV))


def _load_plc_head() -> pd.DataFrame:
    """lignes PLC, encodées comme dans le pipeline gold (id_*)."""
    return pd.DataFrame(pd.read_csv(PLC_CSV))


def _read_jsonl(path: Path) -> list[list[dict]]:
    """Relit un fichier JSONL : une liste de records par ligne."""
    with open(path, encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


@pytest.mark.skipif(
    not (IOT_CSV.exists() and PLC_CSV.exists()),
    reason="datasets silver absents",
)
def test_creates_output_file(tmp_path: Path) -> None:
    """Le fichier de sortie est bien créé, y compris le dossier parent."""
    output = tmp_path / "nested" / "mqtt_send.jsonl"

    record_future_send_in_jsonl(
        _load_iot_head(), _load_plc_head(), output_path=str(output)
    )

    assert output.exists()


@pytest.mark.skipif(
    not (IOT_CSV.exists() and PLC_CSV.exists()),
    reason="datasets silver absents",
)
def test_one_line_per_timestamp(tmp_path: Path) -> None:
    """Une ligne JSONL == un tick (un timestamp), triée croissant."""
    output = tmp_path / "out.jsonl"
    df_iot, df_plc = _load_iot_head(), _load_plc_head()

    record_future_send_in_jsonl(df_iot, df_plc, output_path=str(output))

    lines = _read_jsonl(output)
    expected_ts = sorted(
        set(df_iot["timestamp"]) | set(df_plc["timestamp"])
    )
    assert len(lines) == len(expected_ts)

    # le timestamp de chaque record d'une ligne est unique et suit l'ordre
    seen_ts = [records[0]["timestamp"] for records in lines]
    assert seen_ts == expected_ts


@pytest.mark.skipif(
    not (IOT_CSV.exists() and PLC_CSV.exists()),
    reason="datasets silver absents",
)
def test_every_line_is_valid_json_list(tmp_path: Path) -> None:
    """Chaque ligne est un tableau JSON non vide de dicts."""
    output = tmp_path / "out.jsonl"

    record_future_send_in_jsonl(
        _load_iot_head(), _load_plc_head(), output_path=str(output)
    )

    for records in _read_jsonl(output):
        assert isinstance(records, list)
        assert len(records) > 0
        assert all(isinstance(r, dict) for r in records)


@pytest.mark.skipif(
    not (IOT_CSV.exists() and PLC_CSV.exists()),
    reason="datasets silver absents",
)
def test_records_share_their_line_timestamp(tmp_path: Path) -> None:
    """Tous les records d'une même ligne partagent le même timestamp."""
    output = tmp_path / "out.jsonl"

    record_future_send_in_jsonl(
        _load_iot_head(), _load_plc_head(), output_path=str(output)
    )

    for records in _read_jsonl(output):
        timestamps = {r["timestamp"] for r in records}
        assert len(timestamps) == 1


@pytest.mark.skipif(
    not (IOT_CSV.exists() and PLC_CSV.exists()),
    reason="datasets silver absents",
)
def test_record_structure(tmp_path: Path) -> None:
    """Chaque record = {timestamp, id_machine, <une colonne sensor/plc>}."""
    output = tmp_path / "out.jsonl"

    record_future_send_in_jsonl(
        _load_iot_head(), _load_plc_head(), output_path=str(output)
    )

    allowed_value_keys = set(SENSOR_COLUMNS) | set(PLC_COLUMNS)
    for records in _read_jsonl(output):
        for r in records:
            assert "timestamp" in r
            assert "id_machine" in r
            value_keys = set(r) - {"timestamp", "id_machine"}
            assert len(value_keys) == 1
            assert value_keys.issubset(allowed_value_keys)


@pytest.mark.skipif(
    not (IOT_CSV.exists() and PLC_CSV.exists()),
    reason="datasets silver absents",
)
def test_iot_and_plc_merged_on_same_tick(tmp_path: Path) -> None:
    """Les records IoT et PLC d'un même timestamp sont sur la même ligne."""
    output = tmp_path / "out.jsonl"

    record_future_send_in_jsonl(
        _load_iot_head(), _load_plc_head(), output_path=str(output)
    )

    sensor_set, plc_set = set(SENSOR_COLUMNS), set(PLC_COLUMNS)
    for records in _read_jsonl(output):
        fields = {k for r in records for k in r if k in sensor_set | plc_set}
        # les CSV de test partagent les mêmes timestamps -> IoT ET PLC présents
        assert fields & sensor_set
        assert fields & plc_set


def test_plc_optional(tmp_path: Path) -> None:
    """Sans df_plc, seules les colonnes IoT apparaissent."""
    output = tmp_path / "out.jsonl"
    df_iot = pd.DataFrame(
        {
            "timestamp": ["2026-06-01 00:00:00", "2026-06-01 00:00:30"],
            "machine_id": ["MCH-001", "MCH-001"],
            **{col: [1.0, 2.0] for col in SENSOR_COLUMNS},
        }
    )

    record_future_send_in_jsonl(df_iot, None, output_path=str(output))

    lines = _read_jsonl(output)
    assert len(lines) == 2
    for records in lines:
        for r in records:
            value_keys = set(r) - {"timestamp", "id_machine"}
            assert value_keys.issubset(set(SENSOR_COLUMNS))


def test_na_values_produce_no_record(tmp_path: Path) -> None:
    """Une valeur NA ne génère aucun record."""
    output = tmp_path / "out.jsonl"
    df_iot = pd.DataFrame(
        {
            "timestamp": ["2026-06-01 00:00:00"],
            "machine_id": ["MCH-001"],
            "iot_vitesse_rotation": [6000.0],
            "iot_courant_moteur": [float("nan")],
            "iot_pression_hydraulique": [0.0],
            "iot_temperature": [48.0],
            "iot_vibration_peak": [1.2],
            "iot_charge_moteur": [45.0],
        }
    )

    record_future_send_in_jsonl(df_iot, None, output_path=str(output))

    records = _read_jsonl(output)[0]
    fields = {k for r in records for k in r if k.startswith("iot_")}
    assert "iot_courant_moteur" not in fields
    assert len(records) == len(SENSOR_COLUMNS) - 1


def test_plc_only_timestamp(tmp_path: Path) -> None:
    """Un timestamp présent uniquement côté PLC produit sa propre ligne."""
    output = tmp_path / "out.jsonl"
    df_iot = pd.DataFrame(
        {
            "timestamp": ["2026-06-01 00:00:00"],
            "machine_id": ["MCH-001"],
            **{col: [1.0] for col in SENSOR_COLUMNS},
        }
    )
    df_plc = pd.DataFrame(
        {
            "timestamp": ["2026-06-01 00:00:30"],
            "machine_id": ["MCH-001"],
            **{col: [1] for col in PLC_COLUMNS},
        }
    )

    record_future_send_in_jsonl(df_iot, df_plc, output_path=str(output))

    lines = _read_jsonl(output)
    assert len(lines) == 2
    # 2e ligne = timestamp PLC seul -> uniquement des champs PLC
    plc_line_fields = {
        k for r in lines[1] for k in r if k in set(PLC_COLUMNS)
    }
    assert plc_line_fields
    assert all("iot_" not in k for r in lines[1] for k in r)


def test_records_values_are_preserved(tmp_path: Path) -> None:
    """Les valeurs écrites correspondent aux valeurs du DataFrame source."""
    output = tmp_path / "out.jsonl"
    df_iot = pd.DataFrame(
        {
            "timestamp": ["2026-06-01 00:00:00"],
            "machine_id": ["MCH-042"],
            **{col: [float(i)] for i, col in enumerate(SENSOR_COLUMNS)},
        }
    )

    record_future_send_in_jsonl(df_iot, None, output_path=str(output))

    records = _read_jsonl(output)[0]
    by_field = {
        k: r[k] for r in records for k in r if k not in {"timestamp", "id_machine"}
    }
    for i, col in enumerate(SENSOR_COLUMNS):
        assert by_field[col] == float(i)
    assert all(r["id_machine"] == "MCH-042" for r in records)
