import json
from pathlib import Path

import pandas as pd

from gold.build import build_mqtt_jsonl


def _fixture_csv(path: Path) -> Path:
    pd.DataFrame(
        {
            "machine_id": ["MCH-001", "MCH-002"],
            "timestamp": ["2026-06-01 00:00:00", "2026-06-01 00:00:30"],
            "age_jours": [100, 200],
            "age_virtuel_jours": [100, 200],
            "label_gmao": ["Sain", "Sain"],
            "RUL_jours": [10, 20],
            "secteur": ["A", "B"],
            "type_machine": ["Presse", "Four"],
            "vitesse_rotation_nominal": [6000.0, 6200.0],
            "courant_moteur_nominal": [10.0, 11.0],
            "pression_hydraulique_nominal": [3.0, 3.2],
            "statut_nominal": ["OK", "OK"],
            "type_metal": ["Acier", "Aluminium"],
            "temp_base_moteur": [45.0, 46.0],
            "iot_statut_machine": ["Production", "Arret"],
            "iot_vibration_rms": [0.5, 0.6],
            "iot_vitesse_rotation": [5990.0, 6190.0],
            "iot_courant_moteur": [9.8, 10.9],
            "iot_pression_hydraulique": [2.9, 3.1],
            "iot_temperature": [48.0, 49.0],
            "iot_vibration_peak": [1.1, 1.2],
            "iot_charge_moteur": [40.0, 41.0],
        }
    ).to_csv(path, index=False)
    return path


def _read_jsonl(path: Path) -> list[list[dict]]:
    with open(path, encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


def test_builds_jsonl_with_plc_ids_joined(tmp_path: Path) -> None:
    source_csv = _fixture_csv(tmp_path / "source.csv")
    output_path = tmp_path / "mqtt_iot_plc_send.jsonl"
    type_metal_path = tmp_path / "postgres_type_metal.csv"
    production_status_path = tmp_path / "postgres_production_status.csv"
    pd.DataFrame({"type_metal": ["Acier", "Aluminium"], "id": [1, 2]}).to_csv(type_metal_path, index=False)
    pd.DataFrame({"iot_statut_machine": ["Production", "Arret"], "id": [1, 2]}).to_csv(
        production_status_path, index=False
    )

    result_path = build_mqtt_jsonl.build(
        output_path=output_path,
        source_csv=source_csv,
        type_metal_path=type_metal_path,
        production_status_path=production_status_path,
    )

    assert result_path == output_path
    assert output_path.exists()
    lines = _read_jsonl(output_path)
    assert len(lines) == 2
    all_fields = {k for records in lines for r in records for k in r}
    assert "id_type_metal" in all_fields
    assert "id_status_production" in all_fields


def test_triggers_missing_neighbors(tmp_path: Path) -> None:
    source_csv = _fixture_csv(tmp_path / "source.csv")
    output_path = tmp_path / "mqtt_iot_plc_send.jsonl"
    type_metal_path = tmp_path / "postgres_type_metal.csv"
    production_status_path = tmp_path / "postgres_production_status.csv"

    build_mqtt_jsonl.build(
        output_path=output_path,
        source_csv=source_csv,
        type_metal_path=type_metal_path,
        production_status_path=production_status_path,
    )

    assert type_metal_path.exists()
    assert production_status_path.exists()
    assert output_path.exists()


def test_skips_recomputation_when_output_already_exists(tmp_path: Path) -> None:
    source_csv = _fixture_csv(tmp_path / "source.csv")
    output_path = tmp_path / "mqtt_iot_plc_send.jsonl"
    output_path.write_text('[{"sentinel": true}]\n', encoding="utf-8")
    type_metal_path = tmp_path / "postgres_type_metal.csv"
    production_status_path = tmp_path / "postgres_production_status.csv"

    build_mqtt_jsonl.build(
        output_path=output_path,
        source_csv=source_csv,
        type_metal_path=type_metal_path,
        production_status_path=production_status_path,
    )

    assert _read_jsonl(output_path) == [[{"sentinel": True}]]
    assert not type_metal_path.exists()
    assert not production_status_path.exists()
