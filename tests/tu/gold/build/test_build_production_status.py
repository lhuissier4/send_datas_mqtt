from pathlib import Path

import pandas as pd

from gold.build import build_production_status


def _fixture_csv(path: Path) -> Path:
    pd.DataFrame(
        {
            "iot_statut_machine": ["Production", "Arret", "Production", "Maintenance"],
            "machine_id": ["MCH-001", "MCH-002", "MCH-003", "MCH-004"],
        }
    ).to_csv(path, index=False)
    return path


def test_computes_unique_production_status_with_ids(tmp_path: Path) -> None:
    source_csv = _fixture_csv(tmp_path / "source.csv")
    output_path = tmp_path / "postgres_production_status.csv"

    df = build_production_status.build(output_path=output_path, source_csv=source_csv)

    assert output_path.exists()
    assert sorted(df["iot_statut_machine"]) == ["Arret", "Maintenance", "Production"]
    assert df["id"].is_unique
    assert set(df["id"]) == {1, 2, 3}


def test_skips_recomputation_when_output_already_exists(tmp_path: Path) -> None:
    source_csv = _fixture_csv(tmp_path / "source.csv")
    output_path = tmp_path / "postgres_production_status.csv"
    pd.DataFrame({"iot_statut_machine": ["Existing"], "id": [42]}).to_csv(output_path, index=False)

    df = build_production_status.build(output_path=output_path, source_csv=source_csv)

    assert df.to_dict("records") == [{"iot_statut_machine": "Existing", "id": 42}]
