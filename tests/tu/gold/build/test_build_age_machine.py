from pathlib import Path

import pandas as pd

from gold.build import build_age_machine


def _fixture_csv(path: Path) -> Path:
    pd.DataFrame(
        {
            "machine_id": ["MCH-001", "MCH-001", "MCH-002"],
            "timestamp": [
                "2026-06-01 00:00:00",
                "2026-06-01 00:01:00",
                "2026-06-01 00:00:30",
            ],
            "age_jours": [100, 100, 250],
        }
    ).to_csv(path, index=False)
    return path


def test_computes_one_row_per_machine_with_first_timestamp(tmp_path: Path) -> None:
    source_csv = _fixture_csv(tmp_path / "source.csv")
    output_path = tmp_path / "postgres_age_machine.csv"

    df = build_age_machine.build(output_path=output_path, source_csv=source_csv)

    assert output_path.exists()
    assert sorted(df["id_machine"]) == ["MCH-001", "MCH-002"]
    row = df[df["id_machine"] == "MCH-001"].iloc[0]
    assert row["age_machine_jours"] == 100
    assert row["premier_timestamp"] == "2026-06-01 00:00:00"


def test_skips_recomputation_when_output_already_exists(tmp_path: Path) -> None:
    source_csv = _fixture_csv(tmp_path / "source.csv")
    output_path = tmp_path / "postgres_age_machine.csv"
    pd.DataFrame(
        {"id_machine": ["Existing"], "age_machine_jours": [1], "premier_timestamp": ["2026-01-01 00:00:00"]}
    ).to_csv(output_path, index=False)

    df = build_age_machine.build(output_path=output_path, source_csv=source_csv)

    assert df["id_machine"].tolist() == ["Existing"]
