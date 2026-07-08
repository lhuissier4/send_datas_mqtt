from pathlib import Path

import pandas as pd

from gold.build import build_machine_secteur_historique


def _fixture_csv(path: Path) -> Path:
    pd.DataFrame(
        {
            "machine_id": ["MCH-001", "MCH-001", "MCH-001"],
            "timestamp": [
                "2026-06-01 00:00:00",
                "2026-06-01 00:01:00",
                "2026-06-01 00:02:00",
            ],
            "secteur": ["A", "A", "B"],
        }
    ).to_csv(path, index=False)
    return path


def test_computes_one_row_per_secteur_run(tmp_path: Path) -> None:
    source_csv = _fixture_csv(tmp_path / "source.csv")
    output_path = tmp_path / "postgres_machine_secteur_historique.csv"

    df = build_machine_secteur_historique.build(output_path=output_path, source_csv=source_csv)

    assert output_path.exists()
    assert len(df) == 2
    assert df["secteur"].tolist() == ["A", "B"]


def test_skips_recomputation_when_output_already_exists(tmp_path: Path) -> None:
    source_csv = _fixture_csv(tmp_path / "source.csv")
    output_path = tmp_path / "postgres_machine_secteur_historique.csv"
    pd.DataFrame(
        {"id_machine": ["Existing"], "secteur": ["Z"], "date_mise_en_service": ["2026-01-01 00:00:00"]}
    ).to_csv(output_path, index=False)

    df = build_machine_secteur_historique.build(output_path=output_path, source_csv=source_csv)

    assert df["secteur"].tolist() == ["Z"]
