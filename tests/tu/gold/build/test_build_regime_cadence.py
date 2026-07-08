from pathlib import Path

import pandas as pd

from gold.build import build_regime_cadence


def _fixture_csv(path: Path) -> Path:
    pd.DataFrame(
        {
            "regime_cadence": ["Nominal", "Degrade", "Nominal", "Arret"],
            "machine_id": ["MCH-001", "MCH-002", "MCH-003", "MCH-004"],
        }
    ).to_csv(path, index=False)
    return path


def test_computes_unique_regime_cadence_with_ids(tmp_path: Path) -> None:
    source_csv = _fixture_csv(tmp_path / "source.csv")
    output_path = tmp_path / "postgres_regime_cadence.csv"

    df = build_regime_cadence.build(output_path=output_path, source_csv=source_csv)

    assert output_path.exists()
    assert sorted(df["regime_cadence"]) == ["Arret", "Degrade", "Nominal"]
    assert df["id"].is_unique
    assert set(df["id"]) == {1, 2, 3}


def test_skips_recomputation_when_output_already_exists(tmp_path: Path) -> None:
    source_csv = _fixture_csv(tmp_path / "source.csv")
    output_path = tmp_path / "postgres_regime_cadence.csv"
    pd.DataFrame({"regime_cadence": ["Existing"], "id": [42]}).to_csv(output_path, index=False)

    df = build_regime_cadence.build(output_path=output_path, source_csv=source_csv)

    assert df.to_dict("records") == [{"regime_cadence": "Existing", "id": 42}]
