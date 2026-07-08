from pathlib import Path

import pandas as pd

from gold.build import build_nominale_values


def _fixture_csv(path: Path) -> Path:
    pd.DataFrame(
        {
            "timestamp": ["2026-06-01 00:00:00", "2026-06-01 00:00:30"],
            "secteur": ["A", "B"],
            "machine_id": ["MCH-001", "MCH-002"],
            "vitesse_rotation_nominal": [6000.0, 6200.0],
            "courant_moteur_nominal": [10.0, 11.0],
            "pression_hydraulique_nominal": [3.0, 3.2],
            "statut_nominal": ["OK", "OK"],
            "temp_base_moteur": [45.0, 46.0],
            "type_metal": ["Acier", "Aluminium"],
            "regime_cadence": ["Nominal", "Degrade"],
            "facteur_cadence": [1.0, 0.8],
            "temps_cycle_sec": [12.5, 13.0],
        }
    ).to_csv(path, index=False)
    return path


def test_joins_type_metal_and_regime_cadence_ids(tmp_path: Path) -> None:
    source_csv = _fixture_csv(tmp_path / "source.csv")
    output_path = tmp_path / "postgres_nominale_values.csv"
    type_metal_path = tmp_path / "postgres_type_metal.csv"
    regime_cadence_path = tmp_path / "postgres_regime_cadence.csv"
    pd.DataFrame({"type_metal": ["Acier", "Aluminium"], "id": [1, 2]}).to_csv(type_metal_path, index=False)
    pd.DataFrame({"regime_cadence": ["Nominal", "Degrade"], "id": [1, 2]}).to_csv(
        regime_cadence_path, index=False
    )

    df = build_nominale_values.build(
        output_path=output_path,
        source_csv=source_csv,
        type_metal_path=type_metal_path,
        regime_cadence_path=regime_cadence_path,
    )

    assert output_path.exists()
    assert list(df.columns) == build_nominale_values.NOMINALE_COLUMNS
    assert df.set_index("machine_id")["id_type_metal"].to_dict() == {"MCH-001": 1, "MCH-002": 2}
    assert df.set_index("machine_id")["id_regime_cadence"].to_dict() == {"MCH-001": 1, "MCH-002": 2}


def test_triggers_missing_neighbors(tmp_path: Path) -> None:
    source_csv = _fixture_csv(tmp_path / "source.csv")
    output_path = tmp_path / "postgres_nominale_values.csv"
    type_metal_path = tmp_path / "postgres_type_metal.csv"
    regime_cadence_path = tmp_path / "postgres_regime_cadence.csv"

    assert not type_metal_path.exists()
    assert not regime_cadence_path.exists()

    df = build_nominale_values.build(
        output_path=output_path,
        source_csv=source_csv,
        type_metal_path=type_metal_path,
        regime_cadence_path=regime_cadence_path,
    )

    assert type_metal_path.exists()
    assert regime_cadence_path.exists()
    assert df["id_type_metal"].notna().all()
    assert df["id_regime_cadence"].notna().all()


def test_skips_recomputation_when_output_already_exists(tmp_path: Path) -> None:
    source_csv = _fixture_csv(tmp_path / "source.csv")
    output_path = tmp_path / "postgres_nominale_values.csv"
    type_metal_path = tmp_path / "postgres_type_metal.csv"
    regime_cadence_path = tmp_path / "postgres_regime_cadence.csv"
    pd.DataFrame({"machine_id": ["Existing"]}).to_csv(output_path, index=False)

    df = build_nominale_values.build(
        output_path=output_path,
        source_csv=source_csv,
        type_metal_path=type_metal_path,
        regime_cadence_path=regime_cadence_path,
    )

    assert df["machine_id"].tolist() == ["Existing"]
    assert not type_metal_path.exists()
    assert not regime_cadence_path.exists()
