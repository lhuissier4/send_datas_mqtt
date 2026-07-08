from pathlib import Path

import pandas as pd

from gold.build import build_maintenance_alerte_postgres


def _fixture_csv(path: Path) -> Path:
    pd.DataFrame(
        {
            "machine_id": ["MCH-001"] * 5,
            "timestamp": [f"2026-06-01 00:0{i}:00" for i in range(5)],
            "label_gmao": [
                "Sain",
                "Alerte_P4",
                "Alerte_P5",
                "Maintenance_Correctif_P8",
                "Maintenance_Preventif_PLANIFIEE",
            ],
        }
    ).to_csv(path, index=False)
    return path


def test_build_maintenance_writes_both_csv_when_both_missing(tmp_path: Path) -> None:
    source_csv = _fixture_csv(tmp_path / "source.csv")
    maintenance_path = tmp_path / "postgres_maintenance.csv"
    alerte_path = tmp_path / "postgres_alerte.csv"

    df_maintenance = build_maintenance_alerte_postgres.build_maintenance(
        maintenance_path=maintenance_path, alerte_path=alerte_path, source_csv=source_csv
    )

    assert maintenance_path.exists()
    assert alerte_path.exists()
    assert sorted(df_maintenance["label_gmao"]) == [
        "Maintenance_Correctif_P8",
        "Maintenance_Preventif_PLANIFIEE",
    ]
    df_alerte = pd.read_csv(alerte_path)
    assert sorted(df_alerte["label_gmao"]) == ["Alerte_P4", "Alerte_P5"]
    assert "Sain" not in df_maintenance["label_gmao"].tolist()


def test_build_alerte_writes_both_csv_when_both_missing(tmp_path: Path) -> None:
    source_csv = _fixture_csv(tmp_path / "source.csv")
    maintenance_path = tmp_path / "postgres_maintenance.csv"
    alerte_path = tmp_path / "postgres_alerte.csv"

    df_alerte = build_maintenance_alerte_postgres.build_alerte(
        maintenance_path=maintenance_path, alerte_path=alerte_path, source_csv=source_csv
    )

    assert maintenance_path.exists()
    assert alerte_path.exists()
    assert sorted(df_alerte["label_gmao"]) == ["Alerte_P4", "Alerte_P5"]


def test_build_maintenance_skips_recomputation_when_already_present(tmp_path: Path) -> None:
    source_csv = _fixture_csv(tmp_path / "source.csv")
    maintenance_path = tmp_path / "postgres_maintenance.csv"
    alerte_path = tmp_path / "postgres_alerte.csv"
    pd.DataFrame({"label_gmao": ["Existing"], "id": [42]}).to_csv(maintenance_path, index=False)

    df_maintenance = build_maintenance_alerte_postgres.build_maintenance(
        maintenance_path=maintenance_path, alerte_path=alerte_path, source_csv=source_csv
    )

    assert df_maintenance.to_dict("records") == [{"label_gmao": "Existing", "id": 42}]
    assert not alerte_path.exists()


def test_build_alerte_does_not_overwrite_existing_maintenance(tmp_path: Path) -> None:
    source_csv = _fixture_csv(tmp_path / "source.csv")
    maintenance_path = tmp_path / "postgres_maintenance.csv"
    alerte_path = tmp_path / "postgres_alerte.csv"
    pd.DataFrame({"label_gmao": ["Existing"], "id": [42]}).to_csv(maintenance_path, index=False)

    build_maintenance_alerte_postgres.build_alerte(
        maintenance_path=maintenance_path, alerte_path=alerte_path, source_csv=source_csv
    )

    assert pd.read_csv(maintenance_path).to_dict("records") == [{"label_gmao": "Existing", "id": 42}]
