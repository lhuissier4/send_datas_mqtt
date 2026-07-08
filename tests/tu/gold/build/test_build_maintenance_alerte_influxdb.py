from pathlib import Path

import pandas as pd

from gold.build import build_maintenance_alerte_influxdb


def _fixture_csv(path: Path) -> Path:
    pd.DataFrame(
        {
            "machine_id": ["MCH-001"] * 5,
            "timestamp": [f"2026-06-01 00:0{i}:00" for i in range(5)],
            "label_gmao": [
                "Sain",
                "Alerte_P4",
                "Alerte_P4",
                "Maintenance_Correctif_P8",
                "Sain",
            ],
        }
    ).to_csv(path, index=False)
    return path


def _lookups(tmp_path: Path) -> tuple[Path, Path]:
    maintenance_lookup_path = tmp_path / "postgres_maintenance.csv"
    alerte_lookup_path = tmp_path / "postgres_alerte.csv"
    pd.DataFrame({"label_gmao": ["Maintenance_Correctif_P8"], "id": [8]}).to_csv(
        maintenance_lookup_path, index=False
    )
    pd.DataFrame({"label_gmao": ["Alerte_P4"], "id": [4]}).to_csv(alerte_lookup_path, index=False)
    return maintenance_lookup_path, alerte_lookup_path


def test_alerte_ids_come_from_the_lookup_table_not_raw_rows(tmp_path: Path) -> None:
    """Regression test for the gold_datas.py bug: ids must match the
    label_gmao,id lookup tables, not crash / be wrong because of raw rows."""
    source_csv = _fixture_csv(tmp_path / "source.csv")
    maintenance_lookup_path, alerte_lookup_path = _lookups(tmp_path)
    maintenance_out = tmp_path / "influxdb_maintenance.csv"
    alerte_out = tmp_path / "influxdb_alerte.csv"

    df_alerte = build_maintenance_alerte_influxdb.build_alerte(
        maintenance_path=maintenance_out,
        alerte_path=alerte_out,
        source_csv=source_csv,
        maintenance_lookup_path=maintenance_lookup_path,
        alerte_lookup_path=alerte_lookup_path,
    )

    assert alerte_out.exists()
    assert df_alerte["id_alerte"].tolist() == [4]
    assert df_alerte["debut_alerte"].iloc[0] == "2026-06-01 00:01:00"
    assert df_alerte["fin_alerte"].iloc[0] == "2026-06-01 00:02:00"


def test_build_maintenance_writes_both_and_correct_ids(tmp_path: Path) -> None:
    source_csv = _fixture_csv(tmp_path / "source.csv")
    maintenance_lookup_path, alerte_lookup_path = _lookups(tmp_path)
    maintenance_out = tmp_path / "influxdb_maintenance.csv"
    alerte_out = tmp_path / "influxdb_alerte.csv"

    df_maintenance = build_maintenance_alerte_influxdb.build_maintenance(
        maintenance_path=maintenance_out,
        alerte_path=alerte_out,
        source_csv=source_csv,
        maintenance_lookup_path=maintenance_lookup_path,
        alerte_lookup_path=alerte_lookup_path,
    )

    assert maintenance_out.exists()
    assert alerte_out.exists()
    assert df_maintenance["id_panne"].tolist() == [8]
    assert df_maintenance["debut_panne"].iloc[0] == "2026-06-01 00:03:00"


def test_triggers_missing_postgres_lookup_neighbors(tmp_path: Path) -> None:
    source_csv = _fixture_csv(tmp_path / "source.csv")
    maintenance_lookup_path = tmp_path / "postgres_maintenance.csv"
    alerte_lookup_path = tmp_path / "postgres_alerte.csv"
    maintenance_out = tmp_path / "influxdb_maintenance.csv"
    alerte_out = tmp_path / "influxdb_alerte.csv"

    assert not maintenance_lookup_path.exists()
    assert not alerte_lookup_path.exists()

    df_alerte = build_maintenance_alerte_influxdb.build_alerte(
        maintenance_path=maintenance_out,
        alerte_path=alerte_out,
        source_csv=source_csv,
        maintenance_lookup_path=maintenance_lookup_path,
        alerte_lookup_path=alerte_lookup_path,
    )

    assert maintenance_lookup_path.exists()
    assert alerte_lookup_path.exists()
    lookup = pd.read_csv(alerte_lookup_path)
    expected_id = lookup.set_index("label_gmao").loc["Alerte_P4", "id"]
    assert df_alerte["id_alerte"].tolist() == [expected_id]


def test_skips_recomputation_when_output_already_exists(tmp_path: Path) -> None:
    source_csv = _fixture_csv(tmp_path / "source.csv")
    maintenance_lookup_path, alerte_lookup_path = _lookups(tmp_path)
    maintenance_out = tmp_path / "influxdb_maintenance.csv"
    alerte_out = tmp_path / "influxdb_alerte.csv"
    pd.DataFrame({"id_alerte": [99]}).to_csv(alerte_out, index=False)

    df_alerte = build_maintenance_alerte_influxdb.build_alerte(
        maintenance_path=maintenance_out,
        alerte_path=alerte_out,
        source_csv=source_csv,
        maintenance_lookup_path=maintenance_lookup_path,
        alerte_lookup_path=alerte_lookup_path,
    )

    assert df_alerte["id_alerte"].tolist() == [99]
    assert not maintenance_out.exists()
