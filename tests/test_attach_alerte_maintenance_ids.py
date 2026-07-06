import pandas as pd

from gold.utils import attach_alerte_maintenance_ids


def _df_simule(labels: list[str]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "timestamp": [f"2026-06-01 00:0{i}:00" for i in range(len(labels))],
            "machine_id": ["MCH-001"] * len(labels),
            "label_gmao": labels,
        }
    )


def _df_alerte() -> pd.DataFrame:
    return pd.DataFrame({"label_gmao": ["Alerte_P4", "Alerte_P5"], "id": [4, 5]})


def _df_maintenance() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "label_gmao": ["Maintenance_Correctif_P8", "Maintenance_Preventif_PLANIFIEE"],
            "id": [8, 10],
        }
    )


def test_alerte_row_routes_to_alerte_output_only() -> None:
    df_simule = _df_simule(["Alerte_P4"])

    alerte_df, maintenance_df = attach_alerte_maintenance_ids(
        df_simule, _df_alerte(), _df_maintenance()
    )

    assert len(alerte_df) == 1
    assert len(maintenance_df) == 0


def test_maintenance_row_routes_to_maintenance_output_only() -> None:
    df_simule = _df_simule(["Maintenance_Correctif_P8"])

    alerte_df, maintenance_df = attach_alerte_maintenance_ids(
        df_simule, _df_alerte(), _df_maintenance()
    )

    assert len(alerte_df) == 0
    assert len(maintenance_df) == 1


def test_sain_row_excluded_from_both_outputs() -> None:
    df_simule = _df_simule(["Sain"])

    alerte_df, maintenance_df = attach_alerte_maintenance_ids(
        df_simule, _df_alerte(), _df_maintenance()
    )

    assert len(alerte_df) == 0
    assert len(maintenance_df) == 0


def test_label_column_renamed_per_output() -> None:
    df_simule = _df_simule(["Alerte_P4", "Maintenance_Correctif_P8"])

    alerte_df, maintenance_df = attach_alerte_maintenance_ids(
        df_simule, _df_alerte(), _df_maintenance()
    )

    assert "label_gmao" not in alerte_df.columns
    assert alerte_df.loc[0, "label_alerte"] == "Alerte_P4"

    assert "label_gmao" not in maintenance_df.columns
    assert maintenance_df.loc[0, "label_maintenance"] == "Maintenance_Correctif_P8"


def test_matching_label_gets_correct_id() -> None:
    df_simule = _df_simule(["Alerte_P5", "Maintenance_Preventif_PLANIFIEE"])

    alerte_df, maintenance_df = attach_alerte_maintenance_ids(
        df_simule, _df_alerte(), _df_maintenance()
    )

    assert alerte_df.loc[0, "id_alerte"] == 5
    assert maintenance_df.loc[0, "id_maintenance"] == 10


def test_unmatched_label_is_dropped() -> None:
    df_simule = _df_simule(["Alerte_P99", "Maintenance_Correctif_P99"])

    alerte_df, maintenance_df = attach_alerte_maintenance_ids(
        df_simule, _df_alerte(), _df_maintenance()
    )

    assert len(alerte_df) == 0
    assert len(maintenance_df) == 0
