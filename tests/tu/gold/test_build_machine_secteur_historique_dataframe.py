import pandas as pd

from gold.utils import build_machine_secteur_historique_dataframe


def test_same_sector_streak_becomes_one_row() -> None:
    df = pd.DataFrame(
        {
            "timestamp": ["2026-06-01 00:00:00", "2026-06-01 00:00:30", "2026-06-01 00:01:00"],
            "machine_id": ["MCH-001", "MCH-001", "MCH-001"],
            "secteur": ["Aero", "Aero", "Aero"],
        }
    )

    historique = build_machine_secteur_historique_dataframe(df)

    assert list(historique.columns) == ["id_machine", "secteur", "date_mise_en_service"]
    assert len(historique) == 1
    row = historique.iloc[0]
    assert row["id_machine"] == "MCH-001"
    assert row["secteur"] == "Aero"
    assert row["date_mise_en_service"] == "2026-06-01 00:00:00"


def test_sector_change_starts_a_new_row() -> None:
    df = pd.DataFrame(
        {
            "timestamp": ["2026-06-01 00:00:00", "2026-06-01 00:00:30", "2026-06-01 00:01:00"],
            "machine_id": ["MCH-001", "MCH-001", "MCH-001"],
            "secteur": ["Aero", "Aero", "Naval"],
        }
    )

    historique = build_machine_secteur_historique_dataframe(df)

    assert len(historique) == 2
    assert historique["secteur"].tolist() == ["Aero", "Naval"]
    assert historique["date_mise_en_service"].tolist() == [
        "2026-06-01 00:00:00",
        "2026-06-01 00:01:00",
    ]


def test_sector_recurring_after_a_gap_is_two_rows() -> None:
    df = pd.DataFrame(
        {
            "timestamp": ["2026-06-01 00:00:00", "2026-06-01 00:00:30", "2026-08-01 00:00:00"],
            "machine_id": ["MCH-001", "MCH-001", "MCH-001"],
            "secteur": ["Aero", "Naval", "Aero"],
        }
    )

    historique = build_machine_secteur_historique_dataframe(df)

    assert len(historique) == 3
    assert historique["secteur"].tolist() == ["Aero", "Naval", "Aero"]


def test_different_machines_do_not_merge_rows() -> None:
    df = pd.DataFrame(
        {
            "timestamp": ["2026-06-01 00:00:00", "2026-06-01 00:00:00"],
            "machine_id": ["MCH-001", "MCH-002"],
            "secteur": ["Aero", "Aero"],
        }
    )

    historique = build_machine_secteur_historique_dataframe(df)

    assert len(historique) == 2
    assert set(historique["id_machine"]) == {"MCH-001", "MCH-002"}
