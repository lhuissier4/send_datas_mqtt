import pandas as pd

from gold.utils import build_episode_dataframe


def _build_alerte_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    return build_episode_dataframe(
        df,
        id_column="id_alerte",
        id_output_column="id_alerte",
        start_column="debut_alerte",
        end_column="fin_alerte",
    )


def _build_panne_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    return build_episode_dataframe(
        df,
        id_column="id_maintenance",
        id_output_column="id_panne",
        start_column="debut_panne",
        end_column="fin_panne",
    )


def test_consecutive_same_id_becomes_one_episode() -> None:
    df = pd.DataFrame(
        {
            "timestamp": ["2026-06-01 00:00:00", "2026-06-01 00:00:30", "2026-06-01 00:01:00"],
            "machine_id": ["MCH-001", "MCH-001", "MCH-001"],
            "id_alerte": [4, 4, 4],
        }
    )

    episodes = _build_alerte_dataframe(df)

    assert len(episodes) == 1
    row = episodes.iloc[0]
    assert row["debut_alerte"] == "2026-06-01 00:00:00"
    assert row["fin_alerte"] == "2026-06-01 00:01:00"
    assert row["id_alerte"] == 4
    assert row["id_machine"] == "MCH-001"


def test_id_change_starts_a_new_episode() -> None:
    df = pd.DataFrame(
        {
            "timestamp": ["2026-06-01 00:00:00", "2026-06-01 00:00:30", "2026-06-01 00:01:00"],
            "machine_id": ["MCH-001", "MCH-001", "MCH-001"],
            "id_alerte": [4, 4, 5],
        }
    )

    episodes = _build_alerte_dataframe(df)

    assert len(episodes) == 2
    assert episodes["id_alerte"].tolist() == [4, 5]
    assert episodes.iloc[0]["fin_alerte"] == "2026-06-01 00:00:30"
    assert episodes.iloc[1]["debut_alerte"] == episodes.iloc[1]["fin_alerte"] == "2026-06-01 00:01:00"


def test_same_id_recurring_after_a_gap_is_two_episodes() -> None:
    # Rows for id 4, then id 5, then id 4 again: even though the id repeats,
    # it isn't adjacent to the first block, so it's a separate episode.
    df = pd.DataFrame(
        {
            "timestamp": [
                "2026-06-01 00:00:00",
                "2026-06-01 00:00:30",
                "2026-08-01 00:00:00",
            ],
            "machine_id": ["MCH-001", "MCH-001", "MCH-001"],
            "id_alerte": [4, 5, 4],
        }
    )

    episodes = _build_alerte_dataframe(df)

    assert len(episodes) == 3
    assert episodes["id_alerte"].tolist() == [4, 5, 4]


def test_different_machines_do_not_merge_episodes() -> None:
    df = pd.DataFrame(
        {
            "timestamp": ["2026-06-01 00:00:00", "2026-06-01 00:00:00"],
            "machine_id": ["MCH-001", "MCH-002"],
            "id_alerte": [4, 4],
        }
    )

    episodes = _build_alerte_dataframe(df)

    assert len(episodes) == 2
    assert set(episodes["id_machine"]) == {"MCH-001", "MCH-002"}


def test_build_panne_dataframe_uses_maintenance_column_names() -> None:
    df = pd.DataFrame(
        {
            "timestamp": ["2026-06-01 00:00:00", "2026-06-01 00:00:30"],
            "machine_id": ["MCH-001", "MCH-001"],
            "id_maintenance": [8, 8],
        }
    )

    episodes = _build_panne_dataframe(df)

    assert list(episodes.columns) == ["debut_panne", "fin_panne", "id_panne", "id_machine"]
    assert episodes.iloc[0]["id_panne"] == 8
