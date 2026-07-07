from pathlib import Path

import pandas as pd
import pytest

from gold.correlate_sensor_alerte import (
    correlate,
    list_parquet_files_in_window,
    parse_window_end,
)


def test_parse_window_end_extracts_the_end_timestamp() -> None:
    path = Path("sensor_data_20260601T000000Z_20260602T000000Z.parquet")

    assert parse_window_end(path) == pd.Timestamp(
        "2026-06-02 00:00:00", tz="UTC"
    ).to_pydatetime()


def test_parse_window_end_raises_on_unexpected_filename() -> None:
    with pytest.raises(ValueError, match="inattendu"):
        parse_window_end(Path("not_a_sensor_file.parquet"))


def test_list_parquet_files_in_window_keeps_only_recent_files(tmp_path: Path) -> None:
    recent = tmp_path / "sensor_data_20260601T000000Z_20260610T000000Z.parquet"
    old = tmp_path / "sensor_data_20260101T000000Z_20260102T000000Z.parquet"
    recent.touch()
    old.touch()

    kept = list_parquet_files_in_window(tmp_path, window_days=30)

    assert kept == [recent]


def test_correlate_tags_reading_inside_an_active_alert_window() -> None:
    sensor_df = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2026-06-01 10:30:00"]),
            "id_machine": ["MCH-001"],
        }
    )
    alerte_df = pd.DataFrame(
        {
            "debut_alerte": pd.to_datetime(["2026-06-01 10:00:00"]),
            "fin_alerte": pd.to_datetime(["2026-06-01 11:00:00"]),
            "id_machine": ["MCH-001"],
            "id_alerte": [7],
        }
    )

    result = correlate(sensor_df, alerte_df)

    assert result["id_alerte"].tolist() == [7]


def test_correlate_does_not_tag_reading_after_episode_closed() -> None:
    sensor_df = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2026-06-01 12:00:00"]),
            "id_machine": ["MCH-001"],
        }
    )
    alerte_df = pd.DataFrame(
        {
            "debut_alerte": pd.to_datetime(["2026-06-01 10:00:00"]),
            "fin_alerte": pd.to_datetime(["2026-06-01 11:00:00"]),
            "id_machine": ["MCH-001"],
            "id_alerte": [7],
        }
    )

    result = correlate(sensor_df, alerte_df)

    assert result["id_alerte"].isna().all()


def test_correlate_does_not_tag_reading_before_any_episode_starts() -> None:
    sensor_df = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2026-06-01 09:00:00"]),
            "id_machine": ["MCH-001"],
        }
    )
    alerte_df = pd.DataFrame(
        {
            "debut_alerte": pd.to_datetime(["2026-06-01 10:00:00"]),
            "fin_alerte": pd.to_datetime(["2026-06-01 11:00:00"]),
            "id_machine": ["MCH-001"],
            "id_alerte": [7],
        }
    )

    result = correlate(sensor_df, alerte_df)

    assert result["id_alerte"].isna().all()


def test_correlate_preserves_row_count_with_multiple_readings() -> None:
    sensor_df = pd.DataFrame(
        {
            "timestamp": pd.to_datetime(
                ["2026-06-01 09:00:00", "2026-06-01 10:30:00", "2026-06-01 12:00:00"]
            ),
            "id_machine": ["MCH-001", "MCH-001", "MCH-001"],
        }
    )
    alerte_df = pd.DataFrame(
        {
            "debut_alerte": pd.to_datetime(["2026-06-01 10:00:00"]),
            "fin_alerte": pd.to_datetime(["2026-06-01 11:00:00"]),
            "id_machine": ["MCH-001"],
            "id_alerte": [7],
        }
    )

    result = correlate(sensor_df, alerte_df)

    assert len(result) == 3
    assert result["id_alerte"].tolist() == [pd.NA, 7, pd.NA]
