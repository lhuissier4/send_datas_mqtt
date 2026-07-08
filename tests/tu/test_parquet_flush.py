from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd

from parquet_flush import (
    query_pending,
    read_checkpoint,
    sanitize_filename_component,
    write_checkpoint,
    write_parquet_atomic,
)


def test_sanitize_filename_component_formats_a_compact_utc_timestamp() -> None:
    assert sanitize_filename_component("2026-06-01 10:30:00") == "20260601T103000Z"


def test_checkpoint_round_trip(tmp_path: Path) -> None:
    checkpoint_path = tmp_path / ".flush_checkpoint"

    write_checkpoint(checkpoint_path, "2026-06-01T10:30:00.000Z")

    assert read_checkpoint(checkpoint_path) == "2026-06-01T10:30:00.000Z"


def test_read_checkpoint_defaults_to_epoch_when_missing(tmp_path: Path) -> None:
    checkpoint_path = tmp_path / ".flush_checkpoint"

    assert read_checkpoint(checkpoint_path) == "1970-01-01T00:00:00Z"


def test_write_parquet_atomic_writes_expected_file(tmp_path: Path) -> None:
    df = pd.DataFrame(
        {
            "sensor_timestamp": pd.to_datetime(
                ["2026-06-01 10:00:00", "2026-06-01 11:00:00"]
            ),
            "id_machine": ["MCH-001", "MCH-001"],
        }
    )

    output_path = write_parquet_atomic(df, tmp_path)

    assert output_path.name == "sensor_data_20260601T100000Z_20260601T110000Z.parquet"
    assert output_path.exists()
    result = pd.read_parquet(output_path)
    assert len(result) == 2


def test_query_pending_returns_dataframe_from_mocked_session() -> None:
    session = MagicMock()
    session.post.return_value = MagicMock(
        status_code=200,
        json=lambda: [{"time": "2026-06-01T10:00:00Z", "id_machine": "MCH-001"}],
    )

    result = query_pending(
        session, "http://influx", "2026-06-01T00:00:00Z", "2026-06-02T00:00:00Z"
    )

    assert session.post.call_args.args[0] == "http://influx/api/v3/query_sql"
    assert result["id_machine"].tolist() == ["MCH-001"]


def test_query_pending_returns_empty_dataframe_when_table_not_found() -> None:
    session = MagicMock()
    session.post.return_value = MagicMock(status_code=400, text="table not found")

    result = query_pending(
        session, "http://influx", "2026-06-01T00:00:00Z", "2026-06-02T00:00:00Z"
    )

    assert result.empty
