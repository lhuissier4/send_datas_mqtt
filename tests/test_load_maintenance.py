from unittest.mock import MagicMock

import pandas as pd

from load_maintenance import chunk_to_lines, escape_tag_value, resolve_csv_path, write_batch


def _chunk() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "debut_panne": pd.to_datetime(
                ["2024-08-01 19:03:30", "2024-08-02 13:11:00"]
            ),
            "fin_panne": pd.to_datetime(
                ["2024-08-01 22:56:00", "2024-08-02 14:56:30"]
            ),
            "id_panne": [4, 5],
            "id_machine": ["MCH-001", "MCH-006"],
        }
    )


def test_chunk_to_lines_produces_expected_tags_field_and_time() -> None:
    lines = chunk_to_lines(_chunk())

    assert lines == [
        "maintenance,id_machine=MCH-001,id_panne=4 fin_panne=1722552960000000000i 1722539010000000000",
        "maintenance,id_machine=MCH-006,id_panne=5 fin_panne=1722610590000000000i 1722604260000000000",
    ]


def test_chunk_to_lines_escapes_tag_values_needing_escaping() -> None:
    chunk = pd.DataFrame(
        {
            "debut_panne": pd.to_datetime(["2024-08-01 00:00:00"]),
            "fin_panne": pd.to_datetime(["2024-08-01 01:00:00"]),
            "id_panne": [4],
            "id_machine": ["MCH,001 A=B"],
        }
    )

    lines = chunk_to_lines(chunk)

    assert lines == [
        "maintenance,id_machine=MCH\\,001\\ A\\=B,id_panne=4 "
        "fin_panne=1722474000000000000i 1722470400000000000"
    ]


def test_escape_tag_value_escapes_comma_space_and_equals() -> None:
    result = escape_tag_value(pd.Series(["MCH,001 A=B"]))

    assert result.tolist() == ["MCH\\,001\\ A\\=B"]


def test_escape_tag_value_leaves_plain_values_unchanged() -> None:
    result = escape_tag_value(pd.Series(["MCH-001"]))

    assert result.tolist() == ["MCH-001"]


def test_resolve_csv_path_resolves_relative_default_under_project_root() -> None:
    path = resolve_csv_path()

    assert path.is_absolute()
    assert path.parts[-3:] == ("datas", "gold", "influxdb_maintenance.csv")


def test_write_batch_posts_joined_lines_to_the_write_url() -> None:
    session = MagicMock()
    session.post.return_value = MagicMock(status_code=204)

    write_batch(session, "http://influx/api/v3/write_lp", ["line1", "line2"])

    session.post.assert_called_once()
    assert session.post.call_args.args[0] == "http://influx/api/v3/write_lp"
    assert session.post.call_args.kwargs["data"] == b"line1\nline2"
