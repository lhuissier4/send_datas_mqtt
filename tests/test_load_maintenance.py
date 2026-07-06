import pandas as pd

from load_maintenance import chunk_to_lines


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
