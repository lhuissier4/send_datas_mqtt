import pandas as pd

from gold.utils import sort_dataframe_by_timestamp


def test_rows_ordered_ascending_by_timestamp() -> None:
    df = pd.DataFrame(
        {
            "timestamp": ["2026-06-01 00:01:00", "2026-06-01 00:00:00", "2026-06-01 00:02:00"],
            "value": ["b", "a", "c"],
        }
    )

    sorted_df = sort_dataframe_by_timestamp(df)

    assert sorted_df["timestamp"].tolist() == [
        "2026-06-01 00:00:00",
        "2026-06-01 00:01:00",
        "2026-06-01 00:02:00",
    ]
    assert sorted_df["value"].tolist() == ["a", "b", "c"]


def test_index_is_reset() -> None:
    df = pd.DataFrame({"timestamp": [2, 1], "value": ["b", "a"]})

    sorted_df = sort_dataframe_by_timestamp(df)

    assert sorted_df.index.tolist() == [0, 1]


def test_custom_timestamp_column() -> None:
    df = pd.DataFrame({"ts": [3, 1, 2], "value": ["c", "a", "b"]})

    sorted_df = sort_dataframe_by_timestamp(df, timestamp_column="ts")

    assert sorted_df["value"].tolist() == ["a", "b", "c"]
