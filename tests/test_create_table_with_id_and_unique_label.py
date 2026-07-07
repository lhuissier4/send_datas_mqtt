import pandas as pd
import pytest

from gold.utils import create_table_with_id_and_unique_label


def test_deduplicates_and_assigns_ids_alphabetically() -> None:
    df = pd.DataFrame({"label": ["b", "a", "b", "c"]})

    result = create_table_with_id_and_unique_label(df, "label")

    assert result["label"].tolist() == ["b", "a", "c"]
    assert result["id"].tolist() == [2, 1, 3]


def test_no_columns_raises_value_error() -> None:
    with pytest.raises(ValueError, match="must contain a column"):
        create_table_with_id_and_unique_label(pd.DataFrame(), "label")


def test_missing_label_column_raises_value_error() -> None:
    df = pd.DataFrame({"other": [1]})

    with pytest.raises(ValueError, match="label"):
        create_table_with_id_and_unique_label(df, "label")


def test_empty_dataframe_with_column_raises_value_error() -> None:
    df = pd.DataFrame({"label": pd.Series(dtype=object)})

    with pytest.raises(ValueError, match="at least one row"):
        create_table_with_id_and_unique_label(df, "label")
