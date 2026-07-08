import pandas as pd

from gold.utils import remove_rows_containing_string_in_column


def test_removes_rows_containing_the_string_case_insensitively() -> None:
    df = pd.DataFrame({"label": ["Sain", "PANNE moteur", "Sain", "panne roulement"]})

    result = remove_rows_containing_string_in_column(df, "label", "panne")

    assert result["label"].tolist() == ["Sain", "Sain"]


def test_rows_not_containing_the_string_are_kept_in_order() -> None:
    df = pd.DataFrame({"label": ["a", "b", "c", "d"]})

    result = remove_rows_containing_string_in_column(df, "label", "zzz", max_workers=2)

    assert result["label"].tolist() == ["a", "b", "c", "d"]
