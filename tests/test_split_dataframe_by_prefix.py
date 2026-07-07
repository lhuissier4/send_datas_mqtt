import pandas as pd

from gold.utils import split_dataframe_by_prefix


def test_splits_rows_by_prefix_match() -> None:
    df = pd.DataFrame({"label_gmao": ["Alerte4", "Maintenance8", "Alerte5", "Sain"]})

    matching, non_matching = split_dataframe_by_prefix(df, "label_gmao", "Alerte")

    assert matching["label_gmao"].tolist() == ["Alerte4", "Alerte5"]
    assert non_matching["label_gmao"].tolist() == ["Maintenance8", "Sain"]


def test_nan_values_are_treated_as_non_matching() -> None:
    df = pd.DataFrame({"label_gmao": ["Alerte4", None]})

    matching, non_matching = split_dataframe_by_prefix(df, "label_gmao", "Alerte")

    assert matching["label_gmao"].tolist() == ["Alerte4"]
    assert len(non_matching) == 1
