import pandas as pd

from gold.utils import rename_columns_of_dataframe


def test_renames_columns_in_place_and_returns_none() -> None:
    df = pd.DataFrame({"old_name": [1, 2]})

    result = rename_columns_of_dataframe(df, {"old_name": "new_name"})

    assert result is None
    assert list(df.columns) == ["new_name"]
