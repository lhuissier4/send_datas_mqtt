import pandas as pd
import pytest

from gold.utils import verify_columns


def test_all_required_columns_present_does_not_raise() -> None:
    df = pd.DataFrame({"a": [1], "b": [2]})

    verify_columns(df, ["a", "b"])


def test_missing_column_raises_value_error_naming_it() -> None:
    df = pd.DataFrame({"a": [1]})

    with pytest.raises(ValueError, match="nope"):
        verify_columns(df, ["a", "nope"])
