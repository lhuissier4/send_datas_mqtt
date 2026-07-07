import pandas as pd
import pytest

from gold.utils import verify_all_line_have_same_timestamp


def test_single_shared_timestamp_does_not_raise() -> None:
    df = pd.DataFrame({"timestamp": ["2026-06-01 00:00:00", "2026-06-01 00:00:00"]})

    verify_all_line_have_same_timestamp(df)


def test_mismatched_timestamps_raise_value_error() -> None:
    df = pd.DataFrame({"timestamp": ["2026-06-01 00:00:00", "2026-06-01 00:00:30"]})

    with pytest.raises(ValueError, match="same timestamp"):
        verify_all_line_have_same_timestamp(df)
