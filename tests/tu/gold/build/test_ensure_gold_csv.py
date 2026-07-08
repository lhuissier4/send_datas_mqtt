from pathlib import Path

import pandas as pd

from gold.utils import ensure_gold_csv


def test_reads_existing_csv_without_calling_build_fn(tmp_path: Path) -> None:
    csv_path = tmp_path / "postgres_type_machine.csv"
    pd.DataFrame({"type_machine": ["Presse"], "id": [1]}).to_csv(csv_path, index=False)

    calls = []

    def build_fn() -> pd.DataFrame:
        calls.append(1)
        raise AssertionError("build_fn must not be called when the csv already exists")

    result = ensure_gold_csv(csv_path, build_fn)

    assert calls == []
    assert result.to_dict("records") == [{"type_machine": "Presse", "id": 1}]


def test_calls_build_fn_when_csv_missing(tmp_path: Path) -> None:
    csv_path = tmp_path / "postgres_type_machine.csv"
    expected = pd.DataFrame({"type_machine": ["Presse"], "id": [1]})

    calls = []

    def build_fn() -> pd.DataFrame:
        calls.append(1)
        return expected

    result = ensure_gold_csv(csv_path, build_fn)

    assert calls == [1]
    pd.testing.assert_frame_equal(result, expected)


def test_build_fn_called_only_once_across_repeated_calls(tmp_path: Path) -> None:
    csv_path = tmp_path / "postgres_type_machine.csv"
    calls = []

    def build_fn() -> pd.DataFrame:
        calls.append(1)
        df = pd.DataFrame({"type_machine": ["Presse"], "id": [1]})
        df.to_csv(csv_path, index=False)
        return df

    ensure_gold_csv(csv_path, build_fn)
    ensure_gold_csv(csv_path, build_fn)

    assert calls == [1]
