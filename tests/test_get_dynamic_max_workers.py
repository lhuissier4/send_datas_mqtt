import pytest

from gold.utils import get_dynamic_max_workers


def test_group_count_smaller_than_available_workers_returns_group_count(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("os.cpu_count", lambda: 4)

    assert get_dynamic_max_workers(2) == 2


def test_group_count_larger_than_available_workers_is_capped(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("os.cpu_count", lambda: 4)

    assert get_dynamic_max_workers(10) == 3


def test_unknown_cpu_count_falls_back_to_one_worker(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("os.cpu_count", lambda: None)

    assert get_dynamic_max_workers(5) == 1
