from pathlib import Path

import pandas as pd
import pytest

from gold.build import build_machine, build_type_machine


def _fixture_csv(path: Path) -> Path:
    pd.DataFrame(
        {
            "machine_id": ["MCH-001", "MCH-002"],
            "type_machine": ["Presse", "Four"],
            "timestamp": ["2026-06-01 00:00:00", "2026-06-01 00:00:30"],
        }
    ).to_csv(path, index=False)
    return path


def test_reads_existing_type_machine_neighbor_without_triggering_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source_csv = _fixture_csv(tmp_path / "source.csv")
    output_path = tmp_path / "postgres_machine.csv"
    type_machine_path = tmp_path / "postgres_type_machine.csv"
    pd.DataFrame({"type_machine": ["Presse", "Four"], "id": [1, 2]}).to_csv(type_machine_path, index=False)

    def _fail_if_called(**kwargs):
        raise AssertionError("build_type_machine.build must not be called when its csv already exists")

    monkeypatch.setattr(build_type_machine, "build", _fail_if_called)

    df = build_machine.build(output_path=output_path, source_csv=source_csv, type_machine_path=type_machine_path)

    assert output_path.exists()
    assert set(df["id_machine"]) == {"MCH-001", "MCH-002"}
    assert set(df["id_type_machine"]) == {1, 2}


def test_triggers_type_machine_neighbor_when_missing(tmp_path: Path) -> None:
    source_csv = _fixture_csv(tmp_path / "source.csv")
    output_path = tmp_path / "postgres_machine.csv"
    type_machine_path = tmp_path / "postgres_type_machine.csv"

    assert not type_machine_path.exists()

    df = build_machine.build(output_path=output_path, source_csv=source_csv, type_machine_path=type_machine_path)

    assert type_machine_path.exists()
    assert output_path.exists()
    assert len(df) == 2


def test_skips_recomputation_when_output_already_exists(tmp_path: Path) -> None:
    source_csv = _fixture_csv(tmp_path / "source.csv")
    output_path = tmp_path / "postgres_machine.csv"
    type_machine_path = tmp_path / "postgres_type_machine.csv"
    pd.DataFrame({"id_machine": ["Existing"], "id_type_machine": [42]}).to_csv(output_path, index=False)

    df = build_machine.build(output_path=output_path, source_csv=source_csv, type_machine_path=type_machine_path)

    assert df.to_dict("records") == [{"id_machine": "Existing", "id_type_machine": 42}]
    assert not type_machine_path.exists()
