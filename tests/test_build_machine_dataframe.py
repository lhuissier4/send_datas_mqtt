import pandas as pd

from gold.utils import build_machine_dataframe


def test_joins_each_machine_to_its_type_id() -> None:
    df = pd.DataFrame(
        {
            "machine_id": ["MCH-001", "MCH-001", "MCH-002"],
            "type_machine": ["A", "A", "B"],
            "timestamp": [
                "2026-06-01 00:01:00",
                "2026-06-01 00:00:00",
                "2026-06-01 00:00:00",
            ],
        }
    )
    df_type_machine = pd.DataFrame({"type_machine": ["A", "B"], "id": [1, 2]})

    result = build_machine_dataframe(df, df_type_machine)

    assert list(result.columns) == ["id_machine", "id_type_machine"]
    assert result.set_index("id_machine")["id_type_machine"].to_dict() == {
        "MCH-001": 1,
        "MCH-002": 2,
    }
