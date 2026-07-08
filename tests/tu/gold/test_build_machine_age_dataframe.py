import pandas as pd

from gold.utils import build_machine_age_dataframe


def test_one_row_per_machine_with_earliest_timestamp_and_matching_age() -> None:
    df = pd.DataFrame(
        {
            "machine_id": ["MCH-002", "MCH-001", "MCH-001", "MCH-002"],
            "age_jours": [10, 5, 5, 10],
            "timestamp": [
                "2026-06-02 00:00:00",
                "2026-06-01 00:00:00",
                "2026-06-01 00:01:00",
                "2026-06-02 00:01:00",
            ],
        }
    )

    result = build_machine_age_dataframe(df)

    assert result["id_machine"].tolist() == ["MCH-001", "MCH-002"]
    assert result["age_machine_jours"].tolist() == [5, 10]
    assert result["premier_timestamp"].tolist() == [
        "2026-06-01 00:00:00",
        "2026-06-02 00:00:00",
    ]
