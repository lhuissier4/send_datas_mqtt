from unittest.mock import MagicMock

import pandas as pd

from load_nominal_values import (
    chunk_to_lines,
    escape_tag_value,
    resolve_csv_path,
    write_batch,
)


def _chunk() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "timestamp": pd.to_datetime(["2026-06-01 10:00:00"]),
            "machine_id": ["MCH-001"],
            "statut_nominal": ["OK"],
            "id_type_metal": [1],
            "id_regime_cadence": [2],
            "vitesse_rotation_nominal": [100.0],
            "courant_moteur_nominal": [5.5],
            "pression_hydraulique_nominal": [3.2],
            "temp_base_moteur": [40.0],
            "facteur_cadence": [1.0],
            "temps_cycle_sec": [12.5],
        }
    )


def test_chunk_to_lines_produces_expected_tags_fields_and_time() -> None:
    lines = chunk_to_lines(_chunk())

    assert lines == [
        "nominale_values,machine_id=MCH-001,statut_nominal=OK "
        "vitesse_rotation_nominal=100.0,courant_moteur_nominal=5.5,"
        "pression_hydraulique_nominal=3.2,temp_base_moteur=40.0,"
        "id_type_metal=1,id_regime_cadence=2,facteur_cadence=1.0,"
        "temps_cycle_sec=12.5 1780308000000000000"
    ]


def test_escape_tag_value_escapes_comma_space_and_equals() -> None:
    result = escape_tag_value(pd.Series(["Arret Operateur"]))

    assert result.tolist() == ["Arret\\ Operateur"]


def test_resolve_csv_path_resolves_relative_default_under_project_root() -> None:
    path = resolve_csv_path()

    assert path.is_absolute()
    assert path.parts[-3:] == ("datas", "gold", "postgres_nominale_values.csv")


def test_write_batch_posts_joined_lines_to_the_write_url() -> None:
    session = MagicMock()
    session.post.return_value = MagicMock(status_code=204)

    write_batch(session, "http://influx/api/v3/write_lp", ["line1", "line2"])

    session.post.assert_called_once()
    assert session.post.call_args.args[0] == "http://influx/api/v3/write_lp"
    assert session.post.call_args.kwargs["data"] == b"line1\nline2"
