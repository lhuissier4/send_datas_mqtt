"""Construit `datas/gold/mqtt_iot_plc_send.jsonl` a partir de
`datas/silver/dataset_brut.csv`, `datas/gold/postgres_type_metal.csv` et
`datas/gold/postgres_production_status.csv` (declenche `build_type_metal` /
`build_production_status` si l'un de ces deux derniers n'existe pas encore).

Contrairement aux autres scripts, la sortie n'est pas un csv rechargeable en
DataFrame (c'est un jsonl de rejeu MQTT) : l'auto-verification se fait donc
directement sur l'existence du fichier plutot que via `ensure_gold_csv`.
"""

from pathlib import Path

import pandas as pd

from gold.build import build_production_status, build_type_metal
from gold.utils import ensure_gold_csv, name_csv_file, record_future_send_in_jsonl

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
SILVER_DATASET_BRUT = PROJECT_ROOT / "datas/silver/dataset_brut.csv"
GOLD_DIR = PROJECT_ROOT / "datas/gold"
OUTPUT_PATH = GOLD_DIR / "mqtt_iot_plc_send.jsonl"

IOT_DROPPED_COLUMNS = [
    "age_jours",
    "age_virtuel_jours",
    "label_gmao",
    "RUL_jours",
    "secteur",
    "type_machine",
    "vitesse_rotation_nominal",
    "courant_moteur_nominal",
    "pression_hydraulique_nominal",
    "statut_nominal",
    "type_metal",
    "temp_base_moteur",
    "iot_statut_machine",
    "iot_vibration_rms",
]
PLC_SELECTED_COLUMNS = ["machine_id", "timestamp", "iot_statut_machine", "type_metal"]


def compute(
    df_simule: pd.DataFrame, df_type_metal: pd.DataFrame, df_production_status: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    df_iot = df_simule.drop(columns=IOT_DROPPED_COLUMNS)
    df_plc = df_simule[PLC_SELECTED_COLUMNS]

    # Le timestamp reste une chaine ("%Y-%m-%d %H:%M:%S", largeur fixe) : un
    # tri lexicographique donne deja l'ordre chronologique, et
    # record_future_send_in_jsonl (json.dumps) ne sait pas serialiser un
    # pd.Timestamp - le convertir en datetime64 ici (comme le faisait
    # gold_datas.py) ferait planter l'ecriture du jsonl.
    df_iot = df_iot.sort_values("timestamp").reset_index(drop=True)
    df_plc = df_plc.sort_values("timestamp").reset_index(drop=True)

    df_plc = df_plc.merge(
        df_type_metal[["type_metal", "id"]].rename(columns={"id": "id_type_metal"}),
        on="type_metal",
        how="left",
    ).drop(columns="type_metal")
    df_plc = df_plc.merge(
        df_production_status[["iot_statut_machine", "id"]].rename(columns={"id": "id_status_production"}),
        on="iot_statut_machine",
        how="left",
    ).drop(columns="iot_statut_machine")

    return df_iot, df_plc


def build(
    output_path: Path = OUTPUT_PATH,
    source_csv: Path = SILVER_DATASET_BRUT,
    type_metal_path: Path = build_type_metal.OUTPUT_PATH,
    production_status_path: Path = build_production_status.OUTPUT_PATH,
) -> Path:
    if output_path.exists():
        return output_path

    df_simule = pd.read_csv(source_csv)
    df_type_metal = ensure_gold_csv(
        type_metal_path,
        lambda: build_type_metal.build(output_path=type_metal_path, source_csv=source_csv),
    )
    df_production_status = ensure_gold_csv(
        production_status_path,
        lambda: build_production_status.build(output_path=production_status_path, source_csv=source_csv),
    )
    df_iot, df_plc = compute(df_simule, df_type_metal, df_production_status)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    record_future_send_in_jsonl(df_iot, df_plc, output_path=str(output_path))
    return output_path


if __name__ == "__main__":
    build()
