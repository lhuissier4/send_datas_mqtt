import os
import json
from pathlib import Path
from typing import Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd




def get_dynamic_max_workers(group_count: int) -> int:
    cpu_count = os.cpu_count() or 1

    return min(
        group_count,
        max(1, cpu_count - 1)
    )

def verify_columns(df: pd.DataFrame, columns: list[str])->None:
    missing_columns:list[str] = []
    for column in columns:
        if column not in df.columns:
            missing_columns.append(column)
    if len(missing_columns) != 0:
        raise ValueError(f"Missing column(s) in dataset: \"{", ".join(missing_columns)}\"")

def verify_all_line_have_same_timestamp(df: pd.DataFrame)->None:
    timestamps:list[str] = df["timestamp"].unique().tolist()
    if len(timestamps) != 1:
        raise ValueError("All lines must have the same timestamp")


SENSOR_COLUMNS = [
    "iot_vitesse_rotation", "iot_courant_moteur", "iot_pression_hydraulique",
    "iot_temperature", "iot_vibration_peak", "iot_charge_moteur",
]

PLC_COLUMNS = [
    "id_type_metal", "id_status_production",
]


def _records_by_timestamp(df: pd.DataFrame, value_columns: list[str]) -> pd.Series:
    """
    Transforme un DataFrame en une série indexée par timestamp, dont chaque
    valeur est la liste des records (un dict {timestamp, id_machine, <col>: <val>}
    par valeur de `value_columns`) partageant ce timestamp.

    :param df: DataFrame source (colonnes timestamp, machine_id, *value_columns)
    :param value_columns: colonnes à "melter" en un record chacune
    """
    verify_columns(df, ["timestamp", "machine_id", *value_columns])

    # 1 ligne machine -> 1 ligne par valeur, en une passe vectorisée
    long = df.melt(
        id_vars=["timestamp", "machine_id"],
        value_vars=value_columns,
        var_name="field",
        value_name="value",
    )

    # pas de record pour une valeur NA
    long = long.dropna(subset=["value"])

    # un dict par enregistrement (chaque valeur garde sa propre clé)
    long["record"] = [
        {"timestamp": t, "id_machine": m, field: v}
        for t, m, field, v in zip(long["timestamp"], long["machine_id"], long["field"], long["value"])
    ]

    # regroupe par timestamp -> une liste de records par tick
    return long.groupby("timestamp", sort=True)["record"].apply(list)


def record_future_send_in_jsonl(
    df_iot: pd.DataFrame,
    df_plc: pd.DataFrame | None = None,
    output_path: str = "../datas/gold/mqtt_send.jsonl",
) -> None:
    """
    Enregistre les données à envoyer dans UN seul fichier JSONL.
    Chaque ligne = la liste des records (IoT + PLC) partageant le même
    timestamp (= un "tick" à envoyer simultanément au broker MQTT).

    Les lignes PLC ayant le même timestamp qu'un tick IoT sont ajoutées dans
    le même tableau ; un timestamp présent uniquement côté PLC produit une
    ligne contenant seulement ses records PLC.

    Approche vectorisée + écriture séquentielle unique : très rapide même
    sur des millions de lignes / centaines de milliers de timestamps.

    :param df_iot: DataFrame IoT (colonnes timestamp, machine_id, iot_*)
    :param df_plc: DataFrame PLC optionnel (colonnes timestamp, machine_id, id_*)
    :param output_path: chemin du fichier .jsonl de sortie
    """
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

    iot_groups = _records_by_timestamp(df_iot, SENSOR_COLUMNS)

    if df_plc is not None:
        plc_groups = _records_by_timestamp(df_plc, PLC_COLUMNS)
    else:
        plc_groups = pd.Series(dtype=object)

    # union triée des timestamps présents côté IoT et/ou PLC
    timestamps = sorted(set(iot_groups.index) | set(plc_groups.index))

    # une seule ouverture de fichier, une ligne JSON par timestamp
    with open(output_path, "w", encoding="utf-8") as f:
        for ts in timestamps:
            records = []
            if ts in iot_groups.index:
                records.extend(iot_groups.loc[ts])
            if ts in plc_groups.index:
                records.extend(plc_groups.loc[ts])
            f.write(json.dumps(records, ensure_ascii=False))
            f.write("\n")

def name_csv_file(folder_path: Optional[str | Path] ,type_dst:str, filename:str, extension:str=".csv")->str:
    if folder_path:
        folder_path = Path(folder_path)
    else:
        folder_path = ""
    return f"{Path(folder_path, type_dst+ "_" +filename+extension)}"