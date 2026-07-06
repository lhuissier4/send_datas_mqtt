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
def split_dataframe_by_prefix(
    df: pd.DataFrame,
    column_name: str,
    prefix: str
) -> tuple[pd.DataFrame, pd.DataFrame]:
    # Crée un masque booléen : True si la valeur commence par le préfixe
    mask = df[column_name].astype(str).str.startswith(prefix, na=False)

    # DataFrame avec les lignes qui commencent par le préfixe
    matching_df = df[mask].copy()

    # DataFrame avec les autres lignes
    non_matching_df = df[~mask].copy()

    return matching_df, non_matching_df


def remove_rows_containing_string_in_column(
    df: pd.DataFrame,
    column_name: str,
    string_to_remove: str,
    max_workers: int | None = None
) -> pd.DataFrame:
    # Détermine automatiquement le nombre de threads disponibles
    if max_workers is None:
        max_workers = os.cpu_count() or 1

    # Évite de créer plus de threads que de lignes
    max_workers = min(max_workers, len(df))

    # Découpe le DataFrame en morceaux
    chunk_size = max(1, len(df) // max_workers)

    chunks = [
        df.iloc[start:start + chunk_size]
        for start in range(0, len(df), chunk_size)
    ]

    def filter_chunk(chunk: pd.DataFrame) -> pd.DataFrame:
        # Supprime les lignes où la colonne contient le string recherché
        mask = ~chunk[column_name].astype(str).str.contains(
            string_to_remove,
            case=False,
            na=False,
            regex=False
        )

        return chunk[mask]

    # Exécute le filtrage en parallèle
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        filtered_chunks = list(executor.map(filter_chunk, chunks))

    # Fusionne les morceaux filtrés
    return pd.concat(filtered_chunks, ignore_index=True)


def create_table_with_id_and_unique_label(df:pd.DataFrame, label_column:str)->pd.DataFrame:
    if len(df.columns.tolist()) == 0:
        raise ValueError("The dataframe must contain a column")
    if label_column not in df.columns:
        raise ValueError(f"The dataframe not the column {label_column}")
    if len(df)==0:
        raise ValueError("The dataframe must contain at least one row")

    df = df[[label_column]].drop_duplicates(subset=[label_column])
    df["id"] = df[label_column].astype("category").cat.codes + 1
    return df

def rename_columns_of_dataframe(df:pd.DataFrame, mapping:dict[str,str])-> None:
    return df.rename(columns=mapping, inplace=True)