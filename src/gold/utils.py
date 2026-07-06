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


def _verify_sorted_by_timestamp(df: pd.DataFrame, name: str) -> None:
    if not df["timestamp"].is_monotonic_increasing:
        raise ValueError(f"{name} must be sorted ascending by 'timestamp'")


def _tick_records(group: pd.DataFrame, value_columns: list[str]) -> list[dict]:
    """
    Convertit un seul tick (lignes partageant un même timestamp) en la liste
    de ses records (un dict {timestamp, id_machine, <col>: <val>} par valeur
    non-NA de `value_columns`). Le melt reste vectorisé, mais borné à ce
    tick : jamais à la totalité du dataset.
    """
    long = group.melt(
        id_vars=["timestamp", "machine_id"],
        value_vars=value_columns,
        var_name="field",
        value_name="value",
    ).dropna(subset=["value"])

    return [
        {"timestamp": t, "id_machine": m, field: v}
        for t, m, field, v in zip(long["timestamp"], long["machine_id"], long["field"], long["value"])
    ]


def _iter_ticks(df: pd.DataFrame, value_columns: list[str]):
    """
    Itère (timestamp, records) tick par tick, sans jamais matérialiser plus
    d'un tick à la fois. `df` doit déjà être trié par timestamp.

    :param df: DataFrame source (colonnes timestamp, machine_id, *value_columns)
    :param value_columns: colonnes à "melter" en un record chacune
    """
    verify_columns(df, ["timestamp", "machine_id", *value_columns])

    for ts, group in df.groupby("timestamp", sort=False):
        yield ts, _tick_records(group, value_columns)


_END = object()


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

    df_iot et df_plc doivent déjà être triés par timestamp croissant. La
    fusion se fait en streaming (merge à deux curseurs, comme un
    merge-sort) : un seul tick à la fois est tenu en mémoire, ce qui borne
    la RAM utilisée quel que soit le nombre total de lignes.

    :param df_iot: DataFrame IoT (colonnes timestamp, machine_id, iot_*), trié par timestamp
    :param df_plc: DataFrame PLC optionnel (colonnes timestamp, machine_id, id_*), trié par timestamp
    :param output_path: chemin du fichier .jsonl de sortie
    :raises ValueError: si df_iot ou df_plc n'est pas trié par timestamp croissant
    """
    _verify_sorted_by_timestamp(df_iot, "df_iot")
    if df_plc is not None:
        _verify_sorted_by_timestamp(df_plc, "df_plc")

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

    iot_ticks = _iter_ticks(df_iot, SENSOR_COLUMNS)
    plc_ticks = _iter_ticks(df_plc, PLC_COLUMNS) if df_plc is not None else iter(())

    with open(output_path, "w", encoding="utf-8") as f:
        iot_ts, iot_records = next(iot_ticks, (_END, None))
        plc_ts, plc_records = next(plc_ticks, (_END, None))

        while iot_ts is not _END or plc_ts is not _END:
            if plc_ts is _END or (iot_ts is not _END and iot_ts < plc_ts):
                records = iot_records
                iot_ts, iot_records = next(iot_ticks, (_END, None))
            elif iot_ts is _END or plc_ts < iot_ts:
                records = plc_records
                plc_ts, plc_records = next(plc_ticks, (_END, None))
            else:  # même timestamp des deux côtés -> fusion sur la même ligne
                records = iot_records + plc_records
                iot_ts, iot_records = next(iot_ticks, (_END, None))
                plc_ts, plc_records = next(plc_ticks, (_END, None))

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