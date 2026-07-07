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


def sort_dataframe_by_timestamp(df: pd.DataFrame, timestamp_column: str = "timestamp") -> pd.DataFrame:
    return df.sort_values(by=timestamp_column, ascending=True).reset_index(drop=True)


def build_episode_dataframe(
    df: pd.DataFrame,
    id_column: str,
    id_output_column: str,
    start_column: str,
    end_column: str,
    machine_column: str = "machine_id",
    timestamp_column: str = "timestamp",
) -> pd.DataFrame:
    """
    Regroupe les lignes consécutives (pour une même machine) partageant le
    même `id_column` en un épisode : une ligne = un `id` continu, avec son
    timestamp de début/fin. Un épisode s'arrête dès que l'id change ou que
    la machine change ; il n'y a pas de notion de durée maximale entre deux
    lignes (le découpage se fait uniquement sur la succession des lignes du
    DataFrame, pas sur un écart de temps).

    Le tri se fait par (machine, timestamp) et non par timestamp seul : les
    machines partagent souvent les mêmes timestamps (relevés en parallèle),
    donc un tri par timestamp seul entrelacerait leurs lignes et casserait
    la détection de "lignes consécutives pour une même machine".
    """
    df = df.sort_values(by=[machine_column, timestamp_column], ascending=True).reset_index(drop=True)

    new_episode = (
        (df[machine_column] != df[machine_column].shift())
        | (df[id_column] != df[id_column].shift())
    )
    episode_id = new_episode.cumsum()

    return df.groupby(episode_id).agg(**{
        start_column: (timestamp_column, "min"),
        end_column: (timestamp_column, "max"),
        id_output_column: (id_column, "first"),
        "id_machine": (machine_column, "first"),
    }).reset_index(drop=True)


def build_machine_age_dataframe(
    df: pd.DataFrame,
    machine_column: str = "machine_id",
    age_column: str = "age_jours",
    timestamp_column: str = "timestamp",
    id_output_column: str = "id_machine",
    age_output_column: str = "age_machine_jours",
    timestamp_output_column: str = "premier_timestamp",
) -> pd.DataFrame:
    """
    Construit un DataFrame avec, pour chaque machine, son id, son âge et le
    premier timestamp auquel elle apparaît dans le jeu de données.
    """
    verify_columns(df, [machine_column, age_column, timestamp_column])

    df_sorted = sort_dataframe_by_timestamp(df, timestamp_column)

    return df_sorted.groupby(machine_column, sort=False).agg(**{
        id_output_column: (machine_column, "first"),
        age_output_column: (age_column, "first"),
        timestamp_output_column: (timestamp_column, "first"),
    }).reset_index(drop=True)


def build_machine_dataframe(
    df: pd.DataFrame,
    df_type_machine: pd.DataFrame,
    machine_column: str = "machine_id",
    type_column: str = "type_machine",
    timestamp_column: str = "timestamp",
    id_output_column: str = "id_machine",
    type_id_output_column: str = "id_type_machine",
) -> pd.DataFrame:
    """
    Construit un DataFrame avec, pour chaque machine, son id et l'id de son
    type (jointure du label `type_column` sur la table de référence
    `df_type_machine`, colonnes `type_column, id` - cf. postgres_type_machine.csv).
    """
    verify_columns(df, [machine_column, type_column, timestamp_column])
    verify_columns(df_type_machine, [type_column, "id"])

    df_sorted = sort_dataframe_by_timestamp(df, timestamp_column)

    machines = df_sorted.groupby(machine_column, sort=False).agg(**{
        id_output_column: (machine_column, "first"),
        type_column: (type_column, "first"),
    }).reset_index(drop=True)

    return machines.merge(
        df_type_machine[[type_column, "id"]].rename(columns={"id": type_id_output_column}),
        on=type_column,
        how="inner",
    )[[id_output_column, type_id_output_column]]


def build_machine_secteur_historique_dataframe(
    df: pd.DataFrame,
    machine_column: str = "machine_id",
    secteur_column: str = "secteur",
    timestamp_column: str = "timestamp",
    id_output_column: str = "id_machine",
    date_output_column: str = "date_mise_en_service",
) -> pd.DataFrame:
    """
    Construit l'historique des secteurs par machine : une ligne par
    "run" de valeurs consécutives de `secteur_column` (pour une même
    machine, triées par timestamp), avec le timestamp de début du run
    comme `date_output_column`. Aucune ligne n'est émise pour une lecture
    dont le secteur est identique à la précédente.
    """
    episodes = build_episode_dataframe(
        df,
        id_column=secteur_column,
        id_output_column=secteur_column,
        start_column=date_output_column,
        end_column="_fin_secteur",
        machine_column=machine_column,
        timestamp_column=timestamp_column,
    )

    return episodes[[id_output_column, secteur_column, date_output_column]]


def attach_alerte_maintenance_ids(
    df_simule: pd.DataFrame,
    df_alerte: pd.DataFrame,
    df_maintenance: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Sépare `df_simule` (colonne `label_gmao`) en un DataFrame alerte et un
    DataFrame maintenance, et associe à chacun le `id` numérique
    correspondant à son label depuis `df_alerte` / `df_maintenance`
    (lookup tables `label_gmao, id`). Les lignes ne correspondant à aucun
    des deux préfixes (ex: `Sain`), ainsi que celles dont le label n'a pas
    de correspondance dans la lookup table, sont exclues du résultat.
    """
    alerte_rows, remainder = split_dataframe_by_prefix(df_simule, "label_gmao", "Alerte")
    maintenance_rows, _ = split_dataframe_by_prefix(remainder, "label_gmao", "Maintenance")

    rename_columns_of_dataframe(alerte_rows, {"label_gmao": "label_alerte"})
    rename_columns_of_dataframe(maintenance_rows, {"label_gmao": "label_maintenance"})

    alerte_rows = alerte_rows.merge(
        df_alerte.rename(columns={"label_gmao": "label_alerte", "id": "id_alerte"}),
        on="label_alerte",
        how="inner",
    )
    maintenance_rows = maintenance_rows.merge(
        df_maintenance.rename(columns={"label_gmao": "label_maintenance", "id": "id_maintenance"}),
        on="label_maintenance",
        how="inner",
    )

    return alerte_rows, maintenance_rows