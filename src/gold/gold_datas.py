"""Construit les fichiers `datas/gold/postgres_*.csv` et le JSONL de rejeu
MQTT (`datas/gold/mqtt_iot_plc_send.jsonl`) a partir du bronze
(`datas/bronze/dataset_brut.csv`) et du silver (`datas/silver/dataset_iot.csv`,
`datas/silver/dataset_plc.csv`).

Version executable de `gold_datas.ipynb` (portage direct, memes calculs et
memes fichiers de sortie).
"""

from pathlib import Path

import pandas as pd

from utils import (
    build_machine_age_dataframe,
    build_machine_dataframe,
    build_machine_secteur_historique_dataframe,
    create_table_with_id_and_unique_label,
    name_csv_file,
    record_future_send_in_jsonl,
    remove_rows_containing_string_in_column,
    split_dataframe_by_prefix, attach_alerte_maintenance_ids, sort_dataframe_by_timestamp, build_episode_dataframe,
)


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
GOLD_DIR = PROJECT_ROOT / "datas/gold"


def main() -> None:
    df_simule:pd.DataFrame = pd.DataFrame(pd.read_csv(r"../../datas/silver/dataset_brut.csv"))
    df_iot = df_simule.drop(columns=[
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
        "iot_vibration_rms"
    ])
    df_plc = df_simule[[
        "machine_id",
        "timestamp",
        "iot_statut_machine",
        "type_metal",
    ]]

    df_iot["timestamp"] = pd.to_datetime(
        df_iot["timestamp"],
        format="%Y-%m-%d %H:%M:%S"
    )
    df_plc["timestamp"] = pd.to_datetime(
        df_plc["timestamp"],
        format="%Y-%m-%d %H:%M:%S"
    )
    df_iot.info()

    df_iot = (
        df_iot.sort_values("timestamp")
        .reset_index(drop=True)
    )
    df_plc = (
        df_plc.sort_values("timestamp")
        .reset_index(drop=True)
    )

    GOLD_DIR.mkdir(parents=True, exist_ok=True)

    df_type_machine = df_simule[["type_machine"]].drop_duplicates(subset=["type_machine"])
    df_type_machine["id"] = df_type_machine["type_machine"].astype("category").cat.codes + 1

    df_type_metal = df_simule[["type_metal"]].drop_duplicates(subset=["type_metal"])
    df_type_metal["id"] = df_type_metal["type_metal"].astype("category").cat.codes + 1

    df_regime_cadence = df_simule[["regime_cadence"]].drop_duplicates(subset=["regime_cadence"])
    df_regime_cadence["id"] = df_regime_cadence["regime_cadence"].astype("category").cat.codes + 1

    df_plc = df_plc.merge(
        df_type_metal[["type_metal", "id"]].rename(columns={"id": "id_type_metal"}),
        on="type_metal",
        how="left",
    ).drop(columns="type_metal")

    df_production_status = df_simule[["iot_statut_machine"]].drop_duplicates(subset=["iot_statut_machine"])
    df_production_status["id"] = df_production_status["iot_statut_machine"].astype("category").cat.codes + 1

    df_plc = df_plc.merge(
        df_production_status[["iot_statut_machine", "id"]].rename(columns={"id": "id_status_production"}),
        on="iot_statut_machine",
        how="left",
    ).drop(columns="iot_statut_machine")

    df_nominale = df_simule.merge(
        df_type_metal[["type_metal", "id"]].rename(columns={"id": "id_type_metal"}),
        on="type_metal",
        how="left",
    ).merge(
        df_regime_cadence[["regime_cadence", "id"]].rename(columns={"id": "id_regime_cadence"}),
        on="regime_cadence",
        how="left",
    )[[
        "timestamp",
        "secteur",
        "machine_id",
        "vitesse_rotation_nominal",
        "courant_moteur_nominal",
        "pression_hydraulique_nominal",
        "statut_nominal",
        "temp_base_moteur",
        "id_type_metal",
        "id_regime_cadence",
        "facteur_cadence",
        "temps_cycle_sec",
    ]]

    df_type_machine.to_csv(
        name_csv_file(folder_path=GOLD_DIR, filename="type_machine", extension=".csv", type_dst="postgres"),
        index=False, encoding="utf-8",
    )
    df_type_metal.to_csv(
        name_csv_file(folder_path=GOLD_DIR, filename="type_metal", extension=".csv", type_dst="postgres"),
        index=False, encoding="utf-8",
    )
    df_regime_cadence.to_csv(
        name_csv_file(folder_path=GOLD_DIR, filename="regime_cadence", extension=".csv", type_dst="postgres"),
        index=False, encoding="utf-8",
    )
    df_production_status.to_csv(
        name_csv_file(folder_path=GOLD_DIR, filename="production_status", extension=".csv", type_dst="postgres"),
        index=False, encoding="utf-8",
    )
    df_nominale.to_csv(
        name_csv_file(folder_path=GOLD_DIR, filename="nominale_values", extension=".csv", type_dst="postgres"),
        index=False, encoding="utf-8",
    )

    df_maintenance = remove_rows_containing_string_in_column(
        df=df_simule, column_name="label_gmao", string_to_remove="Sain",
    )
    df_alerte_splited, df_maintenance_splited = split_dataframe_by_prefix(df_maintenance, "label_gmao", "Alerte")
    df_maintenance_unique = create_table_with_id_and_unique_label(df_maintenance_splited, "label_gmao")
    df_alerte_unique = create_table_with_id_and_unique_label(df_alerte_splited, "label_gmao")

    df_maintenance_unique.to_csv(
        name_csv_file(folder_path=GOLD_DIR, filename="maintenance", extension=".csv", type_dst="postgres"),
        index=False, encoding="utf-8",
    )
    df_alerte_unique.to_csv(
        name_csv_file(folder_path=GOLD_DIR, filename="alerte", extension=".csv", type_dst="postgres"),
        index=False, encoding="utf-8",
    )

    df_age = build_machine_age_dataframe(df_simule)
    df_age.to_csv(
        name_csv_file(folder_path=GOLD_DIR, filename="age_machine", extension=".csv", type_dst="postgres"),
        index=False, encoding="utf-8",
    )

    df_machine = build_machine_dataframe(df_simule, df_type_machine)
    df_machine.to_csv(
        name_csv_file(folder_path=GOLD_DIR, filename="machine", extension=".csv", type_dst="postgres"),
        index=False, encoding="utf-8",
    )

    df_machine_secteur_historique = build_machine_secteur_historique_dataframe(df_simule)
    df_machine_secteur_historique.to_csv(
        name_csv_file(folder_path=GOLD_DIR, filename="machine_secteur_historique", extension=".csv", type_dst="postgres"),
        index=False, encoding="utf-8",
    )

    df_iot = df_iot.sort_values("timestamp").reset_index(drop=True)
    df_plc = df_plc.sort_values("timestamp").reset_index(drop=True)
    record_future_send_in_jsonl(df_iot, df_plc, output_path=str(GOLD_DIR / "mqtt_iot_plc_send.jsonl"))

    df_alert_complet, df_maintenance_complet = attach_alerte_maintenance_ids(
        df_simule=df_simule,
        df_maintenance=df_maintenance,
        df_alerte=df_alerte_unique
    )
    df_alert_filtered = df_alert_complet[["timestamp", "machine_id", "id_alerte"]]
    df_maintenance_filtered = df_maintenance_complet[["timestamp", "machine_id", "id_maintenance"]]
    df_alert_filtered = sort_dataframe_by_timestamp(df_alert_filtered)
    df_maintenance_filtered = sort_dataframe_by_timestamp(df_maintenance_filtered)
    df_maintenance_clean = build_episode_dataframe(
        df_maintenance_filtered,
        id_column="id_maintenance",
        id_output_column="id_panne",
        start_column="debut_panne",
        end_column="fin_panne",
    )
    df_alert_clean = build_episode_dataframe(
        df_alert_filtered,
        id_column="id_alerte",
        id_output_column="id_alerte",
        start_column="debut_alerte",
        end_column="fin_alerte",
    )
    df_maintenance_clean.to_csv(
        name_csv_file(
            folder_path=GOLD_DIR,
            filename="maintenance",
            extension=".csv",
            type_dst="influxdb"
        ), index=False, encoding='utf-8'
    )
    df_alert_clean.to_csv(
        name_csv_file(
            folder_path=GOLD_DIR,
            filename="alerte",
            extension=".csv",
            type_dst="influxdb"
        ), index=False, encoding='utf-8'
    )
if __name__ == "__main__":
    main()
