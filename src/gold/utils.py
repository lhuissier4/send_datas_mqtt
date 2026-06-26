import os
from typing import Any
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd


class Sensor:
    def __init__(self, timestamp: str, id_machine: str, others_data: dict[str, Any]):
        self.timestamp:str = timestamp
        self.id_machine:str = id_machine
        self.others_data:dict[str, Any] = others_data
    def get_data(self) -> dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "id_machine": self.id_machine,
            **self.others_data
        }


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


def create_list_of_list_of_sensor(df: pd.DataFrame, columns: list[str])-> list[Sensor]:
    """

    :param df: DataFrame ou toutes les lignes ont le meme timestamp
    :param columns: colonnes du DataFrame
    :return: liste de liste de capteurs : tous seront à envoyer simultanément au broker MQTT
    """
    verify_columns(df,columns)
    verify_all_line_have_same_timestamp(df)
    list_of_machines_sensors:list[Sensor] = []
    print(f"Timestamp: {df["timestamp"].unique().tolist()}")
    for line in df.itertuples(): # iteration sur chaque ligne (une ligne par machine)
        list_of_machines_sensors.append(
            Sensor(
                timestamp=line.timestamp,
                id_machine=line.machine_id,
                others_data={"iot_vitesse_rotation": line.iot_vitesse_rotation}
            )
        )
        list_of_machines_sensors.append(
            Sensor(
                timestamp=line.timestamp,
                id_machine=line.machine_id,
                others_data={"iot_courant_moteur": line.iot_vitesse_rotation}
            )
        )
        list_of_machines_sensors.append(
            Sensor(
                timestamp=line.timestamp,
                id_machine=line.machine_id,
                others_data={"iot_pression_hydraulique": line.iot_vitesse_rotation}
            )
        )
        list_of_machines_sensors.append(
            Sensor(
                timestamp=line.timestamp,
                id_machine=line.machine_id,
                others_data={"iot_temperature": line.iot_vitesse_rotation}
            )
        )
        list_of_machines_sensors.append(
            Sensor(
                timestamp=line.timestamp,
                id_machine=line.machine_id,
                others_data={"iot_vibration_peak": line.iot_vitesse_rotation}
            )
        )
        list_of_machines_sensors.append(
            Sensor(
                timestamp=line.timestamp,
                id_machine=line.machine_id,
                others_data={"iot_charge_moteur": line.iot_vitesse_rotation}
            )
        )

    return list_of_machines_sensors

def split_df_by_timestamp_and_create_list_of_sensor(df: pd.DataFrame)->list[list[Sensor]]:
    columns = ["timestamp", "machine_id", "iot_vitesse_rotation", "iot_courant_moteur", "iot_pression_hydraulique", "iot_temperature", "iot_vibration_peak", "iot_charge_moteur"]
    verify_columns(df,columns)
    results:list[list[Sensor]] = []
    with ThreadPoolExecutor(max_workers=get_dynamic_max_workers(60)) as executor:
        futures = []

        for group_value, group_df in df.groupby("timestamp"):
            future = executor.submit(
                create_list_of_list_of_sensor,
                group_df,
                columns
            )
            futures.append(future)

        for future in as_completed(futures):
            results.append(future.result())
    return results