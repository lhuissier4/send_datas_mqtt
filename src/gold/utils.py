import os
import json
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

def record_future_send_in_json(df: pd.DataFrame, output_dir: str = "../datas/gold")->list[list[Sensor]]:
    columns = ["timestamp", "machine_id", "iot_vitesse_rotation", "iot_courant_moteur", "iot_pression_hydraulique", "iot_temperature", "iot_vibration_peak", "iot_charge_moteur"]
    verify_columns(df,columns)
    results:list[list[Sensor]] = []
    for index, (group_value, group_df) in enumerate(df.groupby("timestamp")):
        record_list_of_sensor_to_json(
            list_of_sensor=create_list_of_list_of_sensor(
                df=group_df,
                columns=columns
            ),
            output_dir=output_dir,
            index=index+1
        )

    return results

def record_list_of_sensor_to_json(list_of_sensor: list[Sensor], index:int, output_dir: str = "../datas/gold")->None:
    """
    Crée un fichier JSON par liste de capteurs et l'enregistre dans output_dir.
    Les fichiers sont numérotés à partir de 1 : 1.json, 2.json, ...

    :param index:
    :param list_of_sensor: liste de listes de capteurs
    :param output_dir: dossier de destination des fichiers JSON
    """
    os.makedirs(output_dir, exist_ok=True)
    file_path = os.path.join(output_dir, f"{index}.json")
    data = [sensor.get_data() for sensor in list_of_sensor]
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)