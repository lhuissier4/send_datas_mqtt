"""Scinde `datas/silver/dataset_brut.csv` en deux, de part et d'autre de
CUTOFF (par defaut : l'heure actuelle) :

- la partie anterieure a CUTOFF est ecrite directement en stockage froid
  (`bdd/parquet/sensor_data_{start}_{end}.parquet`, un fichier par jour),
  exactement dans le format que produit `parquet_flush.py` en production ;
- la partie posterieure est ecrite dans `datas/gold/mqtt_iot_plc_send.jsonl`
  (meme format que `gold/build/build_mqtt_jsonl.py`, reutilise ici), destinee
  a etre rejouee en direct par `mqtt_send.py`.

Sans ce script, `mqtt_send.py` rejoue tout le dataset au compte-goutte
(`SEND_INTERVAL_SECONDS` par ligne) du premier au dernier tick : pour un
dataset simule couvrant des mois, l'essentiel serait donc "du passe" qui
n'aurait aucune raison de transiter par le flux MQTT temps reel avant de
devenir exploitable. Ce script permet de charger ce passe directement en
stockage froid, et de ne laisser au rejeu MQTT que la partie "presente"
(a partir de CUTOFF).

Les autres artefacts golds (tables de reference Postgres, valeurs nominales,
episodes alerte/maintenance) ne sont pas concernes par cette coupure : ils
sont charges en une seule fois, dans leur integralite, par leurs scripts
habituels (`gold.gold_datas`, `load_alerte.py`, `load_maintenance.py`,
`load_nominal_values.py`) - un episode dont le debut est posterieur a CUTOFF
n'est de toute facon jamais considere actif avant son debut reel par
`rul_inference/service.py::_reconstruire_label_gmao`.

A lancer AVANT `python -m gold.gold_datas` (qui ne regenere jamais un
`mqtt_iot_plc_send.jsonl` deja present, cf. `build_mqtt_jsonl.py::build`) :
dans l'autre ordre, `gold_datas` aurait deja ecrit la version complete
(non coupee) du jsonl, et ce script refuse de l'ecraser (cf. `main`).
"""

import os
from datetime import datetime
from pathlib import Path

import pandas as pd

from gold.build import build_production_status, build_type_metal
from gold.build.build_mqtt_jsonl import compute as compute_iot_plc
from gold.utils import PLC_COLUMNS, SENSOR_COLUMNS, ensure_gold_csv, record_future_send_in_jsonl
from parquet_flush import write_parquet_atomic

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
SILVER_DATASET_BRUT = PROJECT_ROOT / "datas/silver/dataset_brut.csv"
GOLD_DIR = PROJECT_ROOT / "datas/gold"
MQTT_JSONL_OUTPUT_PATH = GOLD_DIR / "mqtt_iot_plc_send.jsonl"
PARQUET_OUTPUT_DIR = PROJECT_ROOT / "bdd/parquet"


def load_lookups(source_csv: Path = SILVER_DATASET_BRUT) -> tuple[pd.DataFrame, pd.DataFrame]:
    df_type_metal = ensure_gold_csv(
        build_type_metal.OUTPUT_PATH, lambda: build_type_metal.build(source_csv=source_csv)
    )
    df_production_status = ensure_gold_csv(
        build_production_status.OUTPUT_PATH, lambda: build_production_status.build(source_csv=source_csv)
    )
    return df_type_metal, df_production_status


def melt_to_sensor_data(df: pd.DataFrame, value_columns: list[str]) -> pd.DataFrame:
    """Convertit un dataframe large (une colonne par capteur/PLC) en format
    long (une ligne par mesure) : colonnes id_machine, sensor, value,
    sensor_timestamp -- meme schema que `sensor_staging` cote InfluxDB (cf.
    `telegraf/telegraf.conf`, processeur starlark qui fait la meme
    transformation en production a partir du flux MQTT).
    """
    colonnes_presentes = [c for c in value_columns if c in df.columns]
    long = df.melt(
        id_vars=["timestamp", "machine_id"],
        value_vars=colonnes_presentes,
        var_name="sensor",
        value_name="value",
    )
    long = long.dropna(subset=["value"])
    return long.rename(columns={"machine_id": "id_machine", "timestamp": "sensor_timestamp"})


def write_cold_storage(df_past_iot: pd.DataFrame, df_past_plc: pd.DataFrame, output_dir: Path) -> int:
    """Ecrit la partie passee en parquet, un fichier par jour calendaire (cf.
    module docstring) -- reutilise `parquet_flush.py::write_parquet_atomic`
    pour garantir un nom/schema de fichier identique a celui produit en
    production. Retourne le nombre de fichiers ecrits.
    """
    long = pd.concat(
        [
            melt_to_sensor_data(df_past_iot, SENSOR_COLUMNS),
            melt_to_sensor_data(df_past_plc, PLC_COLUMNS),
        ],
        ignore_index=True,
    )
    if long.empty:
        return 0

    long["sensor_timestamp"] = pd.to_datetime(long["sensor_timestamp"])
    long["time"] = long["sensor_timestamp"]
    long["value"] = long["value"].astype(float)

    output_dir.mkdir(parents=True, exist_ok=True)
    nb_fichiers = 0
    for _, groupe in long.groupby(long["sensor_timestamp"].dt.date, sort=True):
        write_parquet_atomic(groupe, output_dir)
        nb_fichiers += 1
    return nb_fichiers


def split(
    cutoff: pd.Timestamp,
    source_csv: Path = SILVER_DATASET_BRUT,
    parquet_output_dir: Path = PARQUET_OUTPUT_DIR,
    jsonl_output_path: Path = MQTT_JSONL_OUTPUT_PATH,
) -> None:
    df_type_metal, df_production_status = load_lookups(source_csv)
    df_simule = pd.read_csv(source_csv)
    df_iot, df_plc = compute_iot_plc(df_simule, df_type_metal, df_production_status)

    # `timestamp` reste une chaine dans df_iot/df_plc (cf.
    # build_mqtt_jsonl.py::compute -- necessaire pour record_future_send_in_jsonl,
    # qui ne sait pas serialiser un pd.Timestamp) : on ne compare que via des
    # series auxiliaires, sans jamais modifier la colonne d'origine.
    passe_iot = pd.to_datetime(df_iot["timestamp"]) < cutoff
    passe_plc = pd.to_datetime(df_plc["timestamp"]) < cutoff

    print(f"[split] Cutoff : {cutoff}", flush=True)
    print(f"[split] IoT : {passe_iot.sum()} lignes passees, {(~passe_iot).sum()} presentes", flush=True)
    print(f"[split] PLC : {passe_plc.sum()} lignes passees, {(~passe_plc).sum()} presentes", flush=True)

    nb_fichiers = write_cold_storage(df_iot[passe_iot], df_plc[passe_plc], parquet_output_dir)
    print(f"[split] {nb_fichiers} fichiers parquet ecrits -> {parquet_output_dir}", flush=True)

    jsonl_output_path.parent.mkdir(parents=True, exist_ok=True)
    record_future_send_in_jsonl(
        df_iot[~passe_iot].reset_index(drop=True),
        df_plc[~passe_plc].reset_index(drop=True),
        output_path=str(jsonl_output_path),
    )
    print(f"[split] Partie presente ecrite -> {jsonl_output_path}", flush=True)


def main() -> None:
    cutoff_env = os.getenv("SPLIT_COLD_STORAGE_CUTOFF")
    cutoff = pd.Timestamp(cutoff_env) if cutoff_env else pd.Timestamp(datetime.now())

    if MQTT_JSONL_OUTPUT_PATH.exists():
        raise SystemExit(
            f"{MQTT_JSONL_OUTPUT_PATH} existe deja. Ce script doit tourner AVANT "
            "`python -m gold.gold_datas` (qui ne le regenere jamais si le fichier "
            "existe deja) : supprimez-le si vous voulez relancer le split avec un "
            "autre CUTOFF."
        )

    split(cutoff)


if __name__ == "__main__":
    main()
