"""Entraine (pipeline CI/CD) le modele de RUL a partir des donnees de production.

Sources assemblees (cf. `rul_data_assembly.py` pour le detail des mappings) :
- `sensor_data` correle avec les episodes d'alerte (cf. `correlate_sensor_alerte.py`),
  qui lit lui-meme les lectures capteur brutes (Parquet, exportees depuis InfluxDB
  `sensor_staging`) et la mesure `alerte` d'InfluxDB `sensor_live`. Contient
  aussi, sans distinction, les lectures PLC (`id_type_metal`,
  `id_status_production`) publiees sur le meme flux MQTT.
- InfluxDB `sensor_live`, mesures `maintenance` et `nominale_values`.
- Les tables de reference Postgres deja migrees (Flyway) : `type_metal`,
  `type_machine`, `production_status`, `regime_cadence`, `type_alerte`,
  `type_maintenance`, `age_machine`.

Le nettoyage / feature engineering / entrainement Cox proprement dits sont
portes depuis `Nettoyage_RUL_survie_cox.ipynb` dans `rul_pipeline.py` (partage
avec `rul_inference/model.py` pour l'inference). Ce fichier assemble les
donnees de production dans le format attendu par `rul_pipeline` (via
`rul_data_assembly.py`), entraine un modele *candidat*, et ne le promeut en
production (`RUL_MODEL_PATH`, consomme par `rul_inference/model.py`) que si
son c-index depasse celui du modele actuellement deploye (cf.
`promouvoir_si_meilleur` -- la "suite de tests CI/CD" avant mise en
production). Declenchement mensuel : cf. `train_rul_model_scheduler.py`.

Colonnes du notebook toujours sans source de production identifiee (cf.
`rul_data_assembly.py` pour le detail) : `secteur`, `type_machine` (pas de
table `machine`), `nb_pieces_cumule`, `nb_pieces_intervalle`,
`observation_operateur`, `iot_vibration_rms`. `rul_pipeline` est tolerant a
leur absence (features associees simplement non calculees).
"""

import os
from pathlib import Path

import joblib
import pandas as pd
import psycopg2
import requests
from dotenv import load_dotenv

import rul_data_assembly
import rul_pipeline

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
load_dotenv(PROJECT_ROOT / ".env")

CORRELATED_SENSOR_CSV_PATH = os.getenv(
    "CORRELATE_SENSOR_ALERTE_OUTPUT_PATH",
    "./datas/gold/sensor_data_alerte_correlated.csv",
)

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5433"))
POSTGRES_USER = os.getenv("POSTGRES_USER", "user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")
POSTGRES_DB = os.getenv("POSTGRES_DB", "postgres")

INFLUXDB_LIVE_HOST = os.getenv("INFLUXDB_LIVE_HOST", "localhost")
INFLUXDB_LIVE_PORT = int(os.getenv("INFLUXDB_LIVE_PORT", "8183"))
INFLUXDB_DATABASE_LIVE = os.getenv("INFLUXDB_DATABASE_LIVE", "sensor_live")
INFLUXDB_LIVE_TOKEN = os.getenv("INFLUXDB_LIVE_TOKEN", "apiv3_mspr2-live-dev-token")

# Tables de reference deja migrees (cf. bdd/migrations) : les seules
# disponibles cote Postgres pour l'instant, pas de table `machine`.
POSTGRES_LOOKUP_TABLES = [
    "type_metal",
    "type_machine",
    "production_status",
    "regime_cadence",
    "type_alerte",
    "type_maintenance",
    "age_machine",
]

RUL_MODEL_PATH = os.getenv("RUL_MODEL_PATH", "models/rul_cox_model.joblib")
RUL_CANDIDATE_MODEL_PATH = os.getenv("RUL_CANDIDATE_MODEL_PATH", "models/rul_cox_model.candidate.joblib")


def resolve_path(value: str) -> Path:
    path = Path(value)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path


def load_correlated_sensor_data() -> pd.DataFrame:
    """Charge le CSV produit par `correlate_sensor_alerte.py` (format long)."""
    path = resolve_path(CORRELATED_SENSOR_CSV_PATH)
    return pd.read_csv(path, parse_dates=["timestamp"])


def fetch_postgres_lookups() -> dict[str, pd.DataFrame]:
    """Charge les tables de reference Postgres deja migrees (cf. POSTGRES_LOOKUP_TABLES)."""
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        dbname=POSTGRES_DB,
    )
    try:
        return {
            table: pd.read_sql(f"SELECT * FROM {table}", conn)
            for table in POSTGRES_LOOKUP_TABLES
        }
    finally:
        conn.close()


def build_training_dataset(sensor_df: pd.DataFrame, postgres_lookups: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Assemble les sources de production (cf. rul_data_assembly) puis applique
    le nettoyage / feature engineering (cf. rul_pipeline.construire_features)."""
    base_url = f"http://{INFLUXDB_LIVE_HOST}:{INFLUXDB_LIVE_PORT}"
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {INFLUXDB_LIVE_TOKEN}"})
    try:
        maintenance_df = rul_data_assembly.fetch_maintenance(session, base_url, INFLUXDB_DATABASE_LIVE)
        # Bornee a la fenetre des donnees capteur en cours de traitement (+
        # marge d'un jour, pour que meme les toutes premieres lignes trouvent
        # un releve nominal a leur date ou avant via le merge_asof backward
        # de merger_valeurs_nominales) : cf. fetch_nominale_values pour le
        # pourquoi (un fetch complet, non borne, echoue en pratique).
        nominale_df = rul_data_assembly.fetch_nominale_values(
            session,
            base_url,
            INFLUXDB_DATABASE_LIVE,
            start=sensor_df["timestamp"].min() - pd.Timedelta(days=1),
            end=sensor_df["timestamp"].max(),
        )
    finally:
        session.close()
    print(f"[train] {len(maintenance_df)} episodes de maintenance/panne recuperes", flush=True)
    print(f"[train] {len(nominale_df)} releves de valeurs nominales recuperes", flush=True)

    wide_df = rul_data_assembly.pivot_sensor_readings_to_wide(sensor_df)
    wide_df = rul_data_assembly.decoder_lectures_plc(wide_df, postgres_lookups)
    wide_df = rul_data_assembly.merger_valeurs_nominales(wide_df, nominale_df, postgres_lookups)
    wide_df = rul_data_assembly.construire_age_jours(wide_df, postgres_lookups["age_machine"])
    wide_df = rul_data_assembly.reconstruire_label_gmao(wide_df, maintenance_df, postgres_lookups)
    wide_df = rul_data_assembly.construire_cible_survie_depuis_episodes(wide_df, maintenance_df)
    print(f"[train] {len(wide_df)} lignes assemblees au format large", flush=True)

    return rul_pipeline.construire_features(wide_df)


def train_model(df_feat: pd.DataFrame) -> dict:
    """Entraine le modele candidat et l'exporte vers RUL_CANDIDATE_MODEL_PATH.

    Ne remplace jamais directement le modele de production (cf.
    `promouvoir_si_meilleur`) : un candidat moins bon qu'un fit precedent en
    cours d'ecriture ne doit jamais pouvoir corrompre le modele que
    `rul_inference/service.py` utilise deja.
    """
    bundle = rul_pipeline.entrainer_pipeline_complet(df_feat)
    candidate_path = resolve_path(RUL_CANDIDATE_MODEL_PATH)
    candidate_path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(bundle, candidate_path)
    print(f"[train] Modele candidat exporte -> {candidate_path}", flush=True)
    print(f"[train] Metriques candidat : {bundle['metriques']}", flush=True)
    return bundle


def charger_bundle_production() -> dict | None:
    """Charge le bundle actuellement en production (None si aucun n'a encore ete promu)."""
    model_path = resolve_path(RUL_MODEL_PATH)
    if not model_path.exists():
        return None
    return joblib.load(model_path)


def promouvoir_si_meilleur(candidate: dict) -> bool:
    """Compare le candidat au modele de production sur le c-index, et ne promeut
    (copie vers RUL_MODEL_PATH) que s'il est meilleur (ou si aucun modele
    n'existe encore). Retourne True si le candidat a ete promu.

    C'est la "suite de tests CI/CD" : un nouveau modele moins performant que
    celui deja en service n'est jamais deploye en inference.
    """
    bundle_actuel = charger_bundle_production()
    c_index_candidat = candidate["metriques"]["c_index"]
    c_index_actuel = bundle_actuel["metriques"]["c_index"] if bundle_actuel is not None else None

    print(
        f"[train] c-index candidat={c_index_candidat:.3f} "
        f"vs production={'aucun modele' if c_index_actuel is None else f'{c_index_actuel:.3f}'}",
        flush=True,
    )

    if c_index_actuel is not None and c_index_candidat <= c_index_actuel:
        print("[train] Candidat pas meilleur que la production : promotion annulee.", flush=True)
        return False

    model_path = resolve_path(RUL_MODEL_PATH)
    model_path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(candidate, model_path)
    print(f"[train] Nouveau modele promu en production -> {model_path}", flush=True)
    return True


def main() -> None:
    sensor_df = load_correlated_sensor_data()
    print(f"[train] {len(sensor_df)} lectures capteur chargees (format long)", flush=True)

    lookups = fetch_postgres_lookups()
    for table, df in lookups.items():
        print(f"[train] table Postgres '{table}' : {len(df)} lignes", flush=True)

    df_feat = build_training_dataset(sensor_df, lookups)
    candidate = train_model(df_feat)
    promouvoir_si_meilleur(candidate)


if __name__ == "__main__":
    main()
