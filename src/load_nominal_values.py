"""Charge les valeurs nominales (datas/gold/postgres_nominale_values.csv) dans
InfluxDB (base `sensor_live`), a la place de la table Postgres `nominale_values`.

Script autonome, a lancer une fois (ou a nouveau si besoin) comme les autres
scripts du projet - cf. `openspec/changes/migrate-nominal-values-influxdb/design.md`.

Contrairement a `mqtt_live_monitor.py` (flux continu), ce script fait une seule
passe sur un fichier statique (~14.7M lignes) : le CSV est lu par blocs
(`NOMINAL_VALUES_CHUNK_SIZE` lignes) pour ne jamais charger tout le fichier en
memoire, et chaque bloc est envoye en un seul write groupe vers InfluxDB.

Le timestamp de chaque ligne est celui du CSV (pas l'heure d'ingestion) : il ne
s'agit pas d'un flux rejoue en temps reel mais d'un chargement historique
ponctuel, donc l'heure d'ecriture n'a pas de sens ici. Une re-execution du
script est sans risque : InfluxDB ecrase un point existant partage la meme
serie (tags) et le meme timestamp, donc rejouer le meme CSV produit exactement
les memes points.
"""

import os
import signal
import time
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

INFLUXDB_HOST = os.getenv("INFLUXDB_LIVE_HOST", "localhost")
INFLUXDB_PORT = int(os.getenv("INFLUXDB_LIVE_PORT", "8183"))
INFLUXDB_DATABASE_LIVE = os.getenv("INFLUXDB_DATABASE_LIVE", "sensor_live")
INFLUXDB_LIVE_TOKEN = os.getenv("INFLUXDB_LIVE_TOKEN", "apiv3_mspr2-live-dev-token")

NOMINAL_VALUES_CSV_PATH = os.getenv(
    "NOMINAL_VALUES_CSV_PATH", "./datas/gold/postgres_nominale_values.csv"
)
NOMINAL_VALUES_CHUNK_SIZE = int(os.getenv("NOMINAL_VALUES_CHUNK_SIZE", "20000"))

MEASUREMENT = "nominale_values"
TAG_COLUMNS = ["machine_id", "statut_nominal"]
FIELD_COLUMNS = [
    "vitesse_rotation_nominal",
    "courant_moteur_nominal",
    "pression_hydraulique_nominal",
    "temp_base_moteur",
]

_running = True


def _stop(signum, frame):
    global _running
    _running = False
    print("\nArret demande, le bloc en cours sera termine puis le script s'arretera...", flush=True)


def resolve_csv_path() -> Path:
    path = Path(NOMINAL_VALUES_CSV_PATH)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path


def escape_tag_value(series: pd.Series) -> pd.Series:
    # Line protocol : virgule, espace et signe egal doivent etre echappes
    # dans une valeur de tag (cf. "Arret Operateur" dans le CSV).
    return (
        series.astype(str)
        .str.replace(",", "\\,", regex=False)
        .str.replace("=", "\\=", regex=False)
        .str.replace(" ", "\\ ", regex=False)
    )


def chunk_to_lines(chunk: pd.DataFrame) -> list[str]:
    tags = ""
    for column in TAG_COLUMNS:
        tags += f",{column}=" + escape_tag_value(chunk[column])

    fields = chunk[FIELD_COLUMNS[0]].astype(str)
    fields = f"{FIELD_COLUMNS[0]}=" + fields
    for column in FIELD_COLUMNS[1:]:
        fields = fields + f",{column}=" + chunk[column].astype(str)

    # pandas >=2.0 parse_dates produit un datetime64[us] par defaut ; write_lp
    # attend des nanosecondes, d'ou la conversion explicite avant l'astype(int64).
    epoch_ns = chunk["timestamp"].astype("datetime64[ns]").astype("int64").astype(str)

    lines = MEASUREMENT + tags + " " + fields + " " + epoch_ns
    return lines.tolist()


def write_batch(session: requests.Session, write_url: str, lines: list[str]) -> None:
    response = session.post(
        write_url,
        params={"db": INFLUXDB_DATABASE_LIVE},
        data="\n".join(lines).encode("utf-8"),
    )
    response.raise_for_status()


def main() -> None:
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    csv_path = resolve_csv_path()
    write_url = f"http://{INFLUXDB_HOST}:{INFLUXDB_PORT}/api/v3/write_lp"
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {INFLUXDB_LIVE_TOKEN}"})

    print(
        f"Chargement de {csv_path} -> base InfluxDB {INFLUXDB_DATABASE_LIVE} "
        f"(table {MEASUREMENT}, blocs de {NOMINAL_VALUES_CHUNK_SIZE} lignes)",
        flush=True,
    )

    started_at = time.monotonic()
    rows_written = 0
    try:
        reader = pd.read_csv(
            csv_path, chunksize=NOMINAL_VALUES_CHUNK_SIZE, parse_dates=["timestamp"]
        )
        for chunk in reader:
            if not _running:
                break
            lines = chunk_to_lines(chunk)
            write_batch(session, write_url, lines)
            rows_written += len(lines)
            elapsed = time.monotonic() - started_at
            print(
                f"[load] {rows_written} lignes ecrites "
                f"({rows_written / elapsed:.0f} lignes/s)",
                flush=True,
            )
    finally:
        session.close()
        print(f"Termine : {rows_written} lignes chargees.", flush=True)


if __name__ == "__main__":
    main()
