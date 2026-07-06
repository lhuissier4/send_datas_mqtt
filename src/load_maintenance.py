"""Charge les episodes de maintenance (datas/gold/influxdb_maintenance.csv)
dans InfluxDB (base `sensor_live`), table `maintenance`.

Script autonome, a lancer une fois (ou a nouveau si besoin) comme les autres
scripts du projet - cf. `openspec/changes/load-maintenance-alerte-influxdb/design.md`.

Chaque ligne du CSV est un episode de panne (debut_panne, fin_panne,
id_panne, id_machine). Le CSV est lu par blocs (`MAINTENANCE_CHUNK_SIZE`
lignes) pour ne jamais charger tout le fichier en memoire, et chaque bloc
est envoye en un seul write groupe vers InfluxDB.

debut_panne est le timestamp du point (le moment ou l'episode commence) ;
fin_panne est conserve comme field en epoch nanosecondes (pas une simple
chaine d'affichage) pour rester un vrai timestamp interrogeable. Les deux
valeurs sont celles du CSV, sans transformation ni decalage.

Une re-execution du script est sans risque : InfluxDB ecrase un point
existant partageant la meme serie (tags) et le meme timestamp, donc rejouer
le meme CSV produit exactement les memes points.
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

MAINTENANCE_CSV_PATH = os.getenv(
    "MAINTENANCE_CSV_PATH", "./datas/gold/influxdb_maintenance.csv"
)
MAINTENANCE_CHUNK_SIZE = int(os.getenv("MAINTENANCE_CHUNK_SIZE", "20000"))

MEASUREMENT = "maintenance"
TAG_COLUMNS = ["id_machine", "id_panne"]
FIELD_COLUMN = "fin_panne"
TIME_COLUMN = "debut_panne"

_running = True


def _stop(signum, frame):
    global _running
    _running = False
    print("\nArret demande, le bloc en cours sera termine puis le script s'arretera...", flush=True)


def resolve_csv_path() -> Path:
    path = Path(MAINTENANCE_CSV_PATH)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path


def escape_tag_value(series: pd.Series) -> pd.Series:
    # Line protocol : virgule, espace et signe egal doivent etre echappes
    # dans une valeur de tag.
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

    # pandas >=2.0 parse_dates produit un datetime64[us] par defaut ; write_lp
    # attend des nanosecondes, d'ou la conversion explicite avant l'astype(int64).
    fin_epoch_ns = chunk[FIELD_COLUMN].astype("datetime64[ns]").astype("int64").astype(str)
    fields = f"{FIELD_COLUMN}=" + fin_epoch_ns + "i"

    debut_epoch_ns = chunk[TIME_COLUMN].astype("datetime64[ns]").astype("int64").astype(str)

    lines = MEASUREMENT + tags + " " + fields + " " + debut_epoch_ns
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
        f"(table {MEASUREMENT}, blocs de {MAINTENANCE_CHUNK_SIZE} lignes)",
        flush=True,
    )

    started_at = time.monotonic()
    rows_written = 0
    try:
        reader = pd.read_csv(
            csv_path,
            chunksize=MAINTENANCE_CHUNK_SIZE,
            parse_dates=[TIME_COLUMN, FIELD_COLUMN],
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
