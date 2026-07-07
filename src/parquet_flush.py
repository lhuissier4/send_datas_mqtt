"""Exporte periodiquement la base InfluxDB `sensor_staging` vers Parquet.

Logique (cf. openspec/changes/migrate-storage-parquet-influxdb/design.md) :
`sensor_staging` est traite comme une file d'attente. A chaque intervalle,
le job interroge le contenu ecrit depuis le dernier flush jusqu'a
`now() - SAFETY_MARGIN`, l'exporte dans un fichier Parquet, puis avance un
checkpoint local jusqu'a cette borne.

InfluxDB 3 Core ne supporte pas la suppression par predicat/plage temporelle
(seulement la suppression d'une base ou d'une table entiere) : contrairement
au script original (InfluxDB 2 + Flux), on ne supprime donc plus les points
exportes. La purge est assuree par la retention de la base `sensor_staging`
(cf. INFLUXDB_STAGING_RETENTION, appliquee a la creation de la base dans
docker-compose.yml) ; le checkpoint local garantit qu'on n'exporte jamais
deux fois le meme point tant que ce fichier n'est pas perdu.
"""

import os
import signal
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

INFLUXDB_HOST = os.getenv("INFLUXDB_STAGING_HOST", "localhost")
INFLUXDB_PORT = int(os.getenv("INFLUXDB_STAGING_PORT", "8182"))
INFLUXDB_DATABASE_STAGING = os.getenv("INFLUXDB_DATABASE_STAGING", "sensor_staging")
INFLUXDB_STAGING_TOKEN = os.getenv("INFLUXDB_STAGING_TOKEN", "apiv3_mspr2-staging-dev-token")

PARQUET_FLUSH_INTERVAL_MINUTES = float(os.getenv("PARQUET_FLUSH_INTERVAL_MINUTES", "5"))
PARQUET_OUTPUT_DIR = os.getenv("PARQUET_OUTPUT_DIR", "./bdd/parquet")
PARQUET_FLUSH_SAFETY_MARGIN_SECONDS = float(
    os.getenv("PARQUET_FLUSH_SAFETY_MARGIN_SECONDS", "30")
)

EPOCH_RFC3339 = "1970-01-01T00:00:00Z"
# Format compact (sans ':') pour un nom de fichier valide sur tous les OS.
FILENAME_TS_FORMAT = "%Y%m%dT%H%M%SZ"
CHECKPOINT_FILENAME = ".flush_checkpoint"

_running = True


def _stop(signum, frame):
    global _running
    _running = False
    print("\nArret demande, fermeture...", flush=True)


def resolve_output_dir() -> Path:
    path = Path(PARQUET_OUTPUT_DIR)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    path.mkdir(parents=True, exist_ok=True)
    return path


def read_checkpoint(checkpoint_path: Path) -> str:
    if not checkpoint_path.exists():
        return EPOCH_RFC3339
    return checkpoint_path.read_text().strip() or EPOCH_RFC3339


def write_checkpoint(checkpoint_path: Path, cutoff_rfc3339: str) -> None:
    tmp_path = checkpoint_path.with_suffix(".tmp")
    tmp_path.write_text(cutoff_rfc3339)
    tmp_path.rename(checkpoint_path)


def query_pending(
    session: requests.Session, base_url: str, start_rfc3339: str, cutoff_rfc3339: str
) -> pd.DataFrame:
    query = (
        "SELECT * FROM sensor_data "
        f"WHERE time > '{start_rfc3339}' AND time <= '{cutoff_rfc3339}' "
        "ORDER BY time"
    )
    response = session.post(
        f"{base_url}/api/v3/query_sql",
        json={"db": INFLUXDB_DATABASE_STAGING, "q": query},
    )
    if response.status_code == 400 and "not found" in response.text:
        # La table `sensor_data` n'existe pas encore : Telegraf ne l'a pas
        # creee car aucun point n'a jamais ete ecrit dans sensor_staging.
        return pd.DataFrame()
    response.raise_for_status()
    return pd.DataFrame.from_records(response.json())


def sanitize_filename_component(value: str) -> str:
    ts = pd.to_datetime(value, utc=True)
    return ts.strftime(FILENAME_TS_FORMAT)


def write_parquet_atomic(df: pd.DataFrame, output_dir: Path) -> Path:
    ts_min = sanitize_filename_component(df["sensor_timestamp"].min())
    ts_max = sanitize_filename_component(df["sensor_timestamp"].max())
    final_path = output_dir / f"sensor_data_{ts_min}_{ts_max}.parquet"

    fd, tmp_name = tempfile.mkstemp(dir=output_dir, suffix=".parquet.tmp")
    os.close(fd)
    tmp_path = Path(tmp_name)
    # mkstemp cree le fichier en 0600 par defaut : le conteneur tourne en
    # root, donc sans ce chmod les parquets ne seraient lisibles par aucun
    # script lance cote hote (ex. correlate_sensor_alerte.py).
    os.chmod(tmp_path, 0o644)
    try:
        df.to_parquet(tmp_path, index=False)
        tmp_path.rename(final_path)
    except BaseException:
        tmp_path.unlink(missing_ok=True)
        raise
    return final_path


def run_flush(session: requests.Session, base_url: str, output_dir: Path, checkpoint_path: Path) -> None:
    cutoff = datetime.now(timezone.utc) - timedelta(
        seconds=PARQUET_FLUSH_SAFETY_MARGIN_SECONDS
    )
    cutoff_rfc3339 = cutoff.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    start_rfc3339 = read_checkpoint(checkpoint_path)

    df = query_pending(session, base_url, start_rfc3339, cutoff_rfc3339)
    if df.empty:
        print(f"[flush] Rien a exporter avant {cutoff_rfc3339}", flush=True)
        write_checkpoint(checkpoint_path, cutoff_rfc3339)
        return

    final_path = write_parquet_atomic(df, output_dir)
    print(f"[flush] {len(df)} points exportes -> {final_path.name}", flush=True)

    write_checkpoint(checkpoint_path, cutoff_rfc3339)
    print(f"[flush] Checkpoint avance jusqu'a {cutoff_rfc3339}", flush=True)


def main() -> None:
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    output_dir = resolve_output_dir()
    checkpoint_path = output_dir / CHECKPOINT_FILENAME
    base_url = f"http://{INFLUXDB_HOST}:{INFLUXDB_PORT}"
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {INFLUXDB_STAGING_TOKEN}"})
    interval_seconds = PARQUET_FLUSH_INTERVAL_MINUTES * 60
    print(
        f"Flush toutes les {PARQUET_FLUSH_INTERVAL_MINUTES} min "
        f"vers {output_dir} (base {INFLUXDB_DATABASE_STAGING})",
        flush=True,
    )

    try:
        while _running:
            run_flush(session, base_url, output_dir, checkpoint_path)
            waited = 0.0
            while _running and waited < interval_seconds:
                time.sleep(min(0.5, interval_seconds - waited))
                waited += 0.5
    finally:
        session.close()
        print("Deconnecte.", flush=True)


if __name__ == "__main__":
    main()
