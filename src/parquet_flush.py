"""Exporte periodiquement le bucket InfluxDB `sensor_staging` vers Parquet.

Logique (cf. openspec/changes/migrate-storage-parquet-influxdb/design.md) :
`sensor_staging` est traite comme une file d'attente. A chaque intervalle,
le job interroge tout le contenu du bucket jusqu'a `now() - SAFETY_MARGIN`,
l'exporte dans un fichier Parquet, puis supprime exactement cette plage du
bucket une fois le fichier confirme sur disque. Une fenetre vide ne produit
aucun fichier.
"""

import os
import signal
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient
from influxdb_client.client.delete_api import DeleteApi

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

INFLUXDB_HOST = os.getenv("INFLUXDB_STAGING_HOST", "localhost")
INFLUXDB_PORT = int(os.getenv("INFLUXDB_STAGING_PORT", "8087"))
INFLUXDB_TOKEN = os.getenv("INFLUXDB_STAGING_TOKEN", "iot-staging-token")
INFLUXDB_ORG = os.getenv("INFLUXDB_STAGING_ORG", "iot")
INFLUXDB_BUCKET_STAGING = os.getenv("INFLUXDB_BUCKET_STAGING", "sensor_staging")

PARQUET_FLUSH_INTERVAL_MINUTES = float(os.getenv("PARQUET_FLUSH_INTERVAL_MINUTES", "5"))
PARQUET_OUTPUT_DIR = os.getenv("PARQUET_OUTPUT_DIR", "./bdd/parquet")
PARQUET_FLUSH_SAFETY_MARGIN_SECONDS = float(
    os.getenv("PARQUET_FLUSH_SAFETY_MARGIN_SECONDS", "30")
)

EPOCH_RFC3339 = "1970-01-01T00:00:00Z"
# Format compact (sans ':') pour un nom de fichier valide sur tous les OS.
FILENAME_TS_FORMAT = "%Y%m%dT%H%M%SZ"

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


def build_client() -> InfluxDBClient:
    url = f"http://{INFLUXDB_HOST}:{INFLUXDB_PORT}"
    return InfluxDBClient(url=url, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)


def query_pending(client: InfluxDBClient, cutoff_rfc3339: str) -> pd.DataFrame:
    query = f'''
from(bucket: "{INFLUXDB_BUCKET_STAGING}")
  |> range(start: {EPOCH_RFC3339}, stop: {cutoff_rfc3339})
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
'''
    df = client.query_api().query_data_frame(query, org=INFLUXDB_ORG)
    if isinstance(df, list):
        df = pd.concat(df, ignore_index=True) if df else pd.DataFrame()
    return df


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
    try:
        df.to_parquet(tmp_path, index=False)
        tmp_path.rename(final_path)
    except BaseException:
        tmp_path.unlink(missing_ok=True)
        raise
    return final_path


def delete_exported_range(client: InfluxDBClient, cutoff_rfc3339: str) -> None:
    delete_api: DeleteApi = client.delete_api()
    delete_api.delete(
        start=EPOCH_RFC3339,
        stop=cutoff_rfc3339,
        predicate='_measurement="sensor_data"',
        bucket=INFLUXDB_BUCKET_STAGING,
        org=INFLUXDB_ORG,
    )


def run_flush(client: InfluxDBClient, output_dir: Path) -> None:
    cutoff = datetime.now(timezone.utc) - timedelta(
        seconds=PARQUET_FLUSH_SAFETY_MARGIN_SECONDS
    )
    cutoff_rfc3339 = cutoff.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

    df = query_pending(client, cutoff_rfc3339)
    if df.empty:
        print(f"[flush] Rien a exporter avant {cutoff_rfc3339}", flush=True)
        return

    final_path = write_parquet_atomic(df, output_dir)
    print(f"[flush] {len(df)} points exportes -> {final_path.name}", flush=True)

    delete_exported_range(client, cutoff_rfc3339)
    print(f"[flush] Plage exportee supprimee de {INFLUXDB_BUCKET_STAGING}", flush=True)


def main() -> None:
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    output_dir = resolve_output_dir()
    client = build_client()
    interval_seconds = PARQUET_FLUSH_INTERVAL_MINUTES * 60
    print(
        f"Flush toutes les {PARQUET_FLUSH_INTERVAL_MINUTES} min "
        f"vers {output_dir} (bucket {INFLUXDB_BUCKET_STAGING})",
        flush=True,
    )

    try:
        while _running:
            run_flush(client, output_dir)
            waited = 0.0
            while _running and waited < interval_seconds:
                time.sleep(min(0.5, interval_seconds - waited))
                waited += 0.5
    finally:
        client.close()
        print("Deconnecte.", flush=True)


if __name__ == "__main__":
    main()
