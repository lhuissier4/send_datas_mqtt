"""Correle les lectures capteur du stockage froid (bdd/parquet/*.parquet)
avec les episodes d'alerte de la base InfluxDB `sensor_live` (mesure
`alerte`), et ecrit le resultat dans
`datas/gold/sensor_data_alerte_correlated.csv`.

Cf. openspec/changes/correlate-sensor-data-alerte/design.md.

`alerte` est tire d'InfluxDB en une seule requete (pas une requete par
fichier parquet) : la table complete tient en quelques dizaines de Mo, donc
la charger une fois en memoire est largement suffisant. Seuls les fichiers
parquet dont la fenetre se situe dans les `CORRELATE_SENSOR_ALERTE_WINDOW_DAYS`
derniers jours sont lus, en se basant sur l'intervalle encode dans le nom de
fichier (`sensor_data_{start}_{end}.parquet`) : les fichiers hors fenetre ne
sont jamais ouverts. La jointure elle-meme est un `merge_asof` vectorise
(par machine, sur le timestamp), pas une boucle ligne par ligne.

Les fichiers parquet source et les donnees InfluxDB ne sont jamais modifies :
seule une nouvelle table CSV derivee est ecrite, comme les autres scripts du
pipeline gold (`gold_datas.py`, `load_*.py`).
"""

import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
load_dotenv(PROJECT_ROOT / ".env")

INFLUXDB_LIVE_HOST = os.getenv("INFLUXDB_LIVE_HOST", "localhost")
INFLUXDB_LIVE_PORT = int(os.getenv("INFLUXDB_LIVE_PORT", "8183"))
INFLUXDB_DATABASE_LIVE = os.getenv("INFLUXDB_DATABASE_LIVE", "sensor_live")
INFLUXDB_LIVE_TOKEN = os.getenv("INFLUXDB_LIVE_TOKEN", "apiv3_mspr2-live-dev-token")

PARQUET_OUTPUT_DIR = os.getenv("PARQUET_OUTPUT_DIR", "./bdd/parquet")
CORRELATE_SENSOR_ALERTE_WINDOW_DAYS = float(
    os.getenv("CORRELATE_SENSOR_ALERTE_WINDOW_DAYS", "180")
)
CORRELATE_SENSOR_ALERTE_OUTPUT_PATH = os.getenv(
    "CORRELATE_SENSOR_ALERTE_OUTPUT_PATH",
    "./datas/gold/sensor_data_alerte_correlated.csv",
)

# Meme format que FILENAME_TS_FORMAT dans src/parquet_flush.py.
FILENAME_TS_FORMAT = "%Y%m%dT%H%M%SZ"
PARQUET_FILENAME_PATTERN = re.compile(
    r"^sensor_data_(\d{8}T\d{6}Z)_(\d{8}T\d{6}Z)\.parquet$"
)


def resolve_path(value: str) -> Path:
    path = Path(value)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path


def parse_window_end(path: Path) -> datetime:
    match = PARQUET_FILENAME_PATTERN.match(path.name)
    if not match:
        raise ValueError(
            f"Nom de fichier parquet inattendu (ne correspond pas a "
            f"sensor_data_{{start}}_{{end}}.parquet) : {path.name}"
        )
    end_str = match.group(2)
    return datetime.strptime(end_str, FILENAME_TS_FORMAT).replace(tzinfo=timezone.utc)


def list_parquet_files_in_window(output_dir: Path, window_days: float) -> list[Path]:
    # La fenetre est relative aux donnees elles-memes (fin la plus recente
    # trouvee dans bdd/parquet), pas a l'heure reelle : ce jeu de donnees est
    # simule et ne s'arrete pas forcement a "maintenant", donc caler la
    # fenetre sur l'horloge systeme exclurait tout si les parquets sont plus
    # anciens (ou plus recents) que la date du jour.
    entries = [(path, parse_window_end(path)) for path in sorted(output_dir.glob("*.parquet"))]
    if not entries:
        return []
    reference_now = max(end for _, end in entries)
    cutoff = reference_now - timedelta(days=window_days)
    return [path for path, end in entries if end >= cutoff]


def load_sensor_readings(files: list[Path]) -> pd.DataFrame:
    if not files:
        return pd.DataFrame(columns=["timestamp", "id_machine"])
    df = pd.concat((pd.read_parquet(path) for path in files), ignore_index=True)
    # Les parquets ecrits par parquet_flush.py portent la colonne
    # "sensor_timestamp" (pas "timestamp") : cf. write_parquet_atomic().
    df = df.rename(columns={"sensor_timestamp": "timestamp"})
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df.sort_values("timestamp").reset_index(drop=True)


def empty_alerte_dataframe() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "debut_alerte": pd.Series(dtype="datetime64[ns]"),
            "fin_alerte": pd.Series(dtype="datetime64[ns]"),
            "id_machine": pd.Series(dtype="object"),
            "id_alerte": pd.Series(dtype="int64"),
        }
    )


def fetch_alerte(session: requests.Session, base_url: str) -> pd.DataFrame:
    response = session.post(
        f"{base_url}/api/v3/query_sql",
        json={"db": INFLUXDB_DATABASE_LIVE, "q": "SELECT * FROM alerte ORDER BY time"},
    )
    if response.status_code == 400 and "not found" in response.text:
        # La mesure `alerte` n'existe pas encore (aucun point jamais ecrit) :
        # cf. le meme garde-fou dans query_pending() de src/parquet_flush.py.
        return empty_alerte_dataframe()
    response.raise_for_status()
    df = pd.DataFrame.from_records(response.json())
    if df.empty:
        return empty_alerte_dataframe()
    df = df.rename(columns={"time": "debut_alerte"})
    # Le "time" retourne par query_sql est une chaine RFC3339 (UTC) ; on la
    # rend naive pour rester coherent avec le "timestamp" (naive) des parquets.
    df["debut_alerte"] = pd.to_datetime(df["debut_alerte"]).dt.tz_localize(None)
    df["fin_alerte"] = pd.to_datetime(df["fin_alerte"], unit="ns")
    df["id_alerte"] = df["id_alerte"].astype(int)
    return df.sort_values("debut_alerte").reset_index(drop=True)


def correlate(sensor_df: pd.DataFrame, alerte_df: pd.DataFrame) -> pd.DataFrame:
    rows_before = len(sensor_df)

    # merge_asof exige un dtype strictement identique des deux cotes, aussi
    # bien pour "by" que pour "on" : pd.read_parquet et
    # pd.DataFrame.from_records n'inferent pas toujours le meme dtype de
    # chaine (object vs string[...]) pour id_machine, et parse_dates ne
    # produit pas toujours la meme resolution (us vs ns, meme remarque que
    # dans load_nominal_values.py) pour les colonnes timestamp.
    sensor_df = sensor_df.assign(
        id_machine=sensor_df["id_machine"].astype(str),
        timestamp=sensor_df["timestamp"].astype("datetime64[ns]"),
    )
    alerte_df = alerte_df.assign(
        id_machine=alerte_df["id_machine"].astype(str),
        debut_alerte=alerte_df["debut_alerte"].astype("datetime64[ns]"),
        fin_alerte=alerte_df["fin_alerte"].astype("datetime64[ns]"),
    )

    merged = pd.merge_asof(
        sensor_df,
        alerte_df[["debut_alerte", "fin_alerte", "id_machine", "id_alerte"]],
        left_on="timestamp",
        right_on="debut_alerte",
        by="id_machine",
        direction="backward",
    )

    # L'episode trouve par merge_asof est le plus recent debut_alerte <=
    # timestamp, mais il peut deja etre termine : dans ce cas la lecture ne
    # correspond a aucune alerte active.
    closed_before_reading = merged["timestamp"] >= merged["fin_alerte"]
    merged.loc[closed_before_reading, "id_alerte"] = pd.NA
    merged["id_alerte"] = merged["id_alerte"].astype("Int64")

    assert len(merged) == rows_before, (
        f"La jointure a change le nombre de lignes ({rows_before} -> "
        f"{len(merged)}) : un fan-out inattendu s'est produit."
    )

    return merged.drop(columns=["debut_alerte", "fin_alerte"])


def main() -> None:
    output_dir = resolve_path(PARQUET_OUTPUT_DIR)
    files = list_parquet_files_in_window(output_dir, CORRELATE_SENSOR_ALERTE_WINDOW_DAYS)
    print(
        f"[correlate] {len(files)} fichiers parquet dans la fenetre des "
        f"{CORRELATE_SENSOR_ALERTE_WINDOW_DAYS:.0f} derniers jours (sur "
        f"{sum(1 for _ in output_dir.glob('*.parquet'))} au total)",
        flush=True,
    )
    sensor_df = load_sensor_readings(files)
    print(f"[correlate] {len(sensor_df)} lectures capteur chargees", flush=True)

    base_url = f"http://{INFLUXDB_LIVE_HOST}:{INFLUXDB_LIVE_PORT}"
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {INFLUXDB_LIVE_TOKEN}"})
    try:
        alerte_df = fetch_alerte(session, base_url)
    finally:
        session.close()
    print(f"[correlate] {len(alerte_df)} episodes d'alerte recuperes depuis InfluxDB", flush=True)

    correlated = correlate(sensor_df, alerte_df)

    output_path = resolve_path(CORRELATE_SENSOR_ALERTE_OUTPUT_PATH)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    correlated.to_csv(output_path, index=False, encoding="utf-8") #remplacer par le code d'entraînement
    matched = int(correlated["id_alerte"].notna().sum())
    print(
        f"[correlate] {len(correlated)} lignes ecrites -> {output_path} "
        f"({matched} avec une alerte active)",
        flush=True,
    )


if __name__ == "__main__":
    main()
