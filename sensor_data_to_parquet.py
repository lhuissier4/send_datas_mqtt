"""Convertit le JSONL capteurs (`mqtt_iot_plc_send.jsonl`) en fichiers Parquet,
par lots de `PARQUET_CHUNK_LINES` lignes (10 par defaut), en parallele.

Le JSONL source contient une ligne = un tick = une liste de records
"melted" {timestamp, id_machine, <capteur>: valeur} (cf. mqtt_send.py,
src/gold/utils.py:record_future_send_in_jsonl). Chaque lot de N lignes est
repivote en lignes larges (une ligne par (timestamp, id_machine), toutes
les mesures du tick regroupees en colonnes) puis ecrit dans un Parquet dont
le nom porte le timestamp du premier et du dernier tick du lot :
sensor_data_<debut>_<fin>.parquet (ex : sensor_data_20240801T000000Z_20240801T000230Z.parquet)

Parallelisation : une 1re passe (legere, sequentielle) releve les offsets
d'octets de chaque debut de lot, afin de decouper le fichier en plages
contigues sans le charger entierement en memoire. Chaque worker traite
ensuite sa plage et ecrit ses propres fichiers Parquet.
"""

import json
import os
import tempfile
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
from multiprocessing import cpu_count
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv

from src.gold.utils import PLC_COLUMNS, SENSOR_COLUMNS

PROJECT_ROOT = Path(__file__).resolve().parent
load_dotenv(PROJECT_ROOT / ".env")

JSONL_PATH = os.getenv("JSONL_PATH", "datas/gold/mqtt_iot_plc_send.jsonl")
PARQUET_OUTPUT_DIR = os.getenv("PARQUET_OUTPUT_DIR", "./bdd/parquet")
CHUNK_LINES = int(os.getenv("PARQUET_CHUNK_LINES", "10"))

TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
# Format compact (sans ':') pour un nom de fichier valide sur tous les OS.
FILENAME_TS_FORMAT = "%Y%m%dT%H%M%SZ"

META_KEYS = {"timestamp", "id_machine"}
COLUMNS = ["timestamp", "id_machine", *SENSOR_COLUMNS, *PLC_COLUMNS]

WRITE_OPTS = dict(compression="snappy")


def resolve_path(value: str) -> Path:
    path = Path(value)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path


def index_chunk_offsets(jsonl_path: Path, chunk_lines: int) -> list[int]:
    """Releve l'offset d'octet du debut de chaque lot de `chunk_lines` lignes.

    Passe sequentielle et legere (pas de parsing JSON) : elle sert
    uniquement a decouper le fichier en plages alignees sur des lots
    complets, pour repartir le travail entre workers.
    """
    offsets = []
    with jsonl_path.open("rb") as fh:
        offset = 0
        line_no = 0
        for line in fh:
            if line_no % chunk_lines == 0:
                offsets.append(offset)
            offset += len(line)
            line_no += 1
    return offsets


def split_ranges(offsets: list[int], num_workers: int) -> list[tuple[int, int | None]]:
    """Repartit les offsets de lots entre workers, en plages [debut, fin[ contigues.

    `fin=None` signifie "jusqu'a la fin du fichier" (dernier worker).
    """
    n = len(offsets)
    num_workers = max(1, min(num_workers, n))
    ranges = []
    for i in range(num_workers):
        start_idx = i * n // num_workers
        end_idx = (i + 1) * n // num_workers
        if start_idx == end_idx:
            continue
        start_offset = offsets[start_idx]
        end_offset = offsets[end_idx] if end_idx < n else None
        ranges.append((start_offset, end_offset))
    return ranges


def format_ts(raw_ts: str) -> str:
    return datetime.strptime(raw_ts, TIMESTAMP_FORMAT).strftime(FILENAME_TS_FORMAT)


def pivot_to_wide_rows(lines: list[list[dict]]) -> tuple[list[dict], str, str]:
    """Repivote un lot de lignes "melted" en lignes larges par (timestamp, id_machine)."""
    rows: dict[tuple[str, str], dict] = {}
    ts_min = ts_max = None

    for records in lines:
        for rec in records:
            ts = rec["timestamp"]
            machine = rec["id_machine"]
            ts_min = ts if ts_min is None or ts < ts_min else ts_min
            ts_max = ts if ts_max is None or ts > ts_max else ts_max

            row = rows.setdefault((ts, machine), {"timestamp": ts, "id_machine": machine})
            for field, value in rec.items():
                if field not in META_KEYS:
                    row[field] = value

    ordered = sorted(rows.values(), key=lambda r: (r["timestamp"], r["id_machine"]))
    return ordered, ts_min, ts_max


def write_chunk_parquet(rows: list[dict], ts_min: str, ts_max: str, output_dir: Path) -> Path:
    table = pa.table({col: pa.array([r.get(col) for r in rows]) for col in COLUMNS})

    final_path = output_dir / f"sensor_data_{format_ts(ts_min)}_{format_ts(ts_max)}.parquet"
    fd, tmp_name = tempfile.mkstemp(dir=output_dir, suffix=".parquet.tmp")
    os.close(fd)
    tmp_path = Path(tmp_name)
    try:
        pq.write_table(table, tmp_path, **WRITE_OPTS)
        tmp_path.replace(final_path)
    except BaseException:
        tmp_path.unlink(missing_ok=True)
        raise
    return final_path


def process_range(
    jsonl_path_str: str,
    start_offset: int,
    end_offset: int | None,
    chunk_lines: int,
    output_dir_str: str,
) -> list[str]:
    """Worker : lit sa plage d'octets, regroupe par lots de `chunk_lines` et ecrit un Parquet par lot."""
    jsonl_path = Path(jsonl_path_str)
    output_dir = Path(output_dir_str)
    written: list[str] = []
    buffer: list[list[dict]] = []

    with jsonl_path.open("rb") as fh:
        fh.seek(start_offset)
        pos = start_offset
        while end_offset is None or pos < end_offset:
            line = fh.readline()
            if not line:
                break
            pos += len(line)
            line = line.strip()
            if not line:
                continue
            buffer.append(json.loads(line))
            if len(buffer) == chunk_lines:
                rows, ts_min, ts_max = pivot_to_wide_rows(buffer)
                written.append(str(write_chunk_parquet(rows, ts_min, ts_max, output_dir)))
                buffer = []

    if buffer:
        rows, ts_min, ts_max = pivot_to_wide_rows(buffer)
        written.append(str(write_chunk_parquet(rows, ts_min, ts_max, output_dir)))

    return written


if __name__ == "__main__":
    jsonl_path = resolve_path(JSONL_PATH)
    if not jsonl_path.exists():
        raise FileNotFoundError(f"Fichier introuvable : {jsonl_path}")

    output_dir = resolve_path(PARQUET_OUTPUT_DIR)
    output_dir.mkdir(parents=True, exist_ok=True)

    start = time.perf_counter()

    print(f"Indexation de {jsonl_path} (lots de {CHUNK_LINES} lignes)...")
    offsets = index_chunk_offsets(jsonl_path, CHUNK_LINES)
    if not offsets:
        print("Fichier vide, rien a faire.")
        raise SystemExit(0)

    num_workers = max(cpu_count() - 1, 1)
    ranges = split_ranges(offsets, num_workers)
    print(f"{len(offsets)} lot(s) a produire, {len(ranges)} worker(s)...")

    total_files = 0
    with ProcessPoolExecutor(max_workers=len(ranges)) as executor:
        futures = {
            executor.submit(process_range, str(jsonl_path), start_off, end_off, CHUNK_LINES, str(output_dir)): (start_off, end_off)
            for start_off, end_off in ranges
        }
        for fut in as_completed(futures):
            written = fut.result()
            total_files += len(written)
            print(f"  worker termine : {len(written)} fichier(s)")

    print(f"{total_files} fichier(s) Parquet ecrits dans {output_dir}")
    print(f"Termine en {time.perf_counter() - start:.2f}s")
