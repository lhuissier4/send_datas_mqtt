"""Construit `datas/gold/influxdb_maintenance.csv` et
`datas/gold/influxdb_alerte.csv` (episodes avec debut/fin) a partir de
`datas/silver/dataset_brut.csv`, `datas/gold/postgres_maintenance.csv` et
`datas/gold/postgres_alerte.csv` (declenche
`build_maintenance_alerte_postgres` si l'un de ces deux derniers n'existe
pas encore).

Correction par rapport a `gold_datas.py` : `attach_alerte_maintenance_ids`
est appelee avec les tables de lookup `label_gmao, id` (le contenu de
`postgres_maintenance.csv` / `postgres_alerte.csv`), pas les lignes brutes
pre-split - cf.
`openspec/changes/split-gold-datas-per-dataset/design.md` (Context).

Comme pour le postgres, les deux fichiers partagent un seul calcul ; chaque
accesseur se verifie independamment via `ensure_gold_csv`.
"""

from pathlib import Path

import pandas as pd

from gold.build import build_maintenance_alerte_postgres
from gold.utils import (
    attach_alerte_maintenance_ids,
    build_episode_dataframe,
    ensure_gold_csv,
    name_csv_file,
    sort_dataframe_by_timestamp,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
SILVER_DATASET_BRUT = PROJECT_ROOT / "datas/silver/dataset_brut.csv"
GOLD_DIR = PROJECT_ROOT / "datas/gold"
MAINTENANCE_OUTPUT_PATH = Path(
    name_csv_file(folder_path=GOLD_DIR, filename="maintenance", extension=".csv", type_dst="influxdb")
)
ALERTE_OUTPUT_PATH = Path(
    name_csv_file(folder_path=GOLD_DIR, filename="alerte", extension=".csv", type_dst="influxdb")
)


def compute(
    df_simule: pd.DataFrame, df_maintenance_lookup: pd.DataFrame, df_alerte_lookup: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Retourne (df_maintenance_clean, df_alerte_clean), sans rien ecrire."""
    df_alert_complet, df_maintenance_complet = attach_alerte_maintenance_ids(
        df_simule=df_simule,
        df_maintenance=df_maintenance_lookup,
        df_alerte=df_alerte_lookup,
    )
    df_alert_filtered = df_alert_complet[["timestamp", "machine_id", "id_alerte"]]
    df_maintenance_filtered = df_maintenance_complet[["timestamp", "machine_id", "id_maintenance"]]
    df_alert_filtered = sort_dataframe_by_timestamp(df_alert_filtered)
    df_maintenance_filtered = sort_dataframe_by_timestamp(df_maintenance_filtered)

    df_maintenance_clean = build_episode_dataframe(
        df_maintenance_filtered,
        id_column="id_maintenance",
        id_output_column="id_panne",
        start_column="debut_panne",
        end_column="fin_panne",
    )
    df_alert_clean = build_episode_dataframe(
        df_alert_filtered,
        id_column="id_alerte",
        id_output_column="id_alerte",
        start_column="debut_alerte",
        end_column="fin_alerte",
    )
    return df_maintenance_clean, df_alert_clean


def _load_lookups(
    source_csv: Path, maintenance_lookup_path: Path, alerte_lookup_path: Path
) -> tuple[pd.DataFrame, pd.DataFrame]:
    df_maintenance_lookup = ensure_gold_csv(
        maintenance_lookup_path,
        lambda: build_maintenance_alerte_postgres.build_maintenance(
            maintenance_path=maintenance_lookup_path, alerte_path=alerte_lookup_path, source_csv=source_csv
        ),
    )
    df_alerte_lookup = ensure_gold_csv(
        alerte_lookup_path,
        lambda: build_maintenance_alerte_postgres.build_alerte(
            maintenance_path=maintenance_lookup_path, alerte_path=alerte_lookup_path, source_csv=source_csv
        ),
    )
    return df_maintenance_lookup, df_alerte_lookup


def build_maintenance(
    maintenance_path: Path = MAINTENANCE_OUTPUT_PATH,
    alerte_path: Path = ALERTE_OUTPUT_PATH,
    source_csv: Path = SILVER_DATASET_BRUT,
    maintenance_lookup_path: Path = build_maintenance_alerte_postgres.MAINTENANCE_OUTPUT_PATH,
    alerte_lookup_path: Path = build_maintenance_alerte_postgres.ALERTE_OUTPUT_PATH,
) -> pd.DataFrame:
    def _compute() -> pd.DataFrame:
        df_simule = pd.read_csv(source_csv)
        df_maintenance_lookup, df_alerte_lookup = _load_lookups(
            source_csv, maintenance_lookup_path, alerte_lookup_path
        )
        df_maintenance_clean, df_alert_clean = compute(df_simule, df_maintenance_lookup, df_alerte_lookup)
        maintenance_path.parent.mkdir(parents=True, exist_ok=True)
        df_maintenance_clean.to_csv(maintenance_path, index=False, encoding="utf-8")
        if not alerte_path.exists():
            alerte_path.parent.mkdir(parents=True, exist_ok=True)
            df_alert_clean.to_csv(alerte_path, index=False, encoding="utf-8")
        return df_maintenance_clean

    return ensure_gold_csv(maintenance_path, _compute)


def build_alerte(
    maintenance_path: Path = MAINTENANCE_OUTPUT_PATH,
    alerte_path: Path = ALERTE_OUTPUT_PATH,
    source_csv: Path = SILVER_DATASET_BRUT,
    maintenance_lookup_path: Path = build_maintenance_alerte_postgres.MAINTENANCE_OUTPUT_PATH,
    alerte_lookup_path: Path = build_maintenance_alerte_postgres.ALERTE_OUTPUT_PATH,
) -> pd.DataFrame:
    def _compute() -> pd.DataFrame:
        df_simule = pd.read_csv(source_csv)
        df_maintenance_lookup, df_alerte_lookup = _load_lookups(
            source_csv, maintenance_lookup_path, alerte_lookup_path
        )
        df_maintenance_clean, df_alert_clean = compute(df_simule, df_maintenance_lookup, df_alerte_lookup)
        alerte_path.parent.mkdir(parents=True, exist_ok=True)
        df_alert_clean.to_csv(alerte_path, index=False, encoding="utf-8")
        if not maintenance_path.exists():
            maintenance_path.parent.mkdir(parents=True, exist_ok=True)
            df_maintenance_clean.to_csv(maintenance_path, index=False, encoding="utf-8")
        return df_alert_clean

    return ensure_gold_csv(alerte_path, _compute)


if __name__ == "__main__":
    build_maintenance()
    build_alerte()
