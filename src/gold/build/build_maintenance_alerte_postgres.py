"""Construit `datas/gold/postgres_maintenance.csv` et
`datas/gold/postgres_alerte.csv` (tables de reference `label_gmao, id`) a
partir de `datas/silver/dataset_brut.csv`.

Les deux fichiers partagent le meme calcul (retrait des lignes "Sain" puis
split par prefixe "Alerte"/"Maintenance" sur `label_gmao`) : ce sont deux
sorties d'un seul script plutot que deux scripts qui se declencheraient l'un
l'autre pour rien - cf.
`openspec/changes/split-gold-datas-per-dataset/design.md` (decision 2).

Chaque accesseur (`build_maintenance`/`build_alerte`) se verifie
independamment via `ensure_gold_csv` : si les deux csv sont absents, le
premier appele calcule et ecrit les deux ; si un seul est absent, il est
(re)calcule et ecrit sans re-ecrire celui deja present.
"""

from pathlib import Path

import pandas as pd

from gold.utils import (
    create_table_with_id_and_unique_label,
    ensure_gold_csv,
    name_csv_file,
    remove_rows_containing_string_in_column,
    split_dataframe_by_prefix,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
SILVER_DATASET_BRUT = PROJECT_ROOT / "datas/silver/dataset_brut.csv"
GOLD_DIR = PROJECT_ROOT / "datas/gold"
MAINTENANCE_OUTPUT_PATH = Path(
    name_csv_file(folder_path=GOLD_DIR, filename="maintenance", extension=".csv", type_dst="postgres")
)
ALERTE_OUTPUT_PATH = Path(
    name_csv_file(folder_path=GOLD_DIR, filename="alerte", extension=".csv", type_dst="postgres")
)


def compute(df_simule: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Retourne (df_maintenance_unique, df_alerte_unique), sans rien ecrire."""
    df_maintenance = remove_rows_containing_string_in_column(
        df=df_simule, column_name="label_gmao", string_to_remove="Sain",
    )
    df_alerte_splited, df_maintenance_splited = split_dataframe_by_prefix(df_maintenance, "label_gmao", "Alerte")
    df_maintenance_unique = create_table_with_id_and_unique_label(df_maintenance_splited, "label_gmao")
    df_alerte_unique = create_table_with_id_and_unique_label(df_alerte_splited, "label_gmao")
    return df_maintenance_unique, df_alerte_unique


def build_maintenance(
    maintenance_path: Path = MAINTENANCE_OUTPUT_PATH,
    alerte_path: Path = ALERTE_OUTPUT_PATH,
    source_csv: Path = SILVER_DATASET_BRUT,
) -> pd.DataFrame:
    def _compute() -> pd.DataFrame:
        df_simule = pd.read_csv(source_csv)
        df_maintenance, df_alerte = compute(df_simule)
        maintenance_path.parent.mkdir(parents=True, exist_ok=True)
        df_maintenance.to_csv(maintenance_path, index=False, encoding="utf-8")
        if not alerte_path.exists():
            alerte_path.parent.mkdir(parents=True, exist_ok=True)
            df_alerte.to_csv(alerte_path, index=False, encoding="utf-8")
        return df_maintenance

    return ensure_gold_csv(maintenance_path, _compute)


def build_alerte(
    maintenance_path: Path = MAINTENANCE_OUTPUT_PATH,
    alerte_path: Path = ALERTE_OUTPUT_PATH,
    source_csv: Path = SILVER_DATASET_BRUT,
) -> pd.DataFrame:
    def _compute() -> pd.DataFrame:
        df_simule = pd.read_csv(source_csv)
        df_maintenance, df_alerte = compute(df_simule)
        alerte_path.parent.mkdir(parents=True, exist_ok=True)
        df_alerte.to_csv(alerte_path, index=False, encoding="utf-8")
        if not maintenance_path.exists():
            maintenance_path.parent.mkdir(parents=True, exist_ok=True)
            df_maintenance.to_csv(maintenance_path, index=False, encoding="utf-8")
        return df_alerte

    return ensure_gold_csv(alerte_path, _compute)


if __name__ == "__main__":
    build_maintenance()
    build_alerte()
