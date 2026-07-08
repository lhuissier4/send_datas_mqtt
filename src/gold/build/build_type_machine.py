"""Construit `datas/gold/postgres_type_machine.csv` (table de reference
`type_machine, id`) a partir de `datas/silver/dataset_brut.csv`.

Script autonome et memoise : si le csv de sortie existe deja, il est relu
tel quel (aucun recalcul) - cf. `ensure_gold_csv` dans `gold/utils.py` et
`openspec/changes/split-gold-datas-per-dataset/design.md`.
"""

from pathlib import Path

import pandas as pd

from gold.utils import ensure_gold_csv, name_csv_file

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
SILVER_DATASET_BRUT = PROJECT_ROOT / "datas/silver/dataset_brut.csv"
GOLD_DIR = PROJECT_ROOT / "datas/gold"
OUTPUT_PATH = Path(
    name_csv_file(folder_path=GOLD_DIR, filename="type_machine", extension=".csv", type_dst="postgres")
)


def compute(df_simule: pd.DataFrame) -> pd.DataFrame:
    df_type_machine = df_simule[["type_machine"]].drop_duplicates(subset=["type_machine"])
    df_type_machine["id"] = df_type_machine["type_machine"].astype("category").cat.codes + 1
    return df_type_machine


def build(output_path: Path = OUTPUT_PATH, source_csv: Path = SILVER_DATASET_BRUT) -> pd.DataFrame:
    def _compute() -> pd.DataFrame:
        df_simule = pd.read_csv(source_csv)
        df_type_machine = compute(df_simule)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df_type_machine.to_csv(output_path, index=False, encoding="utf-8")
        return df_type_machine

    return ensure_gold_csv(output_path, _compute)


if __name__ == "__main__":
    build()
