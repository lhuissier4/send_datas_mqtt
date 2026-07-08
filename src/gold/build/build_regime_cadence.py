"""Construit `datas/gold/postgres_regime_cadence.csv` (table de reference
`regime_cadence, id`) a partir de `datas/silver/dataset_brut.csv`.

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
    name_csv_file(folder_path=GOLD_DIR, filename="regime_cadence", extension=".csv", type_dst="postgres")
)


def compute(df_simule: pd.DataFrame) -> pd.DataFrame:
    df_regime_cadence = df_simule[["regime_cadence"]].drop_duplicates(subset=["regime_cadence"])
    df_regime_cadence["id"] = df_regime_cadence["regime_cadence"].astype("category").cat.codes + 1
    return df_regime_cadence


def build(output_path: Path = OUTPUT_PATH, source_csv: Path = SILVER_DATASET_BRUT) -> pd.DataFrame:
    def _compute() -> pd.DataFrame:
        df_simule = pd.read_csv(source_csv)
        df_regime_cadence = compute(df_simule)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df_regime_cadence.to_csv(output_path, index=False, encoding="utf-8")
        return df_regime_cadence

    return ensure_gold_csv(output_path, _compute)


if __name__ == "__main__":
    build()
