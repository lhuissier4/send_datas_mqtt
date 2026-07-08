"""Construit `datas/gold/postgres_machine.csv` (`id_machine, id_type_machine`)
a partir de `datas/silver/dataset_brut.csv` et de
`datas/gold/postgres_type_machine.csv` (declenche `build_type_machine` si ce
dernier n'existe pas encore).

Script autonome et memoise : si le csv de sortie existe deja, il est relu
tel quel (aucun recalcul) - cf. `ensure_gold_csv` dans `gold/utils.py` et
`openspec/changes/split-gold-datas-per-dataset/design.md`.
"""

from pathlib import Path

import pandas as pd

from gold.build import build_type_machine
from gold.utils import build_machine_dataframe, ensure_gold_csv, name_csv_file

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
SILVER_DATASET_BRUT = PROJECT_ROOT / "datas/silver/dataset_brut.csv"
GOLD_DIR = PROJECT_ROOT / "datas/gold"
OUTPUT_PATH = Path(
    name_csv_file(folder_path=GOLD_DIR, filename="machine", extension=".csv", type_dst="postgres")
)


def build(
    output_path: Path = OUTPUT_PATH,
    source_csv: Path = SILVER_DATASET_BRUT,
    type_machine_path: Path = build_type_machine.OUTPUT_PATH,
) -> pd.DataFrame:
    def _compute() -> pd.DataFrame:
        df_simule = pd.read_csv(source_csv)
        df_type_machine = ensure_gold_csv(
            type_machine_path,
            lambda: build_type_machine.build(output_path=type_machine_path, source_csv=source_csv),
        )
        df_machine = build_machine_dataframe(df_simule, df_type_machine)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df_machine.to_csv(output_path, index=False, encoding="utf-8")
        return df_machine

    return ensure_gold_csv(output_path, _compute)


if __name__ == "__main__":
    build()
