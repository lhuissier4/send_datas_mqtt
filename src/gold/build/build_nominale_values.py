"""Construit `datas/gold/postgres_nominale_values.csv` a partir de
`datas/silver/dataset_brut.csv`, `datas/gold/postgres_type_metal.csv` et
`datas/gold/postgres_regime_cadence.csv` (declenche `build_type_metal` /
`build_regime_cadence` si l'un de ces deux derniers n'existe pas encore).

Script autonome et memoise : si le csv de sortie existe deja, il est relu
tel quel (aucun recalcul) - cf. `ensure_gold_csv` dans `gold/utils.py` et
`openspec/changes/split-gold-datas-per-dataset/design.md`.
"""

from pathlib import Path

import pandas as pd

from gold.build import build_regime_cadence, build_type_metal
from gold.utils import ensure_gold_csv, name_csv_file

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
SILVER_DATASET_BRUT = PROJECT_ROOT / "datas/silver/dataset_brut.csv"
GOLD_DIR = PROJECT_ROOT / "datas/gold"
OUTPUT_PATH = Path(
    name_csv_file(folder_path=GOLD_DIR, filename="nominale_values", extension=".csv", type_dst="postgres")
)

NOMINALE_COLUMNS = [
    "timestamp",
    "secteur",
    "machine_id",
    "vitesse_rotation_nominal",
    "courant_moteur_nominal",
    "pression_hydraulique_nominal",
    "statut_nominal",
    "temp_base_moteur",
    "id_type_metal",
    "id_regime_cadence",
    "facteur_cadence",
    "temps_cycle_sec",
]


def compute(df_simule: pd.DataFrame, df_type_metal: pd.DataFrame, df_regime_cadence: pd.DataFrame) -> pd.DataFrame:
    return df_simule.merge(
        df_type_metal[["type_metal", "id"]].rename(columns={"id": "id_type_metal"}),
        on="type_metal",
        how="left",
    ).merge(
        df_regime_cadence[["regime_cadence", "id"]].rename(columns={"id": "id_regime_cadence"}),
        on="regime_cadence",
        how="left",
    )[NOMINALE_COLUMNS]


def build(
    output_path: Path = OUTPUT_PATH,
    source_csv: Path = SILVER_DATASET_BRUT,
    type_metal_path: Path = build_type_metal.OUTPUT_PATH,
    regime_cadence_path: Path = build_regime_cadence.OUTPUT_PATH,
) -> pd.DataFrame:
    def _compute() -> pd.DataFrame:
        df_simule = pd.read_csv(source_csv)
        df_type_metal = ensure_gold_csv(
            type_metal_path,
            lambda: build_type_metal.build(output_path=type_metal_path, source_csv=source_csv),
        )
        df_regime_cadence = ensure_gold_csv(
            regime_cadence_path,
            lambda: build_regime_cadence.build(output_path=regime_cadence_path, source_csv=source_csv),
        )
        df_nominale = compute(df_simule, df_type_metal, df_regime_cadence)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df_nominale.to_csv(output_path, index=False, encoding="utf-8")
        return df_nominale

    return ensure_gold_csv(output_path, _compute)


if __name__ == "__main__":
    build()
