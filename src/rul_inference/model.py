"""Inference RUL (Remaining Useful Life) en temps reel.

Charge le bundle (modele Cox + imputer + encodages) exporte par
`gold/train_rul_model.py` (portage de `Nettoyage_RUL_survie_cox.ipynb`, cf.
`gold/rul_pipeline.py` pour la logique de nettoyage/encodage partagee avec
l'entrainement) et l'applique aux dernieres valeurs capteurs connues par
machine.

`rul_inference/service.py` reconstitue `machine_id`, `age_jours`, les valeurs
nominales, `type_metal`/`iot_statut_machine` (lectures PLC) et `label_gmao`
(via Postgres/InfluxDB, cf. `_build_contexte_machine`,
`_decoder_contexte_plc`, `_reconstruire_label_gmao`) avant d'appeler
`predict_rul`, qui derive ensuite ici les colonnes calculees a partir de ce
vecteur reconstitue : `type_censure` depuis `label_gmao` (cf.
`_ajouter_type_censure_si_possible`, reproduit
`rul_pipeline.py::calculer_type_censure`) et les ecarts/ratios nominal-mesure
(cf. `_ajouter_ecarts_si_possible`, reproduit
`ajouter_ecarts_nominal_mesure`/`ajouter_ratio_nominal_mesure`). Reste hors de
portee de l'inference temps reel (feature non calculable a partir d'un seul
point) : les moyennes/ecarts-types glissants (`ajouter_stats_glissantes`, qui
ont besoin d'un historique de mesures) et les colonnes sans source de
production identifiee (`secteur`, `nb_pieces_cumule`,
`observation_operateur`... cf. `rul_data_assembly.py`) -- elles restent NaN,
gerees par l'imputer sauvegarde a l'entrainement.

Tant qu'aucun modele n'a ete entraine (`RUL_MODEL_PATH` introuvable),
`predict_rul` retombe sur une valeur fixe plutot que de planter.
"""

import os
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

from gold.rul_pipeline import (
    CAPTEURS_IOT,
    ajouter_ecarts_nominal_mesure,
    ajouter_ratio_nominal_mesure,
    calculer_type_censure,
    preparer_vecteur_inference,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

# (capteur brut, valeur nominale correspondante) attendus ensemble par
# ajouter_ecarts_nominal_mesure/ajouter_ratio_nominal_mesure (cf. rul_pipeline.py) :
# on ne les applique que si les deux sont presents, pour ne jamais planter sur
# un vecteur partiellement reconstitue (machine tout juste demarree, cache
# nominal pas encore charge...).
_PAIRES_ECART_NOMINAL = [
    ("iot_vitesse_rotation", "vitesse_rotation_nominal"),
    ("iot_courant_moteur", "courant_moteur_nominal"),
    ("iot_pression_hydraulique", "pression_hydraulique_nominal"),
    ("iot_temperature", "temp_base_moteur"),
]

RUL_MODEL_PATH = os.getenv("RUL_MODEL_PATH", "models/rul_cox_model.joblib")

_bundle = None


def _resolve_model_path() -> Path:
    path = Path(RUL_MODEL_PATH)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path


def load_model():
    """Charge le bundle entraine par le pipeline CI/CD (cf. train_rul_model.py).

    Renvoie None si aucun modele n'a encore ete exporte : `predict_rul` retombe
    alors sur une valeur fixe plutot que de planter.
    """
    global _bundle
    model_path = _resolve_model_path()
    if not model_path.exists():
        print(f"[rul_inference] Aucun modele trouve a {model_path}, placeholder actif.", flush=True)
        _bundle = None
        return _bundle
    _bundle = joblib.load(model_path)
    print(f"[rul_inference] Modele charge depuis {model_path}.", flush=True)
    return _bundle


def _ajouter_ecarts_si_possible(features_brutes: dict) -> dict:
    """Reproduit les ecarts/ratios nominal-mesure de l'entrainement (cf.
    `rul_pipeline.py::ajouter_ecarts_nominal_mesure`/`ajouter_ratio_nominal_mesure`)
    sur un vecteur d'inference.

    Une paire (capteur, nominal) pas encore disponible (machine tout juste
    demarree, cache nominal pas encore charge...) produit un ecart NaN plutot
    que de faire planter la prediction -- gere ensuite par l'imputer comme
    n'importe quelle feature manquante.
    """
    colonnes = {}
    for brut, nominal in _PAIRES_ECART_NOMINAL:
        colonnes[brut] = float(features_brutes[brut]) if features_brutes.get(brut) is not None else np.nan
        colonnes[nominal] = float(features_brutes[nominal]) if features_brutes.get(nominal) is not None else np.nan

    ligne = pd.DataFrame([colonnes])
    ligne = ajouter_ecarts_nominal_mesure(ligne)
    ligne = ajouter_ratio_nominal_mesure(ligne)
    ecarts = {col: val for col, val in ligne.iloc[0].items() if col not in colonnes}
    return {**features_brutes, **ecarts}


def _ajouter_type_censure_si_possible(features_brutes: dict) -> dict:
    """Deduit `type_censure` de `label_gmao` (cf.
    `rul_pipeline.py::calculer_type_censure`), si `label_gmao` a pu etre
    reconstitue (cf. `service.py::_reconstruire_label_gmao`). Sans
    `label_gmao` connu (machine dont on n'a pas encore d'horodatage), on
    n'invente pas de valeur par defaut : `type_censure` reste absent, impute
    comme n'importe quelle feature manquante.
    """
    if "label_gmao" not in features_brutes:
        return features_brutes
    ligne = pd.DataFrame([{"label_gmao": features_brutes["label_gmao"]}])
    ligne = calculer_type_censure(ligne)
    return {**features_brutes, "type_censure": ligne.iloc[0]["type_censure"]}


def predict_rul(id_machine: str, features: dict[str, float]) -> dict[str, float]:
    """Estime la RUL (en jours) et un score de risque a partir des dernieres valeurs capteurs.

    `features` contient les dernieres valeurs capteurs connues (cf.
    `CAPTEURS_IOT`) completees par `service.py` avec `age_jours` et les
    valeurs nominales de la machine (cf. `_build_contexte_machine`). Les
    ecarts/ratios nominal-mesure sont recalcules ici (cf.
    `_ajouter_ecarts_si_possible`) exactement comme a l'entrainement. Les
    colonnes encore sans source de production (cf. module docstring) restent
    absentes : imputees comme a l'entrainement.
    """
    if _bundle is None:
        return {"rul_jours_estime": -1.0, "risque": 0.0}

    features_brutes = {**features, "machine_id": id_machine}
    features_brutes = _ajouter_type_censure_si_possible(features_brutes)
    features_brutes = _ajouter_ecarts_si_possible(features_brutes)
    ligne = preparer_vecteur_inference(
        features_brutes,
        _bundle["mappings_categories"],
        _bundle["colonnes_gardees"],
        _bundle["colonnes_onehot"],
    )
    ligne_imp = _bundle["imputer"].transform(ligne)

    modele = _bundle["modele"]
    risque = float(modele.predict(ligne_imp)[0])

    courbe = modele.predict_survival_function(ligne_imp)[0]
    temps, survie = courbe.x, courbe(courbe.x)
    sous_mediane = temps[survie <= 0.5]
    rul_jours_estime = float(sous_mediane[0]) if len(sous_mediane) else float(temps[-1])

    return {"rul_jours_estime": rul_jours_estime, "risque": risque}
