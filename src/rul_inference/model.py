"""Inference RUL (Remaining Useful Life) en temps reel.

Charge le bundle (modele Cox + imputer + encodages) exporte par
`gold/train_rul_model.py` (portage de `Nettoyage_RUL_survie_cox.ipynb`, cf.
`gold/rul_pipeline.py` pour la logique de nettoyage/encodage partagee avec
l'entrainement) et l'applique aux dernieres valeurs capteurs connues par
machine.

Tant qu'aucun modele n'a ete entraine (`RUL_MODEL_PATH` introuvable), ou tant
que le cache temps reel ne contient que les capteurs bruts (cf. limite
documentee dans `rul_inference/service.py` : la reconstitution complete du
vecteur de features -- machine_id, valeurs nominales, historique -- reste un
TODO cote pipeline de production), `predict_rul` retombe sur une valeur fixe
plutot que de planter.
"""

import os
from pathlib import Path

import joblib

from gold.rul_pipeline import CAPTEURS_IOT, preparer_vecteur_inference

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

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


def predict_rul(id_machine: str, features: dict[str, float]) -> dict[str, float]:
    """Estime la RUL (en jours) et un score de risque a partir des dernieres valeurs capteurs.

    `features` contient les dernieres valeurs connues (pas forcement toutes
    presentes) pour les capteurs de `CAPTEURS_IOT`. Les autres colonnes
    attendues par le modele (machine_id, valeurs nominales, historique...) sont
    absentes tant que le pipeline de production ne les reconstitue pas (cf.
    TODO dans `train_rul_model.py`) : elles sont imputees comme a
    l'entrainement.
    """
    if _bundle is None:
        return {"rul_jours_estime": -1.0, "risque": 0.0}

    features_brutes = {**features, "machine_id": id_machine}
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
