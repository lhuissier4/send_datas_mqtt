"""Pipeline de nettoyage, feature engineering et entrainement Cox pour la RUL.

Portage de `Nettoyage_RUL_survie_cox.ipynb` en fonctions reutilisables, partagees
entre `train_rul_model.py` (entrainement mensuel) et `rul_inference/model.py`
(inference temps reel) : les deux doivent appliquer *exactement* le meme
pretraitement, d'ou la mutualisation ici plutot qu'une duplication.

Format d'entree attendu par `construire_features` : une ligne par
`(machine_id, timestamp)`, avec les colonnes brutes du simulateur (capteurs IoT,
valeurs nominales, `label_gmao`, `iot_statut_machine`, `age_jours`,
`nb_pieces_cumule`, categorielles machine...). Cote production, `sensor_data` est
en format long (cf. `mqtt_send.py`) ; le pivot vers ce format large reste un TODO
(cf. `train_rul_model.py::pivot_sensor_readings_to_wide`) -- ce module ne
resout pas cet ecart, il porte uniquement la logique de nettoyage/entrainement
du notebook telle quelle.
"""

import numpy as np
import pandas as pd
from sklearn.feature_selection import mutual_info_regression
from sklearn.impute import SimpleImputer
from sksurv.linear_model import CoxPHSurvivalAnalysis
from sksurv.metrics import cumulative_dynamic_auc
from sksurv.util import Surv

# Cf. CAPTEURS_IOT dans mqtt_send.py / rul_inference/model.py.
CAPTEURS_IOT = [
    "iot_vitesse_rotation",
    "iot_courant_moteur",
    "iot_pression_hydraulique",
    "iot_temperature",
    "iot_vibration_rms",
    "iot_vibration_peak",
    "iot_charge_moteur",
]

# Sous-ensemble reellement bruite dans le generateur (cf. notebook, section 3) :
# vitesse_rotation et charge_moteur ne recoivent aucun bruit gaussien -> exclus
# du detecteur stuck-at, qui sinon les confond avec des consignes stables.
CAPTEURS_BRUITES = [
    "iot_courant_moteur",
    "iot_pression_hydraulique",
    "iot_temperature",
    "iot_vibration_rms",
    "iot_vibration_peak",
]

COLONNES_FUITE = ["age_virtuel_jours"]  # jamais utilisables, ni en feature ni en audit-input
COLONNE_CIBLE = "RUL_jours"
COLONNE_LABEL_GMAO = "label_gmao"

STATUTS_EXCLUS_ENTRAINEMENT = ["Arrêt Opérateur", "Arrêt Sous-Charge"]
STATUTS_EXCLUS_FINAL = ["Maintenance", "Panne"]
EVENEMENTS_CENSURANTS = ["Maintenance", "Panne"]

COLONNES_CATEGORIELLES = [
    "machine_id",
    "type_machine",
    "secteur",
    "type_metal",
    "statut_nominal",
    "regime_cadence",
    "iot_statut_machine",
    COLONNE_LABEL_GMAO,
    "type_censure",
    "observation_operateur",
]

TOP_K = 25
COLONNES_TOUJOURS_GARDEES = ["machine_id", "age_jours"]
GRANULARITE_JOURS = 0.1


# --- Section 3 : traitement des defauts capteurs IoT ---------------------------------


def traiter_pertes_reseau(df: pd.DataFrame, colonnes: list[str], limite_pas: int = 3) -> pd.DataFrame:
    df = df.copy()
    for col in colonnes:
        df[f"{col}_etait_manquant"] = df[col].isna().astype(int)
    for _, idx in df.groupby("machine_id").groups.items():
        sous_df = df.loc[idx].sort_values("timestamp")
        df.loc[sous_df.index, colonnes] = sous_df[colonnes].interpolate(
            method="linear", limit=limite_pas, limit_direction="both"
        )
    return df


def detecter_capteurs_bloques(df: pd.DataFrame, colonnes: list[str], seuil_pas_identiques: int = 8) -> pd.DataFrame:
    df = df.copy()
    for col in colonnes:
        df[f"{col}_bloque_suspect"] = 0
        for _, sous_idx in df.groupby("machine_id").groups.items():
            sous = df.loc[sous_idx].sort_values("timestamp")
            valeurs = sous[col].values
            meme_que_precedent = np.isclose(valeurs, np.roll(valeurs, 1), equal_nan=False)
            meme_que_precedent[0] = False
            run_id = (~meme_que_precedent).cumsum()
            compte_par_run = pd.Series(run_id).map(pd.Series(run_id).value_counts())
            suspect = (compte_par_run.values >= seuil_pas_identiques) & (~np.isnan(valeurs))
            df.loc[sous.index[suspect], f"{col}_bloque_suspect"] = 1
        # Valeur figee conservee telle quelle : le flag porte l'information, au
        # modele (ou a un filtrage explicite) de decider de s'appuyer dessus.
    return df


def traiter_spikes_mad(df: pd.DataFrame, colonnes: list[str], seuil_mad: float = 8.0, fenetre: int = 241) -> pd.DataFrame:
    df = df.copy()
    for col in colonnes:
        df[f"{col}_spike_suspect"] = 0
        for _, sous_idx in df.groupby("machine_id").groups.items():
            sous = df.loc[sous_idx].sort_values("timestamp")
            mediane_glissante = sous[col].rolling(fenetre, center=True, min_periods=15).median()
            ecart_abs = (sous[col] - mediane_glissante).abs()
            mad_glissant = ecart_abs.rolling(fenetre, center=True, min_periods=15).median()
            score = ecart_abs / (mad_glissant.replace(0, np.nan) * 1.4826)
            suspect = (score > seuil_mad).fillna(False)
            df.loc[sous.index[suspect.values], col] = np.nan
            df.loc[sous.index[suspect.values], f"{col}_spike_suspect"] = 1
    return df


def nettoyer_capteurs_iot(df_valide: pd.DataFrame) -> pd.DataFrame:
    """Enchaine stuck-at (capteurs bruites) -> spikes -> pertes reseau, dans cet ordre.

    Ne traite que les capteurs reellement presents : `iot_vibration_rms` par
    exemple n'est jamais publie par MQTT en production (cf.
    `gold/utils.py::SENSOR_COLUMNS`), donc absent du dataframe large assemble
    par `rul_data_assembly.py`.
    """
    capteurs_iot_presents = [c for c in CAPTEURS_IOT if c in df_valide.columns]
    capteurs_bruites_presents = [c for c in CAPTEURS_BRUITES if c in df_valide.columns]
    df_capteurs = detecter_capteurs_bloques(df_valide, capteurs_bruites_presents, seuil_pas_identiques=8)
    df_capteurs = traiter_spikes_mad(df_capteurs, capteurs_iot_presents, seuil_mad=8.0, fenetre=241)
    df_capteurs = traiter_pertes_reseau(df_capteurs, capteurs_iot_presents, limite_pas=3)
    return df_capteurs


# --- Section 4 : feature engineering --------------------------------------------------


def ajouter_temps_depuis_dernier_arret(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["jours_depuis_dernier_arret"] = np.nan
    for _, idx in df.groupby("machine_id").groups.items():
        sous = df.loc[idx].sort_values("timestamp")
        est_arret = sous["iot_statut_machine"].isin(["Maintenance", "Panne"])
        dernier_ts_arret = sous["timestamp"].where(est_arret).ffill()
        delta_jours = (sous["timestamp"] - dernier_ts_arret).dt.total_seconds() / 86400.0
        delta_jours = delta_jours.fillna(sous["age_jours"])
        df.loc[sous.index, "jours_depuis_dernier_arret"] = delta_jours.values
    return df


def ajouter_pieces_depuis_dernier_arret(df: pd.DataFrame) -> pd.DataFrame:
    if "nb_pieces_cumule" not in df.columns:
        # Pas de source de production identifiee pour nb_pieces_cumule (cf.
        # rul_data_assembly.py) : feature non calculable, on ne l'ajoute pas
        # plutot que d'injecter une colonne entierement NaN.
        return df
    df = df.copy()
    df["pieces_depuis_dernier_arret"] = np.nan
    for _, idx in df.groupby("machine_id").groups.items():
        sous = df.loc[idx].sort_values("timestamp")
        est_arret = sous["iot_statut_machine"].isin(["Maintenance", "Panne"])
        pieces_a_larret = sous["nb_pieces_cumule"].where(est_arret).ffill().fillna(0)
        df.loc[sous.index, "pieces_depuis_dernier_arret"] = (sous["nb_pieces_cumule"] - pieces_a_larret).values
    return df


def ajouter_ecarts_nominal_mesure(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["ecart_vitesse"] = df["iot_vitesse_rotation"] - df["vitesse_rotation_nominal"]
    df["ecart_courant"] = df["iot_courant_moteur"] - df["courant_moteur_nominal"]
    df["ecart_pression"] = df["iot_pression_hydraulique"] - df["pression_hydraulique_nominal"]
    df["ecart_temp"] = df["iot_temperature"] - df["temp_base_moteur"]
    return df


def ajouter_ratio_nominal_mesure(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["ecart_relatif_vitesse"] = (
        (df["iot_vitesse_rotation"] - df["vitesse_rotation_nominal"]) / df["vitesse_rotation_nominal"].replace(0, np.nan)
    )
    df["ecart_relatif_courant"] = (
        (df["iot_courant_moteur"] - df["courant_moteur_nominal"]) / df["courant_moteur_nominal"].replace(0, np.nan)
    )
    df["ecart_relatif_pression"] = (
        (df["iot_pression_hydraulique"] - df["pression_hydraulique_nominal"]) / df["pression_hydraulique_nominal"].replace(0, np.nan)
    )
    df["ecart_relatif_temp"] = (
        (df["iot_temperature"] - df["temp_base_moteur"]) / df["temp_base_moteur"].replace(0, np.nan)
    )
    return df


def ajouter_stats_glissantes(df: pd.DataFrame, colonnes: list[str], fenetre_pas: int = 60) -> pd.DataFrame:
    df = df.copy()
    for col in colonnes:
        df[f"{col}_moy_glissante"] = np.nan
        df[f"{col}_std_glissante"] = np.nan
        for _, idx in df.groupby("machine_id").groups.items():
            sous = df.loc[idx].sort_values("timestamp")
            df.loc[sous.index, f"{col}_moy_glissante"] = sous[col].rolling(fenetre_pas, min_periods=10).mean().values
            df.loc[sous.index, f"{col}_std_glissante"] = sous[col].rolling(fenetre_pas, min_periods=10).std().values
    return df


def ajouter_features_observation_operateur(df: pd.DataFrame, fenetre_heures: float = 24) -> pd.DataFrame:
    if "observation_operateur" not in df.columns:
        # Pas de source de production identifiee pour observation_operateur
        # (cf. rul_data_assembly.py) : feature non calculable.
        return df
    df = df.copy()
    df["signalement_present"] = df["observation_operateur"].notna().astype(int)
    fenetre_pas = int(fenetre_heures * 3600 / 30)
    df["signalement_recent_24h"] = 0
    for _, idx in df.groupby("machine_id").groups.items():
        sous = df.loc[idx].sort_values("timestamp")
        df.loc[sous.index, "signalement_recent_24h"] = (
            sous["signalement_present"].rolling(fenetre_pas, min_periods=1).max().values
        )
    return df


# --- Sections 1-2 : censure "presente" et filtrage des lignes -------------------------


def calculer_type_censure(df: pd.DataFrame) -> pd.DataFrame:
    """`type_censure`, calcule uniquement a partir de l'etat de `label_gmao` a l'instant present."""
    df = df.copy()
    df["type_censure"] = "Sain"
    libelle = df[COLONNE_LABEL_GMAO].astype(str)
    df.loc[libelle.str.startswith("Alerte") | libelle.str.contains("Correctif"), "type_censure"] = "Correctif"
    df.loc[libelle.str.contains("Preventif"), "type_censure"] = "Preventif"
    return df


def calculer_type_censure_futur_audit(df: pd.DataFrame) -> pd.DataFrame:
    """Trace d'audit (jamais une feature) : vers quel type d'evenement une ligne compte a rebours.

    Doit etre calculee avant tout filtrage des lignes Maintenance/Panne (cf. notebook,
    section 2) : c'est sur ces lignes que l'information est portee, avant d'etre
    remontee vers le passe par machine.
    """
    df = df.copy()
    df["type_censure_futur_audit"] = pd.Series(pd.NA, index=df.index, dtype="object")
    for _, idx in df.groupby("machine_id").groups.items():
        sous = df.loc[idx].sort_values("timestamp")
        est_evenement = sous["iot_statut_machine"].isin(EVENEMENTS_CENSURANTS)
        valeur_a_l_evenement = sous["type_censure"].where(est_evenement)
        df.loc[sous.index, "type_censure_futur_audit"] = valeur_a_l_evenement.bfill().values
    return df


def filtrer_lignes_entrainement(df: pd.DataFrame) -> pd.DataFrame:
    """Retire RUL_jours manquant + statuts Arret Operateur/Sous-Charge (section 2)."""
    df_valide = df[df[COLONNE_CIBLE].notna()].copy()
    df_valide = df_valide[~df_valide["iot_statut_machine"].isin(STATUTS_EXCLUS_ENTRAINEMENT)].copy()
    return df_valide


def construire_features(df_brut: pd.DataFrame) -> pd.DataFrame:
    """Nettoyage + feature engineering complet (sections 1 a 5 du notebook).

    `type_censure_futur_audit` est inclus dans le resultat (necessaire pour
    construire `event_observe` ensuite) mais ne doit jamais entrer dans X
    (cf. `construire_jeu_entrainement`).
    """
    df = calculer_type_censure(df_brut)
    df = calculer_type_censure_futur_audit(df)  # avant tout filtrage
    df_valide = filtrer_lignes_entrainement(df)
    df_capteurs = nettoyer_capteurs_iot(df_valide)
    df_feat = ajouter_temps_depuis_dernier_arret(df_capteurs)
    df_feat = ajouter_pieces_depuis_dernier_arret(df_feat)
    df_feat = ajouter_ecarts_nominal_mesure(df_feat)
    df_feat = ajouter_ratio_nominal_mesure(df_feat)
    capteurs_iot_presents = [c for c in CAPTEURS_IOT if c in df_feat.columns]
    df_feat = ajouter_stats_glissantes(df_feat, capteurs_iot_presents, fenetre_pas=60)
    df_feat = ajouter_features_observation_operateur(df_feat, fenetre_heures=24)
    return df_feat


# --- Section 6-8 : encodage, retrait Maintenance/Panne, cible de survie ---------------


def encoder_categorielles(df: pd.DataFrame, colonnes: list[str] = COLONNES_CATEGORIELLES) -> tuple[pd.DataFrame, dict]:
    # Certaines categorielles n'ont pas de source de production identifiee
    # (secteur, type_machine... cf. rul_data_assembly.py) : on encode celles
    # reellement presentes plutot que de planter sur les autres.
    colonnes = [c for c in colonnes if c in df.columns]
    df_encode = df.copy()
    mappings: dict[str, dict[int, object]] = {}
    for col in colonnes:
        df_encode[col] = df_encode[col].astype("category")
        mappings[col] = dict(enumerate(df_encode[col].cat.categories))
        df_encode[col] = df_encode[col].cat.codes
    return df_encode, mappings


def construire_jeu_entrainement(df_feat: pd.DataFrame) -> dict:
    """Encodage + retrait Maintenance/Panne + construction event_observe/X/y (sections 6 a 8).

    Retourne un dict : X_train, X_test, y_train, y_test (format scikit-survival),
    cible_train/timestamp_train (necessaires pour la fenetre de recence et la
    selection de features par information mutuelle), mappings (categorielles).
    """
    df_encode, mappings = encoder_categorielles(df_feat)

    inv_map_statut = {label: code for code, label in mappings["iot_statut_machine"].items()}
    codes_exclus_final = [inv_map_statut[s] for s in STATUTS_EXCLUS_FINAL if s in inv_map_statut]
    df_entrainement = df_encode[~df_encode["iot_statut_machine"].isin(codes_exclus_final)].copy()

    # event=True seulement si une panne corrective a ete reellement observee dans la
    # fenetre ; Preventif/Sain/NaN -> censure (event=False, cf. section 7).
    df_entrainement["event_observe"] = df_entrainement["type_censure_futur_audit"] == "Correctif"

    date_coupure = df_entrainement["timestamp"].quantile(0.8)
    masque_test = df_entrainement["timestamp"] >= date_coupure

    colonnes_exclues = [
        COLONNE_CIBLE,
        "timestamp",
        "type_censure_futur_audit",
        "event_observe",
    ] + COLONNES_FUITE
    colonnes_X = [c for c in df_entrainement.columns if c not in colonnes_exclues]

    X_train = df_entrainement.loc[~masque_test, colonnes_X].reset_index(drop=True)
    X_test = df_entrainement.loc[masque_test, colonnes_X].reset_index(drop=True)

    temps_train = np.round(df_entrainement.loc[~masque_test, COLONNE_CIBLE].values / GRANULARITE_JOURS) * GRANULARITE_JOURS
    temps_test = np.round(df_entrainement.loc[masque_test, COLONNE_CIBLE].values / GRANULARITE_JOURS) * GRANULARITE_JOURS

    y_train = Surv.from_arrays(event=df_entrainement.loc[~masque_test, "event_observe"].values, time=temps_train)
    y_test = Surv.from_arrays(event=df_entrainement.loc[masque_test, "event_observe"].values, time=temps_test)

    return {
        "X_train": X_train,
        "X_test": X_test,
        "y_train": y_train,
        "y_test": y_test,
        "cible_train": df_entrainement.loc[~masque_test, COLONNE_CIBLE].reset_index(drop=True),
        "timestamp_train": df_entrainement.loc[~masque_test, "timestamp"].reset_index(drop=True),
        "mappings": mappings,
    }


def appliquer_fenetre_recence(
    X_train: pd.DataFrame,
    cible_train: pd.Series,
    timestamp_train: pd.Series,
    y_train,
    fenetre_jours: float = 90,
) -> tuple[pd.DataFrame, pd.Series, "np.ndarray"]:
    """Ne garde que les lignes de train recentes (usure comparable, cf. notebook section 8bis)."""
    date_limite = timestamp_train.max() - pd.Timedelta(days=fenetre_jours)
    masque_recent = (timestamp_train >= date_limite).values
    index_recent = X_train.index[masque_recent]
    return (
        X_train.loc[index_recent].reset_index(drop=True),
        cible_train.loc[index_recent].reset_index(drop=True),
        y_train[index_recent],
    )


# --- Section 8bis-9 : selection de features, one-hot, imputation ---------------------


def selectionner_top_features(
    X_train: pd.DataFrame,
    X_test: pd.DataFrame,
    cible_train: pd.Series,
    top_k: int = TOP_K,
    colonnes_toujours_gardees: list[str] = COLONNES_TOUJOURS_GARDEES,
) -> tuple[pd.DataFrame, pd.DataFrame, list[str]]:
    X_train_fillna_mi = X_train.fillna(X_train.median())
    mi_scores = mutual_info_regression(X_train_fillna_mi, cible_train, random_state=42)
    mi_ranking = pd.Series(mi_scores, index=X_train.columns).sort_values(ascending=False)
    colonnes_gardees = list(dict.fromkeys(list(colonnes_toujours_gardees) + mi_ranking.head(top_k).index.tolist()))
    return X_train[colonnes_gardees], X_test[colonnes_gardees], colonnes_gardees


def encoder_one_hot(
    X_train: pd.DataFrame, X_test: pd.DataFrame, colonnes_categorielles: list[str] = COLONNES_CATEGORIELLES
) -> tuple[pd.DataFrame, pd.DataFrame]:
    colonnes_restantes = [c for c in colonnes_categorielles if c in X_train.columns]
    X_train = pd.get_dummies(X_train, columns=colonnes_restantes, drop_first=True)
    X_test = pd.get_dummies(X_test, columns=colonnes_restantes, drop_first=True)
    # Alignement train/test : une categorie absente d'un cote doit donner la meme
    # colonne des deux cotes, remplie a 0.
    X_test = X_test.reindex(columns=X_train.columns, fill_value=0)
    return X_train, X_test


def imputer_donnees(X_train: pd.DataFrame, X_test: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, SimpleImputer]:
    imputer = SimpleImputer(strategy="median")
    X_train_imp = pd.DataFrame(imputer.fit_transform(X_train), columns=X_train.columns, index=X_train.index)
    X_test_imp = pd.DataFrame(imputer.transform(X_test), columns=X_test.columns, index=X_test.index)
    return X_train_imp, X_test_imp, imputer


# --- Section 10 : entrainement et evaluation du modele de Cox ------------------------


def entrainer_modele_cox(X_train_imp: pd.DataFrame, y_train, alpha: float = 0.1) -> CoxPHSurvivalAnalysis:
    modele = CoxPHSurvivalAnalysis(alpha=alpha)
    modele.fit(X_train_imp, y_train)
    return modele


def evaluer_modele(
    modele: CoxPHSurvivalAnalysis,
    X_test_imp: pd.DataFrame,
    y_train,
    y_test,
    horizons_jours: tuple[float, ...] = (1, 2, 7),
    n_lignes_score: int = 20000,
) -> dict:
    """C-index (sous-echantillonne, cf. notebook : cout O(n^2)) + AUC dynamique par horizon."""
    rng = np.random.RandomState(42)
    index_score = rng.choice(len(X_test_imp), size=min(n_lignes_score, len(X_test_imp)), replace=False)

    c_index = modele.score(X_test_imp.iloc[index_score], y_test[index_score])
    risque = modele.predict(X_test_imp.iloc[index_score])
    auc_par_horizon, auc_moyen = cumulative_dynamic_auc(y_train, y_test[index_score], risque, list(horizons_jours))

    return {
        "c_index": float(c_index),
        "auc_par_horizon": dict(zip(horizons_jours, (float(a) for a in auc_par_horizon))),
        "auc_moyen": float(auc_moyen),
        "n_lignes_score": int(len(index_score)),
    }


def entrainer_pipeline_complet(
    df_feat: pd.DataFrame,
    top_k: int = TOP_K,
    fenetre_recence_jours: float = 90,
    alpha_cox: float = 0.1,
    horizons_jours: tuple[float, ...] = (1, 2, 7),
) -> dict:
    """Enchaine sections 6 a 10 : encodage -> split -> fenetre de recence -> selection
    de features -> one-hot -> imputation -> fit Cox -> evaluation.

    Retourne un bundle serialisable (joblib) : `modele`, `imputer`,
    `mappings_categories`, `colonnes_gardees` (top-k avant one-hot) et
    `colonnes_onehot` (colonnes finales de X_train apres one-hot) -- ces trois
    derniers sont necessaires pour reproduire exactement le meme encodage a
    l'inference (cf. `preparer_vecteur_inference`).
    """
    jeu = construire_jeu_entrainement(df_feat)
    X_train, cible_train, y_train = appliquer_fenetre_recence(
        jeu["X_train"], jeu["cible_train"], jeu["timestamp_train"], jeu["y_train"], fenetre_recence_jours,
    )
    X_test, y_test = jeu["X_test"], jeu["y_test"]

    X_train, X_test, colonnes_gardees = selectionner_top_features(X_train, X_test, cible_train, top_k)
    X_train, X_test = encoder_one_hot(X_train, X_test)
    X_train_imp, X_test_imp, imputer = imputer_donnees(X_train, X_test)

    modele = entrainer_modele_cox(X_train_imp, y_train, alpha=alpha_cox)
    metriques = evaluer_modele(modele, X_test_imp, y_train, y_test, horizons_jours)

    return {
        "modele": modele,
        "imputer": imputer,
        "mappings_categories": jeu["mappings"],
        "colonnes_gardees": colonnes_gardees,
        "colonnes_onehot": list(X_train.columns),
        "metriques": metriques,
    }


# --- Inference temps reel : reproduire le meme encodage sur un vecteur brut ----------


def preparer_vecteur_inference(
    features_brutes: dict,
    mappings_categories: dict,
    colonnes_gardees: list[str],
    colonnes_onehot: list[str],
) -> pd.DataFrame:
    """Applique le meme encodage qu'a l'entrainement a un vecteur de features brutes.

    `features_brutes` : dict de valeurs brutes (memes noms de colonnes que le
    dataframe d'entrainement, ex. `machine_id`, les 7 capteurs IoT...). Les
    colonnes de `colonnes_gardees` absentes de `features_brutes` (features pas
    encore reconstituees cote production, cf. TODO dans train_rul_model.py) sont
    laissees a NaN -- gerees par l'imputer sauvegarde a l'entrainement. Une
    categorie jamais vue a l'entrainement est traitee comme la categorie de
    reference (memes colonnes one-hot que `drop_first=True`, toutes a 0).
    """
    ligne = pd.DataFrame([features_brutes])

    for col, mapping in mappings_categories.items():
        if col not in ligne.columns:
            continue
        inverse = {label: code for code, label in mapping.items()}
        ligne[col] = ligne[col].map(inverse)

    for col in colonnes_gardees:
        if col not in ligne.columns:
            ligne[col] = np.nan
    ligne = ligne[colonnes_gardees]

    colonnes_categorielles_presentes = [c for c in COLONNES_CATEGORIELLES if c in ligne.columns]
    ligne = pd.get_dummies(ligne, columns=colonnes_categorielles_presentes, drop_first=True)
    ligne = ligne.reindex(columns=colonnes_onehot, fill_value=0)
    return ligne
