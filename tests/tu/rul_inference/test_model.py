import math

from rul_inference.model import _ajouter_ecarts_si_possible, _ajouter_type_censure_si_possible


def test_ajouter_ecarts_si_possible_calcule_les_ecarts_et_ratios_quand_tout_est_present() -> None:
    features = {
        "iot_vitesse_rotation": 1490.0,
        "vitesse_rotation_nominal": 1500.0,
        "iot_courant_moteur": 12.5,
        "courant_moteur_nominal": 12.0,
        "iot_pression_hydraulique": 79.0,
        "pression_hydraulique_nominal": 80.0,
        "iot_temperature": 41.0,
        "temp_base_moteur": 40.0,
    }

    enrichi = _ajouter_ecarts_si_possible(features)

    assert enrichi["ecart_vitesse"] == -10.0
    assert enrichi["ecart_relatif_vitesse"] == (1490.0 - 1500.0) / 1500.0
    assert enrichi["ecart_courant"] == 0.5
    assert enrichi["ecart_pression"] == -1.0
    assert enrichi["ecart_temp"] == 1.0
    # les valeurs d'origine restent inchangees
    assert enrichi["iot_vitesse_rotation"] == 1490.0


def test_ajouter_ecarts_si_possible_ne_plante_pas_sur_un_vecteur_partiel() -> None:
    enrichi = _ajouter_ecarts_si_possible({"iot_vitesse_rotation": 1490.0, "machine_id": "M1"})

    assert math.isnan(enrichi["ecart_vitesse"])
    assert enrichi["machine_id"] == "M1"


def test_ajouter_ecarts_si_possible_sur_dict_vide_ne_plante_pas() -> None:
    enrichi = _ajouter_ecarts_si_possible({})

    assert math.isnan(enrichi["ecart_vitesse"])


def test_ajouter_type_censure_si_possible_deduit_correctif_d_une_alerte() -> None:
    enrichi = _ajouter_type_censure_si_possible({"label_gmao": "Alerte Vibration"})

    assert enrichi["type_censure"] == "Correctif"


def test_ajouter_type_censure_si_possible_deduit_preventif_d_une_maintenance_preventive() -> None:
    enrichi = _ajouter_type_censure_si_possible({"label_gmao": "Maintenance Preventif"})

    assert enrichi["type_censure"] == "Preventif"


def test_ajouter_type_censure_si_possible_deduit_sain() -> None:
    enrichi = _ajouter_type_censure_si_possible({"label_gmao": "Sain"})

    assert enrichi["type_censure"] == "Sain"


def test_ajouter_type_censure_si_possible_sans_label_gmao_ne_plante_pas() -> None:
    enrichi = _ajouter_type_censure_si_possible({"iot_vitesse_rotation": 1000.0})

    assert "type_censure" not in enrichi
