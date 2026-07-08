from datetime import datetime, timedelta

import pandas as pd

import rul_inference.service as service

MAINTENANT = datetime(2024, 6, 1, 12, 0, 0)


def _reset_reference_cache() -> None:
    service._age_machine_by_id.clear()
    service._type_metal_by_id.clear()
    service._regime_cadence_by_id.clear()
    service._production_status_by_id.clear()
    service._type_alerte_label_by_id.clear()
    service._type_maintenance_label_by_id.clear()
    service._nominale_by_machine.clear()
    service._dernier_arret_par_machine.clear()
    service._alerte_episodes = pd.DataFrame(columns=["machine_id", "debut", "fin", "id_alerte"])
    service._maintenance_episodes = pd.DataFrame(columns=["machine_id", "debut", "fin", "id_panne"])


def test_build_contexte_machine_reconstitue_age_jours_et_valeurs_nominales() -> None:
    _reset_reference_cache()
    service._age_machine_by_id["M1"] = {
        "age_machine_jours": 100.0,
        "premier_timestamp": MAINTENANT - timedelta(days=10),
    }
    service._type_metal_by_id[1] = "Acier"
    service._regime_cadence_by_id[2] = "Normal"
    service._nominale_by_machine["M1"] = {
        "statut_nominal": "Nominal",
        "vitesse_rotation_nominal": 1500.0,
        "courant_moteur_nominal": 12.0,
        "pression_hydraulique_nominal": 80.0,
        "temp_base_moteur": 40.0,
        "id_type_metal": 1.0,
        "id_regime_cadence": 2.0,
    }

    contexte = service._build_contexte_machine("M1", MAINTENANT)

    assert 109.9 < contexte["age_jours"] < 110.1
    assert contexte["type_metal"] == "Acier"
    assert contexte["regime_cadence"] == "Normal"
    assert contexte["vitesse_rotation_nominal"] == 1500.0
    _reset_reference_cache()


def test_build_contexte_machine_renvoie_un_dict_vide_pour_une_machine_inconnue() -> None:
    _reset_reference_cache()

    assert service._build_contexte_machine("INCONNUE", MAINTENANT) == {}


def test_build_contexte_machine_sans_horodatage_ne_calcule_pas_age_jours() -> None:
    """Sans horodatage connu du flux, on ne doit pas deviner age_jours a partir
    de l'horloge du conteneur (cf. mqtt_send.py qui rejoue un flux historique)."""
    _reset_reference_cache()
    service._age_machine_by_id["M1"] = {
        "age_machine_jours": 100.0,
        "premier_timestamp": MAINTENANT - timedelta(days=10),
    }

    assert service._build_contexte_machine("M1", None) == {}
    _reset_reference_cache()


def test_decoder_contexte_plc_decode_type_metal_et_statut() -> None:
    _reset_reference_cache()
    service._type_metal_by_id[1] = "Acier"
    service._production_status_by_id[1] = "Production"

    contexte = service._decoder_contexte_plc(
        "M1", {"id_type_metal": 1.0, "id_status_production": 1.0}, MAINTENANT, age_jours=42.0
    )

    assert contexte["type_metal"] == "Acier"
    assert contexte["iot_statut_machine"] == "Production"
    # aucun arret encore observe -> repli sur age_jours (cf. rul_pipeline.py::ajouter_temps_depuis_dernier_arret)
    assert contexte["jours_depuis_dernier_arret"] == 42.0
    _reset_reference_cache()


def test_decoder_contexte_plc_recalcule_jours_depuis_dernier_arret_apres_maintenance() -> None:
    _reset_reference_cache()
    service._production_status_by_id[3] = "Maintenance"
    service._production_status_by_id[1] = "Production"

    en_maintenance = service._decoder_contexte_plc("M1", {"id_status_production": 3.0}, MAINTENANT, age_jours=42.0)
    assert en_maintenance["jours_depuis_dernier_arret"] == 0.0

    deux_jours_plus_tard = MAINTENANT + timedelta(days=2)
    apres = service._decoder_contexte_plc("M1", {"id_status_production": 1.0}, deux_jours_plus_tard, age_jours=42.0)
    assert abs(apres["jours_depuis_dernier_arret"] - 2.0) < 1e-9
    _reset_reference_cache()


def test_reconstruire_label_gmao_par_defaut_sain_et_none_sans_horodatage() -> None:
    _reset_reference_cache()

    assert service._reconstruire_label_gmao("M1", MAINTENANT) == "Sain"
    assert service._reconstruire_label_gmao("M1", None) is None
    _reset_reference_cache()


def test_reconstruire_label_gmao_detecte_un_episode_alerte_actif_et_son_expiration() -> None:
    _reset_reference_cache()
    service._type_alerte_label_by_id[10] = "Alerte Vibration"
    service._alerte_episodes = pd.DataFrame(
        [{"machine_id": "M1", "debut": MAINTENANT - timedelta(hours=1), "fin": MAINTENANT + timedelta(hours=1), "id_alerte": 10}]
    )

    assert service._reconstruire_label_gmao("M1", MAINTENANT) == "Alerte Vibration"
    assert service._reconstruire_label_gmao("M1", MAINTENANT + timedelta(hours=2)) == "Sain"
    _reset_reference_cache()


def test_reconstruire_label_gmao_priorise_alerte_sur_maintenance_et_filtre_par_machine() -> None:
    _reset_reference_cache()
    service._type_alerte_label_by_id[10] = "Alerte Vibration"
    service._type_maintenance_label_by_id[20] = "Maintenance Preventif"
    service._alerte_episodes = pd.DataFrame(
        [{"machine_id": "M1", "debut": MAINTENANT - timedelta(hours=1), "fin": MAINTENANT + timedelta(hours=1), "id_alerte": 10}]
    )
    service._maintenance_episodes = pd.DataFrame(
        [{"machine_id": "M1", "debut": MAINTENANT - timedelta(hours=1), "fin": MAINTENANT + timedelta(hours=1), "id_panne": 20}]
    )

    assert service._reconstruire_label_gmao("M1", MAINTENANT) == "Alerte Vibration"
    assert service._reconstruire_label_gmao("M2", MAINTENANT) == "Sain"
    _reset_reference_cache()
