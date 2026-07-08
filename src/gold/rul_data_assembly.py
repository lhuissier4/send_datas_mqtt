"""Assemble les donnees de production en un dataframe large compatible avec
`rul_pipeline.construire_features` (format attendu : une ligne par
`(machine_id, timestamp)`).

Sources reelles utilisees (aucune n'est un oracle, contrairement au notebook) :
- `sensor_data` (Parquet, deja correle avec `alerte` par
  `correlate_sensor_alerte.py`) : format long, une ligne par capteur *ou*
  lecture PLC -- `id_type_metal`/`id_status_production` transitent sur le
  meme flux MQTT que les capteurs IoT (cf. `gold/utils.py::PLC_COLUMNS`,
  publies comme n'importe quelle mesure par `mqtt_send.py`), donc `sensor_data`
  les contient deja, indistinguables des capteurs sans decodage explicite.
- InfluxDB `sensor_live`, mesures `maintenance` (episodes de panne/preventif,
  cf. `load_maintenance.py`) et `nominale_values` (valeurs nominales par
  machine, cf. `load_nominal_values.py`).
- Tables de reference Postgres : `type_metal`, `production_status`,
  `regime_cadence`, `type_alerte`, `type_maintenance`, `age_machine`.

Cible de survie : le notebook lit `RUL_jours` directement depuis le
simulateur (oracle). En production cet oracle n'existe pas -- le temps de
survie est donc recalcule ici a partir des vrais episodes `maintenance`
(cf. `construire_cible_survie_depuis_episodes`) : pour chaque ligne, le temps
jusqu'au prochain arret reellement observe de la machine (ou, si aucun arret
futur n'est encore connu, une censure a la derniere donnee disponible), et
`event=True` uniquement si cet arret est un Correctif reel.

Colonnes du notebook toujours sans source de production identifiee, et donc
absentes du dataframe retourne : `secteur`, `type_machine` (pas de table
`machine`), `nb_pieces_cumule`, `nb_pieces_intervalle`, `observation_operateur`,
et `iot_vibration_rms` (jamais publie par MQTT, cf.
`gold/utils.py::SENSOR_COLUMNS`). `rul_pipeline` est tolerant a leur absence
(cf. les gardes ajoutees dans `encoder_categorielles`,
`ajouter_pieces_depuis_dernier_arret` et
`ajouter_features_observation_operateur`).
"""

import numpy as np
import pandas as pd

# Lectures PLC publiees sur le meme flux MQTT que les capteurs IoT (cf.
# gold/utils.py::PLC_COLUMNS) : presentes dans sensor_data comme n'importe
# quel "sensor", a decoder via les tables de reference Postgres.
COLONNES_PLC = ["id_type_metal", "id_status_production"]

# Nom de la colonne Postgres associee a l'id apres decodage (table -> colonne).
DECODAGE_PLC = {
    "id_type_metal": ("type_metal", "type_metal"),
    "id_status_production": ("production_status", "iot_statut_machine"),
}


def pivot_sensor_readings_to_wide(sensor_df: pd.DataFrame) -> pd.DataFrame:
    """Reconstitue une ligne par (machine_id, timestamp) avec un capteur/PLC par colonne.

    Le pivot est direct (pas d'alignement flou par tolerance temporelle) :
    `timestamp` (renomme depuis `sensor_timestamp`, cf. `correlate_sensor_alerte.py`)
    est la donnee du simulateur reproduite a l'identique sur toutes les
    mesures d'un meme "tick" MQTT (cf. `mqtt_send.py`/`record_future_send_in_jsonl`),
    donc un simple `pivot_table` regroupe correctement les mesures d'une meme
    ligne logique sans risque de decalage.
    """
    colonnes_a_garder = [c for c in sensor_df.columns if c not in ("sensor", "value")]

    wide = sensor_df.pivot_table(
        index=["id_machine", "timestamp"],
        columns="sensor",
        values="value",
        aggfunc="first",
    ).reset_index()
    wide.columns.name = None

    # Colonnes portees par chaque ligne brute (ex. id_alerte, deja correle en
    # amont) mais pas "meltees" en sensor/value : constantes par tick, on les
    # recupere via un simple premier releve par (machine, timestamp).
    colonnes_constantes = [c for c in colonnes_a_garder if c not in ("id_machine", "timestamp")]
    if colonnes_constantes:
        constantes = sensor_df.groupby(["id_machine", "timestamp"])[colonnes_constantes].first().reset_index()
        wide = wide.merge(constantes, on=["id_machine", "timestamp"], how="left")

    return wide.rename(columns={"id_machine": "machine_id"}).sort_values(["machine_id", "timestamp"]).reset_index(drop=True)


def decoder_lectures_plc(df_wide: pd.DataFrame, postgres_lookups: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Decode `id_type_metal`/`id_status_production` (lectures PLC) via les tables Postgres.

    Remplace chaque colonne d'id par la colonne de label correspondante
    (`type_metal`, `iot_statut_machine`), attendue telle quelle par
    `rul_pipeline`.
    """
    df = df_wide.copy()
    for colonne_id, (table, colonne_label) in DECODAGE_PLC.items():
        if colonne_id not in df.columns:
            continue
        lookup = postgres_lookups[table][[colonne_label, "id"]].rename(columns={"id": colonne_id})
        df[colonne_id] = df[colonne_id].round().astype("Int64")
        lookup[colonne_id] = lookup[colonne_id].astype("Int64")
        df = df.merge(lookup, on=colonne_id, how="left").drop(columns=[colonne_id])
    return df


def fetch_maintenance(session, base_url: str, database: str) -> pd.DataFrame:
    """Episodes de maintenance/panne (mesure InfluxDB `maintenance`, cf. load_maintenance.py).

    Le tag InfluxDB s'appelle `id_machine` (cf. load_maintenance.py::TAG_COLUMNS) ;
    renomme ici en `machine_id` pour rester coherent avec le reste de ce module.
    """
    response = session.post(
        f"{base_url}/api/v3/query_sql",
        json={"db": database, "q": "SELECT * FROM maintenance ORDER BY time"},
    )
    if response.status_code == 400 and "not found" in response.text:
        return pd.DataFrame(columns=["debut_panne", "fin_panne", "machine_id", "id_panne"])
    response.raise_for_status()
    df = pd.DataFrame.from_records(response.json())
    if df.empty:
        return pd.DataFrame(columns=["debut_panne", "fin_panne", "machine_id", "id_panne"])
    df = df.rename(columns={"time": "debut_panne", "id_machine": "machine_id"})
    df["debut_panne"] = pd.to_datetime(df["debut_panne"]).dt.tz_localize(None)
    df["fin_panne"] = pd.to_datetime(df["fin_panne"], unit="ns")
    df["id_panne"] = df["id_panne"].astype(int)
    return df.sort_values("debut_panne").reset_index(drop=True)


def fetch_nominale_values(session, base_url: str, database: str) -> pd.DataFrame:
    """Valeurs nominales par machine (mesure InfluxDB `nominale_values`, cf. load_nominal_values.py)."""
    response = session.post(
        f"{base_url}/api/v3/query_sql",
        json={"db": database, "q": "SELECT * FROM nominale_values ORDER BY time"},
    )
    if response.status_code == 400 and "not found" in response.text:
        return pd.DataFrame(columns=["timestamp", "machine_id"])
    response.raise_for_status()
    df = pd.DataFrame.from_records(response.json())
    if df.empty:
        return pd.DataFrame(columns=["timestamp", "machine_id"])
    df = df.rename(columns={"time": "timestamp"})
    df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.tz_localize(None)
    return df.sort_values("timestamp").reset_index(drop=True)


def merger_valeurs_nominales(
    df_wide: pd.DataFrame, nominale_df: pd.DataFrame, postgres_lookups: dict[str, pd.DataFrame]
) -> pd.DataFrame:
    """Rattache a chaque ligne les dernieres valeurs nominales connues de sa machine (merge_asof)."""
    if nominale_df.empty:
        return df_wide

    df = df_wide.assign(machine_id=df_wide["machine_id"].astype(str), timestamp=df_wide["timestamp"].astype("datetime64[ns]"))
    nominale = nominale_df.assign(
        machine_id=nominale_df["machine_id"].astype(str), timestamp=nominale_df["timestamp"].astype("datetime64[ns]")
    )

    if "id_regime_cadence" in nominale.columns:
        lookup = postgres_lookups["regime_cadence"][["regime_cadence", "id"]].rename(columns={"id": "id_regime_cadence"})
        nominale["id_regime_cadence"] = nominale["id_regime_cadence"].astype(float).round().astype("Int64")
        lookup["id_regime_cadence"] = lookup["id_regime_cadence"].astype("Int64")
        nominale = nominale.merge(lookup, on="id_regime_cadence", how="left").drop(columns=["id_regime_cadence"])

    colonnes_a_fusionner = [c for c in nominale.columns if c not in ("machine_id", "timestamp", "id_type_metal")]

    return pd.merge_asof(
        df.sort_values("timestamp"),
        nominale.sort_values("timestamp")[["timestamp", "machine_id", *colonnes_a_fusionner]],
        on="timestamp",
        by="machine_id",
        direction="backward",
    )


def construire_age_jours(df_wide: pd.DataFrame, age_machine_lookup: pd.DataFrame) -> pd.DataFrame:
    """Calcule `age_jours` a partir de la table Postgres `age_machine` (age fige au 1er releve)."""
    df = df_wide.assign(machine_id=df_wide["machine_id"].astype(str))
    age_machine = age_machine_lookup.assign(id_machine=age_machine_lookup["id_machine"].astype(str))
    age_machine = age_machine.rename(columns={"id_machine": "machine_id"})
    age_machine["premier_timestamp"] = pd.to_datetime(age_machine["premier_timestamp"])

    df = df.merge(age_machine[["machine_id", "age_machine_jours", "premier_timestamp"]], on="machine_id", how="left")
    delta_jours = (df["timestamp"] - df["premier_timestamp"]).dt.total_seconds() / 86400.0
    df["age_jours"] = df["age_machine_jours"].astype(float) + delta_jours
    return df.drop(columns=["age_machine_jours", "premier_timestamp"])


def reconstruire_label_gmao(
    df_wide: pd.DataFrame,
    maintenance_df: pd.DataFrame,
    postgres_lookups: dict[str, pd.DataFrame],
) -> pd.DataFrame:
    """Reconstruit `label_gmao` ("Sain" par defaut) a partir des episodes alerte/maintenance actifs.

    Cote alerte, reutilise `id_alerte` (deja calcule par
    `correlate_sensor_alerte.py::correlate`, NA si aucune alerte active a
    cette ligne) plutot que de refaire le meme merge_asof. Cote maintenance
    (pas encore correle en amont), meme logique que `correlate()` : episode le
    plus recent dont le debut precede la ligne, ignore si deja termine. Les
    deux sont ensuite decodes via `type_alerte`/`type_maintenance`.
    """
    df = df_wide.assign(machine_id=df_wide["machine_id"].astype(str), timestamp=df_wide["timestamp"].astype("datetime64[ns]"))
    df["label_gmao"] = "Sain"

    if "id_alerte" in df.columns:
        lookup_alerte = postgres_lookups["type_alerte"][["label_gmao", "id"]].rename(
            columns={"id": "id_alerte", "label_gmao": "label_alerte"}
        )
        df["id_alerte"] = df["id_alerte"].astype(float).round().astype("Int64")
        lookup_alerte["id_alerte"] = lookup_alerte["id_alerte"].astype("Int64")
        df = df.merge(lookup_alerte, on="id_alerte", how="left")
        alerte_active = df["label_alerte"].notna()
        df.loc[alerte_active, "label_gmao"] = df.loc[alerte_active, "label_alerte"]
        # id_alerte n'est qu'un artefact transitoire pour reconstruire label_gmao
        # : son information est deja capturee par label_gmao/type_censure, il ne
        # doit pas devenir une feature numerique (id arbitraire, souvent NaN).
        df = df.drop(columns=["label_alerte", "id_alerte"])

    if not maintenance_df.empty:
        episodes = maintenance_df.assign(
            machine_id=maintenance_df["machine_id"].astype(str),
            debut_panne=maintenance_df["debut_panne"].astype("datetime64[ns]"),
            fin_panne=maintenance_df["fin_panne"].astype("datetime64[ns]"),
        )
        merged = pd.merge_asof(
            df.sort_values("timestamp"),
            episodes[["debut_panne", "fin_panne", "machine_id", "id_panne"]],
            left_on="timestamp",
            right_on="debut_panne",
            by="machine_id",
            direction="backward",
        )
        maintenance_active = (merged["timestamp"] < merged["fin_panne"]).values
        lookup_maintenance = postgres_lookups["type_maintenance"][["label_gmao", "id"]].rename(
            columns={"id": "id_panne", "label_gmao": "label_maintenance"}
        )
        merged = merged.merge(lookup_maintenance, on="id_panne", how="left")
        # Priorite a l'alerte deja affectee ci-dessus (elle correspond a un
        # ticket GMAO deja ouvert, information plus specifique qu'un simple
        # statut machine en Maintenance/Panne).
        toujours_sain = (df["label_gmao"] == "Sain").values
        a_ecraser = maintenance_active & toujours_sain
        df.loc[a_ecraser, "label_gmao"] = merged.loc[a_ecraser, "label_maintenance"].values

    return df


def construire_cible_survie_depuis_episodes(df_wide: pd.DataFrame, maintenance_df: pd.DataFrame) -> pd.DataFrame:
    """Reconstruit `RUL_jours` (temps de survie) depuis les vrais episodes de maintenance/panne.

    Pas d'oracle en production (contrairement au notebook) : pour chaque
    ligne, temps = duree jusqu'au prochain arret reellement observe de cette
    machine (`type_censure_futur_audit` deduit du type d'evenement -- pas de
    distinction Correctif/Preventif fiable ici sans le label complet, cf.
    reconstruire_label_gmao qui s'en charge deja pour le feature `label_gmao` ;
    on se contente ici de l'evenement 'maintenance' generique -> le
    `event_observe` final est recalcule par `rul_pipeline` a partir de
    `type_censure_futur_audit`, lui-meme derive de `label_gmao`). Si aucun
    arret futur n'est encore connu (censure), le temps est la duree jusqu'a la
    derniere donnee disponible pour cette machine.
    """
    df = df_wide.assign(machine_id=df_wide["machine_id"].astype(str), timestamp=df_wide["timestamp"].astype("datetime64[ns]"))

    if maintenance_df.empty:
        df["RUL_jours"] = np.nan
        return df

    episodes = maintenance_df.assign(
        machine_id=maintenance_df["machine_id"].astype(str),
        debut_panne=maintenance_df["debut_panne"].astype("datetime64[ns]"),
    ).sort_values("debut_panne")

    prochain = pd.merge_asof(
        df.sort_values("timestamp"),
        episodes[["debut_panne", "machine_id"]],
        left_on="timestamp",
        right_on="debut_panne",
        by="machine_id",
        direction="forward",
    )
    dernier_ts_connu = df.groupby("machine_id")["timestamp"].transform("max")

    a_un_arret_futur = prochain["debut_panne"].notna().values
    duree_jusqu_a_larret = (prochain["debut_panne"] - prochain["timestamp"]).dt.total_seconds() / 86400.0
    duree_censuree = (dernier_ts_connu - df["timestamp"]).dt.total_seconds() / 86400.0

    df["RUL_jours"] = np.where(a_un_arret_futur, duree_jusqu_a_larret.values, duree_censuree.values)
    return df
