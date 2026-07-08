"""Service d'inference RUL en temps reel, independant de mqtt_live_monitor.py.

S'abonne au meme flux MQTT (`usine/iot/#`) et reconstitue par machine le dernier
etat connu de chaque capteur : un message MQTT ne porte qu'un seul capteur (cf.
`measure_to_line` dans mqtt_live_monitor.py et `mqtt_send.py`), donc une prediction
a besoin d'accumuler les dernieres valeurs de plusieurs capteurs avant d'etre
declenchee. Toutes les INFERENCE_INTERVAL_SECONDS, le modele (cf. model.py) est
appele pour chaque machine et le resultat est ecrit dans InfluxDB (measurement
`rul_prediction`, base sensor_live).

Service volontairement separe de l'ingestion (mqtt_live_monitor.py) : un modele plus
lourd (ou une erreur d'inference) ne doit pas pouvoir ralentir/casser l'ecriture des
donnees brutes, et le pipeline CI/CD de reentrainement doit pouvoir redeployer ce
service seul, sans toucher a l'ingestion.

Le modele est aussi recharge toutes les MODEL_RELOAD_INTERVAL_SECONDS, pour
prendre en compte un nouveau modele promu par `train_rul_model.py` sans avoir
a redemarrer ce service (les deux services partagent le meme fichier via le
volume Docker `./models`, cf. docker-compose.yml).

Sur le meme cycle, les donnees de reference necessaires a `rul_inference/model.py`
pour reconstituer `age_jours` et les valeurs nominales de chaque machine (cf.
`_build_contexte_machine`) sont aussi rechargees : table Postgres `age_machine`
(age fige + date du premier releve, comme a l'entrainement cf.
`gold/rul_data_assembly.py::construire_age_jours`), lookups `type_metal`/
`regime_cadence`, et dernier releve InfluxDB `nominale_values` par machine
(cf. `_fetch_nominale_by_machine` -- une requete bornee dans le temps, pas un
`fetch_nominale_values` complet : cf. sa docstring pour le pourquoi). Ces
donnees changeant rarement, un cache rafraichi toutes les
MODEL_RELOAD_INTERVAL_SECONDS suffit -- inutile de les requeter a chaque tick
d'inference.

`label_gmao` est reconstitue par le meme biais que cote entrainement (cf.
`gold/rul_data_assembly.py::reconstruire_label_gmao`) : pas via MQTT (dedie aux
capteurs IoT), mais via les episodes `alerte`/`maintenance` d'InfluxDB (cf.
`gold/correlate_sensor_alerte.py::fetch_alerte` et
`rul_data_assembly.py::fetch_maintenance`, reutilises tels quels) et les
lookups Postgres `type_alerte`/`type_maintenance`. La liste complete des
episodes est mise en cache (rafraichie au meme rythme que le reste) ;
`_reconstruire_label_gmao` determine a chaque tick, pour l'horodatage connu de
la machine, si un episode est actif (alerte prioritaire sur maintenance, meme
regle qu'a l'entrainement).
"""

import json
import os
import signal
import threading
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import paho.mqtt.client as mqtt
import psycopg2
import requests
from dotenv import load_dotenv
from influxdb_client_3 import Point

from gold import correlate_sensor_alerte, rul_data_assembly
from rul_inference.model import CAPTEURS_IOT, load_model, predict_rul

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
load_dotenv(PROJECT_ROOT / ".env")

MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USERNAME = os.getenv("MQTT_USERNAME") or None
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD") or None
MQTT_KEEPALIVE = int(os.getenv("MQTT_KEEPALIVE", "60"))
MQTT_QOS = int(os.getenv("MQTT_QOS", "1"))
MQTT_TOPIC_PREFIX = os.getenv("MQTT_TOPIC_PREFIX", "usine/iot")

INFLUXDB_HOST = os.getenv("INFLUXDB_LIVE_HOST", "localhost")
INFLUXDB_PORT = int(os.getenv("INFLUXDB_LIVE_PORT", "8183"))
INFLUXDB_DATABASE_LIVE = os.getenv("INFLUXDB_DATABASE_LIVE", "sensor_live")
INFLUXDB_LIVE_TOKEN = os.getenv("INFLUXDB_LIVE_TOKEN", "apiv3_mspr2-live-dev-token")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5433"))
POSTGRES_USER = os.getenv("POSTGRES_USER", "user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")
POSTGRES_DB = os.getenv("POSTGRES_DB", "postgres")

INFERENCE_INTERVAL_SECONDS = float(os.getenv("INFERENCE_INTERVAL_SECONDS", "60"))
# Permet de reprendre un modele nouvellement promu (cf.
# train_rul_model.py::promouvoir_si_meilleur), et de rafraichir le cache de
# donnees de reference (cf. _reload_reference_data), sans redemarrer ce service.
MODEL_RELOAD_INTERVAL_SECONDS = float(os.getenv("MODEL_RELOAD_INTERVAL_SECONDS", "300"))

# Cles qui ne sont pas des valeurs de capteur (cf. mqtt_send.py).
META_KEYS = {"timestamp", "id_machine"}

_running = True

# Derniere valeur connue par machine/capteur : chaque message MQTT ne porte qu'un
# seul capteur, on reconstitue donc ici l'etat courant d'une machine pour pouvoir
# lancer une prediction dessus.
_latest_lock = threading.Lock()
_latest_by_machine: dict[str, dict[str, float]] = {}

# Donnees de reference pour reconstituer age_jours/valeurs nominales (cf.
# _build_contexte_machine), rafraichies par _reload_reference_data.
_reference_lock = threading.Lock()
_age_machine_by_id: dict[str, dict] = {}
_type_metal_by_id: dict[int, str] = {}
_regime_cadence_by_id: dict[int, str] = {}
_production_status_by_id: dict[int, str] = {}
_nominale_by_machine: dict[str, dict] = {}

# Episodes alerte/maintenance (colonnes normalisees : machine_id, debut, fin,
# id_alerte/id_panne) et lookups de decodage, pour reconstituer label_gmao
# (cf. _reconstruire_label_gmao) exactement comme
# gold/rul_data_assembly.py::reconstruire_label_gmao a l'entrainement.
_alerte_episodes = pd.DataFrame(columns=["machine_id", "debut", "fin", "id_alerte"])
_maintenance_episodes = pd.DataFrame(columns=["machine_id", "debut", "fin", "id_panne"])
_type_alerte_label_by_id: dict[int, str] = {}
_type_maintenance_label_by_id: dict[int, str] = {}

# Lectures PLC publiees sur le meme flux MQTT que les capteurs IoT (cf.
# gold/rul_data_assembly.py::COLONNES_PLC) : machine_id/id_status_production
# arrivent comme n'importe quel "capteur", a decoder via _production_status_by_id
# / _type_metal_by_id une fois recus (cf. _decoder_contexte_plc).
CAPTEURS_ET_PLC = set(CAPTEURS_IOT) | set(rul_data_assembly.COLONNES_PLC)

# Horodatage (cf. mqtt_send.py : celui du jeu de donnees rejoue, pas l'heure
# d'ingestion) et dernier arret (Maintenance/Panne) connus par machine --
# necessaires pour recalculer age_jours et jours_depuis_dernier_arret comme a
# l'entrainement (cf. rul_pipeline.py::ajouter_temps_depuis_dernier_arret),
# sans dependre de l'heure "reelle" du conteneur qui peut diverger de celle du
# flux rejoue.
_latest_timestamp_by_machine: dict[str, datetime] = {}
_dernier_arret_par_machine: dict[str, datetime] = {}


def _stop(signum, frame):
    global _running
    _running = False
    print("\nArret demande, fermeture...", flush=True)


def _on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        topic = f"{MQTT_TOPIC_PREFIX}/#"
        client.subscribe(topic, qos=MQTT_QOS)
        print(f"Connecte au broker mqtt://{MQTT_HOST}:{MQTT_PORT}, abonne a {topic}", flush=True)
    else:
        print(f"Echec de connexion au broker : {reason_code}", flush=True)


def _on_disconnect(client, userdata, disconnect_flags, reason_code, properties):
    # reason_code != 0 => deconnexion non sollicitee (broker coupe, reseau...).
    # loop_start() relance automatiquement les tentatives de reconnexion, et
    # _on_connect resouscrit une fois la connexion retablie.
    print(
        f"Deconnecte du broker (rc={reason_code}) ; reconnexion en cours...",
        flush=True,
    )


def build_mqtt_client() -> mqtt.Client:
    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id="rul-inference-service",
    )
    if MQTT_USERNAME:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    client.on_connect = _on_connect
    client.on_disconnect = _on_disconnect
    # Reconnexion automatique avec backoff exponentiel entre 1 et 30 s.
    client.reconnect_delay_set(min_delay=1, max_delay=30)
    client.connect_async(MQTT_HOST, MQTT_PORT, MQTT_KEEPALIVE)
    return client


def _connect_postgres():
    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        dbname=POSTGRES_DB,
    )


def _fetch_age_machine() -> dict[str, dict]:
    """Table Postgres `age_machine` (age fige au 1er releve + date de ce releve).

    Meme table/logique que `gold/rul_data_assembly.py::construire_age_jours` :
    `age_jours` a un instant donne = age_machine_jours + jours ecoules depuis
    premier_timestamp.
    """
    conn = _connect_postgres()
    try:
        df = pd.read_sql("SELECT id_machine, age_machine_jours, premier_timestamp FROM age_machine", conn)
    finally:
        conn.close()
    df["premier_timestamp"] = pd.to_datetime(df["premier_timestamp"])
    return {
        str(row.id_machine): {
            "age_machine_jours": float(row.age_machine_jours),
            "premier_timestamp": row.premier_timestamp,
        }
        for row in df.itertuples()
    }


def _fetch_label_lookup(table: str, label_column: str) -> dict[int, str]:
    """Lookup Postgres id -> label (cf. gold/rul_data_assembly.py::DECODAGE_PLC)."""
    conn = _connect_postgres()
    try:
        df = pd.read_sql(f"SELECT id, {label_column} FROM {table}", conn)
    finally:
        conn.close()
    return dict(zip(df["id"].astype(int), df[label_column]))


NOMINALE_VALUES_LOOKBACK_DAYS = float(os.getenv("NOMINALE_VALUES_LOOKBACK_DAYS", "7"))


def _fetch_nominale_by_machine(session: requests.Session, base_url: str) -> dict[str, dict]:
    """Dernier releve InfluxDB `nominale_values` connu pour chaque machine.

    Filtre sur une fenetre temporelle recente (`NOMINALE_VALUES_LOOKBACK_DAYS`)
    plutot que :
    - `rul_data_assembly.fetch_nominale_values` (`SELECT * FROM nominale_values`
      complet, utilise a l'entrainement ou tout l'historique est reellement
      necessaire pour le merge_asof) : sur un volume de reference charge en
      bloc (cf. `gold/split_cold_storage.py`), le tri pandas qui suivait a
      fait planter InfluxDB en pratique
      (`ResourcesExhausted("Memory Exhausted while Sorting")`, DataFusion ne
      peut pas deborder sur disque) ;
    - un filtre par tag `machine_id` seul (teste en pratique : ~4-8s et une
      pression memoire croissante par machine, cf. warning InfluxDB "dedup
      fanout too wide" -- les petits fragments issus d'un chargement en bloc
      melangent toutes les machines, un filtre par tag ne permet donc pas de
      les ecarter).
    Le filtre temporel, lui, permet a InfluxDB d'ecarter la quasi-totalite
    des fragments par leurs statistiques min/max de colonne (teste : <1s sur
    une fenetre de plusieurs jours, contre plusieurs secondes -- voire un
    crash -- sans lui).

    Bornee aussi en haut (`time <= now()`) : sans cette borne, un jeu de
    donnees charge en bloc et couvrant une periode "future" (cf.
    `gold/split_cold_storage.py` -- seule la partie posterieure au cutoff est
    rejouee en direct, mais `nominale_values` est charge en une fois, dans
    son integralite) ferait remonter le tout dernier releve connu -- date
    dans le futur -- plutot que le plus recent reellement "connu" a l'instant
    present.
    """
    query = (
        "SELECT * FROM nominale_values "
        f"WHERE time >= now() - INTERVAL '{NOMINALE_VALUES_LOOKBACK_DAYS} days' "
        "AND time <= now() "
        "ORDER BY time"
    )
    response = session.post(
        f"{base_url}/api/v3/query_sql",
        json={"db": INFLUXDB_DATABASE_LIVE, "q": query},
    )
    if response.status_code == 400 and "not found" in response.text:
        return {}
    response.raise_for_status()
    df = pd.DataFrame.from_records(response.json())
    if df.empty:
        return {}
    latest = df.groupby("machine_id").last()
    return latest.to_dict(orient="index")


def _fetch_alerte_episodes(session: requests.Session, base_url: str) -> pd.DataFrame:
    """Episodes `alerte` InfluxDB (cf. gold/correlate_sensor_alerte.py::fetch_alerte,
    reutilise tel quel), colonnes normalisees comme _maintenance_episodes."""
    df = correlate_sensor_alerte.fetch_alerte(session, base_url)
    return df.rename(columns={"id_machine": "machine_id", "debut_alerte": "debut", "fin_alerte": "fin"}).assign(
        machine_id=lambda d: d["machine_id"].astype(str)
    )


def _fetch_maintenance_episodes(session: requests.Session, base_url: str) -> pd.DataFrame:
    """Episodes `maintenance` InfluxDB (cf. gold/rul_data_assembly.py::fetch_maintenance,
    reutilise tel quel), colonnes normalisees comme _alerte_episodes."""
    df = rul_data_assembly.fetch_maintenance(session, base_url, INFLUXDB_DATABASE_LIVE)
    return df.rename(columns={"debut_panne": "debut", "fin_panne": "fin"}).assign(
        machine_id=lambda d: d["machine_id"].astype(str)
    )


def _reload_reference_data(session: requests.Session, base_url: str) -> None:
    """Rafraichit le cache de donnees de reference (cf. module docstring).

    Un echec (Postgres/InfluxDB temporairement indisponible) ne doit pas faire
    planter le service : le cache precedent reste utilise jusqu'au prochain
    cycle.
    """
    try:
        age_machine = _fetch_age_machine()
        type_metal = _fetch_label_lookup("type_metal", "type_metal")
        regime_cadence = _fetch_label_lookup("regime_cadence", "regime_cadence")
        production_status = _fetch_label_lookup("production_status", "iot_statut_machine")
        type_alerte = _fetch_label_lookup("type_alerte", "label_gmao")
        type_maintenance = _fetch_label_lookup("type_maintenance", "label_gmao")
        nominale = _fetch_nominale_by_machine(session, base_url)
        alerte_episodes = _fetch_alerte_episodes(session, base_url)
        maintenance_episodes = _fetch_maintenance_episodes(session, base_url)
    except Exception as exc:  # noqa: BLE001 - cache precedent conserve, pas d'arret du service
        print(f"[rul_inference] Echec du rechargement des donnees de reference : {exc!r}", flush=True)
        return

    global _alerte_episodes, _maintenance_episodes
    with _reference_lock:
        _age_machine_by_id.clear()
        _age_machine_by_id.update(age_machine)
        _type_metal_by_id.clear()
        _type_metal_by_id.update(type_metal)
        _regime_cadence_by_id.clear()
        _regime_cadence_by_id.update(regime_cadence)
        _production_status_by_id.clear()
        _production_status_by_id.update(production_status)
        _type_alerte_label_by_id.clear()
        _type_alerte_label_by_id.update(type_alerte)
        _type_maintenance_label_by_id.clear()
        _type_maintenance_label_by_id.update(type_maintenance)
        _nominale_by_machine.clear()
        _nominale_by_machine.update(nominale)
        _alerte_episodes = alerte_episodes
        _maintenance_episodes = maintenance_episodes
    print(
        f"[rul_inference] Donnees de reference rechargees : {len(age_machine)} ages machine, "
        f"{len(nominale)} valeurs nominales, {len(alerte_episodes)} episodes alerte, "
        f"{len(maintenance_episodes)} episodes maintenance.",
        flush=True,
    )


def _build_contexte_machine(id_machine: str, horodatage: datetime | None) -> dict[str, float | str]:
    """Reconstitue `age_jours` et les valeurs nominales d'une machine depuis le
    cache de reference (cf. _reload_reference_data), pour completer les
    capteurs bruts avant d'appeler `predict_rul`.

    `horodatage` : dernier timestamp connu du flux MQTT pour cette machine (cf.
    `_latest_timestamp_by_machine`) -- celui du jeu de donnees rejoue par
    `mqtt_send.py`, pas l'heure "reelle". A l'entrainement, `age_jours` est
    calcule contre ce meme timestamp (cf.
    `gold/rul_data_assembly.py::construire_age_jours`) : utiliser l'heure du
    conteneur ici donnerait un age incoherent si le flux rejoue des donnees
    passees ou futures par rapport a l'horloge reelle.
    """
    with _reference_lock:
        age = _age_machine_by_id.get(id_machine)
        nominale = _nominale_by_machine.get(id_machine)
        type_metal_by_id = _type_metal_by_id
        regime_cadence_by_id = _regime_cadence_by_id

    contexte: dict[str, float | str] = {}
    if age is not None and horodatage is not None:
        delta_jours = (horodatage - age["premier_timestamp"]).total_seconds() / 86400.0
        contexte["age_jours"] = age["age_machine_jours"] + delta_jours

    if nominale is not None:
        for champ in (
            "statut_nominal",
            "vitesse_rotation_nominal",
            "courant_moteur_nominal",
            "pression_hydraulique_nominal",
            "temp_base_moteur",
        ):
            valeur = nominale.get(champ)
            if valeur is not None:
                contexte[champ] = valeur

        id_type_metal = nominale.get("id_type_metal")
        if id_type_metal is not None:
            label = type_metal_by_id.get(int(round(id_type_metal)))
            if label is not None:
                contexte["type_metal"] = label

        id_regime_cadence = nominale.get("id_regime_cadence")
        if id_regime_cadence is not None:
            label = regime_cadence_by_id.get(int(round(id_regime_cadence)))
            if label is not None:
                contexte["regime_cadence"] = label

    return contexte


def _decoder_episode_actif(
    episodes: pd.DataFrame, id_machine: str, horodatage: datetime, colonne_id: str, lookup: dict[int, str]
) -> str | None:
    """Label de l'episode (alerte ou maintenance, colonnes normalisees par
    _fetch_alerte_episodes/_fetch_maintenance_episodes) le plus recent pour
    cette machine dont le debut precede `horodatage`, s'il est toujours actif
    (`horodatage < fin`) -- meme logique que
    gold/rul_data_assembly.py::reconstruire_label_gmao, pour une seule machine
    et un seul instant plutot qu'un merge_asof vectorise sur tout un dataframe.
    """
    if episodes.empty:
        return None
    sous = episodes[(episodes["machine_id"] == id_machine) & (episodes["debut"] <= horodatage)]
    if sous.empty:
        return None
    dernier = sous.loc[sous["debut"].idxmax()]
    if horodatage >= dernier["fin"]:
        return None
    return lookup.get(int(dernier[colonne_id]))


def _reconstruire_label_gmao(id_machine: str, horodatage: datetime | None) -> str | None:
    """Reconstitue `label_gmao` pour une machine a un instant donne : "Sain" par
    defaut, ecrase par un episode alerte ou maintenance actif (alerte
    prioritaire, meme regle qu'a l'entrainement, cf.
    `gold/rul_data_assembly.py::reconstruire_label_gmao`).

    Retourne None si `horodatage` est inconnu (pas encore de message recu pour
    cette machine) : mieux vaut laisser `label_gmao` absent (impute) que de
    deviner "Sain" par defaut sans reference temporelle.
    """
    if horodatage is None:
        return None

    with _reference_lock:
        alerte_episodes = _alerte_episodes
        maintenance_episodes = _maintenance_episodes
        type_alerte_label_by_id = _type_alerte_label_by_id
        type_maintenance_label_by_id = _type_maintenance_label_by_id

    label = _decoder_episode_actif(alerte_episodes, id_machine, horodatage, "id_alerte", type_alerte_label_by_id)
    if label is not None:
        return label

    label = _decoder_episode_actif(
        maintenance_episodes, id_machine, horodatage, "id_panne", type_maintenance_label_by_id
    )
    if label is not None:
        return label

    return "Sain"


def _on_message(client, userdata, msg):
    try:
        measure = json.loads(msg.payload.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        print(f"Message illisible sur {msg.topic} : {exc}", flush=True)
        return
    id_machine = measure.get("id_machine", "unknown")
    sensor = next((k for k in measure if k not in META_KEYS), None)
    if sensor is None or sensor not in CAPTEURS_ET_PLC:
        return
    horodatage = measure.get("timestamp")
    with _latest_lock:
        _latest_by_machine.setdefault(id_machine, {})[sensor] = float(measure[sensor])
        if horodatage is not None:
            _latest_timestamp_by_machine[id_machine] = pd.to_datetime(horodatage).to_pydatetime().replace(tzinfo=None)


def _decoder_contexte_plc(
    id_machine: str, valeurs: dict, horodatage: datetime | None, age_jours: float | None
) -> dict[str, float | str]:
    """Decode les lectures PLC brutes (`id_type_metal`/`id_status_production`, cf.
    `gold/rul_data_assembly.py::DECODAGE_PLC`) recues sur le meme flux MQTT que
    les capteurs IoT, et calcule `jours_depuis_dernier_arret` (meme logique
    qu'a l'entrainement, cf. `rul_pipeline.py::ajouter_temps_depuis_dernier_arret` :
    temps ecoule depuis le dernier statut Maintenance/Panne observe pour cette
    machine, ou `age_jours` si aucun arret n'a encore ete vu).
    """
    with _reference_lock:
        type_metal_by_id = _type_metal_by_id
        production_status_by_id = _production_status_by_id

    resultat: dict[str, float | str] = {}

    id_type_metal = valeurs.get("id_type_metal")
    if id_type_metal is not None:
        label = type_metal_by_id.get(int(round(id_type_metal)))
        if label is not None:
            resultat["type_metal"] = label

    statut = None
    id_status_production = valeurs.get("id_status_production")
    if id_status_production is not None:
        statut = production_status_by_id.get(int(round(id_status_production)))
        if statut is not None:
            resultat["iot_statut_machine"] = statut

    if horodatage is not None:
        with _latest_lock:
            if statut in ("Maintenance", "Panne"):
                _dernier_arret_par_machine[id_machine] = horodatage
            dernier_arret = _dernier_arret_par_machine.get(id_machine)
        if dernier_arret is not None:
            resultat["jours_depuis_dernier_arret"] = (horodatage - dernier_arret).total_seconds() / 86400.0
        elif age_jours is not None:
            resultat["jours_depuis_dernier_arret"] = age_jours

    return resultat


def run_inference_tick(session: requests.Session, write_url: str) -> None:
    with _latest_lock:
        snapshot = {id_machine: dict(valeurs) for id_machine, valeurs in _latest_by_machine.items()}
        horodatages = dict(_latest_timestamp_by_machine)

    if not snapshot:
        return

    lines = []
    for id_machine, valeurs in snapshot.items():
        horodatage = horodatages.get(id_machine)
        contexte = _build_contexte_machine(id_machine, horodatage)
        contexte_plc = _decoder_contexte_plc(id_machine, valeurs, horodatage, contexte.get("age_jours"))
        capteurs = {k: v for k, v in valeurs.items() if k in CAPTEURS_IOT}
        features = {**contexte, **contexte_plc, **capteurs}
        label_gmao = _reconstruire_label_gmao(id_machine, horodatage)
        if label_gmao is not None:
            features["label_gmao"] = label_gmao
        prediction = predict_rul(id_machine, features)
        point = (
            Point("rul_prediction")
            .tag("id_machine", id_machine)
            .field("rul_jours_estime", prediction["rul_jours_estime"])
            .field("risque", prediction["risque"])
            .time(time.time_ns(), write_precision="ns")
        )
        lines.append(point.to_line_protocol())
        print(f"[inference] {id_machine} -> {prediction}", flush=True)

    response = session.post(
        write_url,
        params={"db": INFLUXDB_DATABASE_LIVE},
        data="\n".join(lines).encode("utf-8"),
    )
    response.raise_for_status()
    print(f"[inference] {len(lines)} predictions ecrites", flush=True)


def main() -> None:
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    base_url = f"http://{INFLUXDB_HOST}:{INFLUXDB_PORT}"
    write_url = f"{base_url}/api/v3/write_lp"
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {INFLUXDB_LIVE_TOKEN}"})

    load_model()
    _reload_reference_data(session, base_url)

    mqtt_client = build_mqtt_client()
    mqtt_client.on_message = _on_message
    mqtt_client.loop_start()

    print(
        f"Service d'inference RUL demarre -> base InfluxDB {INFLUXDB_DATABASE_LIVE} "
        f"(inference toutes les {INFERENCE_INTERVAL_SECONDS}s)",
        flush=True,
    )
    try:
        waited_inference = 0.0
        waited_reload = 0.0
        while _running:
            time.sleep(0.5)
            waited_inference += 0.5
            waited_reload += 0.5
            if waited_inference >= INFERENCE_INTERVAL_SECONDS:
                run_inference_tick(session, write_url)
                waited_inference = 0.0
            if waited_reload >= MODEL_RELOAD_INTERVAL_SECONDS:
                load_model()
                _reload_reference_data(session, base_url)
                waited_reload = 0.0
    finally:
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
        session.close()
        print("Deconnecte.", flush=True)


if __name__ == "__main__":
    main()
