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
"""

import os
import json
import signal
import threading
import time
from pathlib import Path

import paho.mqtt.client as mqtt
import requests
from dotenv import load_dotenv
from influxdb_client_3 import Point

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

INFERENCE_INTERVAL_SECONDS = float(os.getenv("INFERENCE_INTERVAL_SECONDS", "60"))
# Permet de reprendre un modele nouvellement promu (cf.
# train_rul_model.py::promouvoir_si_meilleur) sans redemarrer ce service.
MODEL_RELOAD_INTERVAL_SECONDS = float(os.getenv("MODEL_RELOAD_INTERVAL_SECONDS", "300"))

# Cles qui ne sont pas des valeurs de capteur (cf. mqtt_send.py).
META_KEYS = {"timestamp", "id_machine"}

_running = True

# Derniere valeur connue par machine/capteur : chaque message MQTT ne porte qu'un
# seul capteur, on reconstitue donc ici l'etat courant d'une machine pour pouvoir
# lancer une prediction dessus.
_latest_lock = threading.Lock()
_latest_by_machine: dict[str, dict[str, float]] = {}


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


def _on_message(client, userdata, msg):
    try:
        measure = json.loads(msg.payload.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        print(f"Message illisible sur {msg.topic} : {exc}", flush=True)
        return
    id_machine = measure.get("id_machine", "unknown")
    sensor = next((k for k in measure if k not in META_KEYS), None)
    if sensor is None or sensor not in CAPTEURS_IOT:
        return
    with _latest_lock:
        _latest_by_machine.setdefault(id_machine, {})[sensor] = float(measure[sensor])


def run_inference_tick(session: requests.Session, write_url: str) -> None:
    with _latest_lock:
        snapshot = {id_machine: dict(valeurs) for id_machine, valeurs in _latest_by_machine.items()}

    if not snapshot:
        return

    lines = []
    for id_machine, features in snapshot.items():
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

    write_url = f"http://{INFLUXDB_HOST}:{INFLUXDB_PORT}/api/v3/write_lp"
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {INFLUXDB_LIVE_TOKEN}"})

    load_model()

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
                waited_reload = 0.0
    finally:
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
        session.close()
        print("Deconnecte.", flush=True)


if __name__ == "__main__":
    main()
