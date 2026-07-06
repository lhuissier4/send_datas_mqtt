"""Observe en direct le flux MQTT et le persiste dans InfluxDB (sensor_live).

Script autonome, independant du pipeline Telegraf -> sensor_staging -> Parquet :
il s'abonne a `usine/iot/#`, affiche chaque mesure recue et l'ecrit dans la
base `sensor_live` (jamais purgee). Cf. `mqtt_send.py` pour les conventions
de connexion/reconnexion reprises ici.

Les points ne sont pas ecrits un par un : chaque write_lp vers InfluxDB 3 Core
attend ~1s (son intervalle de flush WAL), et `mqtt_send.py` publie toutes les
mesures d'une ligne quasi simultanement. Avec une ecriture HTTP par message,
le traitement d'une rafale plafonne a ~1 msg/s et continue de vider la file
plusieurs minutes apres l'arret de l'envoi. Les points sont donc mis en
memoire tampon et envoyes groupes (plusieurs lignes de line protocol par
requete) toutes les LIVE_FLUSH_INTERVAL_SECONDS.
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

PROJECT_ROOT = Path(__file__).resolve().parent.parent
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

LIVE_FLUSH_INTERVAL_SECONDS = float(os.getenv("LIVE_FLUSH_INTERVAL_SECONDS", "30"))

# Cles qui ne sont pas des valeurs de capteur (cf. mqtt_send.py).
META_KEYS = {"timestamp", "id_machine"}

_running = True
_buffer_lock = threading.Lock()
_buffer: list[str] = []


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
        client_id="mqtt-live-monitor",
    )
    if MQTT_USERNAME:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    client.on_connect = _on_connect
    client.on_disconnect = _on_disconnect
    # Reconnexion automatique avec backoff exponentiel entre 1 et 30 s.
    client.reconnect_delay_set(min_delay=1, max_delay=30)
    client.connect_async(MQTT_HOST, MQTT_PORT, MQTT_KEEPALIVE)
    return client


def measure_to_line(measure: dict) -> str | None:
    id_machine = measure.get("id_machine", "unknown")
    timestamp = measure.get("timestamp", "?")
    sensor = next((k for k in measure if k not in META_KEYS), None)
    if sensor is None:
        return None
    value = measure[sensor]

    print(f"[{timestamp}] {id_machine} {sensor} = {value}", flush=True)

    # Timestamp explicite (ns) : sans lui, InfluxDB horodate au moment de la
    # requete HTTP, donc tous les points d'un meme flush groupe partageraient
    # le meme _time. Avec des tags identiques (meme machine/capteur, ce qui
    # arrive forcement entre deux flushs), ils s'ecraseraient entre eux au
    # lieu de coexister.
    point = (
        Point("sensor_data")
        .tag("id_machine", id_machine)
        .tag("sensor", sensor)
        .field("value", float(value))
        .field("sensor_timestamp", str(timestamp))
        .time(time.time_ns(), write_precision="ns")
    )
    return point.to_line_protocol()


def build_on_message():
    def _on_message(client, userdata, msg):
        try:
            measure = json.loads(msg.payload.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            print(f"Message illisible sur {msg.topic} : {exc}", flush=True)
            return
        line = measure_to_line(measure)
        if line is None:
            return
        with _buffer_lock:
            _buffer.append(line)

    return _on_message


def flush_buffer(session: requests.Session, write_url: str, database: str) -> None:
    with _buffer_lock:
        if not _buffer:
            return
        batch = list(_buffer)
        _buffer.clear()

    response = session.post(
        write_url,
        params={"db": database},
        data="\n".join(batch).encode("utf-8"),
    )
    response.raise_for_status()
    print(f"[flush] {len(batch)} points ecrits", flush=True)


def main() -> None:
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    write_url = f"http://{INFLUXDB_HOST}:{INFLUXDB_PORT}/api/v3/write_lp"
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {INFLUXDB_LIVE_TOKEN}"})

    mqtt_client = build_mqtt_client()
    mqtt_client.on_message = build_on_message()
    mqtt_client.loop_start()

    print(
        f"Ecoute en direct -> base InfluxDB {INFLUXDB_DATABASE_LIVE} "
        f"(flush groupe toutes les {LIVE_FLUSH_INTERVAL_SECONDS}s)",
        flush=True,
    )
    try:
        waited = 0.0
        while _running:
            time.sleep(0.5)
            waited += 0.5
            if waited >= LIVE_FLUSH_INTERVAL_SECONDS:
                flush_buffer(session, write_url, INFLUXDB_DATABASE_LIVE)
                waited = 0.0
    finally:
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
        flush_buffer(session, write_url, INFLUXDB_DATABASE_LIVE)
        session.close()
        print("Deconnecte.", flush=True)


if __name__ == "__main__":
    main()
