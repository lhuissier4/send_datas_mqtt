"""Observe en direct le flux MQTT et le persiste dans InfluxDB (sensor_live).

Script autonome, independant du pipeline Telegraf -> sensor_staging -> Parquet :
il s'abonne a `usine/iot/#`, affiche chaque mesure recue et l'ecrit dans le
bucket `sensor_live` (jamais purge). Cf. `mqtt_send.py` pour les conventions
de connexion/reconnexion reprises ici.
"""

import os
import json
import signal
from pathlib import Path

import paho.mqtt.client as mqtt
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

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
INFLUXDB_PORT = int(os.getenv("INFLUXDB_LIVE_PORT", "8086"))
INFLUXDB_TOKEN = os.getenv("INFLUXDB_LIVE_TOKEN", "iot-live-token")
INFLUXDB_ORG = os.getenv("INFLUXDB_LIVE_ORG", "iot")
INFLUXDB_BUCKET_LIVE = os.getenv("INFLUXDB_BUCKET_LIVE", "sensor_live")

# Cles qui ne sont pas des valeurs de capteur (cf. mqtt_send.py).
META_KEYS = {"timestamp", "id_machine"}

_running = True


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


def handle_measure(write_api, bucket: str, measure: dict) -> None:
    id_machine = measure.get("id_machine", "unknown")
    timestamp = measure.get("timestamp", "?")
    sensor = next((k for k in measure if k not in META_KEYS), None)
    if sensor is None:
        return
    value = measure[sensor]

    print(f"[{timestamp}] {id_machine} {sensor} = {value}", flush=True)

    point = (
        Point("sensor_data")
        .tag("id_machine", id_machine)
        .tag("sensor", sensor)
        .field("value", float(value))
        .field("sensor_timestamp", str(timestamp))
    )
    write_api.write(bucket=bucket, org=INFLUXDB_ORG, record=point)


def build_on_message(write_api):
    def _on_message(client, userdata, msg):
        try:
            measure = json.loads(msg.payload.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            print(f"Message illisible sur {msg.topic} : {exc}", flush=True)
            return
        handle_measure(write_api, INFLUXDB_BUCKET_LIVE, measure)

    return _on_message


def main() -> None:
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    influx_client = InfluxDBClient(
        url=f"http://{INFLUXDB_HOST}:{INFLUXDB_PORT}",
        token=INFLUXDB_TOKEN,
        org=INFLUXDB_ORG,
    )
    write_api = influx_client.write_api(write_options=SYNCHRONOUS)

    mqtt_client = build_mqtt_client()
    mqtt_client.on_message = build_on_message(write_api)
    mqtt_client.loop_start()

    print(f"Ecoute en direct -> bucket InfluxDB {INFLUXDB_BUCKET_LIVE}", flush=True)
    try:
        while _running:
            signal.pause()
    finally:
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
        influx_client.close()
        print("Deconnecte.", flush=True)


if __name__ == "__main__":
    main()
