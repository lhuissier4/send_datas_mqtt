"""Rejoue le fichier JSONL des capteurs vers un broker Mosquitto dockerise.

Le fichier `mqtt_iot_plc_send.jsonl` contient une ligne JSON par instant.
Chaque ligne est une liste de mesures : {timestamp, id_machine, <capteur>: <valeur>}.

Logique (cf. note mqtt.md) : une mesure = un envoi MQTT.
Pour chaque ligne on publie toutes ses mesures, puis on attend
SEND_INTERVAL_SECONDS avant de passer a la ligne suivante.
"""

import os
import json
import time
import signal
import threading
from pathlib import Path

import paho.mqtt.client as mqtt
from dotenv import load_dotenv

# Racine du projet (le fichier est dans src/)
PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USERNAME = os.getenv("MQTT_USERNAME") or None
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD") or None
MQTT_KEEPALIVE = int(os.getenv("MQTT_KEEPALIVE", "60"))
MQTT_QOS = int(os.getenv("MQTT_QOS", "1"))
MQTT_TOPIC_PREFIX = os.getenv("MQTT_TOPIC_PREFIX", "usine/iot")

JSONL_PATH = os.getenv("JSONL_PATH", "datas/gold/mqtt_iot_plc_send.jsonl")
SEND_INTERVAL_SECONDS = float(os.getenv("SEND_INTERVAL_SECONDS", "30"))

# Delai d'attente max du PUBACK (accuse de reception QoS 1/2), en secondes.
PUBLISH_TIMEOUT_SECONDS = float(os.getenv("PUBLISH_TIMEOUT_SECONDS", "5"))

# Cles qui ne sont pas des valeurs de capteur
META_KEYS = {"timestamp", "id_machine"}

_running = True
# Mis a jour par les callbacks on_connect / on_disconnect.
_connected = threading.Event()


def _stop(signum, frame):
    global _running
    _running = False
    print("\nArret demande, fermeture...", flush=True)


def _on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        _connected.set()
        print(f"Connecte au broker mqtt://{MQTT_HOST}:{MQTT_PORT}", flush=True)
    else:
        _connected.clear()
        print(f"Echec de connexion au broker : {reason_code}", flush=True)


def _on_disconnect(client, userdata, disconnect_flags, reason_code, properties):
    _connected.clear()
    # reason_code != 0 => deconnexion non sollicitee (broker coupe, reseau...).
    # loop_start() relance automatiquement les tentatives de reconnexion.
    print(
        f"Deconnecte du broker (rc={reason_code}) ; reconnexion en cours...",
        flush=True,
    )


def resolve_jsonl_path() -> Path:
    path = Path(JSONL_PATH)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    if not path.exists():
        raise FileNotFoundError(f"Fichier introuvable : {path}")
    return path


def build_client() -> mqtt.Client:
    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
        client_id="send-datas-mqtt",
    )
    if MQTT_USERNAME:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    client.on_connect = _on_connect
    client.on_disconnect = _on_disconnect
    # Reconnexion automatique avec backoff exponentiel entre 1 et 30 s.
    client.reconnect_delay_set(min_delay=1, max_delay=30)
    # connect_async : ne leve pas si le broker est absent au demarrage ;
    # la connexion (et les reconnexions) sont gerees par loop_start().
    client.connect_async(MQTT_HOST, MQTT_PORT, MQTT_KEEPALIVE)
    return client


def publish_line(client: mqtt.Client, measures: list[dict]) -> tuple[int, int]:
    """Publie chaque mesure d'une ligne.

    Avec QoS > 0, on attend le PUBACK (accuse de reception) via
    wait_for_publish : c'est ce qui permet de *savoir* si le message a
    reellement ete livre, meme si le broker tombe.

    Retourne (livres, echecs).
    """
    # 1) On envoie toutes les mesures de la ligne (mise en file rapide).
    pending: list[tuple[str, mqtt.MQTTMessageInfo]] = []
    for measure in measures:
        id_machine = measure.get("id_machine", "unknown")
        sensor = next((k for k in measure if k not in META_KEYS), None)
        if sensor is None:
            continue
        topic = f"{MQTT_TOPIC_PREFIX}/{id_machine}/{sensor}"
        payload = json.dumps(measure, ensure_ascii=False)
        info = client.publish(topic, payload, qos=MQTT_QOS)
        pending.append((topic, info))

    # 2) On confirme la livraison de chacune.
    delivered = 0
    failed = 0
    for topic, info in pending:
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            failed += 1
            print(f"  echec mise en file {topic} (rc={info.rc})", flush=True)
            continue
        if MQTT_QOS == 0:
            delivered += 1
            continue
        try:
            info.wait_for_publish(timeout=PUBLISH_TIMEOUT_SECONDS)
        except (ValueError, RuntimeError) as exc:
            failed += 1
            print(f"  PUBACK non recu {topic} : {exc}", flush=True)
            continue
        if info.is_published():
            delivered += 1
        else:
            failed += 1
            print(f"  PUBACK non recu {topic} (timeout)", flush=True)
    return delivered, failed


def main() -> None:
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    jsonl_path = resolve_jsonl_path()
    client = build_client()
    client.loop_start()
    print(f"Lecture de {jsonl_path}", flush=True)

    try:
        with jsonl_path.open("r", encoding="utf-8") as f:
            for line_no, line in enumerate(f, start=1):
                if not _running:
                    break
                line = line.strip()
                if not line:
                    continue

                # Si le broker est tombe, on attend la reconnexion avant
                # d'envoyer la ligne (sinon les messages partent dans le vide).
                if not _connected.is_set():
                    print("Broker indisponible, attente de reconnexion...", flush=True)
                    while _running and not _connected.wait(timeout=1):
                        pass
                    if not _running:
                        break

                measures = json.loads(line)
                delivered, failed = publish_line(client, measures)
                ts = measures[0].get("timestamp", "?") if measures else "?"
                status = f"{delivered} messages"
                if failed:
                    status += f", {failed} EN ECHEC"
                print(f"[ligne {line_no}] {ts} -> {status}", flush=True)

                # Attente interruptible
                waited = 0.0
                while _running and waited < SEND_INTERVAL_SECONDS:
                    time.sleep(min(0.5, SEND_INTERVAL_SECONDS - waited))
                    waited += 0.5
    finally:
        client.loop_stop()
        client.disconnect()
        print("Deconnecte.", flush=True)


if __name__ == "__main__":
    main()
