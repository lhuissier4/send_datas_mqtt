"""Declenche `train_rul_model.py` a intervalle regulier (mensuel par defaut).

Service autonome (a tourner en continu, cf. docker-compose) plutot qu'un cron
externe : coherent avec les autres scripts a intervalle du projet
(`mqtt_live_monitor.py`, `parquet_flush.py`), et le job d'entrainement a de
toute facon besoin d'acceder aux memes Postgres/InfluxDB internes au reseau
Docker du projet -- un scheduler externe (ex. GitHub Actions) n'y aurait pas
acces sans les exposer publiquement.

Un echec d'un cycle d'entrainement (donnees insuffisantes, erreur de fit...)
n'arrete pas le service : il est logge et le prochain cycle demarre a
l'heure prevue.
"""

import os
import signal
import time

import train_rul_model

TRAIN_INTERVAL_DAYS = float(os.getenv("TRAIN_INTERVAL_DAYS", "30"))

_running = True


def _stop(signum, frame):
    global _running
    _running = False
    print("\nArret demande, fin du cycle en cours puis arret...", flush=True)


def main() -> None:
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    interval_seconds = TRAIN_INTERVAL_DAYS * 86400
    print(
        f"Scheduler d'entrainement RUL demarre (cycle toutes les {TRAIN_INTERVAL_DAYS:.0f} jours)",
        flush=True,
    )
    while _running:
        try:
            train_rul_model.main()
        except Exception as exc:  # noqa: BLE001 - un cycle en echec ne doit pas arreter le scheduler
            print(f"[scheduler] Cycle d'entrainement en echec : {exc!r}", flush=True)

        waited = 0.0
        while _running and waited < interval_seconds:
            time.sleep(min(60.0, interval_seconds - waited))
            waited += 60.0


if __name__ == "__main__":
    main()
