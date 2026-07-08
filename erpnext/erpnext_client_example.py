"""Exemple teste d'integration ERPNext via l'API REST (cf. README.md du dossier).

Preuve de concept : ce script a ete execute avec succes contre une instance
ERPNext temporaire (frappe_docker/pwd.yml). Il ne fait partie d'aucun service
du projet et ne tourne pas en continu -- il montre comment
`gold/train_rul_model.py` pourrait recuperer les informations metier fixes par
machine (type, secteur, date d'arrivee) si un vrai ERPNext etait branche a la
place des tables de reference Postgres actuelles.

Necessite une instance ERPNext accessible (cf. README.md pour en monter une
temporairement) et le DocType `Machine IoT` (cree dans le README, section
"DocType de test").
"""

import os

import requests

ERPNEXT_HOST = os.getenv("ERPNEXT_HOST", "http://localhost:8080")
ERPNEXT_API_KEY = os.getenv("ERPNEXT_API_KEY", "")
ERPNEXT_API_SECRET = os.getenv("ERPNEXT_API_SECRET", "")

DOCTYPE_MACHINE = "Machine IoT"


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"Authorization": f"token {ERPNEXT_API_KEY}:{ERPNEXT_API_SECRET}"})
    return session


def fetch_erpnext_machines(session: requests.Session) -> list[dict]:
    """Recupere les attributs fixes de toutes les machines connues d'ERPNext."""
    response = session.get(
        f"{ERPNEXT_HOST}/api/resource/{DOCTYPE_MACHINE}",
        params={"fields": '["name","type_machine","secteur","date_arrivee"]'},
    )
    response.raise_for_status()
    return response.json()["data"]


def create_erpnext_machine(session: requests.Session, machine: dict) -> dict:
    """Cree une fiche machine cote ERPNext (ex. a l'arrivee d'une nouvelle machine)."""
    response = session.post(f"{ERPNEXT_HOST}/api/resource/{DOCTYPE_MACHINE}", json=machine)
    response.raise_for_status()
    return response.json()["data"]


if __name__ == "__main__":
    session = build_session()
    print("Machines connues :", fetch_erpnext_machines(session))
