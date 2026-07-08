# Integration ERPNext (preuve de concept, teste puis demonte)

Dans la cible d'architecture, l'entrainement mensuel (`gold/train_rul_model.py`)
doit recuperer les informations metier "fixes" par machine (type de machine,
secteur, date d'arrivee...) depuis un Postgres relie a ERPNext. Le Postgres
actuel de ce projet (`mspr2-postgres`) n'a aucun lien avec un vrai ERPNext —
ce dossier documente comment l'integration **aurait** ete faite si ERPNext
tournait reellement, et prouve que le chemin fonctionne : instance ERPNext
temporaire montee via `frappe_docker`, DocType cree, appels REST reels testes
(curl + `requests`), puis instance demontee. Rien ici ne tourne en continu ni
n'est branche dans le docker-compose du projet.

## Pourquoi l'API plutot qu'un acces direct a la base

ERPNext (framework Frappe) gere son propre schema MariaDB et sa propre logique
metier (validations, hooks, notifications). Lire/ecrire directement dans cette
base contournerait cette logique et casserait a la moindre montee de version.
L'integration recommandee passe donc par l'**API REST** de Frappe, avec
authentification par cle/secret API (`Authorization: token <key>:<secret>`),
jamais par une connexion SQL directe.

## Reproduire l'instance temporaire

```sh
git clone --depth 1 https://github.com/frappe/frappe_docker.git
cd frappe_docker
docker compose -p erpnext-tmp -f pwd.yml up -d
```

`pwd.yml` est le quick-start officiel ("Play With Docker") : images
pre-construites (`frappe/erpnext`), pas de build, creation automatique d'un
site (`frontend`, identifiants `Administrator` / `admin`) sur
`http://localhost:8080`. Compter quelques minutes (pull des images +
installation des DocTypes).

Demontage complet (containers + volumes, rien ne persiste) :

```sh
docker compose -p erpnext-tmp -f pwd.yml down -v
```

## Generer une cle API (teste)

```sh
docker compose -p erpnext-tmp -f pwd.yml exec backend \
  bench --site frontend execute frappe.core.doctype.user.user.generate_keys \
  --kwargs '{"user": "Administrator"}'
# -> {"api_key": "...", "api_secret": "..."}
```

En production, on creerait un utilisateur d'integration dedie (role limite
aux DocTypes necessaires) plutot que d'utiliser `Administrator`.

## DocType de test (`Machine IoT`)

Cree via l'API elle-meme (`POST /api/resource/DocType`), pour representer les
attributs fixes d'une machine cote ERPNext : `machine_code`, `type_machine`,
`secteur`, `date_arrivee`. Champs volontairement minimalistes (equivalent
custom DocType plutot que le module `Assets` natif d'ERPNext, qui impose une
chaine Item/Asset Category/Company non pertinente ici).

## Appels REST testes

```sh
API_KEY=...; API_SECRET=...

# Creation
curl -X POST "http://localhost:8080/api/resource/Machine%20IoT" \
  -H "Authorization: token ${API_KEY}:${API_SECRET}" \
  -H "Content-Type: application/json" \
  -d '{"machine_code": "MCH-001", "type_machine": "Tour CNC", "secteur": "Usinage", "date_arrivee": "2022-03-15"}'

# Lecture unitaire
curl -H "Authorization: token ${API_KEY}:${API_SECRET}" \
  "http://localhost:8080/api/resource/Machine%20IoT/MCH-001"

# Liste avec selection de champs
curl -G -H "Authorization: token ${API_KEY}:${API_SECRET}" \
  "http://localhost:8080/api/resource/Machine%20IoT" \
  --data-urlencode 'fields=["name","type_machine","secteur","date_arrivee"]'
```

Equivalent Python (`requests`, meme lib que le reste du projet) :
[`erpnext_client_example.py`](erpnext_client_example.py) — execute avec succes
contre l'instance temporaire ci-dessus (GET unitaire, GET liste, POST).

## Ce qu'il faudrait pour une vraie integration

- Un utilisateur d'integration dedie (pas `Administrator`), avec un role
  limite en lecture seule sur les DocTypes machine.
- `ERPNEXT_HOST` / `ERPNEXT_API_KEY` / `ERPNEXT_API_SECRET` en variables
  d'environnement (meme convention que `INFLUXDB_LIVE_TOKEN` etc. dans
  `.env.example`), pas en dur comme dans l'exemple ci-dessus.
- Cote modelisation : soit un DocType custom comme `Machine IoT`, soit le
  module `Assets` natif si le suivi comptable (amortissement, valeur d'achat)
  est aussi souhaite.
- Brancher `gold/train_rul_model.py::fetch_postgres_lookups` avec l'equivalent
  `fetch_erpnext_machines()` (cf. l'exemple) une fois une vraie instance
  disponible en continu.
