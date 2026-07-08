# send-datas-mqtt

## Lancer la stack

```sh
docker compose up -d
```

Demarre Mosquitto, Telegraf, la base Postgres metier (Flyway applique les
migrations automatiquement -- necessite les CSV `datas/gold/postgres_*.csv`,
cf. [Demarrer a partir d'un dataset simule](#demarrer-a-partir-dun-dataset-simule-rul)
plus bas si ces fichiers n'existent pas encore), deux instances InfluxDB 3
Core separees, Grafana (cf. plus bas), et le pipeline RUL :
- `mspr2-influxdb-live` (port `INFLUXDB_LIVE_PORT`, defaut 8183) : base `sensor_live`
- `mspr2-influxdb-staging` (port `INFLUXDB_STAGING_PORT`, defaut 8182) : base `sensor_staging`,
  creee avec une retention (`INFLUXDB_STAGING_RETENTION`, defaut 24h) qui purge
  automatiquement les points deja exportes en Parquet.
- `mspr2-rul-inference` : inference RUL temps reel (cf.
  [Demarrer a partir d'un dataset simule](#demarrer-a-partir-dun-dataset-simule-rul)).
- `mspr2-train-rul-model` : reentraine le modele RUL tous les
  `TRAIN_INTERVAL_DAYS` (defaut 30 jours).

Le token n'est adopte qu'au tout premier demarrage de chaque instance (volume
de donnees vide) : pour le changer, il faut aussi supprimer le volume Docker
correspondant (`influxdb_live_data_mspr2` / `influxdb_staging_data_mspr2`).
Pour explorer une base avec InfluxDB 3 Explorer/UI, se connecter avec l'URL
`http://localhost:<port>` et le token correspondant (si Explorer tourne
lui-meme dans un conteneur Docker sur le meme reseau, cf. `http/README.md`
pour l'URL interne a utiliser a la place).

Requetes HTTP de test (healthcheck, ecriture, requete SQL) : voir
[`http/influxdb.http`](http/influxdb.http) et [`http/README.md`](http/README.md).

## Demarrer a partir d'un dataset simule (RUL)

Le pipeline RUL (entrainement + inference, cf. `src/gold/train_rul_model.py`
et `src/rul_inference/`) a besoin d'un dataset simule genere en amont (une
ligne par `(machine_id, timestamp)`, colonnes capteurs/PLC/valeurs
nominales/GMAO), depose dans `datas/silver/dataset_brut.csv`. A partir de
la, dans cet ordre :

1. **Scinder passe/present** (avant tout le reste -- l'ordre compte, cf.
   point 2) :
   ```sh
   uv run python -m gold.split_cold_storage
   ```
   `mqtt_send.py` (etape 5) rejoue tout `datas/gold/mqtt_iot_plc_send.jsonl`
   au compte-goutte, du premier au dernier tick, sans distinction
   passe/present : pour un dataset simule couvrant des mois, l'essentiel
   serait donc rejoue en temps reel avant de devenir exploitable. Ce script
   coupe a `SPLIT_COLD_STORAGE_CUTOFF` (par defaut l'heure actuelle) : la
   partie anterieure est ecrite directement en stockage froid (`bdd/parquet/`,
   meme format que `parquet_flush.py`), et seule la partie posterieure reste
   dans le jsonl que `mqtt_send.py` rejoue.

2. **Generer les autres CSV gold** (tables de reference Postgres, valeurs
   nominales, episodes alerte/maintenance) :
   ```sh
   uv run python -m gold.gold_datas
   ```
   A lancer **apres** l'etape 1 : `build_mqtt_jsonl.py` (invoque par
   `gold_datas`) ne regenere jamais un `mqtt_iot_plc_send.jsonl` deja
   present, donc dans l'autre ordre le fichier "present seul" de l'etape 1
   serait ecrase par la version complete (non coupee).

3. **Demarrer la stack** (cf. [Lancer la stack](#lancer-la-stack) plus haut) :
   Flyway applique les migrations Postgres a partir des CSV generes a
   l'etape 2.

4. **Charger les episodes et valeurs nominales dans InfluxDB** (une seule
   fois, scripts ponctuels par blocs -- re-executables sans risque, cf. leur
   docstring) :
   ```sh
   uv run python src/load_alerte.py
   uv run python src/load_maintenance.py
   uv run python src/load_nominal_values.py
   ```

5. **Rejouer la partie "presente" en direct** :
   ```sh
   uv run python src/mqtt_send.py
   ```
   `mspr2-rul-inference` (deja demarre a l'etape 3) reconstitue alors le
   contexte de chaque machine (age, valeurs nominales, statut PLC,
   `label_gmao`) et publie une prediction RUL toutes les
   `INFERENCE_INTERVAL_SECONDS` (mesure InfluxDB `rul_prediction`, base
   `sensor_live`) -- une valeur fixe (`rul_jours_estime: -1.0`) tant qu'aucun
   modele n'a ete entraine (etape 6).

6. **Entrainer un modele RUL** (optionnel : sinon `mspr2-train-rul-model`
   s'en charge automatiquement tous les `TRAIN_INTERVAL_DAYS`, defaut 30
   jours) :
   ```sh
   uv run python src/gold/correlate_sensor_alerte.py
   uv run python src/gold/train_rul_model.py
   ```
   Le premier correle les lectures capteur du stockage froid (fenetre
   `CORRELATE_SENSOR_ALERTE_WINDOW_DAYS`, defaut 180 jours) avec les episodes
   d'alerte InfluxDB (`datas/gold/sensor_data_alerte_correlated.csv`). Le
   second entraine un modele candidat et ne le promeut en production
   (`models/rul_cox_model.joblib`, lu par `mspr2-rul-inference`) que s'il
   depasse le c-index du modele deja deploye. `mspr2-rul-inference` recharge
   un modele nouvellement promu au plus tard toutes les
   `MODEL_RELOAD_INTERVAL_SECONDS` (defaut 300s), sans redemarrage.

## Dashboards Grafana

`docker compose up -d` demarre aussi `mspr2-grafana`
(`http://localhost:<GRAFANA_PORT>`, defaut 3000 ; identifiants
`GRAFANA_ADMIN_USER`/`GRAFANA_ADMIN_PASSWORD`, defaut `admin`/`admin`,
cf. `.env.example`). Deux datasources sont provisionnees automatiquement :
Postgres (`business_mspr`) et InfluxDB `sensor_live` via le plugin
communautaire Infinity (InfluxDB 3 Core n'ayant pas de Flight SQL/gRPC, la
datasource InfluxDB officielle de Grafana ne peut pas s'y connecter — cf.
`openspec/changes/add-grafana-monitoring-dashboard/design.md`).

Trois dashboards sont charges au demarrage (dossier `grafana/dashboards/`) :
- **Taux de panne** : nombre d'episodes d'alerte et taux de panne (temps en
  alerte / duree de la fenetre selectionnee), par machine et par type de
  machine.
- **Maintenance** : frequence et duree des episodes de maintenance, par
  machine et par type de maintenance (label GMAO).
- **Age machine vs taux de panne** : age de chaque machine a cote de son
  taux de panne, pour reperer une correlation age/fiabilite.

**Calcul du "Taux de panne"** : pour chaque machine, sur la fenetre de
temps selectionnee dans le dashboard (coin superieur droit),

```
taux de panne (%) = somme des durees des episodes d'alerte / duree de la fenetre * 100
```

Chaque episode d'alerte est une ligne de la table InfluxDB `alerte`
(`time` = debut, `fin_alerte` = fin) ; sa duree est `fin_alerte - time`. On
somme ces durees pour la machine, on divise par la duree totale de la
fenetre selectionnee (`$__to - $__from`), et on multiplie par 100 — c'est
donc la proportion du temps de la fenetre pendant laquelle la machine
etait en alerte. Requete exacte : voir `grafana/dashboards/failure-rate.json`
(champ `downtime_pct`).

## Scripts Python

Chaque script est independant et se lance comme `mqtt_send.py`, avec sa
configuration dans `.env` (copier `.env.example`).

- **`src/mqtt_send.py`** : rejoue le JSONL de capteurs vers Mosquitto.
  ```sh
  uv run python src/mqtt_send.py
  ```

- **`src/mqtt_live_monitor.py`** : abonne a `usine/iot/#`, affiche chaque
  mesure recue en direct et la persiste dans la base InfluxDB `sensor_live`
  (jamais purgee). Independant de Telegraf et du job de flush.
  ```sh
  uv run python src/mqtt_live_monitor.py
  ```

- **`src/parquet_flush.py`** : exporte periodiquement (toutes les
  `PARQUET_FLUSH_INTERVAL_MINUTES`) le contenu de la base `sensor_staging`
  (alimentee par Telegraf) vers des fichiers Parquet dans `./bdd/parquet`,
  en avancant un checkpoint local (`.flush_checkpoint`) pour ne jamais
  exporter deux fois le meme point. InfluxDB 3 Core ne supportant pas la
  suppression par plage/predicat, la purge des points deja exportes est
  assuree par la retention de la base (cf. plus haut), pas par ce script.
  ```sh
  uv run python src/parquet_flush.py
  ```

- **`src/load_nominal_values.py`** : charge en une seule passe
  `datas/gold/postgres_nominale_values.csv` (valeurs nominales par machine)
  dans la table `nominale_values` de la base InfluxDB `sensor_live`, a la
  place de l'ancienne table Postgres du meme nom. Lecture du CSV par blocs
  (`NOMINAL_VALUES_CHUNK_SIZE`) pour ne pas charger tout le fichier en
  memoire. Re-executable sans risque : rejouer le meme CSV ecrase les memes
  points (memes tags + timestamp) au lieu d'en creer des doublons.
  ```sh
  uv run python src/load_nominal_values.py
  ```

Les quatre peuvent tourner en parallele (le chargement des valeurs nominales
est ponctuel, pas un processus continu) ; arret propre avec `Ctrl+C`
(SIGINT/SIGTERM).
