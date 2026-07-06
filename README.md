# send-datas-mqtt

## Lancer la stack

```sh
docker compose up -d
```

Demarre Mosquitto, Telegraf, la base Postgres metier (Flyway applique les
migrations automatiquement), et deux instances InfluxDB 3 Core separees,
chacune protegee par un token admin (`INFLUXDB_LIVE_TOKEN` /
`INFLUXDB_STAGING_TOKEN`, cf. `.env.example`) :
- `mspr2-influxdb-live` (port `INFLUXDB_LIVE_PORT`, defaut 8183) : base `sensor_live`
- `mspr2-influxdb-staging` (port `INFLUXDB_STAGING_PORT`, defaut 8182) : base `sensor_staging`,
  creee avec une retention (`INFLUXDB_STAGING_RETENTION`, defaut 24h) qui purge
  automatiquement les points deja exportes en Parquet.

Le token n'est adopte qu'au tout premier demarrage de chaque instance (volume
de donnees vide) : pour le changer, il faut aussi supprimer le volume Docker
correspondant (`influxdb_live_data_mspr2` / `influxdb_staging_data_mspr2`).
Pour explorer une base avec InfluxDB 3 Explorer/UI, se connecter avec l'URL
`http://localhost:<port>` et le token correspondant (si Explorer tourne
lui-meme dans un conteneur Docker sur le meme reseau, cf. `http/README.md`
pour l'URL interne a utiliser a la place).

Requetes HTTP de test (healthcheck, ecriture, requete SQL) : voir
[`http/influxdb.http`](http/influxdb.http) et [`http/README.md`](http/README.md).

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

Les trois peuvent tourner en parallele ; arret propre avec `Ctrl+C` (SIGINT/SIGTERM).
