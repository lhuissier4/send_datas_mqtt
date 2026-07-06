# send-datas-mqtt

## Lancer la stack

```sh
docker compose up -d
```

Demarre Mosquitto, Telegraf, la base Postgres metier (Flyway applique les
migrations automatiquement), et deux instances InfluxDB separees :
- `mspr2-influxdb-live` (port `INFLUXDB_LIVE_PORT`, defaut 8086) : bucket `sensor_live`
- `mspr2-influxdb-staging` (port `INFLUXDB_STAGING_PORT`, defaut 8087) : bucket `sensor_staging`

## Scripts Python

Chaque script est independant et se lance comme `mqtt_send.py`, avec sa
configuration dans `.env` (copier `.env.example`).

- **`src/mqtt_send.py`** : rejoue le JSONL de capteurs vers Mosquitto.
  ```sh
  uv run python src/mqtt_send.py
  ```

- **`src/mqtt_live_monitor.py`** : abonne a `usine/iot/#`, affiche chaque
  mesure recue en direct et la persiste dans le bucket InfluxDB `sensor_live`
  (jamais purge). Independant de Telegraf et du job de flush.
  ```sh
  uv run python src/mqtt_live_monitor.py
  ```

- **`src/parquet_flush.py`** : exporte periodiquement (toutes les
  `PARQUET_FLUSH_INTERVAL_MINUTES`) le contenu du bucket `sensor_staging`
  (alimente par Telegraf) vers des fichiers Parquet dans `./bdd/parquet`,
  puis supprime les points exportes du bucket.
  ```sh
  uv run python src/parquet_flush.py
  ```

Les trois peuvent tourner en parallele ; arret propre avec `Ctrl+C` (SIGINT/SIGTERM).
