## 1. Dependencies & environment

- [x] 1.1 Add `influxdb-client` and `pyarrow` to `pyproject.toml` dependencies; update `uv.lock`
- [x] 1.2 Add new `.env` / `.env.example` variables (per-instance): `INFLUXDB_LIVE_HOST`/`PORT`/`TOKEN`/`ORG`, `INFLUXDB_STAGING_HOST`/`PORT`/`TOKEN`/`ORG`, `INFLUXDB_BUCKET_LIVE` (`sensor_live`), `INFLUXDB_BUCKET_STAGING` (`sensor_staging`), `PARQUET_FLUSH_INTERVAL_MINUTES`, `PARQUET_OUTPUT_DIR` (`./bdd/parquet`), `PARQUET_FLUSH_SAFETY_MARGIN_SECONDS`

## 2. InfluxDB infrastructure

- [x] 2.1 Add `mspr2-influxdb-live` and `mspr2-influxdb-staging` services to `docker-compose.yml` (image `influxdb:2`, separate ports/volumes, bootstrap env vars for org/token/bucket per instance)
- [x] 2.2 Each instance bootstraps its own single bucket via `DOCKER_INFLUXDB_INIT_BUCKET` (no shared init script needed since there's one bucket per instance)
- [x] 2.3 Create `./bdd/parquet/` directory (with `.gitkeep`) as the Parquet output target
- [x] 2.4 Verify InfluxDB starts cleanly and both buckets are queryable

## 3. Telegraf reconfiguration

- [x] 3.1 Replace the `outputs.postgresql` block in `telegraf/telegraf.conf` with `outputs.influxdb_v2`, targeting the `sensor_staging` bucket
- [x] 3.2 Remove the `json_time_key` / `json_time_format` / `json_timezone` overrides so InfluxDB assigns `_time` at ingestion
- [x] 3.3 Update the starlark processor so the `timestamp` field is routed into `new.fields["sensor_timestamp"]` instead of being looped over as a sensor tag/value
- [x] 3.4 Verify Telegraf writes correctly-shaped points to `sensor_staging`: `id_machine` tag, `sensor` tag, `value` field, `sensor_timestamp` field, ingestion-time `_time`

## 4. Cold-storage flush job (Script B)

- [x] 4.1 Create `src/parquet_flush.py`: connect to InfluxDB, query all points in `sensor_staging` up to `now() - PARQUET_FLUSH_SAFETY_MARGIN_SECONDS`
- [x] 4.2 Build a DataFrame from the query result; compute `ts_min`/`ts_max` from `sensor_timestamp`; write to a temp file, then atomically rename into `./bdd/parquet/sensor_data_{ts_min}_{ts_max}.parquet`
- [x] 4.3 After the renamed file is confirmed on disk, delete exactly that exported time range from `sensor_staging`
- [x] 4.4 Skip writing and deleting when a run finds no pending points (empty window)
- [x] 4.5 Implement the periodic loop driven by `PARQUET_FLUSH_INTERVAL_MINUTES`, with graceful SIGINT/SIGTERM shutdown, following `mqtt_send.py`'s conventions
- [x] 4.6 Manually verify: run `mqtt_send.py` + Telegraf + this script; confirm Parquet files appear correctly named in `./bdd/parquet` and `sensor_staging` empties out after each successful flush

## 5. Live monitoring script (Script A)

- [x] 5.1 Create `src/mqtt_live_monitor.py`: subscribe to `usine/iot/#` on Mosquitto, reusing `mqtt_send.py`'s env var and connection conventions
- [x] 5.2 Print each received measure to the console in real time
- [x] 5.3 Write each received measure to the `sensor_live` InfluxDB bucket (no deletion/purge logic)
- [x] 5.4 Implement automatic MQTT reconnection (mirroring `mqtt_send.py`'s `reconnect_delay_set`/`on_connect`/`on_disconnect` pattern)
- [x] 5.5 Manually verify: run `mqtt_send.py` + this script; confirm console output and points persisted in `sensor_live`, independent of Script B or Telegraf being up or down

## 6. TimescaleDB removal

- [x] 6.1 Remove the `mspr2-timescaledb` service from `docker-compose.yml`
- [x] 6.2 Delete the `timescaledb/` directory (schema/init scripts no longer used)
- [x] 6.3 Remove now-unused `TIMESCALEDB_*` variables from `.env.example` and confirm no remaining references in `docker-compose.yml` or `telegraf/telegraf.conf`

## 7. End-to-end verification

- [x] 7.1 Run the full stack (Mosquitto, InfluxDB, Telegraf, business Postgres/Flyway unaffected) alongside `mqtt_send.py`, Script A, and Script B
- [x] 7.2 Confirm: live console output appears continuously, `sensor_live` accumulates points, `sensor_staging` drains on each flush, and `./bdd/parquet` accumulates correctly named, non-overlapping files covering the full replay
- [x] 7.3 Add brief run instructions for the two new scripts (e.g. in `note mqtt.md` or a README) so they can be started the same way `mqtt_send.py` is today
