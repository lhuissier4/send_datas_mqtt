## Why

TimescaleDB is the current sink for the MQTT sensor stream (via Telegraf), but the project wants historical data as Parquet files instead of a live time-series database, plus a lightweight way to observe the raw MQTT stream in real time without depending on Telegraf. Both needs are satisfied by routing everything through InfluxDB as an intermediate layer instead of TimescaleDB.

## What Changes

- **BREAKING**: Remove the `mspr2-timescaledb` service and its schema/init scripts from `docker-compose.yml`; TimescaleDB is no longer a target for sensor data.
- Add two separate InfluxDB 2.x services to `docker-compose.yml`, each on its own port with its own bucket: `mspr2-influxdb-live` (`sensor_live`) and `mspr2-influxdb-staging` (`sensor_staging`).
- Reconfigure Telegraf (`telegraf/telegraf.conf`) to write to the `sensor_staging` InfluxDB bucket instead of the TimescaleDB/Postgres output:
  - Drop the `json_time_key` override so InfluxDB assigns `_time` at actual ingestion (wall-clock) time.
  - Update the starlark transform so the sensor's own replayed timestamp is preserved as a regular field (`sensor_timestamp`) instead of being consumed as the point's time or mistaken for a sensor reading.
- Add a new Python script (ingestion-time flush job) that runs on a configurable interval (minutes, via `.env`), reads the `sensor_staging` bucket window written since the last run, writes it to a Parquet file in `./bdd/parquet` named with the min/max `sensor_timestamp` covered, and deletes the exported points from `sensor_staging` after a verified write.
- Add a new standalone Python script that subscribes directly to `usine/iot/#` on Mosquitto, prints each received measure to the console in real time, and writes it to the `sensor_live` InfluxDB bucket (no periodic purge — this bucket is a persistent, independent observability stream, separate from the staging/Parquet pipeline).
- Add InfluxDB client and Parquet-writing dependencies to `pyproject.toml`.
- Add new `.env` / `.env.example` variables for InfluxDB connection (host, port, token, org, bucket names) and the Parquet flush interval.

## Capabilities

### New Capabilities
- `sensor-live-monitoring`: standalone MQTT subscriber that prints sensor measures in real time and persists them to a dedicated, non-purged InfluxDB bucket for live observability.
- `sensor-cold-storage`: end-to-end pipeline (Telegraf → InfluxDB staging bucket → periodic export job) that turns the MQTT sensor stream into Parquet files on disk, replacing TimescaleDB as the historical store.

### Modified Capabilities
- None (no existing `openspec/specs/` capabilities cover the current Telegraf/TimescaleDB pipeline; it has not been previously spec'd).

## Impact

- `docker-compose.yml`: remove `mspr2-timescaledb`; add `mspr2-influxdb-live` and `mspr2-influxdb-staging` (separate instances/ports).
- `telegraf/telegraf.conf`: output plugin and time-handling changes.
- `timescaledb/init/01_schema.sql`: no longer used (TimescaleDB service removed).
- `src/`: two new scripts (live MQTT→InfluxDB monitor, InfluxDB→Parquet flush job).
- `bdd/parquet/`: new output directory for generated Parquet files.
- `.env`, `.env.example`: new InfluxDB and flush-interval variables.
- `pyproject.toml` / `uv.lock`: new dependencies (InfluxDB client, Parquet writer).
- Unaffected: `mqtt_send.py`, Mosquitto, the business `postgres` service and `bdd/migrations` (Flyway).
