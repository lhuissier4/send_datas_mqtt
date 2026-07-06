## Context

Today, Telegraf subscribes to `usine/iot/#` on Mosquitto, reshapes each wide JSON payload into a narrow `sensor_data` metric (via a starlark processor), and writes it into TimescaleDB (`outputs.postgresql`). Telegraf currently overrides the point's time with the sensor's own replayed timestamp (`json_time_key = "timestamp"`), because that timestamp is a *simulated* instant (the JSONL can be replayed at any speed or cover historical dates) — it is not wall-clock time.

The project wants two independent things instead:
1. Historical sensor data as Parquet files on disk (`./bdd/parquet`), replacing TimescaleDB.
2. A separate, simple way to watch the live MQTT stream (console output) and keep it in a persistent store, independent of the Parquet pipeline.

Both are built on InfluxDB 2.x rather than TimescaleDB, but they are two distinct data flows that must not interfere with each other.

## Goals / Non-Goals

**Goals:**
- Replace TimescaleDB with Parquet files as the historical sink for the Telegraf-ingested stream.
- Preserve Telegraf's existing MQTT ingestion and wide→long transform logic; change only its output and time handling.
- Provide a standalone live-observability path (print + persist) that does not compete with or get purged by the Parquet flush job.
- Keep operational style consistent with the existing project: standalone Python scripts run like `mqtt_send.py`, no new Dockerfiles, `.env`-driven configuration.

**Non-Goals:**
- Historical data migration out of TimescaleDB (this is dev/demo data, regenerable from the gold JSONL).
- Retention/compaction strategy for the `sensor_live` bucket (it is intentionally left unbounded for this change).
- Changing the business `postgres` database, `bdd/migrations`, or `mqtt_send.py`.
- A query/analytics layer on top of the generated Parquet files (out of scope; this change only produces them).

## Decisions

**Two separate InfluxDB 2.x instances (`mspr2-influxdb-live`, `mspr2-influxdb-staging`), each on its own port, each with a single bucket.**
Originally scoped as one server with two buckets (bucket-level separation), but revised per explicit preference for two fully independent databases. Each instance gets its own container, volume, port (`INFLUXDB_LIVE_PORT`/`INFLUXDB_STAGING_PORT`), org, and token, bootstrapped via `DOCKER_INFLUXDB_INIT_*` env vars — no shared init script needed since each instance only ever needs one bucket. The trade-off (two containers/volumes/credential sets instead of one) is accepted in exchange for complete operational isolation between the never-purged live stream and the destructively-drained staging queue.

**Telegraf keeps ingesting MQTT; only its output plugin and time handling change.**
Telegraf's `mqtt_consumer` input and starlark wide→long transform already work correctly. Rewriting that logic in a custom Python consumer would duplicate tested behavior for no functional gain. Only `outputs.postgresql` → `outputs.influxdb_v2` (pointed at `sensor_staging`) changes, plus the time-handling decision below.

**InfluxDB `_time` becomes real ingestion (wall-clock) time; the sensor's own timestamp is kept as a plain field (`sensor_timestamp`).**
The flush job needs to window by *when data actually arrived* so a "flush every N minutes" schedule makes sense. The sensor's own timestamp cannot serve that purpose since it is simulated/replayed and can be arbitrarily far from wall-clock time. Concretely: drop `json_time_key`/`json_time_format`/`json_timezone` from `telegraf.conf` (so InfluxDB assigns `_time` dynamically at write), and update the starlark processor to route the `timestamp` field into `new.fields["sensor_timestamp"]` explicitly instead of letting the field loop treat it as another sensor reading (today's loop assumes every field is a sensor value — this breaks the moment `timestamp` stops being consumed as time).

**The flush job (Script B) treats `sensor_staging` as a queue, not a time-range query against a checkpoint.**
Because every successfully exported point is deleted from `sensor_staging`, the bucket's entire current content *is* the pending/unflushed queue. Each run queries the whole bucket up to `now() - SAFETY_MARGIN`, exports whatever is there (if anything) to one Parquet file, and deletes exactly that range on success. This avoids maintaining separate checkpoint state (extra file, extra failure mode) and is self-healing if a run is skipped or delayed — the next run just picks up everything still pending. `SAFETY_MARGIN` (a few multiples of Telegraf's `flush_interval`) exists so the job never queries a point Telegraf might still be in the middle of writing.

**Parquet file naming: `sensor_data_{ts_min}_{ts_max}.parquet`, compact UTC basic ISO8601 (`YYYYMMDDTHHMMSSZ`).**
`ts_min`/`ts_max` are the min/max `sensor_timestamp` (not `_time`) actually present in the exported batch — this makes the filename describe the *simulated measurement window* covered, which is what a downstream reader cares about. The compact format avoids colons (invalid on some filesystems) and sorts lexicographically by date.

**Empty flush windows are skipped.**
If a run finds no pending points in `sensor_staging`, it logs that and writes nothing — no zero-row Parquet files.

**Script A and Script B are standalone long-lived Python processes, run the same way as `mqtt_send.py` (no Dockerfile, `.env`-configured).**
No script in this repo is containerized today; introducing Dockerfiles just for these two would be new infrastructure the project doesn't otherwise need. Both scripts follow the same signal-handling / reconnect conventions already established in `mqtt_send.py`.

## Risks / Trade-offs

- **[Risk]** Race between Telegraf's write and Script B's query at a window boundary could skip or duplicate a point → **Mitigation**: `SAFETY_MARGIN` on the query's upper bound, sized well above Telegraf's `flush_interval`.
- **[Risk]** Process crash mid-write leaves a truncated, unreadable Parquet file → **Mitigation**: write to a temp file, close it, `rename()` atomically into `./bdd/parquet`, and only issue the InfluxDB delete after the renamed file is confirmed on disk.
- **[Risk]** InfluxDB range-delete could remove a late-arriving point that happens to share the exact upper-bound timestamp → **Mitigation**: the safety margin makes this unlikely; accepted as a known limitation given the non-production nature of this data.
- **[Risk]** `sensor_live` bucket grows unbounded (no purge by design) → **Mitigation**: acceptable for this change (Non-Goal); flag for a future retention policy if the project continues past the demo stage.
- **[Risk]** `sensor_timestamp` is stored as a raw string field, not a native timestamp type → **Mitigation**: document the expected format; consumers parse it at read time (pandas/pyarrow), consistent with how `mqtt_send.py` already treats timestamps as opaque strings end-to-end.

## Migration Plan

1. Add the `mspr2-influxdb-live` and `mspr2-influxdb-staging` services to `docker-compose.yml` (bucket/org/token bootstrap via env vars, one bucket per instance) and the corresponding `.env`/`.env.example` entries.
2. Update `telegraf/telegraf.conf`: swap `outputs.postgresql` for `outputs.influxdb_v2` targeting `sensor_staging`, drop the time-key override, and fix the starlark transform to emit `sensor_timestamp` as a field.
3. Implement and manually verify Script B (flush job) against `sensor_staging`, confirming Parquet files land correctly in `./bdd/parquet` and exported points are removed.
4. Implement and manually verify Script A (live monitor) against `sensor_live`, confirming console output and persisted points.
5. Once the Parquet path is verified end-to-end, remove the `mspr2-timescaledb` service from `docker-compose.yml` and delete the now-unused `timescaledb/` directory.

**Rollback**: this is dev/demo data with no production consumers — rollback is a `git revert` of the `docker-compose.yml`/`telegraf.conf` changes and stopping the two new scripts; no data migration is needed since the source JSONL can be replayed again from scratch.

## Open Questions

- Should `sensor_live` eventually get a retention policy, or does "never purged" remain intentional for the life of this project?
- Should `sensor_timestamp` be parsed into a real timestamp column at Parquet-write time (Script B) rather than kept as the original string, to make the output files easier to query downstream?
