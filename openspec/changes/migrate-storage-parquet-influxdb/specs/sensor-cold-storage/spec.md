## ADDED Requirements

### Requirement: Telegraf writes ingested measures to the staging bucket
The system SHALL configure Telegraf to write measures consumed from `usine/iot/#` into the `sensor_staging` InfluxDB bucket instead of TimescaleDB.

#### Scenario: Measure ingested by Telegraf
- **WHEN** Telegraf receives a measure from the `mqtt_consumer` input and applies the wide-to-long transform
- **THEN** the resulting point is written to the `sensor_staging` bucket

### Requirement: Point time reflects ingestion time, not simulated sensor time
The system SHALL let InfluxDB assign each point's `_time` at the moment of ingestion (write time), rather than overriding it with the sensor's own replayed timestamp.

#### Scenario: Replayed historical data ingested
- **WHEN** `mqtt_send.py` replays a measure whose own `timestamp` field is a historical or accelerated simulated instant
- **THEN** the point written to `sensor_staging` has `_time` set to the actual wall-clock ingestion moment, not the simulated `timestamp` value

### Requirement: Original sensor timestamp preserved as a field
The system SHALL preserve the sensor's own `timestamp` value as a distinct field (`sensor_timestamp`) on the ingested point, rather than discarding it or treating it as a sensor reading.

#### Scenario: Timestamp field retained
- **WHEN** a measure containing a `timestamp` key is transformed by Telegraf
- **THEN** the resulting point includes a `sensor_timestamp` field with that value, and does not include `timestamp` as a `sensor` tag or in the numeric `value` field

### Requirement: Periodic export of staged data to Parquet
The system SHALL periodically export the contents of the `sensor_staging` bucket to a Parquet file in `./bdd/parquet`, on an interval configurable in minutes via environment variable.

#### Scenario: Scheduled flush runs
- **WHEN** the configured flush interval elapses
- **THEN** the flush job queries all points currently in `sensor_staging` up to a safety-margin cutoff and writes them to a new Parquet file in `./bdd/parquet`

### Requirement: Parquet file naming reflects the exported time window
The system SHALL name each generated Parquet file using the minimum and maximum `sensor_timestamp` values present in the exported batch, in a filesystem-safe, sortable format.

#### Scenario: File named after export
- **WHEN** a flush produces a non-empty batch with minimum sensor timestamp `ts_min` and maximum sensor timestamp `ts_max`
- **THEN** the written file is named `sensor_data_{ts_min}_{ts_max}.parquet` using a compact UTC ISO8601 basic format

### Requirement: Exported data is removed from staging after a verified export
The system SHALL delete exported points from `sensor_staging` only after confirming the corresponding Parquet file has been fully and successfully written to disk.

#### Scenario: Export succeeds
- **WHEN** a Parquet file for a batch has been written and confirmed on disk
- **THEN** the flush job deletes exactly that batch's time range from `sensor_staging`

#### Scenario: Export fails
- **WHEN** the flush job crashes or errors before the Parquet file is fully written and confirmed
- **THEN** no delete is issued against `sensor_staging`, and the unexported points remain available for the next flush run

### Requirement: Empty flush windows produce no file
The system SHALL skip writing a Parquet file when a flush run finds no pending points in `sensor_staging`.

#### Scenario: No data since last flush
- **WHEN** the flush interval elapses and `sensor_staging` contains no points up to the safety-margin cutoff
- **THEN** no Parquet file is written and no delete is issued
