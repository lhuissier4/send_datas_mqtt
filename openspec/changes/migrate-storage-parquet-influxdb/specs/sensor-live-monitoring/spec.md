## ADDED Requirements

### Requirement: Real-time console output of MQTT measures
The system SHALL subscribe to `usine/iot/#` on the Mosquitto broker and print each received sensor measure to the console as soon as it arrives.

#### Scenario: Measure printed on arrival
- **WHEN** a message is published on a topic under `usine/iot/#`
- **THEN** the live monitor prints the measure's `id_machine`, sensor name, value, and timestamp to stdout without waiting for a batch or interval

### Requirement: Persist live measures to a dedicated InfluxDB bucket
The system SHALL write every received measure into the `sensor_live` InfluxDB bucket, in addition to printing it.

#### Scenario: Measure written to sensor_live
- **WHEN** the live monitor receives a measure from MQTT
- **THEN** it writes a corresponding point to the `sensor_live` bucket with the machine id and sensor as tags and the value as a field

### Requirement: No purge of live bucket data
The system SHALL NOT delete or expire points from the `sensor_live` bucket as part of its own operation.

#### Scenario: Data remains after processing
- **WHEN** a measure has been printed and persisted to `sensor_live`
- **THEN** no subsequent operation of the live monitor removes that point from `sensor_live`

### Requirement: Independence from the cold-storage pipeline
The live monitor SHALL run as a separate process from Telegraf and the Parquet flush job, such that a failure or stoppage of one does not affect the other.

#### Scenario: Flush job stopped
- **WHEN** the Parquet flush job (cold-storage pipeline) is stopped or fails
- **THEN** the live monitor continues printing and persisting measures to `sensor_live` unaffected

#### Scenario: Live monitor stopped
- **WHEN** the live monitor process is stopped or fails
- **THEN** Telegraf continues writing measures to `sensor_staging` and the flush job continues exporting to Parquet unaffected

### Requirement: Automatic MQTT reconnection
The live monitor SHALL automatically attempt to reconnect to the MQTT broker if the connection is lost, without requiring a manual restart.

#### Scenario: Broker temporarily unavailable
- **WHEN** the MQTT broker connection drops
- **THEN** the live monitor retries connecting until it succeeds, and resumes printing and persisting measures once reconnected
