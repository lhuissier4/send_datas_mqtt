-- Schema TimescaleDB pour les mesures capteurs recues via Mosquitto.
-- Format long (narrow) : une ligne = une mesure d'un capteur a un instant.

CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Pas de cle primaire unique : Telegraf insere en mode append-only
-- (il ne fait pas d'upsert), comportement classique pour des metriques.
CREATE TABLE IF NOT EXISTS sensor_data (
    time        TIMESTAMPTZ      NOT NULL,
    id_machine  TEXT             NOT NULL,
    sensor      TEXT             NOT NULL,
    value       DOUBLE PRECISION NOT NULL
);

-- Transforme la table en hypertable (partitionnement automatique par temps).
SELECT create_hypertable('sensor_data', 'time', if_not_exists => TRUE);

-- Index pour les requetes par machine / capteur sur une fenetre temporelle.
CREATE INDEX IF NOT EXISTS idx_sensor_machine_time
    ON sensor_data (id_machine, sensor, time DESC);
