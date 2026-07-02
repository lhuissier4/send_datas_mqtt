CREATE TABLE nominale_values (
    id                            SERIAL       PRIMARY KEY,
    timestamp                     TIMESTAMP,
    machine_id                    VARCHAR(20)  NOT NULL,
    vitesse_rotation_nominal      NUMERIC      NOT NULL,
    courant_moteur_nominal        NUMERIC      NOT NULL,
    pression_hydraulique_nominal  NUMERIC      NOT NULL,
    id_production_status          INTEGER      NOT NULL REFERENCES production_status(id),
    temp_base_moteur              NUMERIC      NOT NULL
);

CREATE TEMP TABLE nominale_values_staging (
    timestamp                     TIMESTAMP,
    machine_id                    VARCHAR(20),
    vitesse_rotation_nominal      NUMERIC,
    courant_moteur_nominal        NUMERIC,
    pression_hydraulique_nominal  NUMERIC,
    statut_nominal                VARCHAR(50),
    temp_base_moteur              NUMERIC
);

COPY nominale_values_staging (timestamp, machine_id, vitesse_rotation_nominal, courant_moteur_nominal, pression_hydraulique_nominal, statut_nominal, temp_base_moteur)
FROM '/datas/gold/postgres_nominale_values.csv'
WITH (FORMAT csv, HEADER true);

INSERT INTO nominale_values (timestamp, machine_id, vitesse_rotation_nominal, courant_moteur_nominal, pression_hydraulique_nominal, id_production_status, temp_base_moteur)
SELECT s.timestamp, s.machine_id, s.vitesse_rotation_nominal, s.courant_moteur_nominal, s.pression_hydraulique_nominal, ps.id, s.temp_base_moteur
FROM nominale_values_staging s
JOIN production_status ps ON ps.iot_statut_machine = s.statut_nominal;

DROP TABLE nominale_values_staging;

