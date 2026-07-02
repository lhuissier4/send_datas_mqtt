CREATE TABLE production_status (
    id                  INTEGER      PRIMARY KEY,
    iot_statut_machine  VARCHAR(50)  NOT NULL UNIQUE
);

COPY production_status (iot_statut_machine, id)
FROM '/datas/gold/postgres_production_status.csv'
WITH (FORMAT csv, HEADER true);
