CREATE TABLE age_machine (
    id_machine         VARCHAR(20)  NOT NULL,
    age_machine_jours  NUMERIC      NOT NULL,
    premier_timestamp  TIMESTAMP    NOT NULL,
    PRIMARY KEY (id_machine, premier_timestamp)
);

COPY age_machine (id_machine, age_machine_jours, premier_timestamp)
FROM '/datas/gold/postgres_age_machine.csv'
WITH (FORMAT csv, HEADER true);
