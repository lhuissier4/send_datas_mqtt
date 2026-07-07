-- date_mise_en_service marks the start of a sector *assignment* for a
-- machine (one row per contiguous run of the same secteur), not the
-- machine's overall commissioning date (see age_machine.premier_timestamp
-- for that).
CREATE TABLE machine_secteur_historique (
    id_machine             VARCHAR(20)   NOT NULL REFERENCES machine(id_machine),
    secteur                VARCHAR(50)   NOT NULL,
    date_mise_en_service   TIMESTAMP     NOT NULL,
    PRIMARY KEY (id_machine, secteur, date_mise_en_service)
);

COPY machine_secteur_historique (id_machine, secteur, date_mise_en_service)
FROM '/datas/gold/postgres_machine_secteur_historique.csv'
WITH (FORMAT csv, HEADER true);
