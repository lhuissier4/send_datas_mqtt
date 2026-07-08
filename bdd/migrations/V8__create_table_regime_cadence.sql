CREATE TABLE regime_cadence (
    id              INTEGER      PRIMARY KEY,
    regime_cadence  VARCHAR(50)  NOT NULL UNIQUE
);

COPY regime_cadence (regime_cadence, id)
FROM '/datas/gold/postgres_regime_cadence.csv'
WITH (FORMAT csv, HEADER true);
