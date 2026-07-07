CREATE TABLE type_alerte(
    id            INTEGER      PRIMARY KEY,
    label_gmao  VARCHAR(50)  NOT NULL UNIQUE
);

COPY type_alerte (label_gmao, id)
FROM '/datas/gold/postgres_alerte.csv'
WITH (FORMAT csv, HEADER true);
