CREATE TABLE type_alerte(
    id            INTEGER      PRIMARY KEY,
    label_gmao  VARCHAR(50)  NOT NULL UNIQUE
);

COPY label_gmao (label_gmao, id)
FROM '/datas/gold/postgres_maintenance.csv'
WITH (FORMAT csv, HEADER true);
