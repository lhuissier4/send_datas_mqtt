CREATE TABLE type_maintenance (
    id            INTEGER      PRIMARY KEY,
    label_gmao  VARCHAR(50)  NOT NULL UNIQUE
);

COPY type_maintenance (label_gmao, id)
FROM '/datas/gold/postgres_maintenance.csv'
WITH (FORMAT csv, HEADER true);
