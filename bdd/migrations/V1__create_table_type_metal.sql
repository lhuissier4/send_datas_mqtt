CREATE TABLE type_metal (
    id          INTEGER      PRIMARY KEY,
    type_metal  VARCHAR(50)  NOT NULL UNIQUE
);

COPY type_metal (type_metal, id)
FROM '/datas/gold/postgres_type_metal.csv'
WITH (FORMAT csv, HEADER true);
