CREATE TABLE type_machine (
    id            INTEGER      PRIMARY KEY,
    type_machine  VARCHAR(50)  NOT NULL UNIQUE
);

COPY type_machine (type_machine, id)
FROM '/datas/gold/postgres_type_machine.csv'
WITH (FORMAT csv, HEADER true);
