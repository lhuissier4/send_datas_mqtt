CREATE TABLE machine (
    id_machine        VARCHAR(20)  PRIMARY KEY,
    id_type_machine   INTEGER      NOT NULL REFERENCES type_machine(id)
);

COPY machine (id_machine, id_type_machine)
FROM '/datas/gold/postgres_machine.csv'
WITH (FORMAT csv, HEADER true);
