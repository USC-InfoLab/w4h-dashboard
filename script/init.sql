CREATE ROLE admin WITH SUPERUSER PASSWORD 'admin';
ALTER ROLE admin with LOGIN;
CREATE DATABASE admin;
GRANT ALL PRIVILEGES ON DATABASE admin TO admin;
\c admin
CREATE EXTENSION pgcrypto;
CREATE TABLE users(	
    id SERIAL PRIMARY KEY,
    name VARCHAR(20),
    password bytea,
    current_db CHAR(30),
    query_history bytea
);

INSERT INTO users (name, password) VALUES('admin',sha256('admin'::bytea));
