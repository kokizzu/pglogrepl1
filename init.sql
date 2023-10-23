
-- already on docker-compose.yaml:
-- CREATE DATABASE pglogrepl;
-- CREATE USER pglogrepl with replication password 'secret';
-- GRANT ALL ON SCHEMA public to pglogrepl;
CREATE TABLE IF NOT EXISTS foo(id BIGSERIAL PRIMARY KEY NOT NULL, name TEXT NOT NULL, updatedAt TIMESTAMP NOT NULL DEFAULT NOW());

-- on pg_hba.conf:
-- host replication pglogrepl 127.0.0.1/32 md5

-- on postgresql.conf:
-- wal_level=logical
-- max_wal_senders=10
-- max_replication_slots=10