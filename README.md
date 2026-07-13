
# PgLogRepl

Example showing how to fetch and decode replication logs from PostgreSQL 14+ using `jackc/pglogrepl` and the `pgoutput` (`RelationMessageV2`) plugin instead of `wal2json`.

## Maintenance checklist

- [x] Go runtime updated to 1.26.5.
- [x] pglogrepl, pgx, gotro, and lexid dependencies refreshed.
- [x] `make test` compiles the example without connecting to PostgreSQL.
- [x] `make verify-dependency-security` and `make vulncheck` check dependency security.
