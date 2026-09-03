#!/bin/sh
# Runs once, automatically, on the primary's FIRST initialization (the
# official postgres image executes every script under
# /docker-entrypoint-initdb.d on an empty data directory only -- never on a
# restart against an existing volume).
#
# The default pg_hba.conf this image generates has a catch-all
# `host all all all scram-sha-256` line, and it is easy to assume that
# covers replication too. It does not: `replication` is a separate
# pseudo-database in pg_hba's own matching rules, and a bare `all` in the
# DATABASE column does not include it (confirmed by reading the running
# primary's own generated pg_hba.conf, not assumed from documentation).
# Without this line, postgres-replica's pg_basebackup -- connecting from a
# different container, not 127.0.0.1 -- is rejected outright.
set -e
echo "host replication all all scram-sha-256" >> "$PGDATA/pg_hba.conf"
