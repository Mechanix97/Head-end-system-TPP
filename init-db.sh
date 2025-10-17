#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    SELECT 'DB exists';  # No hace nada si existe
EOSQL

if ! psql -U "$POSTGRES_USER" -tAc "SELECT 1 FROM pg_database WHERE datname='$POSTGRES_DB'"; then
    psql -U "$POSTGRES_USER" -c "CREATE DATABASE $POSTGRES_DB;"
fi
