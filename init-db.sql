DO
$$
BEGIN
   IF NOT EXISTS (
      SELECT FROM pg_database WHERE datname = 'hes'
   ) THEN
      PERFORM dblink_exec('dbname=postgres', 'CREATE DATABASE hes');
   END IF;
END

BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'connectionstatus') THEN
        CREATE TYPE connectionstatus AS ENUM ('active', 'pending_ack', 'lost');
    END IF;
END   $$;
$$;

\connect hes;

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS T_ACTIVE_CONNECTIONS (
    device_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    ip TEXT,  -- Agregá columnas extras como ip y bucket si no están
    bucket INTEGER,
    last_connection TIMESTAMP NOT NULL DEFAULT NOW(),
    next_wakeup TIMESTAMP,
    status connectionstatus NOT NULL DEFAULT 'active'
);