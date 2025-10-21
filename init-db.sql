DO
$$
BEGIN
   IF NOT EXISTS (
      SELECT FROM pg_database WHERE datname = 'hes'
   ) THEN
      PERFORM dblink_exec('dbname=postgres', 'CREATE DATABASE hes');
   END IF;
END
$$;

\connect hes;

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS T_ACTIVE_CONNECTIONS (
    device_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    last_connection TIMESTAMP NOT NULL DEFAULT NOW(),
    next_wakeup TIMESTAMP,
    ip VARCHAR(20),
    bucket INTEGER,
    status VARCHAR(20) NOT NULL,
    CONSTRAINT valid_status CHECK (status IN ('active', 'pending_ack', 'lost'))
);
