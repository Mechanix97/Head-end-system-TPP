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

DO
$$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'connectionstatus') THEN
        CREATE TYPE connectionstatus AS ENUM ('active', 'pending_ack', 'lost');
    END IF;
END   
$$;

DO
$$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'registrationstatus') THEN
        CREATE TYPE registrationstatus AS ENUM ('registered', 'pending_ack', 'ack_timeout');
    END IF;
END   
$$;

DO
$$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'scheduledstatus') THEN
        CREATE TYPE scheduledstatus AS ENUM ('awaiting', 'lost', 'done');
    END IF;
END   
$$;

\connect hes;

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS T_DEVICES (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    IPv4 TEXT,
    IPv6 TEXT,
    MAC TEXT,
    factory_id BIGINT,
    batch_id BIGINT
);

CREATE TABLE IF NOT EXISTS T_DEVICE_REGISTRATION (
    FK_DEVICE UUID REFERENCES T_DEVICES(id),
    registration_status registrationstatus NOT NULL DEFAULT 'pending_ack',
    registration_time TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS T_BUCKETS (
    FK_DEVICE UUID REFERENCES T_DEVICES(id),
    bucket INTEGER,
    PRIMARY KEY (FK_DEVICE)
);

CREATE TABLE IF NOT EXISTS T_SCHEDULED_CONNECTIONS (
    FK_DEVICE UUID REFERENCES T_DEVICES(id),
    schedule_time TIMESTAMP NOT NULL,
    connection_time TIMESTAMP,
    status scheduledstatus NOT NULL default 'awaiting',
    job_id UUID,
    renewable BOOLEAN NOT NULL DEFAULT true
);
