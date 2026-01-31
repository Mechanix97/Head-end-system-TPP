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
    batch_id BIGINT,
    owner_node_id UUID
);

CREATE INDEX IF NOT EXISTS idx_devices_by_owner ON T_DEVICES(owner_node_id);

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
    renewable BOOLEAN NOT NULL DEFAULT true,
    owner_node_id UUID
);

CREATE INDEX IF NOT EXISTS idx_scheduled_by_owner ON T_SCHEDULED_CONNECTIONS(owner_node_id);

CREATE TABLE IF NOT EXISTS T_NODES (
    node_id UUID PRIMARY KEY,
    node_name VARCHAR(255) NOT NULL UNIQUE,
    cluster_ip VARCHAR(45) NOT NULL,
    cluster_port INTEGER NOT NULL DEFAULT 6570,
    backdoor_port INTEGER NOT NULL DEFAULT 6565,
    status VARCHAR(20) NOT NULL DEFAULT 'starting',
    started_at TIMESTAMP NOT NULL DEFAULT NOW(),
    last_seen TIMESTAMP NOT NULL DEFAULT NOW(),
    bucket_count INTEGER DEFAULT 0,
    device_count INTEGER DEFAULT 0
);

-- T_BUCKET_ASSIGNMENTS is deprecated (device-based delegation replaces bucket-based)
-- Keeping commented for reference during migration
-- CREATE TABLE IF NOT EXISTS T_BUCKET_ASSIGNMENTS (
--     bucket_number INTEGER PRIMARY KEY,
--     owner_node_id UUID REFERENCES T_NODES(node_id) ON DELETE SET NULL,
--     assigned_at TIMESTAMP DEFAULT NOW()
-- );
