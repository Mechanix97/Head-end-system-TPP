CREATE TABLE IF NOT EXISTS T_ACTIVE_CONNECTIONS (
    device_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    last_connection TIMESTAMP NOT NULL DEFAULT NOW(),
    next_wakeup TIMESTAMP,
    ip VARCHAR(20),
    bucket integer,
    status VARCHAR(20) NOT NULL ,
    CONSTRAINT valid_status CHECK (status IN ('active', 'pending_ack', 'lost'))
);