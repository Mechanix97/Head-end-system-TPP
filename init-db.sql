CREATE TABLE IF NOT EXISTS T_ACTIVE_CONNECTIONS (
    device_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    connection_time TIMESTAMP NOT NULL DEFAULT NOW(),
    next_wakeup TIMESTAMP,
    ip VARCHAR(20),
    status VARCHAR(20) NOT NULL DEFAULT 'active',
    CONSTRAINT valid_status CHECK (status IN ('active', 'pending', 'closed'))
);