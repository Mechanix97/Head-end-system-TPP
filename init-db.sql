DO $$ 
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'hes') THEN
        CREATE DATABASE hes;
    END IF;

    \c hes

    IF NOT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'T_ACTIVE_CONNECTIONS') THEN
        CREATE TABLE T_ACTIVE_CONNECTIONS (
            device_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            connection_time TIMESTAMP NOT NULL DEFAULT NOW(),
            next_wakeup TIMESTAMP,
            status VARCHAR(20) NOT NULL DEFAULT 'active',
            CONSTRAINT valid_status CHECK (status IN ('active', 'pending', 'closed'))
        );
    END IF;
END $$;
