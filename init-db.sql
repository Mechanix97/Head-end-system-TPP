DO $$ 
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'hes') THEN
        CREATE DATABASE hes;
    END IF;
END $$;
