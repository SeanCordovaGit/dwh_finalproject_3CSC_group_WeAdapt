-- Create weadapt user with password
CREATE USER weadapt WITH PASSWORD 'weadapt123';

-- Grant privileges on database
GRANT ALL PRIVILEGES ON DATABASE weadapt_dwh TO weadapt;

-- Create read-only user for BI tools
CREATE USER weadapt_readonly WITH PASSWORD 'readonly123';
GRANT CONNECT ON DATABASE weadapt_dwh TO weadapt_readonly;
GRANT USAGE ON SCHEMA public TO weadapt_readonly;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO weadapt_readonly;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO weadapt_readonly;
