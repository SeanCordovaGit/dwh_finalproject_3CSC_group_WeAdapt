-- Create schemas for different layers
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS warehouse;
CREATE SCHEMA IF NOT EXISTS presentation;

-- Grant privileges
GRANT ALL PRIVILEGES ON SCHEMA staging TO shopzada;
GRANT ALL PRIVILEGES ON SCHEMA warehouse TO shopzada;
GRANT ALL PRIVILEGES ON SCHEMA presentation TO shopzada;