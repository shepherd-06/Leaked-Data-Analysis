-- ==============
-- CORE TABLES
-- ==============

CREATE TABLE IF NOT EXISTS records (
    id BIGINT PRIMARY KEY,
    device_ip_addr TEXT,
    timestamp DATE,
    country TEXT,
    keyboard TEXT
);

CREATE TABLE IF NOT EXISTS passwords (
    id BIGSERIAL PRIMARY KEY,
    record_id BIGINT REFERENCES records(id) ON DELETE CASCADE,
    password TEXT
);

CREATE TABLE IF NOT EXISTS domain_names (
    id BIGSERIAL PRIMARY KEY,
    record_id BIGINT REFERENCES records(id) ON DELETE CASCADE,
    domain TEXT
);

CREATE TABLE IF NOT EXISTS services (
    id BIGSERIAL PRIMARY KEY,
    record_id BIGINT REFERENCES records(id) ON DELETE CASCADE,
    service_url TEXT
);

CREATE TABLE IF NOT EXISTS usernames (
    id BIGSERIAL PRIMARY KEY,
    record_id BIGINT REFERENCES records(id) ON DELETE CASCADE,
    username TEXT
);

CREATE TABLE IF NOT EXISTS email_addresses (
    id BIGSERIAL PRIMARY KEY,
    record_id BIGINT REFERENCES records(id) ON DELETE CASCADE,
    email TEXT
);

CREATE TABLE IF NOT EXISTS processed_files (
    filename TEXT PRIMARY KEY,
    processed_at TIMESTAMPTZ DEFAULT NOW(),
    success BOOLEAN NOT NULL,
    error_msg TEXT
);

-- ==============
-- INDEXES
-- ==============

-- records
CREATE INDEX IF NOT EXISTS idx_records_country
    ON records (country);

CREATE INDEX IF NOT EXISTS idx_records_timestamp
    ON records (timestamp);

-- FKs
CREATE INDEX IF NOT EXISTS idx_passwords_record_id
    ON passwords (record_id);

CREATE INDEX IF NOT EXISTS idx_domain_names_record_id
    ON domain_names (record_id);

CREATE INDEX IF NOT EXISTS idx_services_record_id
    ON services (record_id);

CREATE INDEX IF NOT EXISTS idx_usernames_record_id
    ON usernames (record_id);

CREATE INDEX IF NOT EXISTS idx_email_addresses_record_id
    ON email_addresses (record_id);

-- search optimization
CREATE INDEX IF NOT EXISTS idx_email_addresses_email
    ON email_addresses (LOWER(email));

CREATE INDEX IF NOT EXISTS idx_domain_names_domain
    ON domain_names (LOWER(domain));
