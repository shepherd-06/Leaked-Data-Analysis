-- =========================================================
-- Cleaning & Aggregation SQL
--
-- Purpose:
--   This script builds derived tables and aggregates on top
--   of the raw imported data (records, passwords, domains,
--   services, usernames, email_addresses).
--
--   It is designed for:
--     - Faster analysis
--     - Cleaner exports to Tableau / Power BI
--     - Documenting how the dataset was cleaned and summarized
--
-- Notes:
--   - Raw passwords in this dataset are not in plaintext;
--     they appear as transformed / hashed values. We still
--     analyze frequency and reuse, but cannot compute entropy
--     in a meaningful way.
--
-- Prerequisites:
--   Base tables already exist and are populated:
--     records, passwords, domain_names, services,
--     usernames, email_addresses, processed_files
-- =========================================================


-- =========================================================
-- 2A. PER-RECORD AGGREGATES (COUNTS)
--
-- Goal:
--   For each record_id, compute how many passwords, domains,
--   services, usernames, and emails are associated with it.
--
-- Rationale:
--   - These aggregate tables avoid heavy COUNT(DISTINCT ...)
--     across multiple large joins.
--   - They make it easy to build a final per-record summary
--     table (record_aggregates) used by BI tools.
-- =========================================================


-- 2A.1 Aggregate password counts per record
CREATE TABLE IF NOT EXISTS agg_passwords AS
SELECT
    record_id,
    COUNT(*) AS password_count
FROM passwords
GROUP BY record_id;

CREATE INDEX IF NOT EXISTS idx_agg_passwords_record_id
    ON agg_passwords(record_id);


-- 2A.2 Aggregate domain counts per record
CREATE TABLE IF NOT EXISTS agg_domains AS
SELECT
    record_id,
    COUNT(*) AS domain_count
FROM domain_names
GROUP BY record_id;

CREATE INDEX IF NOT EXISTS idx_agg_domains_record_id
    ON agg_domains(record_id);


-- 2A.3 Aggregate service counts per record
CREATE TABLE IF NOT EXISTS agg_services AS
SELECT
    record_id,
    COUNT(*) AS service_count
FROM services
GROUP BY record_id;

CREATE INDEX IF NOT EXISTS idx_agg_services_record_id
    ON agg_services(record_id);


-- 2A.4 Aggregate username counts per record
CREATE TABLE IF NOT EXISTS agg_usernames AS
SELECT
    record_id,
    COUNT(*) AS username_count
FROM usernames
GROUP BY record_id;

CREATE INDEX IF NOT EXISTS idx_agg_usernames_record_id
    ON agg_usernames(record_id);


-- 2A.5 Aggregate email counts per record
CREATE TABLE IF NOT EXISTS agg_emails AS
SELECT
    record_id,
    COUNT(*) AS email_count
FROM email_addresses
GROUP BY record_id;

CREATE INDEX IF NOT EXISTS idx_agg_emails_record_id
    ON agg_emails(record_id);



-- =========================================================
-- 2B. CLEAN / NORMALIZED TABLES
--
-- Goal:
--   Create cleaned versions of domains, emails, and passwords.
--
-- Rationale:
--   - Lowercasing and trimming makes grouping and joins
--     deterministic.
--   - Removing empty / NULL values removes noise before any
--     analysis or export.
-- =========================================================


-- 2B.1 Clean domains:
--   - lowercased
--   - trimmed
--   - NULL / empty domains removed
CREATE TABLE IF NOT EXISTS domain_names_clean AS
SELECT
    id,
    record_id,
    LOWER(TRIM(domain)) AS domain_clean
FROM domain_names
WHERE domain IS NOT NULL
  AND domain <> '';

CREATE INDEX IF NOT EXISTS idx_domain_names_clean_domain
    ON domain_names_clean(domain_clean);


-- 2B.2 Clean emails:
--   - lowercased
--   - trimmed
--   - NULL / empty emails removed
CREATE TABLE IF NOT EXISTS email_addresses_clean AS
SELECT
    id,
    record_id,
    LOWER(TRIM(email)) AS email_clean
FROM email_addresses
WHERE email IS NOT NULL
  AND email <> '';

CREATE INDEX IF NOT EXISTS idx_email_addresses_clean_email
    ON email_addresses_clean(email_clean);


-- 2B.3 Clean passwords:
--   - NULL / empty passwords removed
--   - Note: these are hashed/obfuscated values in this dataset
CREATE TABLE IF NOT EXISTS passwords_clean AS
SELECT
    id,
    record_id,
    password
FROM passwords
WHERE password IS NOT NULL
  AND password <> '';

CREATE INDEX IF NOT EXISTS idx_passwords_clean_record_id
    ON passwords_clean(record_id);



-- =========================================================
-- 2C. FINAL PER-RECORD SUMMARY TABLE
--
-- Table: record_aggregates
--
-- Goal:
--   One row per record_id with:
--     - basic metadata from records
--     - counts of passwords, domains, services,
--       usernames, emails from agg_* tables
--
-- Rationale:
--   - This is the main "fact table" for Tableau / Power BI.
--   - Avoids heavy joins and aggregation inside BI tools.
-- =========================================================


CREATE TABLE IF NOT EXISTS record_aggregates AS
SELECT
    r.id AS record_id,
    r.country,
    r.timestamp,
    r.device_ip_addr,
    COALESCE(p.password_count, 0) AS password_count,
    COALESCE(d.domain_count,   0) AS domain_count,
    COALESCE(s.service_count,  0) AS service_count,
    COALESCE(u.username_count, 0) AS username_count,
    COALESCE(e.email_count,    0) AS email_count
FROM records r
LEFT JOIN agg_passwords p ON p.record_id = r.id
LEFT JOIN agg_domains   d ON d.record_id = r.id
LEFT JOIN agg_services  s ON s.record_id = r.id
LEFT JOIN agg_usernames u ON u.record_id = r.id
LEFT JOIN agg_emails    e ON e.record_id = r.id;

CREATE INDEX IF NOT EXISTS idx_record_aggregates_record_id
    ON record_aggregates(record_id);



-- =========================================================
-- 2D. HIGH-FREQUENCY PASSWORDS (HASHED VALUES)
--
-- Table: weak_passwords
--
-- Goal:
--   Identify passwords (hashed/obfuscated form) that appear
--   extremely frequently across the dataset.
--
-- Rationale:
--   - Even if we don't know the plaintext, repetition gives
--     strong signals of weak / reused choices.
--   - These hashes can be used to tag records that likely
--     used a very common password.
--
-- Note:
--   - The threshold (COUNT(*) > 500) is arbitrary and can be
--     adjusted depending on how strict you want to be.
-- =========================================================


CREATE TABLE IF NOT EXISTS weak_passwords AS
SELECT
    password,
    COUNT(*) AS occurrences,
    COUNT(DISTINCT record_id) AS distinct_records
FROM passwords_clean
GROUP BY password
HAVING COUNT(*) > 500
ORDER BY occurrences DESC;

CREATE INDEX IF NOT EXISTS idx_weak_passwords_password
    ON weak_passwords(password);



-- Optional: flag records that use any "weak" (high-frequency)
-- password hash in the record_aggregates table.
-- This adds a boolean flag for quick filtering in BI tools.

ALTER TABLE record_aggregates
    ADD COLUMN IF NOT EXISTS has_weak_password BOOLEAN;

UPDATE record_aggregates ra
SET has_weak_password = EXISTS (
    SELECT 1
    FROM passwords_clean pc
    JOIN weak_passwords w ON w.password = pc.password
    WHERE pc.record_id = ra.record_id
);



-- =========================================================
-- End of cleaning_and_aggregation.sql
-- =========================================================
