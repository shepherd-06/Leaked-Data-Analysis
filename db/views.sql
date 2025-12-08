-- Views for cleaned / aggregated analysis

-- ========================
-- BASIC AGGREGATION VIEWS
-- ========================

-- Top domains from raw domain_names (no heavy normalization)
CREATE OR REPLACE VIEW vw_top_domains AS
SELECT
    LOWER(TRIM(domain)) AS domain_clean,
    COUNT(*) AS leak_count,
    COUNT(DISTINCT record_id) AS distinct_records
FROM domain_names
WHERE domain IS NOT NULL AND domain <> ''
GROUP BY LOWER(TRIM(domain))
ORDER BY leak_count DESC;

-- Password frequency
CREATE OR REPLACE VIEW vw_password_frequency AS
SELECT
    password,
    COUNT(*) AS occurrences,
    COUNT(DISTINCT record_id) AS distinct_records
FROM passwords
WHERE password IS NOT NULL AND password <> ''
GROUP BY password
ORDER BY occurrences DESC;

-- Country-level stats (with UNKNOWN bucket)
CREATE OR REPLACE VIEW vw_country_stats AS
SELECT
    COALESCE(NULLIF(TRIM(country), ''), 'UNKNOWN') AS country_clean,
    COUNT(*) AS record_count
FROM records
GROUP BY COALESCE(NULLIF(TRIM(country), ''), 'UNKNOWN')
ORDER BY record_count DESC;

-- Raw service URL aggregation
CREATE OR REPLACE VIEW vw_service_usage AS
SELECT
    LOWER(TRIM(service_url)) AS service_clean,
    COUNT(*) AS leak_count,
    COUNT(DISTINCT record_id) AS distinct_records
FROM services
WHERE service_url IS NOT NULL AND service_url <> ''
GROUP BY LOWER(TRIM(service_url))
ORDER BY leak_count DESC;

-- ============================
-- NORMALIZED SERVICE BY HOST
-- ============================

-- Collapse paths, keep host
CREATE OR REPLACE VIEW vw_service_host_usage AS
WITH parsed AS (
    SELECT
        record_id,
        LOWER(
            split_part(
                regexp_replace(service_url, '^https?://', ''),
                '/',
                1
            )
        ) AS host
    FROM services
    WHERE service_url IS NOT NULL
      AND service_url <> ''
)
SELECT
    host,
    COUNT(*) AS leak_count,
    COUNT(DISTINCT record_id) AS distinct_records
FROM parsed
GROUP BY host
ORDER BY leak_count DESC;

-- ============================
-- SERVICES BY BASE DOMAIN
-- ============================

CREATE OR REPLACE VIEW vw_service_domain_usage AS
WITH parsed AS (
    SELECT
        service_url,
        record_id,
        LOWER(
            split_part(
                regexp_replace(service_url, '^https?://', ''),
                '/',
                1
            )
        ) AS host
    FROM services
    WHERE service_url IS NOT NULL
      AND service_url <> ''
),
normalized AS (
    SELECT
        service_url,
        record_id,
        regexp_replace(
            host,
            '^(www\.|m\.|login\.|accounts\.)',
            ''
        ) AS host_stripped
    FROM parsed
),
base AS (
    SELECT
        service_url,
        record_id,
        host_stripped,
        regexp_replace(
            host_stripped,
            '^(.*\.)?([^.]+\.[^.]+)$',
            '\2'
        ) AS base_domain
    FROM normalized
)
SELECT
    base_domain,
    COUNT(*) AS leak_count,
    COUNT(DISTINCT record_id) AS distinct_records
FROM base
WHERE base_domain IS NOT NULL
  AND base_domain <> ''
GROUP BY base_domain
ORDER BY leak_count DESC;

-- ============================
-- DOMAINS BY BASE DOMAIN
-- ============================

CREATE OR REPLACE VIEW vw_domain_base_usage AS
WITH normalized AS (
    SELECT
        record_id,
        LOWER(TRIM(domain)) AS domain_clean
    FROM domain_names
    WHERE domain IS NOT NULL
      AND domain <> ''
),
base AS (
    SELECT
        record_id,
        domain_clean,
        regexp_replace(
            domain_clean,
            '^(www\.|m\.|login\.|accounts\.)',
            ''
        ) AS domain_stripped
    FROM normalized
),
final AS (
    SELECT
        record_id,
        domain_stripped,
        regexp_replace(
            domain_stripped,
            '^(.*\.)?([^.]+\.[^.]+)$',
            '\2'
        ) AS base_domain
    FROM base
)
SELECT
    base_domain,
    COUNT(*) AS leak_count,
    COUNT(DISTINCT record_id) AS distinct_records
FROM final
WHERE base_domain IS NOT NULL
  AND base_domain <> ''
GROUP BY base_domain
ORDER BY leak_count DESC;
