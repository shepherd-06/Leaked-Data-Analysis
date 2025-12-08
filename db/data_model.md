# Data Model & Cleaning

## Core Tables

- **records**
  - 1 row ≈ 1 infected device / JSON file
  - Columns: `id`, `device_ip_addr`, `timestamp`, `country`, `keyboard`

- **passwords**
  - Multiple rows per record
  - Raw passwords captured by malware (only password hash present)

- **domain_names**
  - Domains observed (may contain subdomains and noise)

- **services**
  - Full service URLs (login, signup, reset flows, etc.)

- **usernames**
  - Usernames associated with credentials

- **email_addresses**
  - Emails associated with credentials

- **processed_files**
  - Tracks which JSON files were processed and whether they succeeded

## Cleaning / Normalization Strategy

Implemented as SQL views in `db/views.sql`:

- `vw_password_frequency`  
  - Counts how often each password appears and how many distinct records use it.

- `vw_country_stats`  
  - Normalizes missing country to `UNKNOWN` and gives record counts per country.

- `vw_top_domains`  
  - Lowercases and trims domain strings for basic aggregation.

- `vw_service_usage`  
  - Aggregates by full cleaned URL (service level).

- `vw_service_host_usage`
  - Strips protocol + path, aggregates by host only  
    (e.g. `https://accounts.google.com/signin/...` -> `accounts.google.com`)

- `vw_service_domain_usage`
  - Normalizes hosts to base domains  
    - Removes common prefixes (`www.`, `m.`, `login.`, `accounts.`)  
    - Reduces to last two labels (e.g. `accounts.google.com` → `google.com`)

- `vw_domain_base_usage`
  - Applies similar base-domain logic to `domain_names`.

