# Matomo to Umami Migration Tool

A Python script that migrates analytics data from Matomo to Umami by generating SQL statements for direct database import.

## Features

- **Complete Data Migration**: Transfers sessions, page views, and visitor information
- **Flexible Date Ranges**: Migrate specific time periods or default to last 2 years
- **Data Preservation**: Maintains visitor sessions, browser info, location data, and referrer information

##  Prerequisites

### Software Requirements
- Python 3.7+
- Access to your Matomo instance
- PostgreSQL database (Umami backend)

### Python Dependencies
Install required packages:
```bash
pip install -r requirements.txt
```

## Setup

### 1. Get Your Matomo API Token

You need to generate an API authentication token from your Matomo dashboard.

1. Log in to your Matomo dashboard
2. Go to **Personal Settings** → **Security** → **Auth Tokens**
3. Give it a descriptive name 
5. Untick the "Only allow secure requests" checkbox and click **Create New Token**
6. Copy the generated token (you'll need this for the script)

![image](https://github.com/user-attachments/assets/ce532507-c9b9-4b34-9aa1-71880351782e)


### 2. Find Your Matomo Site ID

1. In your Matomo dashboard, go to **Administration** → **Websites** → **Manage**
2. Note the ID number next to your website (usually 1 for the first site)

### 3. Get Your Umami Website UUID

1. Log in to your Umami dashboard
2. Go to **Settings** → **Websites**
3. Click on **Edit** to view details
4. Copy the Website ID (UUID format)

## Usage

### Basic Usage (Last 2 Years)
```bash
python matomo2umami.py https://your-matomo-url.com SITE_ID MATOMO_TOKEN UMAMI_WEBSITE_UUID
```

### Custom Date Range
```bash
python matomo2umami.py https://your-matomo-url.com SITE_ID MATOMO_TOKEN UMAMI_WEBSITE_UUID --start-date 2023-01-01 --end-date 2023-12-31
```

### Custom Output File
```bash
python matomo2umami.py https://your-matomo-url.com SITE_ID MATOMO_TOKEN UMAMI_WEBSITE_UUID -o my_migration.sql
```

### Complete Example
```bash
python matomo2umami.py https://analytics.example.com 1 1234567890abcdef 550e8400-e29b-41d4-a716-446655440000 --start-date 2024-01-01 --end-date 2024-03-31 -o march_2024_migration.sql
```

## Command Line Arguments

| Argument | Required | Description | Example |
|----------|----------|-------------|---------|
| `matomo_url` | ✅ | Your Matomo installation URL | `https://analytics.example.com` |
| `site_id` | ✅ | Matomo site ID (usually 1) | `1` |
| `token_auth` | ✅ | Matomo API authentication token | `1234567890abcdef` |
| `website_id` | ✅ | Umami website UUID | `550e8400-e29b-41d4-a716-446655440000` |
| `-o, --output` | ❌ | Output SQL file path | `migration.sql` (default) |
| `--start-date` | ❌ | Start date (YYYY-MM-DD) | `2023-01-01` |
| `--end-date` | ❌ | End date (YYYY-MM-DD) | `2023-12-31` |

## Output

The script generates a PostgreSQL-compatible SQL file containing:

- **File Header**: Migration metadata and settings
- **Session Records**: Visitor sessions with device/location info
- **Event Records**: Page view events with URL and referrer data
- **Transaction Wrapper**: BEGIN/COMMIT for safe execution

### Example Output Structure
```sql
-- Generated SQL migration from Matomo to Umami
-- Generated on: 2024-01-15T10:30:00
-- Website ID: 550e8400-e29b-41d4-a716-446655440000
-- Date range: 2023-01-01 to 2023-12-31

BEGIN;

INSERT INTO session (session_id, website_id, browser, os, device, ...) VALUES (...);
INSERT INTO website_event (event_id, website_id, session_id, ...) VALUES (...);

COMMIT;

-- Migration complete
-- Total sessions: 1,234
-- Total events: 5,678
```

## Database Import

After generating the SQL file, import it into your Umami PostgreSQL database:

```bash
# Using psql command line
psql -h localhost -U umami_user -d umami_db -f migration.sql

# Or using PostgreSQL client of your choice
```

**⚠️ Important**: Always backup your Umami database before importing!
