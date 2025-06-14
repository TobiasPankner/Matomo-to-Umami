import argparse
import hashlib
import sys
import uuid
from datetime import datetime, date, timedelta
from typing import Dict, Any
from urllib.parse import urlparse

import requests
import tldextract
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeRemainingColumn, \
    MofNCompleteColumn
from rich.table import Table


def extract_base_domain(url: str):
    try:
        extracted = tldextract.extract(url)
        return f"{extracted.domain}.{extracted.suffix}"
    except:
        return None


def generate_uuid(seed_value: str = None) -> str:
    """Generate a UUID, optionally seeded for consistency"""
    if seed_value:
        # Create deterministic UUID from seed
        hash_value = hashlib.md5(seed_value.encode()).hexdigest()
        return str(uuid.UUID(hash_value))
    return str(uuid.uuid4())


def safe_sql_string(value: Any) -> str:
    """Safely format a value for SQL insertion"""
    if value is None:
        return 'NULL'
    if isinstance(value, str):
        # Escape single quotes and handle special characters
        return f"'{value.replace('\'', '\'\'').replace(chr(0), '')}'"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, bool):
        return 'true' if value else 'false'
    return f"'{str(value)}'"


def parse_timestamp(timestamp: int) -> str:
    """Convert Unix timestamp to PostgreSQL timestamp"""
    dt = datetime.fromtimestamp(timestamp)
    return f"'{dt.isoformat()}'"


def parse_user_agent_info(visit_data: Dict) -> Dict[str, str]:
    """Extract browser, OS, and device info from visit data"""
    browser = visit_data.get('browserName', 'Unknown').lower()
    if "chrome" in browser:
        browser = "chrome"
    elif "edge" in browser:
        browser = "edge-chromium"
    elif "firefox" in browser:
        browser = "firefox"
    elif "opera" in browser:
        browser = "opera"
    elif "mobile safari" in browser:
        browser = "ios"
    elif "safari" in browser:
        browser = "safari"
    elif "yandex" in browser:
        browser = "yandexbrowser"
    elif "samsung" in browser:
        browser = "samsung"
    elif "google search app" in browser:
        browser = "chromium-webview"
    elif "silk" in browser:
        browser = "silk"

    os_info = visit_data.get('operatingSystemName', 'Unknown').lower()
    if "linux" in os_info or "ubuntu" in os_info:
        os_info = "Linux"
    elif "chrome" in os_info:
        os_info = "Chrome OS"
    elif "windows" in os_info:
        os_detail_info = visit_data.get('operatingSystem', 'Unknown').lower()
        if "windows 7" in os_detail_info:
            os_info = "Windows 7"
        elif "windows 8.1" in os_detail_info:
            os_info = "Windows 8.1"
        elif "windows 10" in os_detail_info or "windows 11" in os_detail_info:
            os_info = "Windows 10"
    elif "ios" in os_info:
        os_info = "iOS"
    elif "mac" in os_info:
        os_info = "Mac OS"
    elif "android" in os_info:
        os_info = "Android OS"

    device = visit_data.get('deviceType', 'Unknown').lower()
    if "desktop" in device:
        device = "desktop"
    elif "tablet" in device:
        device = "tablet"
    elif "smartphone" in device or "phablet" in device:
        device = "mobile"

    return {
        'browser': browser[:20],  # Truncate to schema limit
        'os': os_info[:20],
        'device': device[:20]
    }


def create_session_insert(visit_data: Dict, website_id: str, session_mapping: Dict) -> str:
    """Create INSERT statement for session table"""
    session_id = generate_uuid(f"session_{visit_data['idVisit']}")
    session_mapping[visit_data['idVisit']] = session_id

    ua_info = parse_user_agent_info(visit_data)

    # Extract location info
    country = visit_data.get('countryCode', '').upper()[:2] if visit_data.get('countryCode') else None
    region = visit_data.get('regionCode', '')[:20] if visit_data.get('regionCode') else None
    city = visit_data.get('city', '')[:50] if visit_data.get('city') else None

    # Extract screen resolution
    screen = visit_data.get('resolution', '')[:11] if visit_data.get('resolution') else None

    # Extract language
    language = visit_data.get('languageCode', '')[:35] if visit_data.get('languageCode') else None

    # Use first action timestamp for session creation
    created_at = parse_timestamp(visit_data['firstActionTimestamp'])

    return f"""INSERT INTO session (session_id, website_id, browser, os, device, screen, language, country, region, city, distinct_id, created_at) 
VALUES ({safe_sql_string(session_id)}, {safe_sql_string(website_id)}, {safe_sql_string(ua_info['browser'])}, {safe_sql_string(ua_info['os'])}, {safe_sql_string(ua_info['device'])}, {safe_sql_string(screen)}, {safe_sql_string(language)}, {safe_sql_string(country)}, {safe_sql_string(region)}, {safe_sql_string(city)}, {'NULL'}, {created_at});"""


def create_website_event_insert(action: Dict, visit_data: Dict, visit_id: str, website_id: str,
                                session_mapping: Dict) -> str:
    """Create INSERT statement for website_event table"""
    event_id = uuid.uuid4()
    session_id = session_mapping[visit_data['idVisit']]

    # Parse URL
    url = action.get('url', '')
    if '?' in url:
        url_path = url.split('?')[0]
        url_query = url.split('?')[1][:500]  # Truncate to schema limit
    else:
        url_path = url
        url_query = None

    # Extract path (remove domain)
    if url_path.startswith('http'):
        url_path = '/' + '/'.join(url_path.split('/')[3:])
    url_path = url_path[:500]  # Truncate to schema limit

    # Get referrer info
    referrer_url = visit_data.get('referrerUrl', '')
    referrer_domain = extract_base_domain(referrer_url) if referrer_url else None

    parsed_url = urlparse(referrer_url) if referrer_url else None
    referrer_path = parsed_url.path if parsed_url else "/"
    referrer_query = f"?{parsed_url.query}" if parsed_url and parsed_url.query else "" if referrer_url else ""

    referrer_domain = referrer_domain[:500] if referrer_domain else referrer_domain
    referrer_path = referrer_path[:500] if referrer_path else referrer_path
    referrer_query = referrer_query[:500] if referrer_query else referrer_query

    # Page title
    page_title = action.get('pageTitle', action.get('title', ''))[:500] if action.get('pageTitle') or action.get(
        'title') else None

    # Created timestamp
    created_at = parse_timestamp(action['timestamp'])

    # Event type 1 for pageview
    event_type = 1

    # Hostname extraction
    hostname = None
    if url.startswith('http'):
        hostname = url.split('/')[2][:100]

    return f"""INSERT INTO website_event (event_id, website_id, session_id, visit_id, created_at, url_path, url_query, referrer_path, referrer_query, referrer_domain, page_title, event_type, hostname) 
VALUES ({safe_sql_string(event_id)}, {safe_sql_string(website_id)}, {safe_sql_string(session_id)}, {safe_sql_string(visit_id)}, {created_at}, {safe_sql_string(url_path)}, {safe_sql_string(url_query)}, {safe_sql_string(referrer_path)}, {safe_sql_string(referrer_query)}, {safe_sql_string(referrer_domain)}, {safe_sql_string(page_title)}, {event_type}, {safe_sql_string(hostname)});"""


def make_matomo_request(matomo_url: str, site_id: str, token_auth: str, target_date: str):
    """Make API request to Matomo for a single day"""
    params = {
        'module': 'API',
        'method': 'Live.getLastVisitsDetails',
        'idSite': site_id,
        'period': 'day',
        'date': target_date,
        'format': 'JSON',
        'token_auth': token_auth,
        'filter_limit': -1
    }

    try:
        response = requests.get(f"{matomo_url}/index.php", params=params, timeout=300)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error fetching data for {target_date}: {e}")
        return None


def process_day_data(data: list, target_date: date, website_id: str, session_mapping: Dict, output_file) -> int:
    """Process one day's data and write SQL statements directly to file"""
    if not isinstance(data, list):
        return 0

    events_count = 0

    # Write day header
    output_file.write(f"-- Processing data for: {target_date.isoformat()}\n")
    output_file.write("\n")

    for visit_data in data:
        try:
            # Skip if this session was already processed (duplicate visit ID)
            if visit_data['idVisit'] not in session_mapping:
                session_sql = create_session_insert(visit_data, website_id, session_mapping)
                output_file.write(session_sql + "\n")

            # Generate visit ID for this session
            visit_id = generate_uuid(f"visit_{visit_data['idVisit']}")

            # Process actions (page views)
            if 'actionDetails' in visit_data:
                for action in visit_data['actionDetails']:
                    if action.get('type') == 'action':  # Page view
                        event_sql = create_website_event_insert(action, visit_data, visit_id, website_id,
                                                                session_mapping)
                        output_file.write(event_sql + "\n")
                        events_count += 1

            output_file.write("\n")  # Add empty line between visits

        except Exception as e:
            print(f"Error processing visit {visit_data.get('idVisit', 'unknown')} on {target_date}: {e}")
            continue

    return events_count


def parse_date(date_string: str) -> date:
    """Parse date string in YYYY-MM-DD format"""
    try:
        return datetime.strptime(date_string, '%Y-%m-%d').date()
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date format: {date_string}. Expected YYYY-MM-DD")


def migrate_matomo_to_umami(matomo_url: str, site_id: str, token_auth: str, website_id: str,
                            start_date: date = None, end_date: date = None, output_file: str = "migration.sql"):
    console = Console()

    # Validate website_id is a valid UUID format
    try:
        uuid.UUID(website_id)
    except ValueError:
        console.print("[red]❌ Error: website_id must be a valid UUID[/red]")
        sys.exit(1)

    # Set default date range if not provided
    if end_date is None:
        end_date = date.today()
    if start_date is None:
        start_date = end_date - timedelta(days=2 * 365)  # Default to 2 years

    # Validate date range
    if start_date > end_date:
        console.print("[red]❌ Error: start_date must be before end_date[/red]")
        sys.exit(1)

    total_days = (end_date - start_date).days + 1

    # Display initial info panel
    info_table = Table.grid(padding=1)
    info_table.add_column(style="cyan", no_wrap=True)
    info_table.add_column()
    info_table.add_row("📅 Date range:", f"{start_date} to {end_date}")
    info_table.add_row("📊 Total days:", f"{total_days:,}")
    info_table.add_row("🌐 Matomo URL:", matomo_url)
    info_table.add_row("🆔 Site ID:", site_id)
    info_table.add_row("📁 Output file:", output_file)

    console.print(Panel(info_table, title="🚀 [bold green]Matomo to Umami Migration[/bold green]", border_style="green"))

    # Track sessions and statistics
    session_mapping = {}
    total_events = 0
    total_days_processed = 0
    failed_days = 0

    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            # Write header
            f.write("-- Generated SQL migration from Matomo to Umami\n")
            f.write(f"-- Generated on: {datetime.now().isoformat()}\n")
            f.write(f"-- Website ID: {website_id}\n")
            f.write(f"-- Date range: {start_date} to {end_date}\n")
            f.write(f"-- Matomo URL: {matomo_url}\n")
            f.write(f"-- Site ID: {site_id}\n")
            f.write("\n")
            f.write("BEGIN;\n")
            f.write("\n")
            f.write("SET client_encoding = 'UTF8';\n")
            f.write("\n")

            # Progress tracking with Rich
            with Progress(
                    SpinnerColumn(),
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(),
                    MofNCompleteColumn(),
                    TextColumn("•"),
                    TimeRemainingColumn(),
                    console=console,
                    transient=False
            ) as progress:

                main_task = progress.add_task("[green]Processing days...", total=total_days)

                current_date = start_date
                day_counter = 0

                while current_date <= end_date:
                    day_counter += 1
                    iso_date = current_date.strftime('%Y-%m-%d')

                    # Update progress description
                    progress.update(main_task, description=f"[green]Processing {iso_date}...")

                    # Fetch data for this day
                    data = make_matomo_request(matomo_url, site_id, token_auth, iso_date)

                    if data is not None:
                        # Process and write SQL statements immediately
                        day_events = process_day_data(data, current_date, website_id, session_mapping, f)
                        total_events += day_events
                        total_days_processed += 1
                    else:
                        failed_days += 1

                    # Update progress
                    progress.update(main_task, advance=1)

                    current_date += timedelta(days=1)

            # Write footer
            f.write("COMMIT;\n")
            f.write("\n")
            f.write(f"-- Migration complete\n")
            f.write(f"-- Total sessions: {len(session_mapping)}\n")
            f.write(f"-- Total events: {total_events}\n")
            f.write(f"-- Days processed: {total_days_processed}\n")
            f.write(f"-- Failed days: {failed_days}\n")

    except Exception as e:
        console.print(f"[red]❌ Error writing SQL file: {e}[/red]")
        sys.exit(1)

    # Final summary with Rich table
    summary_table = Table.grid(padding=1)
    summary_table.add_column(style="cyan", no_wrap=True)
    summary_table.add_column()
    summary_table.add_row("📁 Output file:", output_file)
    summary_table.add_row("📊 Days processed:", f"{total_days_processed:,}/{total_days:,}")
    summary_table.add_row("❌ Failed days:", f"{failed_days:,}")
    summary_table.add_row("👥 Total sessions:", f"{len(session_mapping):,}")
    summary_table.add_row("📈 Total events:", f"{total_events:,}")
    if total_events > 0 and total_days_processed > 0:
        summary_table.add_row("📊 Avg events/day:", f"{total_events / total_days_processed:.1f}")

    console.print(Panel(summary_table, title="✅ [bold green]Migration Completed Successfully![/bold green]",
                        border_style="green"))


def main():
    parser = argparse.ArgumentParser(
        description='Migrate Matomo analytics data directly to Umami SQL format',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Default: last 2 years
  python matomo2umami.py https://tracking.example.com 1 <MATOMO_TOKEN> <UMAMI_UID>
  
  # Custom date range
  python matomo2umami.py https://tracking.example.com 1 <MATOMO_TOKEN> <UMAMI_UID> --start-date 2023-01-01 --end-date 2023-12-31
  
  # With custom output file
  python matomo2umami.py https://tracking.example.com 1 <MATOMO_TOKEN> <UMAMI_UID> -o my_migration.sql
  
  # Specific month
  python matomo2umami.py https://tracking.example.com 1 <MATOMO_TOKEN> <UMAMI_UID> --start-date 2024-01-01 --end-date 2024-01-31
        """
    )

    parser.add_argument('matomo_url', help='Matomo URL (e.g., https://tracking.example.com)')
    parser.add_argument('site_id', help='Matomo site ID')
    parser.add_argument('token_auth', help='Matomo API authentication token')
    parser.add_argument('website_id', help='Umami website UUID for the target database')
    parser.add_argument('-o', '--output', help='Output SQL file path (default: migration.sql)', default='migration.sql')
    parser.add_argument('--start-date', type=parse_date, help='Start date in YYYY-MM-DD format (default: 2 years ago)')
    parser.add_argument('--end-date', type=parse_date, help='End date in YYYY-MM-DD format (default: today)')

    args = parser.parse_args()

    # Clean up Matomo URL (remove trailing slash)
    matomo_url = args.matomo_url.rstrip('/')

    migrate_matomo_to_umami(
        matomo_url,
        args.site_id,
        args.token_auth,
        args.website_id,
        args.start_date,
        args.end_date,
        args.output
    )


if __name__ == '__main__':
    main()
