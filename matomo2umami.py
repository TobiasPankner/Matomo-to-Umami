import argparse
import hashlib
import sys
import uuid
from datetime import datetime, date, timedelta
from typing import Dict, Any, List, Tuple
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


def safe_sql_value(value: Any) -> str:
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


def create_session_data(visit_data: Dict, website_id: str, session_mapping: Dict) -> Tuple[str, List[str]]:
    """Create session data tuple for batch insert"""
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

    values = [
        safe_sql_value(session_id),
        safe_sql_value(website_id),
        safe_sql_value(ua_info['browser']),
        safe_sql_value(ua_info['os']),
        safe_sql_value(ua_info['device']),
        safe_sql_value(screen),
        safe_sql_value(language),
        safe_sql_value(country),
        safe_sql_value(region),
        safe_sql_value(city),
        'NULL',  # distinct_id
        created_at
    ]

    return session_id, values


def create_website_event_data(action: Dict, visit_data: Dict, visit_id: str, website_id: str,
                              session_mapping: Dict) -> List[str]:
    """Create website event data tuple for batch insert"""
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

    return [
        safe_sql_value(event_id),
        safe_sql_value(website_id),
        safe_sql_value(session_id),
        safe_sql_value(visit_id),
        created_at,
        safe_sql_value(url_path),
        safe_sql_value(url_query),
        safe_sql_value(referrer_path),
        safe_sql_value(referrer_query),
        safe_sql_value(referrer_domain),
        safe_sql_value(page_title),
        str(event_type),
        safe_sql_value(hostname)
    ]


def write_batch_insert(output_file, table_name: str, columns: List[str], values_batch: List[List[str]],
                       batch_size: int = 1000):
    """Write batch INSERT statements to file"""
    if not values_batch:
        return

    # Write in batches to avoid extremely long SQL statements
    for i in range(0, len(values_batch), batch_size):
        batch = values_batch[i:i + batch_size]

        # Create the INSERT statement
        columns_str = ', '.join(columns)
        values_str = ',\n    '.join([f"({', '.join(row)})" for row in batch])

        sql = f"INSERT INTO {table_name} ({columns_str}) VALUES\n    {values_str};\n\n"
        output_file.write(sql)


def make_matomo_request(matomo_url: str, site_id: str, token_auth: str, start_date: str, end_date: str = None):
    """Make API request to Matomo for a date range"""
    date_param = start_date
    period = "day"

    if end_date and end_date != start_date:
        date_param = f"{start_date},{end_date}"
        period = "range"

    params = {
        'module': 'API',
        'method': 'Live.getLastVisitsDetails',
        'idSite': site_id,
        'period': period,
        'date': date_param,
        'format': 'JSON',
        'token_auth': token_auth,
        'filter_limit': -1
    }

    try:
        response = requests.get(f"{matomo_url}/index.php", params=params, timeout=300)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error fetching data for {date_param}: {e}")
        return None


def process_batch_data(all_data: List[Tuple[date, list]], website_id: str, session_mapping: Dict,
                       output_file, batch_size: int = 1000) -> int:
    """Process all collected data and write batch SQL statements"""
    session_columns = [
        'session_id', 'website_id', 'browser', 'os', 'device', 'screen', 'language',
        'country', 'region', 'city', 'distinct_id', 'created_at'
    ]

    event_columns = [
        'event_id', 'website_id', 'session_id', 'visit_id', 'created_at', 'url_path',
        'url_query', 'referrer_path', 'referrer_query', 'referrer_domain', 'page_title',
        'event_type', 'hostname'
    ]

    session_batch = []
    event_batch = []
    events_count = 0

    for target_date, data in all_data:
        if not isinstance(data, list):
            continue

        for visit_data in data:
            try:
                # Skip if this session was already processed (duplicate visit ID)
                if visit_data['idVisit'] not in session_mapping:
                    session_id, session_values = create_session_data(visit_data, website_id, session_mapping)
                    session_batch.append(session_values)

                # Generate visit ID for this session
                visit_id = generate_uuid(f"visit_{visit_data['idVisit']}")

                # Process actions (page views)
                if 'actionDetails' in visit_data:
                    for action in visit_data['actionDetails']:
                        if action.get('type') == 'action':  # Page view
                            event_values = create_website_event_data(action, visit_data, visit_id,
                                                                     website_id, session_mapping)
                            event_batch.append(event_values)
                            events_count += 1

            except Exception as e:
                print(f"Error processing visit {visit_data.get('idVisit', 'unknown')} on {target_date}: {e}")
                continue

        # Write batches when they reach the batch size
        if len(session_batch) >= batch_size:
            write_batch_insert(output_file, 'session', session_columns, session_batch, batch_size)
            session_batch = []

        if len(event_batch) >= batch_size:
            write_batch_insert(output_file, 'website_event', event_columns, event_batch, batch_size)
            event_batch = []

    # Write remaining batches
    if session_batch:
        write_batch_insert(output_file, 'session', session_columns, session_batch, batch_size)

    if event_batch:
        write_batch_insert(output_file, 'website_event', event_columns, event_batch, batch_size)

    return events_count


def parse_date(date_string: str) -> date:
    """Parse date string in YYYY-MM-DD format"""
    try:
        return datetime.strptime(date_string, '%Y-%m-%d').date()
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date format: {date_string}. Expected YYYY-MM-DD")


def migrate_matomo_to_umami(matomo_url: str, site_id: str, token_auth: str, website_id: str,
                            start_date: date = None, end_date: date = None, output_file: str = "migration.sql",
                            batch_size: int = 1000, days_per_request: int = 1):
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
    info_table.add_row("📦 Batch size:", f"{batch_size:,}")
    info_table.add_row("🔄 Days per request:", f"{days_per_request:,}")

    console.print(Panel(info_table, title="🚀 [bold green]Matomo to Umami Migration[/bold green]", border_style="green"))

    # Track sessions and statistics
    session_mapping = {}
    total_events = 0
    total_days_processed = 0
    failed_days = 0

    # Initialize batch containers
    session_batch = []
    event_batch = []

    # Define column names for SQL inserts
    session_columns = [
        'session_id', 'website_id', 'browser', 'os', 'device', 'screen', 'language',
        'country', 'region', 'city', 'distinct_id', 'created_at'
    ]

    event_columns = [
        'event_id', 'website_id', 'session_id', 'visit_id', 'created_at', 'url_path',
        'url_query', 'referrer_path', 'referrer_query', 'referrer_domain', 'page_title',
        'event_type', 'hostname'
    ]

    # Open SQL file for writing
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            # Write header
            f.write("-- Generated SQL migration from Matomo to Umami (Batch Optimized)\n")
            f.write(f"-- Generated on: {datetime.now().isoformat()}\n")
            f.write(f"-- Website ID: {website_id}\n")
            f.write(f"-- Date range: {start_date} to {end_date}\n")
            f.write(f"-- Matomo URL: {matomo_url}\n")
            f.write(f"-- Site ID: {site_id}\n")
            f.write(f"-- Batch size: {batch_size}\n")
            f.write(f"-- Days per request: {days_per_request}\n")
            f.write("\n")
            f.write("BEGIN;\n")
            f.write("\n")
            f.write("SET client_encoding = 'UTF8';\n")
            f.write("\n")

            # Process data day by day
            console.print("\n[bold yellow]Collecting and processing data from Matomo...[/bold yellow]")

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
                collection_task = progress.add_task("[blue]Processing data...", total=total_days)
                current_date = start_date

                while current_date <= end_date:
                    # Calculate the end date for this batch (don't exceed the overall end_date)
                    batch_end_date = min(current_date + timedelta(days=days_per_request - 1), end_date)

                    iso_start_date = current_date.strftime('%Y-%m-%d')
                    iso_end_date = batch_end_date.strftime('%Y-%m-%d')

                    if iso_start_date == iso_end_date:
                        progress.update(collection_task, description=f"[blue]Processing {iso_start_date}...")
                    else:
                        progress.update(collection_task,
                                        description=f"[blue]Processing {iso_start_date} to {iso_end_date}...")

                    # Fetch data for this date range
                    data = make_matomo_request(matomo_url, site_id, token_auth, iso_start_date, iso_end_date)

                    days_in_batch = (batch_end_date - current_date).days + 1
                    days_processed_in_batch = 0

                    if data is not None:
                        # Process this batch's data immediately
                        if isinstance(data, list):
                            for visit_data in data:
                                try:
                                    # Skip if this session was already processed
                                    if visit_data['idVisit'] not in session_mapping:
                                        session_id, session_values = create_session_data(visit_data, website_id,
                                                                                         session_mapping)
                                        session_batch.append(session_values)

                                        # Write session batch if it reaches the limit
                                        if len(session_batch) >= batch_size:
                                            write_batch_insert(f, 'session', session_columns, session_batch, batch_size)
                                            session_batch = []

                                    # Generate visit ID for this session
                                    visit_id = generate_uuid(f"visit_{visit_data['idVisit']}")

                                    # Process actions (page views)
                                    if 'actionDetails' in visit_data:
                                        for action in visit_data['actionDetails']:
                                            if action.get('type') == 'action':  # Page view
                                                event_values = create_website_event_data(action, visit_data, visit_id,
                                                                                         website_id, session_mapping)
                                                event_batch.append(event_values)
                                                total_events += 1

                                                # Write event batch if it reaches the limit
                                                if len(event_batch) >= batch_size:
                                                    write_batch_insert(f, 'website_event', event_columns, event_batch,
                                                                       batch_size)
                                                    event_batch = []

                                except Exception as e:
                                    console.print(
                                        f"[dim]Error processing visit {visit_data.get('idVisit', 'unknown')} on {iso_start_date}: {e}[/dim]")
                                    continue

                            days_processed_in_batch = days_in_batch
                        total_days_processed += days_processed_in_batch
                    else:
                        failed_days += days_in_batch

                    # Update progress
                    progress.update(collection_task, advance=days_in_batch)
                    current_date = batch_end_date + timedelta(days=1)

            # Write any remaining batches
            if session_batch:
                write_batch_insert(f, 'session', session_columns, session_batch, batch_size)

            if event_batch:
                write_batch_insert(f, 'website_event', event_columns, event_batch, batch_size)

            # Write footer
            f.write("COMMIT;\n")
            f.write("\n")
            f.write(f"-- Migration complete\n")
            f.write(f"-- Total sessions: {len(session_mapping)}\n")
            f.write(f"-- Total events: {total_events}\n")
            f.write(f"-- Days processed: {total_days_processed}\n")
            f.write(f"-- Failed days: {failed_days}\n")
            f.write(f"-- Batch size used: {batch_size}\n")
            f.write(f"-- Days per request: {days_per_request}\n")

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
    summary_table.add_row("📦 Batch size:", f"{batch_size:,}")
    summary_table.add_row("🔄 Days per request:", f"{days_per_request:,}")
    if total_events > 0 and total_days_processed > 0:
        summary_table.add_row("📊 Avg events/day:", f"{total_events / total_days_processed:.1f}")

    console.print(Panel(summary_table, title="✅ [bold green]Migration Completed Successfully![/bold green]",
                        border_style="green"))


def main():
    parser = argparse.ArgumentParser(
        description='Migrate Matomo analytics data to Umami SQL format with batch inserts',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Default: last 2 years with 1000 batch size
  python matomo2umami.py https://tracking.example.com 1 <MATOMO_TOKEN> <UMAMI_UID>

  # Custom date range with custom batch size
  python matomo2umami.py https://tracking.example.com 1 <MATOMO_TOKEN> <UMAMI_UID> --start-date 2023-01-01 --end-date 2023-12-31 --batch-size 500

  # With custom output file
  python matomo2umami.py https://tracking.example.com 1 <MATOMO_TOKEN> <UMAMI_UID> -o my_migration.sql

  # Optimize API calls by fetching multiple days per request
  python matomo2umami.py https://tracking.example.com 1 <MATOMO_TOKEN> <UMAMI_UID> --days-per-request 7
        """
    )

    parser.add_argument('matomo_url', help='Matomo URL (e.g., https://tracking.example.com)')
    parser.add_argument('site_id', help='Matomo site ID')
    parser.add_argument('token_auth', help='Matomo API authentication token')
    parser.add_argument('website_id', help='Umami website UUID for the target database')
    parser.add_argument('-o', '--output', help='Output SQL file path (default: migration.sql)', default='migration.sql')
    parser.add_argument('--start-date', type=parse_date, help='Start date in YYYY-MM-DD format (default: 2 years ago)')
    parser.add_argument('--end-date', type=parse_date, help='End date in YYYY-MM-DD format (default: today)')
    parser.add_argument('--batch-size', type=int, default=1000, help='Batch size for INSERT statements (default: 1000)')
    parser.add_argument('--days-per-request', type=int, default=1,
                        help='Number of days to fetch in a single API request (default: 1)')

    args = parser.parse_args()

    # Validate batch size
    if args.batch_size < 1:
        print("❌ Error: batch-size must be at least 1")
        sys.exit(1)

    # Validate days per request
    if args.days_per_request < 1:
        print("❌ Error: days-per-request must be at least 1")
        sys.exit(1)

    # Clean up Matomo URL (remove trailing slash)
    matomo_url = args.matomo_url.rstrip('/')

    migrate_matomo_to_umami(
        matomo_url,
        args.site_id,
        args.token_auth,
        args.website_id,
        args.start_date,
        args.end_date,
        args.output,
        args.batch_size,
        args.days_per_request
    )


if __name__ == '__main__':
    main()
