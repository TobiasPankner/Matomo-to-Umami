import argparse
import os
import subprocess
import sys
import time
from pathlib import Path

from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn

console = Console()

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'umami',
    'user': 'umami',
    'password': 'password'
}
COMPOSE_FILE = "matomo-to-umami-preview/docker-compose.yaml"

# Post-import SQL statement
CREATE_WEBSITE_STATEMENT = """INSERT INTO website
                              VALUES ((SELECT website_id FROM website_event LIMIT 1),
                                      (SELECT hostname FROM website_event LIMIT 1),
                                      (SELECT hostname FROM website_event LIMIT 1),
                                      NULL, NULL,
                                      (SELECT user_id FROM "user" LIMIT 1),
                                      NULL, NULL, NULL, NULL, NULL);"""


def check_sql_file(sql_file):
    """Check if SQL file exists"""
    sql_path = Path(sql_file)

    if not sql_path.exists():
        console.print(f"❌ SQL file not found: {sql_file}")
        return False

    file_size = sql_path.stat().st_size
    size_mb = file_size / (1024 * 1024)
    console.print(f"SQL file found: {sql_file} ({size_mb:.2f} MB)")
    return True


def start_services():
    """Start Docker Compose services"""
    console.print("\n[bold white]Starting services...[/bold white]")

    try:
        with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                TimeElapsedColumn(),
                console=console
        ) as progress:
            task = progress.add_task("Starting Docker services...", total=1)

            # Start services
            result = subprocess.run([
                'docker', 'compose', '-f', COMPOSE_FILE,
                'up', '-d', '--remove-orphans'
            ], capture_output=True, text=True, check=True)

            # Mark as complete
            progress.update(task, completed=1, description="✅ Services started successfully")
            time.sleep(0.5)  # Brief pause to show completion

        return True

    except subprocess.CalledProcessError as e:
        console.print(f"❌ Failed to start services: {e}")
        console.print(f"Error output: {e.stderr}")
        return False


def execute_sql_statement(sql_statement, description="SQL statement"):
    """Execute a single SQL statement"""
    console.print(f"\n[bold white]Executing {description}...[/bold white]")

    try:
        env = os.environ.copy()
        env['PGPASSWORD'] = DB_CONFIG['password']

        cmd = [
            'psql',
            '-h', DB_CONFIG['host'],
            '-p', str(DB_CONFIG['port']),
            '-U', DB_CONFIG['user'],
            '-d', DB_CONFIG['database'],
            '-c', sql_statement,
            '-v', 'ON_ERROR_STOP=1'
        ]

        result = subprocess.run(cmd, env=env, capture_output=True, text=True, check=True)

        if result.stdout.strip():
            console.print(f"[green]✅ {description} executed successfully[/green]")
        else:
            console.print(f"[green]✅ {description} executed successfully[/green]")

        return True

    except subprocess.CalledProcessError as e:
        console.print(f"[red]❌ Failed to execute {description}[/red]")
        console.print(f"[red]Error: {e.stderr.strip() if e.stderr else 'Unknown error'}[/red]")
        return False
    except Exception as e:
        console.print(f"[red]❌ Error executing {description}: {e}[/red]")
        return False


def import_sql_file(sql_file):
    """Import SQL file into PostgreSQL with real-time output"""
    sql_path = Path(sql_file)

    # User confirmation
    console.print(f"\n[yellow]⚠️  This will import the SQL file into the database:[/yellow]")
    console.print(f"Database: {DB_CONFIG['database']}")
    console.print(f"Host: {DB_CONFIG['host']}:{DB_CONFIG['port']}")

    response = console.input("\n[bold cyan]Do you want to proceed with the import? (y/N): [/bold cyan]")

    if response.lower() not in ['y', 'yes']:
        console.print("[yellow]Import cancelled by user.[/yellow]")
        return False

    console.print(f"\n[bold green]Starting import...[/bold green]")
    console.print("[dim]Note: This may take several minutes for large SQL files[/dim]\n")

    try:
        # Use psql to import the file
        env = os.environ.copy()
        env['PGPASSWORD'] = DB_CONFIG['password']

        cmd = [
            'psql',
            '-h', DB_CONFIG['host'],
            '-p', str(DB_CONFIG['port']),
            '-U', DB_CONFIG['user'],
            '-d', DB_CONFIG['database'],
            '-f', str(sql_path),
            '-v', 'ON_ERROR_STOP=1',
            '--echo-errors'
        ]

        # Start the process with real-time output
        process = subprocess.Popen(
            cmd,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,  # Combine stderr with stdout
            text=True,
            bufsize=1,  # Line buffered
            universal_newlines=True
        )

        # Track if we've seen any output
        output_lines = []

        # Use Live display to update the panel content in real-time
        with Live(Panel("Starting import...", title="📥 SQL Import Progress", border_style="blue"),
                  refresh_per_second=4, console=console) as live:

            while True:
                output = process.stdout.readline()
                if output == '' and process.poll() is not None:
                    break
                if output:
                    # Clean up the output and display it
                    line = output.strip()
                    if line:
                        output_lines.append(line)

                        # Keep only the last 15 lines to prevent the panel from getting too long
                        if len(output_lines) > 15:
                            output_lines = output_lines[-15:]

                        # Update the live display with current output
                        panel_content = "\n".join(output_lines) if output_lines else "Processing..."
                        live.update(Panel(panel_content, title="📥 SQL Import Progress", border_style="blue"))

        # Wait for the process to complete
        return_code = process.poll()

        if return_code == 0:
            return True
        else:
            console.print(f"\n[bold red]❌ SQL import failed with return code {return_code}[/bold red]")
            return False

    except FileNotFoundError:
        console.print("❌ psql command not found. Please install PostgreSQL client tools.")
        console.print("   Ubuntu/Debian: sudo apt-get install postgresql-client")
        console.print("   macOS: brew install postgresql")
        console.print("   Windows: Install PostgreSQL")
        return False
    except Exception as e:
        console.print(f"❌ Error importing SQL file: {e}")
        return False


def run_setup(sql_file):
    if not check_sql_file(sql_file):
        return False

    # Start services
    if not start_services():
        return False

    # Import SQL file
    if not import_sql_file(sql_file):
        return False

    # Execute post-import SQL statement
    if not execute_sql_statement(CREATE_WEBSITE_STATEMENT, "website creation statement"):
        console.print("[yellow]⚠️  Post-import statement failed, but main import was successful[/yellow]")
        # Don't return False here - the main import succeeded

    console.print("\nDone. Visit [link=http://localhost:3000]http://localhost:3000[/link] to view the preview")
    console.print("\n[dim]Use 'docker compose down' to stop the services[/dim]")

    return True


def main():
    parser = argparse.ArgumentParser(description="Matomo to Umami preview")
    parser.add_argument('sql_file', nargs='?', default='migration.sql',
                        help='Path to SQL file to import (default: migration.sql)')

    args = parser.parse_args()

    try:
        run_setup(args.sql_file)
    except KeyboardInterrupt:
        console.print("\n\n[yellow]⚠️  Setup interrupted by user[/yellow]")
        console.print("[dim]Use 'docker compose down' to clean up if needed[/dim]")
        sys.exit(1)
    except Exception as e:
        console.print(f"\n❌ Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
