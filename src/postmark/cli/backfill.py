"""Backfill CLI command - trigger Gmail API backfill via Gateway.

This CLI wraps the Gateway /sync/backfill endpoints to trigger and monitor
historical email sync from Gmail API.
"""

import json
import os
import sys
import time
import urllib.error
import urllib.request
from typing import Any

import click

# Default Gateway URL (can be overridden with env var)
DEFAULT_GATEWAY_URL = "http://localhost:8097"


def get_gateway_url() -> str:
    """Get Gateway URL from environment or default."""
    return os.environ.get("CORTEX_GATEWAY_URL", DEFAULT_GATEWAY_URL)


def api_request(
    method: str, path: str, data: dict[str, Any] | None = None
) -> dict[str, Any]:
    """Make an API request to the Gateway.

    Args:
        method: HTTP method (GET, POST)
        path: API path (e.g., "/sync/backfill")
        data: Optional JSON body for POST requests

    Returns:
        Parsed JSON response

    Raises:
        click.ClickException: On API errors
    """
    url = f"{get_gateway_url()}{path}"

    try:
        if data is not None:
            body = json.dumps(data).encode()
            req = urllib.request.Request(
                url, data=body, headers={"Content-Type": "application/json"}
            )
        else:
            req = urllib.request.Request(url)

        req.method = method

        with urllib.request.urlopen(req, timeout=30) as resp:
            result: dict[str, Any] = json.loads(resp.read().decode())
            return result

    except urllib.error.HTTPError as e:
        try:
            error_body = json.loads(e.read().decode())
            error_msg = error_body.get("error", str(e))
        except Exception:
            error_msg = str(e)
        raise click.ClickException(f"API error: {error_msg}")

    except urllib.error.URLError as e:
        raise click.ClickException(f"Cannot connect to Gateway at {url}: {e.reason}")


@click.group()
def backfill() -> None:
    """Trigger and monitor Gmail API backfill operations.

    Backfill fetches historical emails from Gmail API into the pipeline.
    Jobs are queued and processed by the gmail-sync service.
    """
    pass


@backfill.command("trigger")
@click.option(
    "--days",
    type=int,
    help="Number of days to backfill (e.g., --days 7 for last week)",
)
@click.option(
    "--after",
    type=str,
    help="Backfill from date in YYYY-MM-DD format (e.g., --after 2025-01-01)",
)
@click.option(
    "--direct",
    is_flag=True,
    help="Run backfill directly (bypass Gateway, requires Gmail credentials)",
)
def trigger(days: int | None, after: str | None, direct: bool) -> None:
    """Trigger a Gmail API backfill job.

    Either --days or --after must be provided (but not both).

    Examples:

        postmark backfill trigger --days 7

        postmark backfill trigger --after 2025-01-01

        postmark backfill trigger --days 30 --direct
    """
    if days and after:
        raise click.ClickException("Provide either --days or --after, not both")

    if not days and not after:
        raise click.ClickException("Provide either --days or --after")

    if direct:
        # Direct mode - run backfill locally without Gateway
        _run_direct_backfill(days, after)
        return

    # Queue via Gateway API
    data: dict[str, Any] = {}
    if days:
        data["days"] = days
    if after:
        data["after"] = after

    result = api_request("POST", "/sync/backfill", data)

    click.echo(f"Backfill job created:")
    click.echo(f"  ID: {result['id']}")
    click.echo(f"  Status: {result['status']}")
    click.echo(f"  Query: {result['query']}")
    click.echo(
        f"\nUse 'postmark backfill status --id {result['id']}' to check progress"
    )


@backfill.command("status")
@click.option("--id", "job_id", type=str, help="Get status of a specific job")
@click.option(
    "--watch",
    is_flag=True,
    help="Watch job progress until completion (requires --id)",
)
@click.option(
    "--interval",
    type=int,
    default=5,
    help="Poll interval in seconds for --watch (default: 5)",
)
def status(job_id: str | None, watch: bool, interval: int) -> None:
    """Check status of backfill jobs.

    Without --id, lists recent jobs.
    With --id, shows details for a specific job.
    With --watch, polls until job completes.

    Examples:

        postmark backfill status

        postmark backfill status --id abc123

        postmark backfill status --id abc123 --watch
    """
    if watch and not job_id:
        raise click.ClickException("--watch requires --id")

    if job_id:
        if watch:
            _watch_job(job_id, interval)
        else:
            _show_job(job_id)
    else:
        _list_jobs()


@backfill.command("list")
@click.option(
    "--status",
    "filter_status",
    type=click.Choice(["pending", "running", "completed", "cancelled", "failed"]),
    help="Filter by status",
)
@click.option("--limit", type=int, default=20, help="Max jobs to show (default: 20)")
def list_jobs(filter_status: str | None, limit: int) -> None:
    """List backfill jobs.

    Examples:

        postmark backfill list

        postmark backfill list --status running

        postmark backfill list --limit 50
    """
    params = f"?limit={limit}"
    if filter_status:
        params += f"&status={filter_status}"

    result = api_request("GET", f"/sync/backfill{params}")
    jobs = result.get("jobs", [])

    if not jobs:
        click.echo("No backfill jobs found")
        return

    # Print header
    click.echo(
        f"{'ID':<12} {'Status':<10} {'Processed':<10} {'Stored':<8} {'Query':<25}"
    )
    click.echo("-" * 70)

    for job in jobs:
        job_id = job["id"][:10] + ".." if len(job["id"]) > 12 else job["id"]
        click.echo(
            f"{job_id:<12} {job['status']:<10} {job['processed']:<10} "
            f"{job['stored']:<8} {job['query']:<25}"
        )


@backfill.command("cancel")
@click.argument("job_id")
@click.option("--yes", "-y", is_flag=True, help="Skip confirmation")
def cancel(job_id: str, yes: bool) -> None:
    """Cancel a pending or running backfill job.

    Running jobs will stop at the next page boundary.

    Examples:

        postmark backfill cancel abc123

        postmark backfill cancel abc123 --yes
    """
    if not yes:
        click.confirm(f"Cancel backfill job {job_id}?", abort=True)

    result = api_request("POST", f"/sync/backfill/{job_id}/cancel")

    click.echo(f"Job {result['id']} cancelled")


def _show_job(job_id: str) -> None:
    """Show details for a specific job."""
    result = api_request("GET", f"/sync/backfill/{job_id}")

    click.echo(f"Job ID: {result['id']}")
    click.echo(f"Status: {result['status']}")
    click.echo(f"Query: {result['query']}")
    if result.get("days"):
        click.echo(f"Days: {result['days']}")
    if result.get("after_date"):
        click.echo(f"After: {result['after_date']}")
    click.echo(f"Processed: {result['processed']}")
    click.echo(f"Stored: {result['stored']}")
    click.echo(f"Updated: {result['updated']}")
    if result.get("error"):
        click.echo(f"Error: {result['error']}")
    click.echo(f"Created: {result['created_at']}")
    if result.get("started_at"):
        click.echo(f"Started: {result['started_at']}")
    if result.get("completed_at"):
        click.echo(f"Completed: {result['completed_at']}")


def _list_jobs() -> None:
    """List recent jobs."""
    result = api_request("GET", "/sync/backfill?limit=10")
    jobs = result.get("jobs", [])

    if not jobs:
        click.echo("No backfill jobs found")
        return

    click.echo(
        f"{'ID':<12} {'Status':<10} {'Processed':<10} {'Stored':<8} {'Query':<25}"
    )
    click.echo("-" * 70)

    for job in jobs:
        job_id = job["id"][:10] + ".." if len(job["id"]) > 12 else job["id"]
        click.echo(
            f"{job_id:<12} {job['status']:<10} {job['processed']:<10} "
            f"{job['stored']:<8} {job['query']:<25}"
        )


def _watch_job(job_id: str, interval: int) -> None:
    """Watch job progress until completion."""
    terminal_states = {"completed", "cancelled", "failed"}

    while True:
        result = api_request("GET", f"/sync/backfill/{job_id}")

        # Clear line and print progress
        status_str = result["status"]
        processed = result["processed"]
        stored = result["stored"]

        sys.stdout.write(
            f"\rStatus: {status_str} | Processed: {processed} | Stored: {stored}   "
        )
        sys.stdout.flush()

        if status_str in terminal_states:
            click.echo()  # Newline after progress
            if status_str == "completed":
                click.echo(
                    f"Backfill completed: {stored} stored, {result['updated']} updated"
                )
            elif status_str == "cancelled":
                click.echo("Backfill was cancelled")
            else:
                click.echo(f"Backfill failed: {result.get('error', 'Unknown error')}")
            break

        time.sleep(interval)


def _run_direct_backfill(days: int | None, after: str | None) -> None:
    """Run backfill directly without Gateway (requires Gmail credentials)."""
    click.echo("Running direct backfill (bypassing Gateway)...")

    # Import here to avoid loading Gmail dependencies unless needed
    from postmark.services.gmail_sync import GmailSync, GmailSyncConfig

    try:
        config = GmailSyncConfig.from_env()
    except KeyError as e:
        raise click.ClickException(
            f"Missing environment variable: {e}. "
            "Direct mode requires Gmail credentials to be configured."
        )

    sync = GmailSync(config)

    if days:
        stored, updated = sync.backfill(days=days)
    elif after:
        stored, updated = sync.backfill(after=after)
    else:
        raise click.ClickException("Provide either --days or --after")

    click.echo(f"Backfill complete: {stored} stored, {updated} updated")
