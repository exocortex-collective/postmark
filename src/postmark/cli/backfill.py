import gzip

import click


@click.command()
@click.argument("gz", type=click.Path(exists=True))
@click.option(
    "--db-path", default="~/.postmark/emails.db", help="Path to the SQLite DB"
)
@click.option(
    "--parquet-dir",
    default="~/.postmark/parquet",
    help="Directory to store Parquet files",
)
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Perform a dry run without side effects",
)
def main(gz: str, db_path: str, parquet_dir: str, dry_run: bool) -> None:
    # Read the specified gzip file in streaming mode
    count = 0
    with gzip.open(gz, "rb") as f:
        for i, _ in enumerate(f, start=1):
            count = i
            if count % 1000 == 0:
                click.echo(f"Processed {count} lines so far...")
    click.echo(f"Processed {count} lines total (dry run: {dry_run}).")
