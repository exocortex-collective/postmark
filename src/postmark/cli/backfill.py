import click
import tarfile
from pathlib import Path

@click.command()
@click.argument("tgz", type=click.Path(exists=True))
@click.option("--db-path", default="~/.postmark/emails.db", help="Path to the SQLite DB")
@click.option("--parquet-dir", default="~/.postmark/parquet", help="Directory to store Parquet files")
@click.option("--dry-run", is_flag=True, default=False, help="Perform a dry run without side effects")
def main(tgz: str, db_path: str, parquet_dir: str, dry_run: bool) -> None:
    # Open the specified .tgz file for reading with tarfile
    with tarfile.open(tgz, "r:gz") as tar:
        count = 0
        # Iterate over each entry in the .tgz
        for member in tar:
            # Only process regular files ending with ".eml"
            if member.isreg() and member.name.endswith(".eml"):
                count += 1
                # Print progress every 1000 EML files
                if count % 1000 == 0:
                    click.echo(f"Processed {count} EML files so far...")
        # End-of-file summary
        click.echo(f"Processed {count} EML files total (dry run: {dry_run}).")
