import gzip
from pathlib import Path

from click.testing import CliRunner

from postmark.cli.backfill import main


def test_backfill_dry_run(tmp_path: Path) -> None:
    # Create a test .gz containing 3 lines
    gz_file = tmp_path / "test_emails.gz"
    with gzip.open(gz_file, "wb") as f:
        for i in range(3):
            line = f"Header info for email {i}\n"
            f.write(line.encode("utf-8"))

    runner = CliRunner()
    result = runner.invoke(main, [str(gz_file), "--dry-run"], catch_exceptions=False)

    # Verify the output mentions the 3 processed lines
    assert "Processed 3 lines total (dry run: True)." in result.output
    assert result.exit_code == 0
