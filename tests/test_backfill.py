import tarfile
from pathlib import Path

from click.testing import CliRunner

from postmark.cli.backfill import main


def test_backfill_dry_run(tmp_path: Path) -> None:
    # Create a test .gz containing 3 dummy .eml files
    gz_file = tmp_path / "test_emails.gz"
    with tarfile.open(gz_file, "w:gz") as tar:
        for i in range(3):
            eml_path = tmp_path / f"test_{i}.eml"
            eml_path.write_text(f"Header info for email {i}")
            tar.add(eml_path, arcname=f"test_{i}.eml")

    runner = CliRunner()
    result = runner.invoke(main, [str(gz_file), "--dry-run"], catch_exceptions=False)

    # Verify the output mentions the 3 processed files
    assert "Processed 3 EML files total (dry run: True)." in result.output
    assert result.exit_code == 0
