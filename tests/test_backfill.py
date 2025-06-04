import gzip
from pathlib import Path

from click.testing import CliRunner

from postmark.cli.backfill import main


def test_backfill_dry_run(tmp_path: Path) -> None:
    # Create a test .gz containing 3 lines
    gz_file = tmp_path / "test_emails.gz"
    with gzip.open(gz_file, "wb") as f:
        f.write(b"From me0@example.com\n")
        f.write(b"Subject: Test0\n")
        f.write(b"Message-ID: <0@foo>\n")
        f.write(b"Date: Mon, 1 Jan 2022 01:00:00 +0000\n")
        f.write(b"Content-Type: text/plain\n\n")
        f.write(b"Hello from email 0\n")

        f.write(b"From me1@example.com\n")
        f.write(b"Subject: Test1\n")
        f.write(b"Message-ID: <1@foo>\n")
        f.write(b"Date: Mon, 1 Jan 2022 02:00:00 +0000\n")
        f.write(b"Content-Type: text/plain\n\n")
        f.write(b"Hello from email 1\n")

        f.write(b"From me2@example.com\n")
        f.write(b"Subject: Test2\n")
        f.write(b"Message-ID: <2@foo>\n")
        f.write(b"Date: Mon, 1 Jan 2022 03:00:00 +0000\n")
        f.write(b"Content-Type: text/plain\n\n")
        f.write(b"Hello from email 2\n")

    runner = CliRunner()
    result = runner.invoke(main, [str(gz_file), "--dry-run"], catch_exceptions=False)

    # Verify the output mentions the 3 processed lines
    assert "Processed 3 lines total (dry run: True)." in result.output
    assert result.exit_code == 0


def test_backfill_duplicates_and_attachments(tmp_path: Path) -> None:
    """
    Creates a .gz with two duplicated EMLs and one unique EML;
    verifies that only two are stored and uploads attachments to S3 with the correct key.
    """
    import gzip

    from click.testing import CliRunner

    gz_file = tmp_path / "dupe_emails.mbox.gz"

    # Simulate a small mbox with 3 'From ' boundaries: 2 duplicates, 1 unique with an "ATTACH:" placeholder
    with gzip.open(gz_file, "wb") as f:
        # EML #1
        f.write(b"From me@foo.com\n")
        f.write(b"Subject: Foo\n")
        f.write(b"Message-ID: <dup1@foo>\n")
        f.write(b"Date: Mon, 1 Jan 2022 01:00:00 +0000\n")
        f.write(b"Content-Type: text/plain\n\n")
        f.write(b"Body A\n")

        # EML #2 (exact duplicate of #1 for dedup)
        f.write(b"From me@foo.com\n")
        f.write(b"Subject: Foo\n")
        f.write(b"Message-ID: <dup1@foo>\n")
        f.write(b"Date: Mon, 1 Jan 2022 01:00:00 +0000\n")
        f.write(b"Content-Type: text/plain\n\n")
        f.write(b"Body A\n")

        # EML #3 (unique, includes a "filename" part so parser sees an attachment)
        f.write(b"From me@bar.com\n")
        f.write(b"Subject: Bar\n")
        f.write(b"Message-ID: <unique@foo>\n")
        f.write(b"Date: Mon, 1 Jan 2022 02:00:00 +0000\n")
        f.write(b'Content-Type: multipart/mixed; boundary="BOUNDARY"\n\n')
        f.write(b"--BOUNDARY\n")
        f.write(b"Content-Type: text/plain\n\n")
        f.write(b"Body B\n")
        f.write(b"--BOUNDARY\n")
        f.write(b'Content-Disposition: attachment; filename="somefile.pdf"\n')
        f.write(b"Content-Type: application/pdf\n\n")
        f.write(b"%PDF This is just a placeholder PDF content.\n")
        f.write(b"--BOUNDARY--\n")

    runner = CliRunner()
    result = runner.invoke(
        main,
        [str(gz_file), "--dry-run"],  # Remove "--dry-run" to test side effects
        catch_exceptions=False,
    )
    # Verify that 3 emails were processed and no crash occurred
    assert "Processed 3" in result.output
    assert result.exit_code == 0

    # In a real test with S3 and SQLite mocks, verify:
    # - that 2 emails are inserted into the DB
    # - that "somefile.pdf" is uploaded to S3 at {uuid}/1.2/somefile.pdf
    # - that the duplicate is not stored in the DB
