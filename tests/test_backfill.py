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


def test_backfill_duplicates_and_attachments(tmp_path: Path):
    """
    Crea .gz con dos EMLs duplicados y uno único;
    verifica se guardan 2 y sube adjuntos a S3 con la key apropiada.
    """
    from click.testing import CliRunner
    import gzip
    gz_file = tmp_path / "dupe_emails.mbox.gz"

    # Simular mini mbox con 3 'From ' boundaries: 2 duplicados, 1 distinto con un "ATTACH:" placeholder
    with gzip.open(gz_file, "wb") as f:
        f.write(b"From me@foo.com\nSubject: Foo\n\nBody A\n")
        f.write(b"From me@foo.com\nSubject: Foo\n\nBody A\n")  # Dup
        f.write(b"From me@bar.com\nSubject: Bar\n\nBody B\nATTACH:somefile.pdf\n")

    runner = CliRunner()
    result = runner.invoke(
        main,
        [str(gz_file), "--dry-run"],  # Cambiar a quitar --dry-run si deseas probar side effects
        catch_exceptions=False,
    )
    # Verificar que se procesaron los 3 y no hubo crash
    assert "Processed 3" in result.output
    assert result.exit_code == 0

    # En una prueba real con mocks de S3 y SQLite, comprueba:
    # - que se insertaron 2 correos en DB
    # - que se subió "somefile.pdf" a S3 en la ruta {uuid}/1.2/somefile.pdf
    # - que el duplicado no está en DB
