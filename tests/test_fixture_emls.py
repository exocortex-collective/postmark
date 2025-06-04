from pathlib import Path

import pytest

from postmark.email.parser import parse_eml


@pytest.mark.parametrize(
    "eml_path",
    sorted(Path(__file__).parent.joinpath("eml_fixtures").glob("*.eml")),
)  # type: ignore[misc]
def test_eml_fixture_parse(eml_path: Path) -> None:
    """
    Automatically parses each .eml file in tests/eml_fixtures/.
    """
    email_obj = parse_eml(eml_path)
    # Minimal assertion: ensure it parsed successfully (e.g. has a uuid)
    assert email_obj.uuid, f"Email in {eml_path} did not parse properly"
