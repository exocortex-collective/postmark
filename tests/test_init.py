"""Basic tests for Postmark package initialization."""

import postmark


def test_version() -> None:
    """Test that version is accessible."""
    assert postmark.__version__ == "0.0.0"


def test_package_imports() -> None:
    """Test that submodules can be imported."""
    import postmark.cli
    import postmark.email
    import postmark.storage
    import postmark.sync

    # Basic smoke test - modules should import without error
    assert postmark.storage is not None
    assert postmark.email is not None
    assert postmark.sync is not None
    assert postmark.cli is not None
