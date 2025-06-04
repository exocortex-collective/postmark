"""Basic tests for Postmark package initialization."""

import postmark


def test_version():
    """Test that version is accessible."""
    assert postmark.__version__ == "0.0.0"


def test_package_imports():
    """Test that submodules can be imported."""
    import postmark.storage
    import postmark.email
    import postmark.sync
    import postmark.cli
    
    # Basic smoke test - modules should import without error
    assert postmark.storage is not None
    assert postmark.email is not None
    assert postmark.sync is not None
    assert postmark.cli is not None
