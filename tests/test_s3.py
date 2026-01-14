import pytest
from moto import mock_aws

from postmark.storage.s3 import S3Client


@mock_aws
def test_put_and_get() -> None:
    # Instantiate our wrapper - moto intercepts all S3 calls
    s3_client = S3Client(
        endpoint_url=None,  # Not needed with moto
        aws_access_key_id="test-key",
        aws_secret_access_key="test-secret",
        bucket_name="test-bucket",
    )

    # Put a small PDF (mock bytes)
    sample_data = b"%PDF-1.4 mock content"
    test_key = "sample.pdf"
    s3_client.put_bytes(test_key, sample_data)

    # Get it back and assert equality
    retrieved = s3_client.get_bytes(test_key)
    assert retrieved == sample_data


@mock_aws
def test_list_keys() -> None:
    """Test listing keys in S3 bucket."""
    s3_client = S3Client(
        endpoint_url=None,
        aws_access_key_id="test-key",
        aws_secret_access_key="test-secret",
        bucket_name="test-bucket",
    )

    # Put multiple files
    s3_client.put_bytes("file1.pdf", b"content1")
    s3_client.put_bytes("file2.pdf", b"content2")
    s3_client.put_bytes("subdir/file3.pdf", b"content3")

    # List all keys
    all_keys = s3_client.list_keys()
    assert len(all_keys) == 3
    assert "file1.pdf" in all_keys
    assert "file2.pdf" in all_keys
    assert "subdir/file3.pdf" in all_keys


@mock_aws
def test_list_keys_with_prefix() -> None:
    """Test listing keys with prefix filter."""
    s3_client = S3Client(
        endpoint_url=None,
        aws_access_key_id="test-key",
        aws_secret_access_key="test-secret",
        bucket_name="test-bucket",
    )

    # Put files with different prefixes
    s3_client.put_bytes("docs/file1.pdf", b"content1")
    s3_client.put_bytes("docs/file2.pdf", b"content2")
    s3_client.put_bytes("images/pic1.jpg", b"content3")

    # List only docs
    docs_keys = s3_client.list_keys(prefix="docs/")
    assert len(docs_keys) == 2
    assert "docs/file1.pdf" in docs_keys
    assert "docs/file2.pdf" in docs_keys
    assert "images/pic1.jpg" not in docs_keys


@mock_aws
def test_empty_endpoint_url() -> None:
    """Test that empty string endpoint_url is converted to None."""
    # Empty string should be treated as None (AWS S3)
    s3_client = S3Client(
        endpoint_url="",  # Empty string
        aws_access_key_id="test-key",
        aws_secret_access_key="test-secret",
        bucket_name="test-bucket",
    )

    # Should work fine with moto
    s3_client.put_bytes("test.txt", b"data")
    assert s3_client.get_bytes("test.txt") == b"data"
