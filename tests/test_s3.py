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
