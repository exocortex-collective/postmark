import boto3
import pytest
from moto import mock_s3

from postmark.storage.s3 import S3Client


@mock_s3
def test_put_and_get():
    # Setup mock S3
    endpoint_url = "http://localhost:9000"
    client = boto3.resource(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
    )
    client.create_bucket(Bucket="test-bucket")

    # Instantiate our wrapper
    s3_client = S3Client(
        endpoint_url=endpoint_url,
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        bucket_name="test-bucket",
    )

    # Put a small PDF (mock bytes)
    sample_data = b"%PDF-1.4 mock content"
    test_key = "sample.pdf"
    s3_client.put_bytes(test_key, sample_data)

    # Get it back and assert equality
    retrieved = s3_client.get_bytes(test_key)
    assert retrieved == sample_data
