import boto3
from botocore.exceptions import ClientError
from typing import Any


class S3Client:
    def __init__(
        self,
        endpoint_url: str | None,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        bucket_name: str,
    ) -> None:
        kwargs = {
            "aws_access_key_id": aws_access_key_id,
            "aws_secret_access_key": aws_secret_access_key,
        }
        if endpoint_url:
            kwargs["endpoint_url"] = endpoint_url
            
        self.s3: Any = boto3.resource("s3", **kwargs)
        self.bucket_name: str = bucket_name
        self._create_bucket_if_missing()

    def _create_bucket_if_missing(self) -> None:
        try:
            self.s3.meta.client.head_bucket(Bucket=self.bucket_name)
        except ClientError:
            self.s3.create_bucket(Bucket=self.bucket_name)

    def put_bytes(self, key: str, data: bytes) -> None:
        obj = self.s3.Object(self.bucket_name, key)
        obj.put(Body=data)

    def get_bytes(self, key: str) -> bytes:
        obj = self.s3.Object(self.bucket_name, key)
        return obj.get()["Body"].read()

    def list_keys(self, prefix: str = "") -> list[str]:
        bucket = self.s3.Bucket(self.bucket_name)
        return [obj.key for obj in bucket.objects.filter(Prefix=prefix)]
