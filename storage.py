from pathlib import Path
from typing import Optional

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from config import (
    RUSTFS_ENDPOINT_URL,
    RUSTFS_ACCESS_KEY,
    RUSTFS_SECRET_KEY,
    RUSTFS_REGION,
)


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=RUSTFS_ENDPOINT_URL,
        aws_access_key_id=RUSTFS_ACCESS_KEY,
        aws_secret_access_key=RUSTFS_SECRET_KEY,
        region_name=RUSTFS_REGION,
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
        ),
    )


def ensure_bucket(bucket: str) -> None:
    client = get_s3_client()
    try:
        client.head_bucket(Bucket=bucket)
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code")
        if error_code in ("404", "NoSuchBucket", "NotFound"):
            client.create_bucket(Bucket=bucket)
        elif error_code in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            return
        else:
            raise


def upload_file(local_path: Path, bucket: str, key: Optional[str] = None) -> str:
    client = get_s3_client()
    key = key or local_path.name
    client.upload_file(str(local_path), bucket, key)
    return key


def download_file(bucket: str, key: str, dest_path: Path) -> Path:
    client = get_s3_client()
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    client.download_file(bucket, key, str(dest_path))
    return dest_path