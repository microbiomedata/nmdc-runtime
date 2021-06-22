from functools import lru_cache
import os

import boto3

API_SITE_BUCKET = os.getenv("API_SITE_ID")
S3_ID_NS = "do"  # Namespace for Drs Objects in Site S3-bucket store.


@lru_cache
def get_s3_client():
    _session = boto3.session.Session()
    return _session.client(
        "s3",
        region_name=os.getenv("DO_REGION_NAME"),
        endpoint_url=os.getenv("DO_ENDPOINT_URL"),
        aws_access_key_id=os.getenv("DO_SPACES_KEY"),
        aws_secret_access_key=os.getenv("DO_SPACES_SECRET"),
    )


def presigned_url_to_put(
    key, client=None, mime_type=None, bucket=API_SITE_BUCKET, expires_in=300
):
    return client.generate_presigned_url(
        ClientMethod="put_object",
        Params={"Bucket": bucket, "Key": key, "ContentType": mime_type},
        ExpiresIn=expires_in,
    )


def presigned_url_to_get(key, client=None, bucket=API_SITE_BUCKET, expires_in=300):
    return client.generate_presigned_url(
        ClientMethod="get_object",
        Params={"Bucket": bucket, "Key": key},
        ExpiresIn=expires_in,
    )
