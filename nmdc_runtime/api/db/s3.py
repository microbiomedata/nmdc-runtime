import os

import boto3

_state = {"client": None}

API_SITE_BUCKET = os.getenv("API_SITE_ID")


async def get_s3_client():
    if _state["client"] is None:
        _session = boto3.session.Session()
        _client = _session.client(
            "s3",
            region_name=os.getenv("DO_REGION_NAME"),
            endpoint_url=os.getenv("DO_ENDPOINT_URL"),
            aws_access_key_id=os.getenv("DO_SPACES_KEY"),
            aws_secret_access_key=os.getenv("DO_SPACES_SECRET"),
        )
        _state["client"] = _client
    return _state["client"]


def presigned_url_to_put(key, client=None, bucket=API_SITE_BUCKET, expires_in=300):
    return client.generate_presigned_url(
        ClientMethod="put_object",
        Params={"Bucket": bucket, "Key": key},
        ExpiresIn=expires_in,
    )
