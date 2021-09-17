import concurrent.futures
import json
import os.path
import re
import tempfile
from io import StringIO

import requests
from fastapi import APIRouter, UploadFile, File, HTTPException
from jsonschema import Draft7Validator
from nmdc_schema.validate_nmdc_json import get_nmdc_schema
from starlette import status

from nmdc_runtime.api.core.metadata import load_changesheet
from nmdc_runtime.util import nmdc_jsonschema

router = APIRouter()


@router.post("/metadata/changesheets:validate")
async def validate_changesheet(sheet: UploadFile = File(...)):
    """

    Example changesheets:
     - [changesheet-with-separator1.tsv](https://github.com/microbiomedata/nmdc-runtime/blob/main/metadata-translation/notebooks/data/changesheet-with-separator1.tsv)

    """
    content_types = {
        "text/csv": ",",
        "text/tab-separated-values": "\t",
    }
    content_type = sheet.content_type
    filename = sheet.filename
    if content_type not in content_types:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                f"file {filename} has content type '{content_type}'. "
                f"Only {list(content_types)} files are permitted."
            ),
        )
    contents: bytes = await sheet.read()
    stream = StringIO(contents.decode())  # can e.g. import csv; csv.reader(stream)

    try:
        df = load_changesheet(stream, sep=content_types[content_type])
    except ValueError as ve:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(ve))

    return {"dataframe_as_dict": df.to_dict()}


url_pattern = re.compile(r"https?://(?P<domain>[^/]+)/(?P<path>.+)")


def url_to_name(url):
    m = url_pattern.match(url)
    return f"{'.'.join(reversed(m.group('domain').split('.')))}__{m.group('path').replace('/', '.')}"


def result_for_url_to_json_file(data, url, save_dir):

    with open(os.path.join(save_dir, url_to_name(url)), "w") as f:
        json.dump(data.json(), f)


def fetch_downloaded_json(url, save_dir):
    with open(os.path.join(save_dir, url_to_name(url))) as f:
        return json.load(f)


type_collections = {
    f'nmdc:{spec["items"]["$ref"].split("/")[-1]}': collection_name
    for collection_name, spec in nmdc_jsonschema["properties"].items()
    if collection_name.endswith("_set")
}


@router.post("/metadata/json:validate_urls_file")
async def validate_json_urls_file(urls_file: UploadFile = File(...)):
    """

    Given a text file with one URL per line, will try to validate each URL target
    as a NMDC JSON Schema "nmdc:Database" object.

    """
    content_type = urls_file.content_type
    filename = urls_file.filename
    if content_type != "text/plain":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                f"file {filename} has content type '{content_type}'. "
                f"Only 'text/plain' (*.txt) files are permitted."
            ),
        )
    contents: bytes = await urls_file.read()
    stream = StringIO(contents.decode())  # can e.g. import csv; csv.reader(stream)

    urls = [line.strip() for line in stream if line.strip()]

    def load_url(url, timeout):
        return requests.get(url, timeout=timeout)

    with tempfile.TemporaryDirectory() as temp_dir:

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future_to_url = {executor.submit(load_url, url, 5): url for url in urls}
            for future in concurrent.futures.as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    data = future.result()
                    result_for_url_to_json_file(data, url, temp_dir)
                except Exception as exc:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"{url} generated an exception: {exc}",
                    )

        validator = Draft7Validator(get_nmdc_schema())
        validation_errors = {}

        for url in urls:
            docs = fetch_downloaded_json(url, temp_dir)
            if "activity_set" in docs:
                for doc in docs["activity_set"]:
                    doc_type = doc["type"]
                    try:
                        collection_name = type_collections[doc_type]
                    except KeyError:
                        msg = (
                            f"activity_set doc {doc.get('id', '<id missing>')} "
                            f"has type {doc_type}, which is not in NMDC Schema. "
                            "Note: Case is sensitive."
                        )
                        if "activity_set" in validation_errors:
                            validation_errors["activity_set"].append(msg)
                        else:
                            validation_errors["activity_set"] = [msg]
                        continue

                    if collection_name in docs:
                        docs[collection_name].append(doc)
                    else:
                        docs[collection_name] = [doc]
                del docs["activity_set"]

            for coll_name, coll_docs in docs.items():
                errors = list(validator.iter_errors({coll_name: coll_docs}))
                validation_errors[coll_name] = [e.message for e in errors]

        if all(len(v) == 0 for v in validation_errors.values()):
            return {"result": "All Okay!"}
        else:
            return {"result": "errors", "detail": validation_errors}
