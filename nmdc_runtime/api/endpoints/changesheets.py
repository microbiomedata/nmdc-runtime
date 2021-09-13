from io import StringIO

from fastapi import APIRouter, UploadFile, File, HTTPException
from starlette import status

from nmdc_runtime.api.core.changesheets import load_changesheet

router = APIRouter()


@router.post("/changesheets/validate")
async def validate_changesheet(sheet: UploadFile = File(...)):
    """

    Example changesheets:
     - [nmdc-changesheet-ex-01.csv](https://github.com/microbiomedata/nmdc-runtime/blob/main/metadata-translation/examples/changesheet-ex-01.csv)

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
