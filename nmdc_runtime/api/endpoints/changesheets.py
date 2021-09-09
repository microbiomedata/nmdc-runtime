import csv
from io import StringIO

from fastapi import APIRouter, UploadFile, File, HTTPException
from starlette import status

router = APIRouter()


@router.post("/changesheets/validate")
async def validate_changesheet(sheet: UploadFile = File(...)):
    content_type = sheet.content_type
    filename = sheet.filename
    if content_type != "text/csv":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=(
                f"file {filename} has content type '{content_type}'. "
                "Only type 'text/csv' (a CSV file) is permitted."
            ),
        )
    contents: bytes = await sheet.read()
    stream = StringIO(contents.decode())
    csvreader = csv.reader(stream)
    rows = []
    for row in csvreader:
        rows.append(row)

    return {"rows": rows}
