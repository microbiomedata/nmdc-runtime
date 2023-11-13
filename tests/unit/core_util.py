from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from nmdc_runtime.api.core.util import sha256hash_from_file

TEST_FILES_DIR = Path(__file__).parent.parent.joinpath("files")


def test_sha256hash_from_file_is_timestamp_dependent():
    file_path = str(TEST_FILES_DIR.joinpath("test_changesheet_update_one_ph.tsv"))
    ts_1 = datetime.now(tz=ZoneInfo("America/Los_Angeles"))
    ts_2 = ts_1 + timedelta(minutes=1)
    hashes = []
    for ts in (ts_1, ts_2):
        hashes.append(
            sha256hash_from_file(
                file_path=file_path, timestamp=ts.isoformat(timespec="minutes")
            )
        )
    assert hashes[0] != hashes[1]
