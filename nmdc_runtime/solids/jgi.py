import json
from subprocess import Popen, PIPE, STDOUT, CalledProcessError
import tempfile
from zipfile import ZipFile

from dagster import solid


def run_and_log(shell_cmd, context):
    process = Popen(shell_cmd, shell=True, stdout=PIPE, stderr=STDOUT)
    for line in process.stdout:
        context.log.info(line.decode())
    retcode = process.wait()
    if retcode:
        raise CalledProcessError(retcode, process.args)


@solid()
def get_json_db(context):
    with tempfile.TemporaryDirectory() as tmpdirname:
        context.log.info("shallow-cloning nmdc-metadata repo")
        run_and_log(
            "git clone https://github.com/microbiomedata/nmdc-metadata.git"
            f" --branch master --single-branch {tmpdirname}/nmdc-metadata",
            context,
        )

        context.log.info("running `make run-etl` in nmdc-metadata repo")
        run_and_log(f"cd {tmpdirname}/nmdc-metadata" f" && make run-etl", context)

        with ZipFile(
            f"{tmpdirname}/nmdc-metadata/metadata-translation/src/data/nmdc_database.json.zip"
        ) as zf:
            name = zf.namelist()[0]
            with zf.open(name) as f:
                rv = json.load(f)
        context.log.info(str(list(rv.keys())))

    return rv
