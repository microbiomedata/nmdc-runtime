from dagster import op

from nmdc_runtime.lib import gff_to_json


@op
def convert_gff_to_json(gff, md5, informed_by):
    result = gff_to_json.generate_counts(gff, md5, informed_by)
    return result
