"""
Translate an export of the JGI GOLD [1] study, project, and biosample data into JSON conformant with the NMDC JSON schema.
[1] Genomes OnLine Database (GOLD) <https://gold.jgi.doe.gov/>.
"""

from dagster import op, graph

from nmdc_runtime.lib.nmdc_etl_class import NMDC_ETL
from nmdc_runtime.site.translation.util import (
    load_nmdc_etl_class,
    load_mongo_collection,
    preset_prod,
    preset_test,
    schema_validate,
)


@op
def transform_study(_context, nmdc_etl: NMDC_ETL) -> tuple:
    # return {"study_set": nmdc_etl.transform_study()}
    return ("gold.study_set", nmdc_etl.transform_study())


@op
def transform_gold_omics_processing(_context, nmdc_etl: NMDC_ETL) -> tuple:
    return ("gold.omics_processing_set", nmdc_etl.transform_omics_processing())


@op
def transform_biosample(_context, nmdc_etl: NMDC_ETL) -> tuple:
    return ("gold.biosample_set", nmdc_etl.transform_biosample())


@graph
def gold():
    nmdc_etl = load_nmdc_etl_class()
    gold_study = transform_study(nmdc_etl)
    gold_study_validated = schema_validate(gold_study)

    gold_omics_processing = transform_gold_omics_processing(nmdc_etl)
    gold_omics_processing_validated = schema_validate(gold_omics_processing)

    gold_biosample = transform_biosample(nmdc_etl)
    gold_biosample_validated = schema_validate(gold_biosample)

    # load data into mongo
    load_mongo_collection(gold_study_validated)
    load_mongo_collection(gold_omics_processing_validated)
    load_mongo_collection(gold_biosample_validated)


gold_job = gold.to_job(**preset_prod)
test_gold_job = gold.to_job(name="test_gold", **preset_test)
