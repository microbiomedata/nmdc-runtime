<<<<<<< HEAD:nmdc_runtime/dagster/translation/gold.py
"""
Translate an export of the JGI GOLD [1] study, project, and biosample data into JSON conformant with the NMDC JSON schema.
[1] Genomes OnLine Database (GOLD) <https://gold.jgi.doe.gov/>.
"""

from dagster import op, graph

from nmdc_runtime.dagster.translation.util import (
    load_nmdc_etl_class,
    load_mongo_collection,
    preset_prod,
    preset_test,
)
from nmdc_runtime.lib.nmdc_etl_class import NMDC_ETL


@op
def transform_study(_context, nmdc_etl: NMDC_ETL) -> tuple:
    # return {"study_set": nmdc_etl.transform_study()}
    return ("gold.study_set", nmdc_etl.transform_study())


@op
def transform_omics_processing(_context, nmdc_etl: NMDC_ETL) -> tuple:
    return ("gold.omics_processing_set", nmdc_etl.transform_omics_processing())


@op
def transform_biosample(_context, nmdc_etl: NMDC_ETL) -> tuple:
    return ("gold.biosample_set", nmdc_etl.transform_biosample())


@graph
def gold():
    nmdc_etl = load_nmdc_etl_class()
    gold_study = transform_study(nmdc_etl)
    gold_omics_processing = transform_omics_processing(nmdc_etl)
    gold_biosample = transform_biosample(nmdc_etl)

    # load data into mongo
    load_mongo_collection(gold_study)
    load_mongo_collection(gold_omics_processing)
    load_mongo_collection(gold_biosample)


gold_job = gold.to_job(**preset_prod)
test_gold_job = gold.to_job(**preset_test)
=======
# """
# Translating an export of the JGI GOLD [1] database of SQL tables to the NMDC database JSON schema.
#
# [1] Genomes OnLine Database (GOLD) <https://gold.jgi.doe.gov/>.
# """
#
# import os
#
# from dagster import solid
# from git_root import git_root
#
# from nmdc_runtime.site.translation.ops import load_nmdc_etl_class
# from nmdc_runtime.lib.nmdc_etl_class import NMDC_ETL
#
#
# @solid
# def transform_study(context, nmdc_etl: NMDC_ETL) -> tuple:
#     # return {"study_set": nmdc_etl.transform_study()}
#     return ("gold.study_set", nmdc_etl.transform_study())
#
#
# @solid
# def transform_omics_processing(context, nmdc_etl: NMDC_ETL) -> tuple:
#     return ("gold.omics_processing_set", nmdc_etl.transform_omics_processing())
#
#
# @solid
# def transform_biosample(context, nmdc_etl: NMDC_ETL) -> tuple:
#     return ("gold.biosample_set", nmdc_etl.transform_biosample())
#
#
# @solid(required_resource_keys={"mongo"})
# def load_mongo_collection(context, data: tuple):
#     mongo_db = context.resources.mongo.db
#     collecton_name = data[0]  # get collection name
#     documents = data[1]  # get data portion of tuple
#     collecton = mongo_db[collecton_name]  # get mongo collection
#
#     # drop collection if exists
#     collecton.drop()
#
#     # insert data
#     collecton.insert(documents)
#
#
# ### TESTING PIPELINE
# if __name__ == "__main__":
#     from dagster import pipeline
#     from dagster import execute_pipeline
#
#     @pipeline
#     def gold_pipeline1():
#         # load_merged_data_source()
#         db = get_mongo_db()
#         nmdc_etl = load_nmdc_etl_class()
#         gold_study = transform_study(nmdc_etl)
#         gold_omics_processing = transform_omics_processing(nmdc_etl)
#         gold_biosample = transform_biosample(nmdc_etl)
#
#         # load data into mongo
#         load_mongo_collection(db, gold_study)
#         load_mongo_collection(db, gold_omics_processing)
#         load_mongo_collection(db, gold_biosample)
#
#     run_config1 = {
#         "solids": {
#             "load_nmdc_etl_class": {
#                 "config": {
#                     "data_file": git_root(
#                         "metadata-translation/src/data/nmdc_merged_data.tsv.zip"
#                     ),
#                     "sssom_map_file": "",
#                     "spec_file": git_root("nmdc_runtime/lib/nmdc_data_source.yaml"),
#                 }
#             },
#             "get_mongo_db": {
#                 "config": {
#                     "host": os.getenv("MONGO_HOST"),
#                     "username": os.getenv("MONGO_USERNAME"),
#                     "password": os.getenv("MONGO_PASSWORD"),
#                     "dbname": "nmdc_etl_staging",
#                 }
#             },
#         },
#     }
#
#     result = execute_pipeline(gold_pipeline1, run_config=run_config1)
>>>>>>> main:nmdc_runtime/site/translation/gold.py
