# Third-party packages:
import pymongo
from jsonschema import Draft201909Validator
from nmdc_schema.nmdc_data import get_nmdc_jsonschema_dict
from nmdc_schema.migrators.adapters.mongo_adapter import MongoAdapter


MONGO_URL = "mongodb://shalsh:onion-car-bingo-84@localhost:27017/?authSource=admin"

# Mongo client for "origin" MongoDB server.
mongo_client = pymongo.MongoClient(host=MONGO_URL, directConnection=True)

nmdc_jsonschema: dict = get_nmdc_jsonschema_dict()
nmdc_jsonschema_validator = Draft201909Validator(nmdc_jsonschema)


# all_collections = mongo_client["nmdc"].list_collection_names()
all_collections = [
    "biosample_set",
    "collecting_biosamples_from_site_set",
    "data_object_set",
    "extraction_set",
    "field_research_site_set",
    "functional_annotation_agg",
    "functional_annotation_set",
    "genome_feature_set",
    "library_preparation_set",
    "mags_activity_set",
    "metabolomics_analysis_activity_set",
    "metagenome_annotation_activity_set",
    "metagenome_assembly_set",
    "metagenome_sequencing_activity_set",
    "metaproteomics_analysis_set",
    "metatranscriptome_activity_set",
    "non_analysis_activity_set",
    "omics_processing_set",
    "planned_process_set",
    "pooling_set",
    "processed_sample_set",
    "read_based_taxonomy_analysis_activity_set",
    "read_qc_analysis_activity_set",
    "study_set"
]
for c in all_collections:
    print(c)
    collection = mongo_client["nmdc"][c]
    for document in collection.find():
        document_without_underscore_id_key = {key: value for key, value in document.items() if key != "_id"}
        root_to_validate = dict([("study_set", [document_without_underscore_id_key])])
        nmdc_jsonschema_validator.validate(root_to_validate)  # raises exception if invalid
        # print("validation completed")
