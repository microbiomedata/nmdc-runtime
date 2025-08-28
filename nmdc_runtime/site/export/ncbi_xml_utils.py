from io import BytesIO, StringIO
from typing import Any, Dict, List

from nmdc_runtime.api.endpoints.util import strip_oid
from nmdc_runtime.minter.config import typecodes
from lxml import etree
from pymongo.collection import Collection

import csv
import requests


def _build_class_map(class_map_data):
    return {
        entry["name"]: entry["schema_class"].split(":")[1] for entry in class_map_data
    }


def get_classname_from_typecode(doc_id):
    class_map_data = typecodes()
    class_map = _build_class_map(class_map_data)

    typecode = doc_id.split(":")[1].split("-")[0]
    return class_map.get(typecode)


def fetch_data_objects_from_biosamples(
    all_docs_collection: Collection,
    data_object_set: Collection,
    biosamples_list: List[Dict[str, Any]],
) -> List[Dict[str, Dict[str, Any]]]:
    """This method fetches the data objects that are "associated" (derived from/products of)
    with their respective biosamples by iterating over the alldocs collection recursively.
    The methods returns a dictionary with biosample ids as keys and the associated list of
    data objects as values.

    :param all_docs_collection: reference to the alldocs collection
    :param data_object_set: reference to the data_object_set collection
    :param biosamples_list: list of biosamples as JSON documents
    :return: list of dictionaries with biosample ids as keys and associated data objects as values
    """
    biosample_data_objects = []

    def collect_data_objects(doc_ids, collected_objects, unique_ids):
        for doc_id in doc_ids:
            if (
                get_classname_from_typecode(doc_id) == "DataObject"
                and doc_id not in unique_ids
            ):
                data_obj = data_object_set.find_one({"id": doc_id})
                if data_obj:
                    collected_objects.append(strip_oid(data_obj))
                    unique_ids.add(doc_id)

    biosample_data_objects = []

    for biosample in biosamples_list:
        current_ids = [biosample["id"]]
        collected_data_objects = []
        unique_ids = set()

        while current_ids:
            new_current_ids = []
            for current_id in current_ids:
                for doc in all_docs_collection.find({"has_input": current_id}):
                    has_output = doc.get("has_output", [])

                    collect_data_objects(has_output, collected_data_objects, unique_ids)
                    new_current_ids.extend(
                        op
                        for op in has_output
                        if get_classname_from_typecode(op) != "DataObject"
                    )

            current_ids = new_current_ids

        if collected_data_objects:
            biosample_data_objects.append({biosample["id"]: collected_data_objects})

    return biosample_data_objects


def fetch_nucleotide_sequencing_from_biosamples(
    all_docs_collection: Collection,
    data_generation_set: Collection,
    biosamples_list: List[Dict[str, Any]],
) -> List[Dict[str, Dict[str, Any]]]:
    """This method fetches the nucleotide sequencing process records that create data objects
    for biosamples by iterating over the alldocs collection recursively.

    :param all_docs_collection: reference to the alldocs collection
    :param data_generation_set: reference to the data_generation_set collection
    :param biosamples_list: list of biosamples as JSON documents
    :return: list of dictionaries with biosample ids as keys and associated nucleotide sequencing
    process objects as values
    """
    biosample_ntseq_objects = []

    for biosample in biosamples_list:
        current_ids = [biosample["id"]]
        collected_ntseq_objects = []
        processed_ids = set()  # Track already processed nucleotide sequencing IDs

        while current_ids:
            new_current_ids = []
            for current_id in current_ids:
                # Find all documents with current_id as input instead of just one
                for document in all_docs_collection.find({"has_input": current_id}):
                    has_output = document.get("has_output")
                    if not has_output:
                        continue

                    for output_id in has_output:
                        if get_classname_from_typecode(output_id) == "DataObject":
                            # Only process if we haven't seen this document ID before
                            if document["id"] not in processed_ids:
                                nucleotide_sequencing_doc = (
                                    data_generation_set.find_one(
                                        {
                                            "id": document["id"],
                                            "type": "nmdc:NucleotideSequencing",
                                        }
                                    )
                                )
                                if nucleotide_sequencing_doc:
                                    collected_ntseq_objects.append(
                                        strip_oid(nucleotide_sequencing_doc)
                                    )
                                    processed_ids.add(document["id"])
                        else:
                            new_current_ids.append(output_id)

            current_ids = new_current_ids

        if collected_ntseq_objects:
            biosample_ntseq_objects.append({biosample["id"]: collected_ntseq_objects})

    return biosample_ntseq_objects


def fetch_library_preparation_from_biosamples(
    all_docs_collection: Collection,
    material_processing_set: Collection,
    biosamples_list: List[Dict[str, Any]],
) -> List[Dict[str, Dict[str, Any]]]:
    """This method fetches the library preparation process records that create processed samples,
    which are further fed/inputted into (by `has_input` slot) a nucleotide sequencing process
    for biosamples by iterating over the alldocs collection recursively.

    :param all_docs_collection: reference to the alldocs collection
    :param material_processing_set: reference to the material_processing_set collection
    :param biosamples_list: list of biosamples as JSON documents
    :return: list of dictionaries with biosample ids as keys and associated library preparation process
    objects as values
    """
    biosample_lib_prep = []

    for biosample in biosamples_list:
        biosample_id = biosample["id"]

        # Step 1: Find any document with biosample id as has_input
        initial_query = {"has_input": biosample_id}
        initial_document = all_docs_collection.find_one(initial_query)

        if not initial_document:
            continue

        initial_output = initial_document.get("has_output")
        if not initial_output:
            continue

        # Step 2: Use has_output to find the library preparation document
        for output_id in initial_output:
            lib_prep_query = {
                "has_input": output_id,
                "type": {"$in": ["LibraryPreparation"]},
            }
            lib_prep_doc = material_processing_set.find_one(lib_prep_query)

            if lib_prep_doc:
                biosample_lib_prep.append({biosample_id: strip_oid(lib_prep_doc)})
                break  # Stop at the first document that meets the criteria

    return biosample_lib_prep


def handle_quantity_value(slot_value):
    if "has_numeric_value" in slot_value and "has_unit" in slot_value:
        return f"{slot_value['has_numeric_value']} {slot_value['has_unit']}"
    elif (
        "has_maximum_numeric_value" in slot_value
        and "has_minimum_numeric_value" in slot_value
        and "has_unit" in slot_value
    ):
        range_value = f"{slot_value['has_minimum_numeric_value']} - {slot_value['has_maximum_numeric_value']}"
        return f"{range_value} {slot_value['has_unit']}"
    elif "has_raw_value" in slot_value:
        return slot_value["has_raw_value"]
    return "Unknown format"


def handle_text_value(slot_value):
    return slot_value.get("has_raw_value", "Unknown format")


def handle_timestamp_value(slot_value):
    return slot_value.get("has_raw_value", "Unknown format")


def handle_controlled_term_value(slot_value):
    if "term" in slot_value:
        term = slot_value["term"]
        if "name" in term and "id" in term:
            return f"{term['name']} [{term['id']}]"
        elif "id" in term:
            return term["id"]
        elif "name" in term:
            return term["name"]
    elif "has_raw_value" in slot_value:
        return slot_value["has_raw_value"]
    return "Unknown format"


def handle_controlled_identified_term_value(slot_value):
    if "term" in slot_value:
        term = slot_value["term"]
        if "name" in term and "id" in term:
            return f"{term['name']} [{term['id']}]"
        elif "id" in term:
            return term["id"]
    elif "has_raw_value" in slot_value:
        return slot_value["has_raw_value"]
    return "Unknown format"


def handle_geolocation_value(slot_value):
    if "latitude" in slot_value and "longitude" in slot_value:
        return f"{slot_value['latitude']} {slot_value['longitude']}"
    elif "has_raw_value" in slot_value:
        return slot_value["has_raw_value"]
    return "Unknown format"


def handle_float_value(slot_value):
    return f"{slot_value:.2f}"


def handle_string_value(slot_value):
    return f"{slot_value}"


def load_mappings(url):
    response = requests.get(url)
    response.raise_for_status()
    file_content = response.text

    attribute_mappings = {}
    slot_range_mappings = {}
    reader = csv.DictReader(StringIO(file_content), delimiter="\t")
    for row in reader:
        if row["ignore"].strip():
            continue

        json_key = row["nmdc_schema_slot"]
        # attribute mappings
        xml_attribute_name = row["ncbi_biosample_attribute_name"]
        attribute_mappings[json_key] = (
            xml_attribute_name if xml_attribute_name else json_key
        )

        # slot range mappings
        data_type = row["nmdc_schema_slot_range"]
        slot_range_mappings[json_key] = data_type if data_type else "default"

    return attribute_mappings, slot_range_mappings


def check_pooling_for_biosamples(
    material_processing_set: Collection, biosamples_list: List[Dict[str, Any]]
) -> Dict[str, Dict[str, Any]]:
    """Check which biosamples are part of pooling processes and return pooling information.

    The way in which we check if a biosample is part of a Pooling process is by checking if
    the biosample id has been asserted on the `has_input` slot/key of an `nmdc:Pooling` process
    instance.

    :param material_processing_set: reference to the material_processing_set collection
    :param biosamples_list: list of all biosamples to check
    :return: dictionary mapping biosample_id to pooling information (empty dict if not pooled)
    """
    result = {}
    # get list of all biosample IDs that are part of a given study
    biosample_lookup = {bs["id"]: bs for bs in biosamples_list}

    # get list of all pooling processes
    pooling_processes = list(material_processing_set.find({"type": "nmdc:Pooling"}))

    # initialize all biosamples as not pooled
    for biosample in biosamples_list:
        result[biosample["id"]] = {}

    # process each pooling process
    for pooling_process in pooling_processes:
        pooled_biosample_ids = pooling_process.get("has_input", [])

        # get the processed sample output from the pooling process
        has_output = pooling_process.get("has_output", [])
        processed_sample_id = None

        for output_id in has_output:
            if get_classname_from_typecode(output_id) == "ProcessedSample":
                processed_sample_id = output_id
                break

        # aggregate the values on `collection_date` and `depth` slots
        # here, we are collecting the `collection_date` and `depth` values
        # asserted on each of the biosamples that are part of a given pooling
        # process in the following way:
        # example of aggregated `collection_date`: 2017-06-05T16:50Z/2017-06-05T17:47Z
        # example of aggregated `depth`: 0-10 m
        collection_dates = []
        depths = []

        for bs_id in pooled_biosample_ids:
            biosample = biosample_lookup.get(bs_id)
            if not biosample:
                continue

            if "collection_date" in biosample:
                collection_date = biosample["collection_date"]
                if (
                    isinstance(collection_date, dict)
                    and "has_raw_value" in collection_date
                ):
                    collection_dates.append(collection_date["has_raw_value"])
                elif isinstance(collection_date, str):
                    collection_dates.append(collection_date)

            if "depth" in biosample:
                depth = biosample["depth"]
                if isinstance(depth, dict):
                    if "has_numeric_value" in depth:
                        depths.append(depth["has_numeric_value"])
                    elif (
                        "has_minimum_numeric_value" in depth
                        and "has_maximum_numeric_value" in depth
                    ):
                        depths.extend(
                            [
                                depth["has_minimum_numeric_value"],
                                depth["has_maximum_numeric_value"],
                            ]
                        )
                elif isinstance(depth, (int, float)):
                    depths.append(depth)

        # create aggregated (forward slash separated) value for `collection_date`
        aggregated_collection_date = None
        if collection_dates:
            sorted_dates = sorted(collection_dates)
            if len(sorted_dates) > 1:
                aggregated_collection_date = f"{sorted_dates[0]}/{sorted_dates[-1]}"
            else:
                aggregated_collection_date = sorted_dates[0]

        # create aggregated (hyphen separated) value for `depth`
        aggregated_depth = None
        if depths:
            min_depth = min(depths)
            max_depth = max(depths)
            if min_depth != max_depth:
                aggregated_depth = f"{min_depth}-{max_depth} m"
            else:
                aggregated_depth = f"{min_depth} m"

        # update all biosamples that are part of this pooling process
        pooling_info = {
            "processed_sample_id": processed_sample_id,
            "pooling_process_id": pooling_process.get("id"),
            "pooled_biosample_ids": pooled_biosample_ids,
            "aggregated_collection_date": aggregated_collection_date,
            "aggregated_depth": aggregated_depth,
        }

        for bs_id in pooled_biosample_ids:
            if bs_id in result:
                result[bs_id] = pooling_info

    return result


def validate_xml(xml, xsd_url):
    response = requests.get(xsd_url)
    response.raise_for_status()
    xsd_content = response.text

    xml_schema_doc = etree.parse(BytesIO(xsd_content.encode("utf-8")))
    xml_schema = etree.XMLSchema(xml_schema_doc)

    xml_doc = etree.parse(BytesIO(xml.encode("utf-8")))

    if not xml_schema.validate(xml_doc):
        raise ValueError(f"There were errors while validating against: {xsd_url}")

    return True
