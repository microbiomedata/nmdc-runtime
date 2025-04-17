from io import BytesIO, StringIO
from typing import Any, Dict, List, Union

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


def get_instruments(instrument_set_collection):
    # dictionary to capture a list of all instruments
    # Structure of dict:
    # {"instrument_id": {"vendor": "vendor_name", "model": "model_name"}}
    all_instruments = {}

    try:
        query = {"type": "nmdc:Instrument"}
        cursor = instrument_set_collection.find(query)

        for document in cursor:
            instrument_id = document.get("id")
            vendor = document.get("vendor")
            model = document.get("model")

            if not instrument_id or not vendor or not model:
                continue

            all_instruments[instrument_id] = {"vendor": vendor, "model": model}

        return all_instruments
    except Exception as e:
        raise RuntimeError(f"An error occurred while fetching instrument data: {e}")


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

        while current_ids:
            new_current_ids = []
            for current_id in current_ids:
                query = {"has_input": current_id}
                document = all_docs_collection.find_one(query)

                if not document:
                    continue

                has_output = document.get("has_output")
                if not has_output:
                    continue

                for output_id in has_output:
                    if get_classname_from_typecode(output_id) == "DataObject":
                        nucleotide_sequencing_doc = data_generation_set.find_one(
                            {"id": document["id"]}
                        )
                        if nucleotide_sequencing_doc:
                            collected_ntseq_objects.append(
                                strip_oid(nucleotide_sequencing_doc)
                            )
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
        range_value = (
            slot_value["has_maximum_numeric_value"]
            - slot_value["has_minimum_numeric_value"]
        )
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


def check_if_biosample_is_pooled(
    biosample_id: str,
    material_processing_set: Collection,
    all_docs_collection: Collection,
    processed_sample_set: Collection,
) -> dict:
    """
    Checks if a biosample is part of a pooling process (i.e., if it is asserted on the `has_input` key
    of a Pooling record) and if so, traverses the chain to get the library name.

    This method uses all_docs_collection to follow the document chain from biosample through pooling
    and to library preparation.

    Args:
        biosample_id: ID of the biosample to check
        material_processing_set: Reference to the material_processing_set collection
        all_docs_collection: Reference to the all_docs collection
        processed_sample_set: Reference to the processed_sample_set collection

    Returns:
        A dictionary containing:
            'is_pooled': True if biosample is part of a pooling process, False otherwise
            'library_name': The name of the processed sample (output of library prep) if exists
            'pooled_biosamples': List of all biosample IDs in the same pool (if pooled)
    """
    result = {"is_pooled": False, "library_name": None, "pooled_biosamples": []}

    # Step 1: Check if biosample is part of a pooling process
    # (find a Pooling record that has this biosample in its has_input)
    pooling_query = {"has_input": biosample_id, "type": "nmdc:Pooling"}
    pooling_record = material_processing_set.find_one(pooling_query)

    if not pooling_record:
        return result

    # Biosample is part of a pooling process
    result["is_pooled"] = True

    # Get all biosamples that are part of this pooling process
    if "has_input" in pooling_record and isinstance(pooling_record["has_input"], list):
        result["pooled_biosamples"] = pooling_record["has_input"]

    # Get the output of the pooling process
    if "has_output" not in pooling_record or not pooling_record["has_output"]:
        return result

    pooling_output = pooling_record["has_output"]
    if not isinstance(pooling_output, list) or not pooling_output:
        return result

    pooling_output_id = pooling_output[0]  # Assuming there's just one output

    # Step 2: Find the library preparation that has this pooling output as input
    # First, try direct query in material_processing_set
    lib_prep_query = {"has_input": pooling_output_id, "type": "nmdc:LibraryPreparation"}
    lib_prep_record = material_processing_set.find_one(lib_prep_query)

    # If direct query fails, search through all_docs_collection
    if not lib_prep_record:
        # Find any document that has the pooling output as input
        intermediate_query = {"has_input": pooling_output_id}
        intermediate_doc = all_docs_collection.find_one(intermediate_query)

        if intermediate_doc and "has_output" in intermediate_doc:
            for output_id in intermediate_doc.get("has_output", []):
                # Try to find a LibraryPreparation that has this output as input
                lib_prep_query = {
                    "has_input": output_id,
                    "type": "nmdc:LibraryPreparation",
                }
                lib_prep_record = material_processing_set.find_one(lib_prep_query)
                if lib_prep_record:
                    break

                # If that fails, try searching in all_docs_collection
                if not lib_prep_record:
                    lib_prep_query = {
                        "has_input": output_id,
                        "type": "nmdc:LibraryPreparation",
                    }
                    lib_prep_record = all_docs_collection.find_one(lib_prep_query)
                    if lib_prep_record:
                        break

    # If we still couldn't find a library preparation record, try using
    # the technique from fetch_library_preparation_from_biosamples
    if not lib_prep_record:
        # Find any document with the pooling output id as has_input
        initial_query = {"has_input": pooling_output_id}
        initial_document = all_docs_collection.find_one(initial_query)

        if initial_document and "has_output" in initial_document:
            for output_id in initial_document.get("has_output", []):
                lib_prep_query = {
                    "has_input": output_id,
                    "type": {"$in": ["LibraryPreparation", "nmdc:LibraryPreparation"]},
                }
                lib_prep_record = material_processing_set.find_one(lib_prep_query)
                if lib_prep_record:
                    break

                # Try in all_docs_collection as well
                lib_prep_record = all_docs_collection.find_one(lib_prep_query)
                if lib_prep_record:
                    break

    # If we found a library preparation record, extract the processed sample name
    if (
        lib_prep_record
        and "has_output" in lib_prep_record
        and lib_prep_record["has_output"]
    ):
        lib_prep_output = lib_prep_record["has_output"]
        if isinstance(lib_prep_output, list) and lib_prep_output:
            processed_sample_id = lib_prep_output[0]  # Assuming there's just one output

            # Get the processed sample record to retrieve its name
            processed_sample = processed_sample_set.find_one(
                {"id": processed_sample_id}
            )

            if processed_sample and "name" in processed_sample:
                result["library_name"] = processed_sample["name"]

    return result
