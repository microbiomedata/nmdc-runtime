from io import BytesIO, StringIO
from nmdc_runtime.minter.config import typecodes
from lxml import etree

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


def fetch_data_objects_from_biosamples(all_docs_collection, biosamples_list):
    biosample_data_objects = []

    for biosample in biosamples_list:
        current_ids = [biosample["id"]]
        collected_data_objects = []

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
                        data_object_doc = all_docs_collection.find_one(
                            {"id": output_id}
                        )
                        if data_object_doc:
                            collected_data_objects.append(data_object_doc)
                    else:
                        new_current_ids.append(output_id)

            current_ids = new_current_ids

        if collected_data_objects:
            biosample_data_objects.append({biosample["id"]: collected_data_objects})

    return biosample_data_objects


def fetch_omics_processing_from_biosamples(all_docs_collection, biosamples_list):
    biosample_data_objects = []

    for biosample in biosamples_list:
        current_ids = [biosample["id"]]
        collected_data_objects = []

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
                        omics_processing_doc = all_docs_collection.find_one(
                            {"id": document["id"]}
                        )
                        if omics_processing_doc:
                            collected_data_objects.append(omics_processing_doc)
                    else:
                        new_current_ids.append(output_id)

            current_ids = new_current_ids

        if collected_data_objects:
            biosample_data_objects.append({biosample["id"]: collected_data_objects})

    return biosample_data_objects


def fetch_library_preparation_from_biosamples(all_docs_collection, biosamples_list):
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
                "designated_class": "nmdc:LibraryPreparation",
            }
            lib_prep_doc = all_docs_collection.find_one(lib_prep_query)

            if lib_prep_doc:
                biosample_lib_prep.append({biosample_id: lib_prep_doc})
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
