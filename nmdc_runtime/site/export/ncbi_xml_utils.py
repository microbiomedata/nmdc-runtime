from io import StringIO
import csv
import requests


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
