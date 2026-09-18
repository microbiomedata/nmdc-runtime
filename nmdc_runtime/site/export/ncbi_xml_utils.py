from io import BytesIO, StringIO
import math
import re
from functools import partial
from typing import Any, Callable, Dict, List, Optional, Tuple

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


# --- Aggregation of slot values across pooled biosamples --------------------
#
# When several NMDC Biosamples are physically pooled (nmdc:Pooling), the NCBI
# BioSample we submit describes the composite. Values measured on the
# constituent biosamples are combined as follows:
#
# * Slots whose MIxS definition allows a range ("{float} - {float} {unit}")
#   are reported as "min-max unit" (or "min unit" when all constituents agree).
# * Slots that MIxS/NCBI constrain to a single float (e.g. `ph`,
#   `carb_nitro_ratio`) get a slot-specific reducer that computes a
#   scientifically defensible single value.
#
# A value is only aggregated when *every* constituent biosample asserts it
# and all asserted units agree; otherwise the slot is omitted for that pool.

# Slots whose MIxS definition permits a range value.
POOLED_RANGE_SLOTS = (
    "depth",
    "temp",
    "org_carb",
    "nitro",
    "tot_nitro_content",
    "water_content",
    "ammonium_nitrogen",
)

_LEADING_NUMBER_RE = re.compile(r"^\s*([-+]?\d*\.?\d+(?:[eE][-+]?\d+)?)\s*(.*)$")


def _format_number(number: float) -> str:
    """Render a number without a trailing ``.0`` when it is integral."""
    if isinstance(number, float) and number.is_integer():
        return str(int(number))
    return str(number)


def _numeric_values_and_unit(value: Any) -> Optional[Tuple[List[float], str]]:
    """Extract the numeric value(s) and unit from a slot value.

    Handles bare numbers, ``QuantityValue`` dicts (single or min/max), strings
    with a leading number (e.g. ``"0.75 g water/g dry soil"``), and lists of
    any of those. Returns ``None`` if the value cannot be interpreted.
    """
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return [float(value)], ""
    if isinstance(value, str):
        match = _LEADING_NUMBER_RE.match(value)
        if not match:
            return None
        return [float(match.group(1))], match.group(2).strip()
    if isinstance(value, dict):
        unit = str(value.get("has_unit", "")).strip()
        if "has_numeric_value" in value:
            return [float(value["has_numeric_value"])], unit
        if (
            "has_minimum_numeric_value" in value
            and "has_maximum_numeric_value" in value
        ):
            return (
                [
                    float(value["has_minimum_numeric_value"]),
                    float(value["has_maximum_numeric_value"]),
                ],
                unit,
            )
        if "has_raw_value" in value:
            return _numeric_values_and_unit(value["has_raw_value"])
        return None
    if isinstance(value, list):
        numbers, units = [], set()
        for item in value:
            parsed = _numeric_values_and_unit(item)
            if parsed is None:
                return None
            numbers.extend(parsed[0])
            units.add(parsed[1])
        if not numbers or len(units) != 1:
            return None
        return numbers, units.pop()
    return None


def _slot_values(biosamples: List[dict], slot: str) -> Optional[List[Any]]:
    """Return the slot's value from each biosample, or ``None`` unless all have one."""
    values = [biosample.get(slot) for biosample in biosamples]
    if not values or any(value is None for value in values):
        return None
    return values


def _parse_all(values: List[Any]) -> Optional[Tuple[List[float], str]]:
    """Parse every value; require a single common unit."""
    numbers, units = [], set()
    for value in values:
        parsed = _numeric_values_and_unit(value)
        if parsed is None:
            return None
        numbers.extend(parsed[0])
        units.add(parsed[1])
    if not numbers or len(units) != 1:
        return None
    return numbers, units.pop()


def aggregate_range(values: List[Any]) -> Optional[str]:
    """Combine numeric values into ``"min-max unit"`` (or ``"value unit"``)."""
    parsed = _parse_all(values)
    if parsed is None:
        return None
    numbers, unit = parsed
    low, high = min(numbers), max(numbers)
    if low == high:
        range_str = _format_number(low)
    else:
        range_str = f"{_format_number(low)}-{_format_number(high)}"
    return f"{range_str} {unit}".strip()


def aggregate_range_slot(slot: str, biosamples: List[dict]) -> Optional[str]:
    values = _slot_values(biosamples, slot)
    return aggregate_range(values) if values else None


def aggregate_collection_date(biosamples: List[dict]) -> Optional[str]:
    """Report the earliest/latest collection dates as an ISO 8601 interval."""
    values = _slot_values(biosamples, "collection_date")
    if not values:
        return None
    dates = []
    for value in values:
        if isinstance(value, dict) and "has_raw_value" in value:
            dates.append(str(value["has_raw_value"]))
        elif isinstance(value, str):
            dates.append(value)
        else:
            return None
    dates.sort()
    return dates[0] if dates[0] == dates[-1] else f"{dates[0]}/{dates[-1]}"


def aggregate_ph(biosamples: List[dict]) -> Optional[str]:
    """Combine pH values into a single pH.

    MIxS constrains ``ph`` to a single float, and pH is logarithmic, so the
    hydrogen ion concentrations are averaged and converted back rather than
    averaging the pH values directly.
    """
    values = _slot_values(biosamples, "ph")
    if not values:
        return None
    parsed = _parse_all(values)
    if parsed is None:
        return None
    ph_values = parsed[0]
    mean_hydrogen_ion = sum(10**-ph for ph in ph_values) / len(ph_values)
    return _format_number(round(-math.log10(mean_hydrogen_ion), 2))


def aggregate_carb_nitro_ratio(biosamples: List[dict]) -> Optional[str]:
    """Combine carbon/nitrogen ratios into a single ratio.

    A mean of ratios is not the ratio of the pooled material, so when every
    constituent reports ``org_carb`` and ``nitro`` in one common unit, the
    ratio of summed carbon to summed nitrogen is used (this assumes
    equal-mass pooling). Otherwise the mean of the asserted ratios is used.
    """
    carbon = _slot_values(biosamples, "org_carb")
    nitrogen = _slot_values(biosamples, "nitro")
    if carbon and nitrogen:
        parsed_carbon = _parse_all(carbon)
        parsed_nitrogen = _parse_all(nitrogen)
        if (
            parsed_carbon is not None
            and parsed_nitrogen is not None
            and parsed_carbon[1] == parsed_nitrogen[1]
            # every constituent contributed exactly one (non-range) value
            and len(parsed_carbon[0]) == len(biosamples)
            and len(parsed_nitrogen[0]) == len(biosamples)
            and sum(parsed_nitrogen[0]) > 0
        ):
            ratio = sum(parsed_carbon[0]) / sum(parsed_nitrogen[0])
            return _format_number(round(ratio, 2))

    ratios = _slot_values(biosamples, "carb_nitro_ratio")
    if not ratios:
        return None
    parsed = _parse_all(ratios)
    if parsed is None:
        return None
    numbers = parsed[0]
    return _format_number(round(sum(numbers) / len(numbers), 2))


# Slot name -> reducer over the constituent biosamples of a pool.
POOLED_VALUE_REDUCERS: Dict[str, Callable[[List[dict]], Optional[str]]] = {
    "collection_date": aggregate_collection_date,
    "ph": aggregate_ph,
    "carb_nitro_ratio": aggregate_carb_nitro_ratio,
    **{slot: partial(aggregate_range_slot, slot) for slot in POOLED_RANGE_SLOTS},
}


def aggregate_pooled_values(biosamples: List[dict]) -> Dict[str, str]:
    """Compute every aggregatable slot value for a pool of biosamples.

    :return: mapping of NMDC slot name to the aggregated value; slots that
        could not be aggregated are omitted.
    """
    aggregated = {}
    for slot, reducer in POOLED_VALUE_REDUCERS.items():
        value = reducer(biosamples)
        if value is not None:
            aggregated[slot] = value
    return aggregated


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

        # aggregate slot values (collection_date, depth, ph, ...) across the
        # constituent biosamples; see `aggregate_pooled_values`
        pooled_biosamples = [
            biosample_lookup[bs_id]
            for bs_id in pooled_biosample_ids
            if bs_id in biosample_lookup
        ]
        aggregated_values = aggregate_pooled_values(pooled_biosamples)

        # update all biosamples that are part of this pooling process
        pooling_info = {
            "processed_sample_id": processed_sample_id,
            "pooling_process_id": pooling_process.get("id"),
            "pooled_biosample_ids": pooled_biosample_ids,
            "aggregated_values": aggregated_values,
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
