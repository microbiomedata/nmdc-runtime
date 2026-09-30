import os
import re
import datetime
import xml.etree.ElementTree as ET
import xml.dom.minidom

from functools import lru_cache
from typing import Any, Dict, List, Set, Tuple
from urllib.parse import urlparse
from unidecode import unidecode
from nmdc_runtime.site.export.ncbi_xml_utils import (
    handle_controlled_identified_term_value,
    handle_controlled_term_value,
    handle_geolocation_value,
    handle_quantity_value,
    handle_text_value,
    handle_timestamp_value,
    handle_float_value,
    handle_string_value,
    load_mappings,
)
from nmdc_runtime.util import nmdc_schema_view

# NMDC Biosample slots that should never be emitted as NCBI BioSample <Attribute>
# elements, even if they appear in the attribute mapping file.
#
# In addition to the slots listed here, every slot descending from the
# `external_database_identifiers` grouping slot in the NMDC schema (e.g.
# `gold_biosample_identifiers`, `img_identifiers`) is excluded, since NMDC
# does not maintain identifier mappings to external systems in NCBI records.
EXCLUDED_BIOSAMPLE_SLOTS = {"biosample_categories", "type"}


@lru_cache
def get_excluded_biosample_slots() -> frozenset:
    """Return the full set of NMDC Biosample slots to omit from BioSample attributes."""
    external_id_slots = nmdc_schema_view().slot_descendants(
        "external_database_identifiers"
    )
    return frozenset(EXCLUDED_BIOSAMPLE_SLOTS) | frozenset(external_id_slots)


class NCBISubmissionXML:
    def __init__(self, nmdc_study: Any, ncbi_submission_metadata: dict):
        self.root = ET.Element("Submission")

        self.nmdc_study_id = nmdc_study.get("id")
        self.nmdc_study_title = nmdc_study.get("title")
        self.nmdc_study_description = nmdc_study.get("description")
        # get the first INSDC BioProject ID from the NMDC study
        self.ncbi_bioproject_id = nmdc_study.get("insdc_bioproject_identifiers")[0]
        # the value asserted in "insdc_bioproject_identifiers" will be a CURIE, so extract
        # everything after the prefix and delimiter (":")
        self.ncbi_bioproject_id = self.ncbi_bioproject_id.split(":")[-1]

        # Get the first principal investigator from the NMDC study's CRediT associations
        principal_investigator = next(
            ca.get("applies_to_agent")
            for ca in nmdc_study.get("has_credit_associations", [])
            if "Principal Investigator" in ca.get("applied_roles", [])
        )
        self.nmdc_pi_email = principal_investigator.get("email", "")
        nmdc_study_pi_name = principal_investigator.get("name", "").split()
        self.first_name = nmdc_study_pi_name[0]
        self.last_name = nmdc_study_pi_name[1] if len(nmdc_study_pi_name) > 1 else None

        self.nmdc_ncbi_attribute_mapping_file_url = ncbi_submission_metadata.get(
            "nmdc_ncbi_attribute_mapping_file_url"
        )
        self.ncbi_submission_metadata = ncbi_submission_metadata.get(
            "ncbi_submission_metadata", {}
        )
        self.ncbi_biosample_metadata = ncbi_submission_metadata.get(
            "ncbi_biosample_metadata", {}
        )

        # dispatcher dictionary capturing handlers for NMDC object to NCBI flat Attribute
        # type handlers
        self.type_handlers = {
            "QuantityValue": handle_quantity_value,
            "TextValue": handle_text_value,
            "TimestampValue": handle_timestamp_value,
            "ControlledTermValue": handle_controlled_term_value,
            "ControlledIdentifiedTermValue": handle_controlled_identified_term_value,
            "GeolocationValue": handle_geolocation_value,
            "float": handle_float_value,
            "string": handle_string_value,
        }

    def set_element(self, tag, text="", attrib=None, children=None):
        attrib = attrib or {}
        children = children or []
        element = ET.Element(tag, attrib=attrib)
        element.text = text
        for child in children:
            element.append(child)
        return element

    def _has_insdc_biosample_identifier(self, biosample: dict) -> bool:
        """
        Check if a biosample has INSDC biosample identifiers asserted.
        Returns True if the biosample has insdc_biosample_identifiers field with values.
        """
        insdc_ids = biosample.get("insdc_biosample_identifiers", [])
        return isinstance(insdc_ids, list) and len(insdc_ids) > 0

    @staticmethod
    def _biosample_accessions(biosample: dict) -> Set[str]:
        """Return the bare INSDC BioSample accessions asserted on a biosample.

        NMDC stores them as CURIEs such as ``biosample:SAMEA104200253``; NCBI wants
        the accession alone (``SAMEA104200253``).
        """
        insdc_ids = biosample.get("insdc_biosample_identifiers") or []
        if not isinstance(insdc_ids, list):
            return set()
        return {str(i).split(":", 1)[1] if ":" in str(i) else str(i) for i in insdc_ids}

    @classmethod
    def _resolve_existing_biosample_accessions(
        cls,
        nmdc_biosamples: List[Dict[str, Any]],
        pooling_data: Dict[str, Dict[str, Any]],
    ) -> Tuple[Dict[str, str], Dict[str, str]]:
        """Work out which samples already exist at NCBI/ENA and under which accession.

        Some samples in a study were registered with INSDC before NMDC got involved
        (for NEON soil, NEON registered them at ENA, hence ``SAMEA...``). An SRA
        action for such a sample must reference that accession with a
        ``<PrimaryId db="BioSample">``; a ``<SPUID>`` only resolves against BioSamples
        submitted under NMDC's own namespace, which these never were.

        Accessions are asserted on biosamples, but a pooled run belongs to the
        composite sample, so the composite's accession is inferred as the one shared
        by every constituent biosample. A biosample's *own* accession is whatever is
        left after removing the composite's.

        :return: ``(biosample_accessions, pooled_sample_accessions)`` where the first
            maps a biosample id to its own accession and the second maps a pooled
            ProcessedSample id to the composite's accession. A sample whose accession
            cannot be pinned down to exactly one value is left out, in which case the
            exporter falls back to referencing it by SPUID as before.
        """
        accessions = {b["id"]: cls._biosample_accessions(b) for b in nmdc_biosamples}

        pooled_sample_accessions: Dict[str, str] = {}
        members_by_pool: Dict[str, List[str]] = {}
        for biosample_id, pooling_info in pooling_data.items():
            if biosample_id in accessions and pooling_info.get("processed_sample_id"):
                members_by_pool.setdefault(
                    pooling_info["processed_sample_id"], []
                ).append(biosample_id)
        for processed_sample_id, members in members_by_pool.items():
            shared = set.intersection(*(accessions[m] for m in members))
            if len(shared) == 1:
                pooled_sample_accessions[processed_sample_id] = next(iter(shared))

        biosample_accessions: Dict[str, str] = {}
        for biosample_id, own in accessions.items():
            pool_accession = pooled_sample_accessions.get(
                pooling_data.get(biosample_id, {}).get("processed_sample_id")
            )
            own = own - {pool_accession}
            if len(own) == 1:
                biosample_accessions[biosample_id] = next(iter(own))

        return biosample_accessions, pooled_sample_accessions

    @staticmethod
    def _biosample_ref_element(set_element, spuid, org, accession=None):
        """``<AttributeRefId name="BioSample">`` pointing at an existing accession when
        one is known, otherwise at the SPUID of a BioSample in this submission."""
        if accession:
            ref = set_element("PrimaryId", accession, {"db": "BioSample"})
        else:
            ref = set_element("SPUID", spuid, {"spuid_namespace": org})
        return set_element(
            "AttributeRefId",
            attrib={"name": "BioSample"},
            children=[set_element("RefId", children=[ref])],
        )

    def set_description(self, email, first, last, org, date=None):
        date = date or datetime.datetime.now().strftime("%Y-%m-%d")
        description = self.set_element(
            "Description",
            children=[
                self.set_element(
                    "Comment", f"NMDC Submission for {self.nmdc_study_id}"
                ),
                self.set_element(
                    "Organization",
                    attrib={"role": "owner", "type": "center"},
                    children=[
                        self.set_element("Name", org),
                        self.set_element(
                            "Contact",
                            attrib={"email": email},
                            children=[
                                self.set_element(
                                    "Name",
                                    children=[
                                        self.set_element("First", first),
                                        self.set_element("Last", last),
                                    ],
                                )
                            ],
                        ),
                    ],
                ),
                self.set_element("Hold", attrib={"release_date": date}),
            ],
        )
        self.root.append(description)

    def set_descriptor(self, title, description):
        descriptor_elements = []
        descriptor_elements.append(self.set_element("Title", title))
        descriptor_elements.append(
            self.set_element(
                "Description", children=[self.set_element("p", description)]
            )
        )

        return descriptor_elements

    def set_bioproject(self, title, project_id, description, data_type, org):
        action = self.set_element("Action")
        add_data = self.set_element("AddData", attrib={"target_db": "BioProject"})

        data_element = self.set_element("Data", attrib={"content_type": "XML"})
        xml_content = self.set_element("XmlContent")
        project = self.set_element("Project", attrib={"schema_version": "2.0"})

        project_id_element = self.set_element("ProjectID")
        spuid = self.set_element("SPUID", project_id, {"spuid_namespace": org})
        project_id_element.append(spuid)

        descriptor = self.set_descriptor(title, description)
        project_type = self.set_element("ProjectType")
        # "sample_scope" is a enumeration feild. Docs: https://www.ncbi.nlm.nih.gov/data_specs/schema/other/bioproject/Core.xsd
        # scope is "eEnvironment" when "Content of species in a sample is not known, i.e. microbiome,metagenome, etc.."
        project_type_submission = self.set_element(
            "ProjectTypeSubmission", attrib={"sample_scope": "eEnvironment"}
        )
        intended_data_type_set = self.set_element("IntendedDataTypeSet")
        data_type_element = self.set_element("DataType", data_type)

        intended_data_type_set.append(data_type_element)
        project_type_submission.append(intended_data_type_set)
        project_type.append(project_type_submission)

        project.extend([project_id_element] + descriptor + [project_type])

        xml_content.append(project)
        data_element.append(xml_content)
        add_data.append(data_element)

        identifier = self.set_element("Identifier")
        spuid_identifier = self.set_element(
            "SPUID", project_id, {"spuid_namespace": org}
        )
        identifier.append(spuid_identifier)
        add_data.append(identifier)

        action.append(add_data)
        self.root.append(action)

    @staticmethod
    def _split_pooled_and_individual_sequencing(
        pooling_info: Dict[str, Any], ntseq_records: List[Dict[str, Any]]
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Split a pooled biosample's NucleotideSequencing records into the runs that
        were performed on the pool and the runs performed on the biosample by itself.

        A biosample can be sequenced individually *and* as part of a pool. The
        traversal that collects NucleotideSequencing records for a biosample reaches
        both runs, so without this split the individual run's files would be folded
        into the pooled sample's SRA action and attributed to the wrong BioSample.

        `pooling_info["nucleotide_sequencing_ids"]` (see
        `check_pooling_for_biosamples`) lists the runs downstream of the pooled
        ProcessedSample. When it is absent every run is treated as pooled, which
        preserves the previous behavior.
        """
        pooled_ntseq_ids = pooling_info.get("nucleotide_sequencing_ids")
        if pooled_ntseq_ids is None:
            return ntseq_records, []
        pooled_ntseq_ids = set(pooled_ntseq_ids)
        pooled_ntseq_records = [
            n for n in ntseq_records if n.get("id") in pooled_ntseq_ids
        ]
        individual_ntseq_records = [
            n for n in ntseq_records if n.get("id") not in pooled_ntseq_ids
        ]
        return pooled_ntseq_records, individual_ntseq_records

    @classmethod
    def _individually_sequenced_pooled_biosample_ids(
        cls,
        pooling_data: Dict[str, Dict[str, Any]],
        nmdc_nucleotide_sequencing: List[Dict[str, List[Dict[str, Any]]]],
    ) -> Set[str]:
        """Return the ids of pooled biosamples that also have their own sequencing run."""
        result = set()
        for ntseq_dict in nmdc_nucleotide_sequencing:
            for biosample_id, ntseq_records in ntseq_dict.items():
                pooling_info = pooling_data.get(biosample_id, {})
                if not pooling_info or not pooling_info.get("pooling_process_id"):
                    continue
                _, individual_ntseq_records = (
                    cls._split_pooled_and_individual_sequencing(
                        pooling_info, ntseq_records
                    )
                )
                if individual_ntseq_records:
                    result.add(biosample_id)
        return result

    def set_biosample(
        self,
        organism_name,
        org,
        bioproject_id,
        nmdc_biosamples,
        pooled_biosamples_data=None,
        individually_sequenced_biosample_ids=None,
        pooled_sample_accessions=None,
    ):
        attribute_mappings, slot_range_mappings = load_mappings(
            self.nmdc_ncbi_attribute_mapping_file_url
        )
        excluded_slots = get_excluded_biosample_slots()

        # Use provided pooling data or empty dict
        pooling_data = pooled_biosamples_data or {}
        # Composite accessions already registered with INSDC, keyed by pooled
        # ProcessedSample id (see `_resolve_existing_biosample_accessions`).
        pooled_sample_accessions = pooled_sample_accessions or {}
        # Pooled biosamples that were also sequenced on their own get a BioSample
        # of their own (SPUID = biosample id) in addition to the pooled BioSample,
        # so the individual run's SRA action has something to reference.
        individually_sequenced = set(individually_sequenced_biosample_ids or ())

        # Group biosamples by pooling process
        pooling_groups = {}
        individual_biosamples = []

        for biosample in nmdc_biosamples:
            pooling_info = pooling_data.get(biosample["id"], {})
            if pooling_info and pooling_info.get("pooling_process_id"):
                pooling_process_id = pooling_info["pooling_process_id"]
                if pooling_process_id not in pooling_groups:
                    pooling_groups[pooling_process_id] = {
                        "biosamples": [],
                        "pooling_info": pooling_info,
                    }
                pooling_groups[pooling_process_id]["biosamples"].append(biosample)
                if biosample["id"] in individually_sequenced:
                    individual_biosamples.append(biosample)
            else:
                individual_biosamples.append(biosample)

        # Process pooled sample groups - create one <Action> block per pooling process
        for pooling_process_id, group_data in pooling_groups.items():
            # Skip if any biosample in the pool has INSDC identifiers
            if any(
                self._has_insdc_biosample_identifier(biosample)
                for biosample in group_data["biosamples"]
            ):
                continue

            self._create_pooled_biosample_action(
                group_data["biosamples"],
                group_data["pooling_info"],
                organism_name,
                org,
                bioproject_id,
                attribute_mappings,
                slot_range_mappings,
                excluded_slots,
            )

        # Process individual biosamples
        for biosample in individual_biosamples:
            # Skip if the biosample itself is already registered with INSDC. An
            # accession it merely shares with its pool mates belongs to the
            # composite sample, not to this biosample, so it does not count.
            pool_accession = pooled_sample_accessions.get(
                pooling_data.get(biosample["id"], {}).get("processed_sample_id")
            )
            if self._biosample_accessions(biosample) - {pool_accession}:
                continue

            attributes = {}
            sample_id_value = None
            env_package = None

            # Get pooling info for this specific biosample. A pooled biosample that
            # is listed here because it was also sequenced individually is described
            # as itself, not as the pool.
            if biosample["id"] in individually_sequenced:
                pooling_info = {}
            else:
                pooling_info = pooling_data.get(biosample["id"], {})

            for json_key, value in biosample.items():
                if json_key in excluded_slots:
                    continue

                if isinstance(value, list):
                    for item in value:
                        if json_key not in attribute_mappings:
                            continue

                        xml_key = attribute_mappings[json_key]
                        value_type = slot_range_mappings.get(json_key, "string")
                        handler = self.type_handlers.get(
                            value_type, handle_string_value
                        )

                        # Special handling for "elev" key
                        if json_key == "elev":
                            value = f"{float(value)} m"  # Convert to float if possible
                            attributes[xml_key] = value
                            continue  # Skip applying the handler to this key

                        formatted_value = handler(item)

                        # Combine multiple values with a separator for list elements
                        if xml_key in attributes:
                            attributes[xml_key] += f"| {formatted_value}"
                        else:
                            attributes[xml_key] = formatted_value
                    continue

                if json_key == "env_package":
                    env_package = f"MIMS.me.{handle_text_value(value)}.6.0"

                # Special handling for NMDC Biosample "id"
                if json_key == "id":
                    # Use ProcessedSample ID if this is a pooled sample, otherwise use biosample ID
                    if pooling_info and pooling_info.get("processed_sample_id"):
                        sample_id_value = pooling_info["processed_sample_id"]
                    else:
                        sample_id_value = value
                    continue

                if json_key not in attribute_mappings:
                    continue

                xml_key = attribute_mappings[json_key]
                value_type = slot_range_mappings.get(json_key, "string")
                handler = self.type_handlers.get(value_type, handle_string_value)

                # Special handling for "elev" key
                if json_key == "elev":
                    value = f"{float(value)} m"  # Convert to float if possible
                    attributes[xml_key] = value
                    continue  # Skip applying the handler to this key

                # Special handling for "host_taxid"
                if json_key == "host_taxid" and isinstance(value, dict):
                    if "term" in value and "id" in value["term"]:
                        value = re.findall(r"\d+", value["term"]["id"].split(":")[1])[0]
                    attributes[xml_key] = value
                    continue  # Skip applying the handler to this key

                # Special handling for "geo_loc_name" - convert unicode to closest ASCII characters
                if json_key == "geo_loc_name":
                    formatted_value = handler(value)
                    formatted_value_ascii = unidecode(formatted_value)
                    attributes[xml_key] = formatted_value_ascii
                    continue  # Skip applying the handler to this key

                # Default processing for other keys
                formatted_value = handler(value)
                attributes[xml_key] = formatted_value

            # Override with aggregated values for pooled samples
            if pooling_info:
                for slot, value in pooling_info.get("aggregated_values", {}).items():
                    attributes[attribute_mappings.get(slot, slot)] = value

                # Add samp_pooling attribute with semicolon-delimited biosample IDs
                if pooling_info.get("pooled_biosample_ids"):
                    attributes["samp_pooling"] = ";".join(
                        pooling_info["pooled_biosample_ids"]
                    )

            biosample_elements = [
                self.set_element(
                    "SampleId",
                    children=[
                        self.set_element(
                            "SPUID", sample_id_value, {"spuid_namespace": org}
                        )
                    ],
                ),
                self.set_element(
                    "Descriptor",
                    children=[
                        self.set_element(
                            "Title",
                            attributes.get(
                                "name",
                                # fallback title if "name" is not present
                                f"NMDC Biosample {sample_id_value} from {organism_name}, part of {self.nmdc_study_id} study",
                            ),
                        ),
                    ]
                    + (
                        # Add external links for pooled samples
                        [
                            self.set_element(
                                "ExternalLink",
                                attrib={"label": "NMDC Processed Sample"},
                                children=[
                                    self.set_element(
                                        "URL",
                                        f"https://bioregistry.io/{pooling_info['processed_sample_id']}",
                                    )
                                ],
                            ),
                            self.set_element(
                                "ExternalLink",
                                attrib={"label": "NMDC Pooling Process"},
                                children=[
                                    self.set_element(
                                        "URL",
                                        f"https://bioregistry.io/{pooling_info['pooling_process_id']}",
                                    )
                                ],
                            ),
                        ]
                        if pooling_info
                        and pooling_info.get("processed_sample_id")
                        and pooling_info.get("pooling_process_id")
                        else [
                            # Add external link for individual biosamples
                            self.set_element(
                                "ExternalLink",
                                attrib={"label": sample_id_value},
                                children=[
                                    self.set_element(
                                        "URL",
                                        f"https://bioregistry.io/{sample_id_value}",
                                    )
                                ],
                            ),
                        ]
                    ),
                ),
                self.set_element(
                    "Organism",
                    children=[self.set_element("OrganismName", organism_name)],
                ),
                self.set_element(
                    "BioProject",
                    children=[
                        self.set_element(
                            "PrimaryId", bioproject_id, {"db": "BioProject"}
                        )
                    ],
                ),
                self.set_element("Package", env_package),
                self.set_element(
                    "Attributes",
                    children=[
                        self.set_element(
                            "Attribute", attributes[key], {"attribute_name": key}
                        )
                        for key in sorted(attributes)
                        if not (
                            key == "soil_horizon" and attributes[key] == "M horizon"
                        )
                    ]
                    + [
                        self.set_element(
                            "Attribute",
                            "National Microbiome Data Collaborative",
                            {"attribute_name": "broker name"},
                        )
                    ],
                ),
            ]

            action = self.set_element(
                "Action",
                children=[
                    self.set_element(
                        "AddData",
                        attrib={"target_db": "BioSample"},
                        children=[
                            self.set_element(
                                "Data",
                                attrib={"content_type": "XML"},
                                children=[
                                    self.set_element(
                                        "XmlContent",
                                        children=[
                                            self.set_element(
                                                "BioSample",
                                                attrib={"schema_version": "2.0"},
                                                children=biosample_elements,
                                            ),
                                        ],
                                    ),
                                ],
                            ),
                            self.set_element(
                                "Identifier",
                                children=[
                                    self.set_element(
                                        "SPUID",
                                        sample_id_value,
                                        {"spuid_namespace": org},
                                    ),
                                ],
                            ),
                        ],
                    ),
                ],
            )
            self.root.append(action)

    def _create_pooled_biosample_action(
        self,
        biosamples,
        pooling_info,
        organism_name,
        org,
        bioproject_id,
        attribute_mappings,
        slot_range_mappings,
        excluded_slots=frozenset(),
    ):
        # Use the processed sample ID as the primary identifier
        sample_id_value = pooling_info.get("processed_sample_id")
        if not sample_id_value:
            return

        # Aggregate attributes from all biosamples in the pool
        aggregated_attributes = {}
        env_package = None

        # Get title from the first biosample or use processed sample name
        title = pooling_info.get(
            "processed_sample_name", f"Pooled sample {sample_id_value}"
        )

        # Process each biosample to collect and aggregate attributes
        for biosample in biosamples:
            for json_key, value in biosample.items():
                if json_key == "id" or json_key in excluded_slots:
                    continue

                if json_key == "env_package":
                    env_package = f"MIMS.me.{handle_text_value(value)}.6.0"
                    continue

                if isinstance(value, list):
                    for item in value:
                        if json_key not in attribute_mappings:
                            continue

                        xml_key = attribute_mappings[json_key]
                        value_type = slot_range_mappings.get(json_key, "string")
                        handler = self.type_handlers.get(
                            value_type, handle_string_value
                        )

                        # Special handling for "elev" key
                        if json_key == "elev":
                            value = f"{float(value)} m"
                            aggregated_attributes[xml_key] = value
                            continue

                        # Special handling for "host_taxid"
                        if json_key == "host_taxid" and isinstance(value, dict):
                            if "term" in value and "id" in value["term"]:
                                value = re.findall(
                                    r"\d+", value["term"]["id"].split(":")[1]
                                )[0]
                            aggregated_attributes[xml_key] = value
                            continue

                        formatted_value = handler(item)

                        # Special handling for "geo_loc_name" - convert unicode to closest ASCII characters
                        if json_key == "geo_loc_name":
                            formatted_value = unidecode(formatted_value)

                        # For pooled samples, we typically want the first value or aggregate appropriately
                        if xml_key not in aggregated_attributes:
                            aggregated_attributes[xml_key] = formatted_value
                    continue

                if json_key not in attribute_mappings:
                    continue

                xml_key = attribute_mappings[json_key]
                value_type = slot_range_mappings.get(json_key, "string")
                handler = self.type_handlers.get(value_type, handle_string_value)

                # Special handling for "elev" key
                if json_key == "elev":
                    value = f"{float(value)} m"
                    aggregated_attributes[xml_key] = value
                    continue

                # Special handling for "host_taxid"
                if json_key == "host_taxid" and isinstance(value, dict):
                    if "term" in value and "id" in value["term"]:
                        value = re.findall(r"\d+", value["term"]["id"].split(":")[1])[0]
                    aggregated_attributes[xml_key] = value
                    continue

                # Special handling for "geo_loc_name" - convert unicode to closest ASCII characters
                if json_key == "geo_loc_name":
                    formatted_value = handler(value)
                    formatted_value = unidecode(formatted_value)
                    aggregated_attributes[xml_key] = formatted_value
                    continue

                formatted_value = handler(value)

                # For pooled samples, we typically want the first value or aggregate appropriately
                if xml_key not in aggregated_attributes:
                    aggregated_attributes[xml_key] = formatted_value

        # Override with values aggregated across the constituent biosamples
        # (see `aggregate_pooled_values` in ncbi_xml_utils)
        aggregated_value_keys = set()
        for slot, value in pooling_info.get("aggregated_values", {}).items():
            xml_key = attribute_mappings.get(slot, slot)
            aggregated_attributes[xml_key] = value
            aggregated_value_keys.add(xml_key)

        # Add samp_pooling attribute with semicolon-delimited biosample IDs
        if pooling_info.get("pooled_biosample_ids"):
            aggregated_attributes["samp_pooling"] = ";".join(
                pooling_info["pooled_biosample_ids"]
            )

        # Pooled samples only carry attributes that describe the composite:
        # the location/environment attributes shared by all constituents
        # (from neon_soil_example.xml) plus any value we could aggregate.
        allowed_attributes = {
            "collection_date",
            "depth",
            "elev",
            "geo_loc_name",
            "lat_lon",
            "env_broad_scale",
            "env_local_scale",
            "env_medium",
            "samp_pooling",
        } | aggregated_value_keys
        filtered_attributes = {
            k: v for k, v in aggregated_attributes.items() if k in allowed_attributes
        }

        biosample_elements = [
            self.set_element(
                "SampleId",
                children=[
                    self.set_element("SPUID", sample_id_value, {"spuid_namespace": org})
                ],
            ),
            self.set_element(
                "Descriptor",
                children=[
                    self.set_element("Title", title),
                    self.set_element(
                        "ExternalLink",
                        attrib={"label": sample_id_value},
                        children=[
                            self.set_element(
                                "URL",
                                f"https://bioregistry.io/{sample_id_value}",
                            )
                        ],
                    ),
                    self.set_element(
                        "ExternalLink",
                        attrib={"label": pooling_info["pooling_process_id"]},
                        children=[
                            self.set_element(
                                "URL",
                                f"https://bioregistry.io/{pooling_info['pooling_process_id']}",
                            )
                        ],
                    ),
                ]
                + [
                    self.set_element(
                        "ExternalLink",
                        attrib={"label": biosample_id},
                        children=[
                            self.set_element(
                                "URL",
                                f"https://bioregistry.io/{biosample_id}",
                            )
                        ],
                    )
                    for biosample_id in pooling_info.get("pooled_biosample_ids", [])
                ],
            ),
            self.set_element(
                "Organism",
                children=[self.set_element("OrganismName", organism_name)],
            ),
            self.set_element(
                "BioProject",
                children=[
                    self.set_element("PrimaryId", bioproject_id, {"db": "BioProject"})
                ],
            ),
            self.set_element("Package", env_package),
            self.set_element(
                "Attributes",
                children=[
                    self.set_element(
                        "Attribute", filtered_attributes[key], {"attribute_name": key}
                    )
                    for key in sorted(filtered_attributes)
                    if not (
                        key == "soil_horizon"
                        and filtered_attributes[key] == "M horizon"
                    )
                ]
                + [
                    self.set_element(
                        "Attribute",
                        "National Microbiome Data Collaborative",
                        {"attribute_name": "broker name"},
                    )
                ],
            ),
        ]

        action = self.set_element(
            "Action",
            children=[
                self.set_element(
                    "AddData",
                    attrib={"target_db": "BioSample"},
                    children=[
                        self.set_element(
                            "Data",
                            attrib={"content_type": "XML"},
                            children=[
                                self.set_element(
                                    "XmlContent",
                                    children=[
                                        self.set_element(
                                            "BioSample",
                                            attrib={"schema_version": "2.0"},
                                            children=biosample_elements,
                                        ),
                                    ],
                                ),
                            ],
                        ),
                        self.set_element(
                            "Identifier",
                            children=[
                                self.set_element(
                                    "SPUID",
                                    sample_id_value,
                                    {"spuid_namespace": org},
                                ),
                            ],
                        ),
                    ],
                ),
            ],
        )
        self.root.append(action)

    def set_fastq(
        self,
        biosample_data_objects: list,
        bioproject_id: str,
        org: str,
        nmdc_nucleotide_sequencing: list,
        nmdc_biosamples: list,
        nmdc_library_preparation: list,
        all_instruments: dict,
        pooled_biosamples_data=None,
        biosample_accessions=None,
        pooled_sample_accessions=None,
    ):
        bsm_id_name_dict = {
            biosample["id"]: biosample["name"] for biosample in nmdc_biosamples
        }

        # Use provided pooling data or empty dict
        pooling_data = pooled_biosamples_data or {}
        # Samples already registered with INSDC are referenced by accession rather
        # than by SPUID (see `_resolve_existing_biosample_accessions`).
        biosample_accessions = biosample_accessions or {}
        pooled_sample_accessions = pooled_sample_accessions or {}

        ntseq_by_biosample = {}
        for ntseq_dict in nmdc_nucleotide_sequencing:
            for biosample_id, ntseq_records in ntseq_dict.items():
                ntseq_by_biosample.setdefault(biosample_id, []).extend(ntseq_records)

        # Group data objects by pooling process
        pooling_groups = {}
        # Each individual entry is paired with the NucleotideSequencing records
        # its SRA action(s) should be built from.
        individual_entries = []

        for entry in biosample_data_objects:
            pooling_process_id = None
            # Check if any biosample in this entry belongs to a pooling process
            for biosample_id in entry.keys():
                pooling_info = pooling_data.get(biosample_id, {})
                if pooling_info and pooling_info.get("pooling_process_id"):
                    pooling_process_id = pooling_info["pooling_process_id"]
                    break

            if pooling_process_id:
                if pooling_process_id not in pooling_groups:
                    pooling_groups[pooling_process_id] = {
                        "entries": [],
                        "processed_sample_id": pooling_info.get("processed_sample_id"),
                        "processed_sample_name": pooling_info.get(
                            "processed_sample_name", ""
                        ),
                        "nucleotide_sequencing_ids": pooling_info.get(
                            "nucleotide_sequencing_ids"
                        ),
                    }

                # A biosample sequenced both individually and as part of the pool:
                # keep only the pooled run's DataObjects in the pooled entry and
                # emit the individual run as its own entry, attributed to the
                # biosample itself.
                pooled_entry = {}
                for biosample_id, data_objects in entry.items():
                    _, individual_ntseq_records = (
                        self._split_pooled_and_individual_sequencing(
                            pooling_data.get(biosample_id, {}),
                            ntseq_by_biosample.get(biosample_id, []),
                        )
                    )
                    if not individual_ntseq_records:
                        pooled_entry[biosample_id] = data_objects
                        continue

                    individual_dobj_ids = {
                        dobj_id
                        for ntseq in individual_ntseq_records
                        for dobj_id in ntseq.get("has_output", [])
                    }
                    pooled_entry[biosample_id] = [
                        d
                        for d in data_objects
                        if d.get("id") not in individual_dobj_ids
                    ]
                    individual_data_objects = [
                        d for d in data_objects if d.get("id") in individual_dobj_ids
                    ]
                    if individual_data_objects:
                        individual_entries.append(
                            (
                                {biosample_id: individual_data_objects},
                                [{biosample_id: individual_ntseq_records}],
                            )
                        )

                pooling_groups[pooling_process_id]["entries"].append(pooled_entry)
            else:
                individual_entries.append((entry, nmdc_nucleotide_sequencing))

        # Process pooled entries - create one SRA <Action> block per pooling process
        for pooling_process_id, group_data in pooling_groups.items():
            self._create_pooled_sra_action(
                group_data["entries"],
                group_data["processed_sample_id"],
                group_data["processed_sample_name"],
                bioproject_id,
                org,
                nmdc_nucleotide_sequencing,
                nmdc_library_preparation,
                all_instruments,
                bsm_id_name_dict,
                pooled_nucleotide_sequencing_ids=group_data[
                    "nucleotide_sequencing_ids"
                ],
                processed_sample_accession=pooled_sample_accessions.get(
                    group_data["processed_sample_id"]
                ),
            )

        # Process individual entries
        for entry, entry_nucleotide_sequencing in individual_entries:
            fastq_files = []
            biosample_ids = []
            nucleotide_sequencing_ids = {}
            lib_prep_protocol_names = {}
            analyte_category = ""
            library_name = ""
            instrument_vendor = ""
            instrument_model = ""

            for biosample_id, data_objects in entry.items():
                biosample_ids.append(biosample_id)
                for data_object in data_objects:
                    if "url" in data_object:
                        url = urlparse(data_object["url"])
                        file_path = os.path.basename(url.path)
                        fastq_files.append(file_path)

                for ntseq_dict in entry_nucleotide_sequencing:
                    if biosample_id in ntseq_dict:
                        for ntseq in ntseq_dict[biosample_id]:
                            nucleotide_sequencing_ids[biosample_id] = ntseq.get(
                                "id", ""
                            )
                            # Currently, we are making the assumption that only one instrument
                            # is used to sequence a Biosample
                            instrument_used: List[str] = ntseq.get(
                                "instrument_used", []
                            )
                            if not instrument_used:
                                instrument_id = None
                            else:
                                instrument_id = instrument_used[0]

                            instrument = all_instruments.get(instrument_id, {})
                            instrument_vendor = instrument.get("vendor", "")
                            instrument_model = instrument.get("model", "")

                            analyte_category = ntseq.get("analyte_category", "")
                            library_name = bsm_id_name_dict.get(biosample_id, "")

                for lib_prep_dict in nmdc_library_preparation:
                    if biosample_id in lib_prep_dict:
                        lib_prep_protocol_names[biosample_id] = (
                            lib_prep_dict[biosample_id]
                            .get("protocol_link", {})
                            .get("name", "")
                        )

            if fastq_files:
                files_elements = [
                    self.set_element(
                        "File",
                        "",
                        {"file_path": f},
                        [
                            self.set_element(
                                "DataType",
                                "sra-run-fastq" if ".fastq" in f else "generic-data",
                            )
                        ],
                    )
                    for f in fastq_files
                ]

                attribute_elements = [
                    self.set_element(
                        "AttributeRefId",
                        attrib={"name": "BioProject"},
                        children=[
                            self.set_element(
                                "RefId",
                                children=[
                                    self.set_element(
                                        "PrimaryId",
                                        bioproject_id,
                                        {"db": "BioProject"},
                                    )
                                ],
                            )
                        ],
                    )
                ]

                for biosample_id in biosample_ids:
                    attribute_elements.append(
                        self._biosample_ref_element(
                            self.set_element,
                            biosample_id,
                            org,
                            accession=biosample_accessions.get(biosample_id),
                        )
                    )

                sra_attributes = []
                if instrument_vendor == "illumina":
                    sra_attributes.append(
                        self.set_element("Attribute", "ILLUMINA", {"name": "platform"})
                    )
                    if instrument_model == "nextseq_550":
                        sra_attributes.append(
                            self.set_element(
                                "Attribute", "NextSeq 550", {"name": "instrument_model"}
                            )
                        )
                    elif instrument_model == "novaseq_6000":
                        sra_attributes.append(
                            self.set_element(
                                "Attribute",
                                "NovaSeq 6000",
                                {"name": "instrument_model"},
                            )
                        )
                    elif instrument_model == "hiseq":
                        sra_attributes.append(
                            self.set_element(
                                "Attribute", "HiSeq", {"name": "instrument_model"}
                            )
                        )

                if analyte_category == "metagenome":
                    sra_attributes.append(
                        self.set_element(
                            "Attribute", "WGS", {"name": "library_strategy"}
                        )
                    )
                    sra_attributes.append(
                        self.set_element(
                            "Attribute", "METAGENOMIC", {"name": "library_source"}
                        )
                    )
                    sra_attributes.append(
                        self.set_element(
                            "Attribute", "RANDOM", {"name": "library_selection"}
                        )
                    )
                elif analyte_category == "metatranscriptome":
                    sra_attributes.append(
                        self.set_element(
                            "Attribute",
                            "METATRANSCRIPTOMIC",
                            {"name": "library_source"},
                        )
                    )

                has_paired_reads = any(
                    data_object.get("data_object_type", "").lower()
                    == "metagenome raw reads"
                    for data_object in data_objects
                ) or (
                    any(
                        data_object.get("data_object_type", "").lower()
                        == "metagenome raw read 1"
                        for data_object in data_objects
                    )
                    and any(
                        data_object.get("data_object_type", "").lower()
                        == "metagenome raw read 2"
                        for data_object in data_objects
                    )
                )

                if has_paired_reads:
                    sra_attributes.append(
                        self.set_element(
                            "Attribute", "paired", {"name": "library_layout"}
                        )
                    )
                else:
                    sra_attributes.append(
                        self.set_element(
                            "Attribute", "single", {"name": "library_layout"}
                        )
                    )

                # Add library_name attribute
                if library_name:
                    sra_attributes.append(
                        self.set_element(
                            "Attribute", library_name, {"name": "library_name"}
                        )
                    )

                for biosample_id, lib_prep_name in lib_prep_protocol_names.items():
                    sra_attributes.append(
                        self.set_element(
                            "Attribute",
                            lib_prep_name,
                            {"name": "library_construction_protocol"},
                        )
                    )

                for (
                    biosample_id,
                    omics_processing_id,
                ) in nucleotide_sequencing_ids.items():
                    identifier_element = self.set_element(
                        "Identifier",
                        children=[
                            self.set_element(
                                "SPUID", omics_processing_id, {"spuid_namespace": org}
                            )
                        ],
                    )

                    action = self.set_element(
                        "Action",
                        children=[
                            self.set_element(
                                "AddFiles",
                                attrib={"target_db": "SRA"},
                                children=files_elements
                                + attribute_elements
                                + sra_attributes
                                + [identifier_element],
                            ),
                        ],
                    )

                    self.root.append(action)

    def _create_pooled_sra_action(
        self,
        entries,
        processed_sample_id,
        processed_sample_name,
        bioproject_id,
        org,
        nmdc_nucleotide_sequencing,
        nmdc_library_preparation,
        all_instruments,
        bsm_id_name_dict,
        pooled_nucleotide_sequencing_ids=None,
        processed_sample_accession=None,
    ):
        if not processed_sample_id:
            return

        # When known, restrict the NucleotideSequencing records considered to the
        # runs performed on the pool, so a biosample's individual run never
        # becomes the pooled action's identifier or instrument.
        pooled_ntseq_ids = (
            set(pooled_nucleotide_sequencing_ids)
            if pooled_nucleotide_sequencing_ids is not None
            else None
        )

        # Collect all fastq files from all entries
        all_fastq_files = set()
        all_biosample_ids = set()
        nucleotide_sequencing_ids = {}
        lib_prep_protocol_names = {}
        analyte_category = ""
        instrument_vendor = ""
        instrument_model = ""

        for entry in entries:
            for biosample_id, data_objects in entry.items():
                all_biosample_ids.add(biosample_id)
                for data_object in data_objects:
                    if "url" in data_object:
                        url = urlparse(data_object["url"])
                        file_path = os.path.basename(url.path)
                        all_fastq_files.add(file_path)

                # Get nucleotide sequencing info
                for ntseq_dict in nmdc_nucleotide_sequencing:
                    if biosample_id in ntseq_dict:
                        for ntseq in ntseq_dict[biosample_id]:
                            if (
                                pooled_ntseq_ids is not None
                                and ntseq.get("id") not in pooled_ntseq_ids
                            ):
                                continue
                            nucleotide_sequencing_ids[biosample_id] = ntseq.get(
                                "id", ""
                            )
                            instrument_used = ntseq.get("instrument_used", [])
                            if instrument_used:
                                instrument_id = instrument_used[0]
                                instrument = all_instruments.get(instrument_id, {})
                                instrument_vendor = instrument.get("vendor", "")
                                instrument_model = instrument.get("model", "")
                            analyte_category = ntseq.get("analyte_category", "")

                # Get library preparation info
                for lib_prep_dict in nmdc_library_preparation:
                    if biosample_id in lib_prep_dict:
                        lib_prep_protocol_names[biosample_id] = (
                            lib_prep_dict[biosample_id]
                            .get("protocol_link", {})
                            .get("name", "")
                        )

        if all_fastq_files:
            files_elements = [
                self.set_element(
                    "File",
                    "",
                    {"file_path": f},
                    [
                        self.set_element(
                            "DataType",
                            "sra-run-fastq" if ".fastq" in f else "generic-data",
                        )
                    ],
                )
                for f in sorted(all_fastq_files)
            ]

            attribute_elements = [
                self.set_element(
                    "AttributeRefId",
                    attrib={"name": "BioProject"},
                    children=[
                        self.set_element(
                            "RefId",
                            children=[
                                self.set_element(
                                    "PrimaryId",
                                    bioproject_id,
                                    {"db": "BioProject"},
                                )
                            ],
                        )
                    ],
                ),
                # Reference the processed sample, not individual biosamples
                self._biosample_ref_element(
                    self.set_element,
                    processed_sample_id,
                    org,
                    accession=processed_sample_accession,
                ),
            ]

            sra_attributes = []
            if instrument_vendor == "illumina":
                sra_attributes.append(
                    self.set_element("Attribute", "ILLUMINA", {"name": "platform"})
                )
                if instrument_model == "nextseq_550":
                    sra_attributes.append(
                        self.set_element(
                            "Attribute", "NextSeq 550", {"name": "instrument_model"}
                        )
                    )
                elif instrument_model == "novaseq_6000":
                    sra_attributes.append(
                        self.set_element(
                            "Attribute",
                            "NovaSeq 6000",
                            {"name": "instrument_model"},
                        )
                    )
                elif instrument_model == "hiseq":
                    sra_attributes.append(
                        self.set_element(
                            "Attribute", "HiSeq", {"name": "instrument_model"}
                        )
                    )

            if analyte_category == "metagenome":
                sra_attributes.append(
                    self.set_element("Attribute", "WGS", {"name": "library_strategy"})
                )
                sra_attributes.append(
                    self.set_element(
                        "Attribute", "METAGENOMIC", {"name": "library_source"}
                    )
                )
                sra_attributes.append(
                    self.set_element(
                        "Attribute", "RANDOM", {"name": "library_selection"}
                    )
                )
            elif analyte_category == "metatranscriptome":
                sra_attributes.append(
                    self.set_element(
                        "Attribute",
                        "METATRANSCRIPTOMIC",
                        {"name": "library_source"},
                    )
                )

            # Determine library layout based on file patterns
            has_paired_reads = any(
                "_R1" in f and "_R2" in f.replace("_R1", "_R2") in all_fastq_files
                for f in all_fastq_files
                if "_R1" in f
            )

            if has_paired_reads:
                sra_attributes.append(
                    self.set_element("Attribute", "paired", {"name": "library_layout"})
                )
            else:
                sra_attributes.append(
                    self.set_element("Attribute", "single", {"name": "library_layout"})
                )

            # Add library_name attribute using ProcessedSample name
            if processed_sample_name:
                sra_attributes.append(
                    self.set_element(
                        "Attribute", processed_sample_name, {"name": "library_name"}
                    )
                )

            # Add library construction protocol from any of the biosamples
            for biosample_id, lib_prep_name in lib_prep_protocol_names.items():
                if lib_prep_name:
                    sra_attributes.append(
                        self.set_element(
                            "Attribute",
                            lib_prep_name,
                            {"name": "library_construction_protocol"},
                        )
                    )
                    break  # Only add one protocol name

            # Use the first nucleotide sequencing ID as the identifier
            omics_processing_id = None
            for biosample_id, seq_id in nucleotide_sequencing_ids.items():
                if seq_id:
                    omics_processing_id = seq_id
                    break

            if omics_processing_id:
                identifier_element = self.set_element(
                    "Identifier",
                    children=[
                        self.set_element(
                            "SPUID", omics_processing_id, {"spuid_namespace": org}
                        )
                    ],
                )

                action = self.set_element(
                    "Action",
                    children=[
                        self.set_element(
                            "AddFiles",
                            attrib={"target_db": "SRA"},
                            children=files_elements
                            + attribute_elements
                            + sra_attributes
                            + [identifier_element],
                        ),
                    ],
                )

                self.root.append(action)

    def get_submission_xml(
        self,
        biosamples_list: list,
        biosample_nucleotide_sequencing_list: list,
        biosample_data_objects_list: list,
        biosample_library_preparation_list: list,
        instruments_dict: dict,
        pooled_biosamples_data=None,
    ):
        # data_type = None

        biosamples_to_exclude = set()
        for bsm_ntseq in biosample_nucleotide_sequencing_list:
            for bsm_id, ntseq_list in bsm_ntseq.items():
                # Check if any processing_institution is "JGI"
                for ntseq in ntseq_list:
                    if (
                        "processing_institution" in ntseq
                        and ntseq["processing_institution"] == "JGI"
                    ):
                        biosamples_to_exclude.add(bsm_id)
                        break

        # Filter biosample_nucleotide_sequencing_list to exclude JGI records
        filtered_nucleotide_sequencing_list = []
        for bsm_ntseq in biosample_nucleotide_sequencing_list:
            filtered_dict = {}
            for bsm_id, ntseq_list in bsm_ntseq.items():
                if bsm_id not in biosamples_to_exclude:
                    filtered_dict[bsm_id] = ntseq_list
            if filtered_dict:  # Only add non-empty dictionaries
                filtered_nucleotide_sequencing_list.append(filtered_dict)

        # Filter biosamples_list to exclude JGI-processed biosamples
        filtered_biosamples_list = [
            biosample
            for biosample in biosamples_list
            if biosample.get("id") not in biosamples_to_exclude
        ]

        # Get data_type from filtered list
        # for bsm_ntseq in filtered_nucleotide_sequencing_list:
        #     for _, ntseq_list in bsm_ntseq.items():
        #         for ntseq in ntseq_list:
        #             if "analyte_category" in ntseq:
        #                 data_type = handle_string_value(
        #                     ntseq["analyte_category"]
        #                 ).capitalize()

        self.set_description(
            email=self.nmdc_pi_email,
            first=self.first_name,
            last=self.last_name,
            org=self.ncbi_submission_metadata.get("organization", ""),
        )

        # if not self.ncbi_bioproject_id:
        #     self.set_bioproject(
        #         title=self.nmdc_study_title,
        #         project_id=self.ncbi_bioproject_id,
        #         description=self.nmdc_study_description,
        #         data_type=data_type,
        #         org=self.ncbi_submission_metadata.get("organization", ""),
        #     )

        individually_sequenced_biosample_ids = (
            self._individually_sequenced_pooled_biosample_ids(
                pooled_biosamples_data or {}, filtered_nucleotide_sequencing_list
            )
        )
        biosample_accessions, pooled_sample_accessions = (
            self._resolve_existing_biosample_accessions(
                filtered_biosamples_list, pooled_biosamples_data or {}
            )
        )

        self.set_biosample(
            organism_name=self.ncbi_biosample_metadata.get("organism_name", ""),
            org=self.ncbi_submission_metadata.get("organization", ""),
            bioproject_id=self.ncbi_bioproject_id,
            nmdc_biosamples=filtered_biosamples_list,
            pooled_biosamples_data=pooled_biosamples_data,
            individually_sequenced_biosample_ids=individually_sequenced_biosample_ids,
            pooled_sample_accessions=pooled_sample_accessions,
        )

        # Also filter biosample_data_objects_list
        filtered_data_objects_list = []
        acceptable_extensions = [".fastq.gz", ".fastq"]

        for entry in biosample_data_objects_list:
            filtered_entry = {}
            for biosample_id, data_objects in entry.items():
                if biosample_id not in biosamples_to_exclude:
                    # filter data_objects based on acceptable/allowed extensions
                    # for "url" key in data_object
                    filtered_objects = []
                    for data_object in data_objects:
                        if "url" in data_object:
                            url = urlparse(data_object["url"])
                            file_path = os.path.basename(url.path)
                            if any(
                                file_path.endswith(ext) for ext in acceptable_extensions
                            ):
                                filtered_objects.append(data_object)

                    if filtered_objects:
                        filtered_entry[biosample_id] = filtered_objects

            if filtered_entry:  # Only add non-empty entries
                filtered_data_objects_list.append(filtered_entry)

        # Filter library preparation list as well
        filtered_library_preparation_list = []
        for lib_prep_dict in biosample_library_preparation_list:
            filtered_lib_prep = {}
            for biosample_id, lib_prep in lib_prep_dict.items():
                if biosample_id not in biosamples_to_exclude:
                    filtered_lib_prep[biosample_id] = lib_prep
            if filtered_lib_prep:  # Only add non-empty entries
                filtered_library_preparation_list.append(filtered_lib_prep)

        self.set_fastq(
            biosample_data_objects=filtered_data_objects_list,
            bioproject_id=self.ncbi_bioproject_id,
            org=self.ncbi_submission_metadata.get("organization", ""),
            nmdc_nucleotide_sequencing=filtered_nucleotide_sequencing_list,
            nmdc_biosamples=filtered_biosamples_list,
            nmdc_library_preparation=filtered_library_preparation_list,
            all_instruments=instruments_dict,
            pooled_biosamples_data=pooled_biosamples_data,
            biosample_accessions=biosample_accessions,
            pooled_sample_accessions=pooled_sample_accessions,
        )

        rough_string = ET.tostring(self.root, "unicode")
        reparsed = xml.dom.minidom.parseString(rough_string)
        submission_xml = reparsed.toprettyxml(indent="    ", newl="\n")

        # ============= Uncomment the following code to validate the XML against NCBI XSDs ============ #
        # submission_xsd_url = "https://www.ncbi.nlm.nih.gov/viewvc/v1/trunk/submit/public-docs/common/submission.xsd?view=co"
        # validate_xml(submission_xml, submission_xsd_url)

        # bioproject_xsd_url = "https://www.ncbi.nlm.nih.gov/viewvc/v1/trunk/submit/public-docs/bioproject/bioproject.xsd?view=co"
        # validate_xml(submission_xml, bioproject_xsd_url)

        # biosample_xsd_url = "https://www.ncbi.nlm.nih.gov/viewvc/v1/trunk/submit/public-docs/biosample/biosample.xsd?view=co"
        # validate_xml(submission_xml, biosample_xsd_url)

        return submission_xml
