import os
import re
import datetime
import xml.etree.ElementTree as ET
import xml.dom.minidom

from typing import Any, List
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
        self.nmdc_pi_email = nmdc_study.get("principal_investigator", {}).get("email")
        nmdc_study_pi_name = (
            nmdc_study.get("principal_investigator", {}).get("name").split()
        )
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

    def set_biosample(
        self,
        organism_name,
        org,
        bioproject_id,
        nmdc_biosamples,
        pooled_biosamples_data=None,
    ):
        attribute_mappings, slot_range_mappings = load_mappings(
            self.nmdc_ncbi_attribute_mapping_file_url
        )

        # Use provided pooling data or empty dict
        pooling_data = pooled_biosamples_data or {}

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
            else:
                individual_biosamples.append(biosample)

        # Process pooled sample groups - create one <Action> block per pooling process
        for pooling_process_id, group_data in pooling_groups.items():
            self._create_pooled_biosample_action(
                group_data["biosamples"],
                group_data["pooling_info"],
                organism_name,
                org,
                bioproject_id,
                attribute_mappings,
                slot_range_mappings,
            )

        # Process individual biosamples
        for biosample in individual_biosamples:
            attributes = {}
            sample_id_value = None
            env_package = None

            # Get pooling info for this specific biosample
            pooling_info = pooling_data.get(biosample["id"], {})

            for json_key, value in biosample.items():
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
                if pooling_info.get("aggregated_collection_date"):
                    # Find the mapping for collection_date
                    collection_date_key = attribute_mappings.get(
                        "collection_date", "collection_date"
                    )
                    attributes[collection_date_key] = pooling_info[
                        "aggregated_collection_date"
                    ]

                if pooling_info.get("aggregated_depth"):
                    # Find the mapping for depth
                    depth_key = attribute_mappings.get("depth", "depth")
                    attributes[depth_key] = pooling_info["aggregated_depth"]

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
                        else []
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
                if json_key == "id":
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

                formatted_value = handler(value)

                # For pooled samples, we typically want the first value or aggregate appropriately
                if xml_key not in aggregated_attributes:
                    aggregated_attributes[xml_key] = formatted_value

        # Override with aggregated values for pooled samples
        if pooling_info.get("aggregated_collection_date"):
            collection_date_key = attribute_mappings.get(
                "collection_date", "collection_date"
            )
            aggregated_attributes[collection_date_key] = pooling_info[
                "aggregated_collection_date"
            ]

        if pooling_info.get("aggregated_depth"):
            depth_key = attribute_mappings.get("depth", "depth")
            aggregated_attributes[depth_key] = pooling_info["aggregated_depth"]

        # Add samp_pooling attribute with semicolon-delimited biosample IDs
        if pooling_info.get("pooled_biosample_ids"):
            aggregated_attributes["samp_pooling"] = ";".join(
                pooling_info["pooled_biosample_ids"]
            )

        # Filter attributes to only include the ones from neon_soil_example.xml for pooled samples
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
        }
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
    ):
        bsm_id_name_dict = {
            biosample["id"]: biosample["name"] for biosample in nmdc_biosamples
        }

        # Use provided pooling data or empty dict
        pooling_data = pooled_biosamples_data or {}

        # Group data objects by pooling process
        pooling_groups = {}
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
                    }
                pooling_groups[pooling_process_id]["entries"].append(entry)
            else:
                individual_entries.append(entry)

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
            )

        # Process individual entries
        for entry in individual_entries:
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

                for ntseq_dict in nmdc_nucleotide_sequencing:
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
                        self.set_element(
                            "AttributeRefId",
                            attrib={"name": "BioSample"},
                            children=[
                                self.set_element(
                                    "RefId",
                                    children=[
                                        self.set_element(
                                            "SPUID",
                                            biosample_id,
                                            {"spuid_namespace": org},
                                        )
                                    ],
                                )
                            ],
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
    ):
        if not processed_sample_id:
            return

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
                self.set_element(
                    "AttributeRefId",
                    attrib={"name": "BioSample"},
                    children=[
                        self.set_element(
                            "RefId",
                            children=[
                                self.set_element(
                                    "SPUID",
                                    processed_sample_id,
                                    {"spuid_namespace": org},
                                )
                            ],
                        )
                    ],
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

        self.set_biosample(
            organism_name=self.ncbi_biosample_metadata.get("organism_name", ""),
            org=self.ncbi_submission_metadata.get("organization", ""),
            bioproject_id=self.ncbi_bioproject_id,
            nmdc_biosamples=filtered_biosamples_list,
            pooled_biosamples_data=pooled_biosamples_data,
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
