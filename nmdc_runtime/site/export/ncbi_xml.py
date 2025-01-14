import os
import re
import datetime
import xml.etree.ElementTree as ET
import xml.dom.minidom

from typing import Any
from urllib.parse import urlparse
from nmdc_runtime.site.export.ncbi_xml_utils import (
    get_instruments,
    handle_controlled_identified_term_value,
    handle_controlled_term_value,
    handle_geolocation_value,
    handle_quantity_value,
    handle_text_value,
    handle_timestamp_value,
    handle_float_value,
    handle_string_value,
    load_mappings,
    validate_xml,
)


class NCBISubmissionXML:
    def __init__(self, nmdc_study: Any, ncbi_submission_metadata: dict):
        self.root = ET.Element("Submission")

        self.nmdc_study_id = nmdc_study.get("id")
        self.nmdc_study_title = nmdc_study.get("title")
        self.nmdc_study_description = nmdc_study.get("description")
        self.ncbi_bioproject_id = nmdc_study.get("insdc_bioproject_identifiers")
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
    ):
        attribute_mappings, slot_range_mappings = load_mappings(
            self.nmdc_ncbi_attribute_mapping_file_url
        )

        for biosample in nmdc_biosamples:
            attributes = {}
            sample_id_value = None
            env_package = None

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

                        # Special handling for "host_taxid"
                        if json_key == "host_taxid" and isinstance(value, dict):
                            if "term" in value and "id" in value["term"]:
                                value = re.findall(
                                    r"\d+", value["term"]["id"].split(":")[1]
                                )[0]
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

                # Default processing for other keys
                formatted_value = handler(value)
                attributes[xml_key] = formatted_value

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
                            f"NMDC Biosample {sample_id_value} from {organism_name}, part of {self.nmdc_study_id} study",
                        ),
                    ],
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

    def set_fastq(
        self,
        biosample_data_objects: list,
        bioproject_id: str,
        org: str,
        nmdc_nucleotide_sequencing: list,
        nmdc_biosamples: list,
        nmdc_library_preparation: list,
        all_instruments: dict,
    ):
        bsm_id_name_dict = {
            biosample["id"]: biosample["name"] for biosample in nmdc_biosamples
        }

        for entry in biosample_data_objects:
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
                            instrument_id = ntseq.get("instrument_used", "")[0]
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

    def get_submission_xml(
        self,
        biosamples_list: list,
        biosample_nucleotide_sequencing_list: list,
        biosample_data_objects_list: list,
        biosample_library_preparation_list: list,
        instruments_dict: dict,
    ):
        data_type = None
        ncbi_project_id = None
        for bsm_ntseq in biosample_nucleotide_sequencing_list:
            for _, ntseq_list in bsm_ntseq.items():
                for ntseq in ntseq_list:
                    if "analyte_category" in ntseq:
                        data_type = handle_string_value(
                            ntseq["analyte_category"]
                        ).capitalize()

                    if "ncbi_project_name" in ntseq:
                        ncbi_project_id = ntseq["ncbi_project_name"]

        self.set_description(
            email=self.nmdc_pi_email,
            first=self.first_name,
            last=self.last_name,
            org=self.ncbi_submission_metadata.get("organization", ""),
        )

        if not ncbi_project_id:
            self.set_bioproject(
                title=self.nmdc_study_title,
                project_id=ncbi_project_id,
                description=self.nmdc_study_description,
                data_type=data_type,
                org=self.ncbi_submission_metadata.get("organization", ""),
            )

        self.set_biosample(
            organism_name=self.ncbi_biosample_metadata.get("organism_name", ""),
            org=self.ncbi_submission_metadata.get("organization", ""),
            bioproject_id=ncbi_project_id,
            nmdc_biosamples=biosamples_list,
        )

        self.set_fastq(
            biosample_data_objects=biosample_data_objects_list,
            bioproject_id=ncbi_project_id,
            org=self.ncbi_submission_metadata.get("organization", ""),
            nmdc_nucleotide_sequencing=biosample_nucleotide_sequencing_list,
            nmdc_biosamples=biosamples_list,
            nmdc_library_preparation=biosample_library_preparation_list,
            all_instruments=instruments_dict,
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
