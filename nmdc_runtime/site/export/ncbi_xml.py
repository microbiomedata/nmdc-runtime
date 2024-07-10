import os
import datetime
import xml.etree.ElementTree as ET
import xml.dom.minidom

from typing import Any
from urllib.parse import urlparse
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

    def set_description(self, email, user, first, last, org, date=None):
        date = date or datetime.datetime.now().strftime("%Y-%m-%d")
        description = self.set_element(
            "Description",
            children=[
                self.set_element(
                    "Comment", f"NMDC Submission for {self.nmdc_study_id}"
                ),
                self.set_element("Submitter", attrib={"user_name": user}),
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
        nmdc_omics_processing,
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
                    continue  # Skip processing for list values

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
                            f"NMDC Biosample {sample_id_value} from {organism_name} part of {self.nmdc_study_id} study",
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
    ):
        for entry in biosample_data_objects:
            fastq_files = []
            biosample_ids = []

            for biosample_id, data_objects in entry.items():
                biosample_ids.append(biosample_id)
                for data_object in data_objects:
                    if "url" in data_object:
                        url = urlparse(data_object["url"])
                        file_path = os.path.join(
                            os.path.basename(os.path.dirname(url.path)),
                            os.path.basename(url.path),
                        )
                        fastq_files.append(file_path)

            if fastq_files:
                files_elements = [
                    self.set_element(
                        "File",
                        "",
                        {"file_path": f},
                        [self.set_element("DataType", "generic-data")],
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
                                        "SPUID",
                                        bioproject_id,
                                        {"spuid_namespace": org},
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

                identifier_element = self.set_element(
                    "Identifier",
                    children=[
                        self.set_element(
                            "SPUID", bioproject_id, {"spuid_namespace": org}
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
                            + [identifier_element],
                        ),
                    ],
                )

                self.root.append(action)

    def get_submission_xml(
        self,
        biosamples_list: list,
        biosample_omics_processing_list: list,
        biosample_data_objects_list: list,
    ):
        data_type = None
        ncbi_project_id = None
        for bsm_omprc in biosample_omics_processing_list:
            for _, omprc_list in bsm_omprc.items():
                for omprc in omprc_list:
                    if "omics_type" in omprc:
                        data_type = handle_text_value(omprc["omics_type"]).capitalize()

                    if "ncbi_project_name" in omprc:
                        ncbi_project_id = omprc["ncbi_project_name"]

        self.set_description(
            email=self.nmdc_pi_email,
            user="National Microbiome Data Collaborative (NMDC)",
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
            nmdc_omics_processing=biosample_omics_processing_list,
        )

        self.set_fastq(
            biosample_data_objects=biosample_data_objects_list,
            bioproject_id=ncbi_project_id,
            org=self.ncbi_submission_metadata.get("organization", ""),
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
