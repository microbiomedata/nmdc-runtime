import datetime
import xml.etree.ElementTree as ET
import xml.dom.minidom

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
from nmdc_runtime.site.export.nmdc_api_client import NMDCApiClient


class NCBISubmissionXML:
    def __init__(self, ncbi_submission_fields: dict):
        self.root = ET.Element("Submission")
        self.nmdc_study_id = ncbi_submission_fields.get("nmdc_study_id")
        self.nmdc_ncbi_attribute_mapping_file_url = ncbi_submission_fields.get(
            "nmdc_ncbi_attribute_mapping_file_url"
        )
        self.ncbi_submission_metadata = ncbi_submission_fields.get(
            "ncbi_submission_metadata", {}
        )
        self.ncbi_bioproject_metadata = ncbi_submission_fields.get(
            "ncbi_bioproject_metadata", {}
        )
        self.ncbi_biosample_metadata = ncbi_submission_fields.get(
            "ncbi_biosample_metadata", {}
        )
        self.nmdc_api_client = NMDCApiClient()

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
        package,
        org,
        nmdc_biosamples,
    ):
        attribute_mappings, slot_range_mappings = load_mappings(
            self.nmdc_ncbi_attribute_mapping_file_url
        )

        for biosample in nmdc_biosamples:
            attributes = {}
            sample_id_value = None

            for json_key, value in biosample.items():
                if isinstance(value, list):
                    continue  # Skip processing for list values

                # Special handling for NMDC Biosample "id"
                if json_key == "id":
                    sample_id_value = value
                    continue

                xml_key = attribute_mappings.get(json_key, json_key)
                value_type = slot_range_mappings.get(json_key, "string")
                handler = self.type_handlers.get(value_type, handle_string_value)

                formatted_value = handler(value)
                attributes[xml_key] = formatted_value

            # Create the BioSample XML block with these attributes for each biosample
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
                self.set_element("Package", package),
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

    def get_submission_xml(self):
        self.set_description(
            email=self.ncbi_submission_metadata.get("email", ""),
            user=self.ncbi_submission_metadata.get("user", ""),
            first=self.ncbi_submission_metadata.get("first", ""),
            last=self.ncbi_submission_metadata.get("last", ""),
            org=self.ncbi_submission_metadata.get("organization", ""),
        )

        self.set_bioproject(
            title=self.ncbi_bioproject_metadata.get("title", ""),
            project_id=self.ncbi_bioproject_metadata.get("project_id", ""),
            description=self.ncbi_bioproject_metadata.get("description", ""),
            data_type=self.ncbi_bioproject_metadata.get("data_type", ""),
            org=self.ncbi_submission_metadata.get("organization", ""),
        )

        biosamples_list = self.nmdc_api_client.get_biosamples_part_of_study(
            self.nmdc_study_id
        )

        self.set_biosample(
            organism_name=self.ncbi_biosample_metadata.get("organism_name", ""),
            package=self.ncbi_biosample_metadata.get("package", ""),
            org=self.ncbi_submission_metadata.get("organization", ""),
            nmdc_biosamples=biosamples_list,
        )

        rough_string = ET.tostring(self.root, "unicode")
        reparsed = xml.dom.minidom.parseString(rough_string)
        return reparsed.toprettyxml(indent="    ", newl="\n")
