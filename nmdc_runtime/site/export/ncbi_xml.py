import json
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


class NCBISubmissionXML:
    def __init__(
        self, study_id: str, org="National Microbiome Data Collaborative (NMDC)"
    ):
        self.root = ET.Element("Submission")
        self.study_id = study_id
        self.org = org

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

    def set_description(
        self, email="aclum@lbl.gov", user="NMDC", first="Alicia", last="Clum", date=None
    ):
        date = date or datetime.datetime.now().strftime("%Y-%m-%d")
        description = self.set_element(
            "Description",
            children=[
                self.set_element("Comment", f"NMDC Submission for {self.study_id}"),
                self.set_element("Submitter", attrib={"user_name": user}),
                self.set_element(
                    "Organization",
                    attrib={"role": "owner", "type": "center"},
                    children=[
                        self.set_element("Name", self.org),
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

    def set_descriptor(self, title, description, url):
        descriptor_elements = []
        descriptor_elements.append(self.set_element("Title", title))
        descriptor_elements.append(
            self.set_element(
                "Description", children=[self.set_element("p", description)]
            )
        )

        external_resources = json.loads(url)
        for label, link in external_resources.items():
            external_link = self.set_element("ExternalLink", attrib={"label": label})
            url_element = self.set_element("URL", link)
            external_link.append(url_element)
            descriptor_elements.append(external_link)

        return descriptor_elements

    def set_bioproject(self, title, project_id, description, data_type, url):
        action = self.set_element("Action")
        add_data = self.set_element("AddData", attrib={"target_db": "BioProject"})

        data_element = self.set_element("Data", attrib={"content_type": "XML"})
        xml_content = self.set_element("XmlContent")
        project = self.set_element("Project", attrib={"schema_version": "2.0"})

        project_id_element = self.set_element("ProjectID")
        spuid = self.set_element("SPUID", project_id, {"spuid_namespace": self.org})
        project_id_element.append(spuid)

        descriptor = self.set_descriptor(title, description, url)
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
            "SPUID", project_id, {"spuid_namespace": self.org}
        )
        identifier.append(spuid_identifier)
        add_data.append(identifier)

        action.append(add_data)
        self.root.append(action)

    def set_biosample(
        self,
        title,
        spuid,
        sid,
        name,
        pkg,
        nmdc_biosample,
    ):
        attribute_mappings, slot_range_mappings = load_mappings(
            "https://raw.githubusercontent.com/microbiomedata/nmdc-schema/issue-1940/assets/ncbi_mappings/ncbi_attribute_mappings_filled.tsv"
        )

        attributes = {}
        for json_key, value in nmdc_biosample.items():
            if isinstance(value, list):
                continue

            xml_key = attribute_mappings.get(json_key, json_key)
            value_type = slot_range_mappings.get(json_key, "string")
            handler = self.type_handlers.get(value_type, handle_string_value)

            formatted_value = handler(value)
            attributes[xml_key] = formatted_value

        # Create the BioSample XML block with these attributes
        biosample_elements = [
            self.set_element(
                "SampleId",
                children=[
                    self.set_element("SPUID", sid, {"spuid_namespace": self.org})
                ],
            ),
            self.set_element(
                "Descriptor",
                children=[
                    self.set_element("Title", title),
                    self.set_element(
                        "Description", children=[self.set_element("p", spuid)]
                    ),
                ],
            ),
            self.set_element(
                "Organism", children=[self.set_element("OrganismName", name)]
            ),
            self.set_element("Package", pkg),
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
                                        )
                                    ],
                                )
                            ],
                        ),
                        self.set_element(
                            "Identifier",
                            children=[
                                self.set_element(
                                    "SPUID", sid, {"spuid_namespace": self.org}
                                )
                            ],
                        ),
                    ],
                )
            ],
        )
        self.root.append(action)

    def get_submission_xml(self):
        self.set_description()

        # initialize/make call to self.set_bioproject() here

        # TODO: iterate over all biosamples in the study
        # make call to self.set_biosample() here

        rough_string = ET.tostring(self.root, "unicode")
        reparsed = xml.dom.minidom.parseString(rough_string)
        return reparsed.toprettyxml(indent="    ", newl="\n")
