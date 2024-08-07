from unittest.mock import MagicMock
import pytest
import xml.etree.ElementTree as ET

from nmdc_runtime.site.export.ncbi_xml import NCBISubmissionXML
from nmdc_runtime.site.export.ncbi_xml_utils import (
    load_mappings,
    handle_quantity_value,
    handle_text_value,
    handle_timestamp_value,
    handle_controlled_term_value,
    handle_controlled_identified_term_value,
    handle_geolocation_value,
    handle_float_value,
    handle_string_value,
)

MOCK_NMDC_STUDY = {
    "id": "nmdc:sty-11-34xj1150",
    "name": "National Ecological Observatory Network: soil metagenomes (DP1.10107.001)",
    "description": "This study contains the quality-controlled laboratory metadata and minimally processed sequence data from NEON's soil microbial shotgun metagenomics sequencing. Typically, measurements are done on plot-level composite samples and represent up to three randomly selected sampling locations within a plot.",
    "gold_study_identifiers": ["gold:Gs0144570", "gold:Gs0161344"],
    "principal_investigator": {
        "has_raw_value": "Kate Thibault",
        "email": "kthibault@battelleecology.org",
        "name": "Kate Thibault",
        "orcid": "orcid:0000-0003-3477-6424",
        "profile_image_url": "https://portal.nersc.gov/project/m3408/profile_images/thibault_katy.jpg",
    },
    "title": "National Ecological Observatory Network: soil metagenomes (DP1.10107.001)",
    "type": "nmdc:Study",
    "websites": [
        "https://data.neonscience.org/data-products/DP1.10107.001",
        "https://data.neonscience.org/api/v0/documents/NEON.DOC.014048vO",
        "https://data.neonscience.org/api/v0/documents/NEON_metagenomes_userGuide_vE.pdf",
    ],
    "study_image": [
        {
            "url": "https://portal.nersc.gov/project/m3408/profile_images/nmdc_sty-11-34xj1150.jpg"
        }
    ],
    "funding_sources": [
        "NSF#1724433 National Ecological Observatory Network: Operations Activities"
    ],
    "has_credit_associations": [
        {
            "applies_to_person": {
                "name": "Hugh Cross",
                "email": "crossh@battelleecology.org",
                "orcid": "orcid:0000-0002-6745-9479",
            },
            "applied_roles": ["Methodology", "Data curation"],
        },
        {
            "applies_to_person": {
                "name": "Samantha Weintraub-Leff",
                "email": "sweintraub@battelleecology.org",
                "orcid": "orcid:0000-0003-4789-5086",
            },
            "applied_roles": ["Methodology", "Data curation"],
        },
        {
            "applies_to_person": {
                "name": "Kate Thibault",
                "email": "kthibault@battelleecology.org",
                "orcid": "orcid:0000-0003-3477-6424",
            },
            "applied_roles": ["Principal Investigator"],
        },
    ],
    "part_of": ["nmdc:sty-11-nxrz9m96"],
    "study_category": "consortium",
    "insdc_bioproject_identifiers": ["bioproject:PRJNA1029061"],
    "homepage_website": ["https://www.neonscience.org/"],
}

MOCK_NCBI_SUBMISSION_METADATA = {
    "nmdc_ncbi_attribute_mapping_file_url": "http://example.com/mappings.tsv",
    "ncbi_submission_metadata": {
        "organization": "Test Org",
    },
    "ncbi_biosample_metadata": {
        "organism_name": "E. coli",
    },
}


@pytest.fixture
def ncbi_submission_client():
    return NCBISubmissionXML(
        nmdc_study=MOCK_NMDC_STUDY,
        ncbi_submission_metadata=MOCK_NCBI_SUBMISSION_METADATA,
    )


@pytest.fixture
def nmdc_biosample():
    return [
        {
            "analysis_type": ["metagenomics"],
            "biosample_categories": ["NEON"],
            "collection_date": {"has_raw_value": "2015-07-21T18:00Z"},
            "depth": {
                "has_maximum_numeric_value": 1,
                "has_minimum_numeric_value": 0,
                "has_unit": "meters",
            },
            "elev": 1179.5,
            "env_broad_scale": {
                "term": {"id": "ENVO:01000253", "name": "freshwater river biome"}
            },
            "env_local_scale": {"term": {"id": "ENVO:03600094", "name": "stream pool"}},
            "env_medium": {"term": {"id": "ENVO:00002007", "name": "sediment"}},
            "geo_loc_name": {"has_raw_value": "USA: Colorado, Arikaree River"},
            "id": "nmdc:bsm-12-p9q5v236",
            "lat_lon": {"latitude": 39.758206, "longitude": -102.447148},
            "name": "ARIK.20150721.AMC.EPIPSAMMON.3",
            "part_of": ["nmdc:sty-11-34xj1150"],
            "type": "nmdc:Biosample",
        }
    ]


@pytest.fixture
def omics_processing_list():
    return [
        {
            "has_input": ["nmdc:procsm-12-ehktny16"],
            "has_output": ["nmdc:dobj-12-1zv4q961", "nmdc:dobj-12-b3ft7a80"],
            "id": "nmdc:omprc-12-zqm9p096",
            "instrument_name": "Illumina NextSeq550",
            "name": "Terrestrial soil microbial communities - ARIK.20150721.AMC.EPIPSAMMON.3-DNA1",
            "ncbi_project_name": "PRJNA406976",
            "omics_type": {"has_raw_value": "metagenome"},
            "part_of": ["nmdc:sty-11-34xj1150"],
            "processing_institution": "Battelle",
            "type": "nmdc:OmicsProcessing",
        }
    ]


@pytest.fixture
def data_objects_list():
    return [
        {
            "data_object_type": "Metagenome Raw Read 1",
            "description": "sequencing results for BMI_HVKNKBGX5_Tube347_R1",
            "id": "nmdc:dobj-12-b3ft7a80",
            "md5_checksum": "cae0a9342d786e731ae71f6f37b76120",
            "name": "BMI_HVKNKBGX5_Tube347_R1.fastq.gz",
            "type": "nmdc:DataObject",
            "url": "https://storage.neonscience.org/neon-microbial-raw-seq-files/2023/BMI_HVKNKBGX5_mms_R1/BMI_HVKNKBGX5_Tube347_R1.fastq.gz",
        },
        {
            "data_object_type": "Metagenome Raw Read 2",
            "description": "sequencing results for BMI_HVKNKBGX5_Tube347_R2",
            "id": "nmdc:dobj-12-1zv4q961",
            "md5_checksum": "7340fe25644183a4f56d36ce52389d83",
            "name": "BMI_HVKNKBGX5_Tube347_R2.fastq.gz",
            "type": "nmdc:DataObject",
            "url": "https://storage.neonscience.org/neon-microbial-raw-seq-files/2023/BMI_HVKNKBGX5_mms_R2/BMI_HVKNKBGX5_Tube347_R2.fastq.gz",
        },
    ]


@pytest.fixture
def library_preparation_dict():
    return {
        "end_date": "2018-06-20",
        "has_input": ["nmdc:procsm-12-sb2v8f15"],
        "has_output": ["nmdc:procsm-12-ehktny16"],
        "id": "nmdc:libprp-12-5hhdd393",
        "processing_institution": "Battelle",
        "start_date": "2015-07-21T18:00Z",
        "protocol_link": {"name": "BMI_metagenomicsSequencingSOP_v1"},
    }


class TestNCBISubmissionXML:
    def test_set_element(self, ncbi_submission_client):
        element = ncbi_submission_client.set_element("Test", "Hello", {"attr": "value"})
        assert element.tag == "Test"
        assert element.text == "Hello"
        assert element.attrib == {"attr": "value"}

    def test_set_description(self, ncbi_submission_client):
        ncbi_submission_client.set_description(
            ncbi_submission_client.nmdc_pi_email,
            "Kate",
            "Thibault",
            "NSF National Ecological Observatory Network",
        )
        description = ET.tostring(
            ncbi_submission_client.root.find("Description"), "unicode"
        )

        root = ET.fromstring(description)
        comment = root.find("Comment").text
        org_name = root.find("Organization/Name").text
        contact_email = root.find("Organization/Contact").attrib["email"]
        contact_first = root.find("Organization/Contact/Name/First").text
        contact_last = root.find("Organization/Contact/Name/Last").text

        assert comment == f"NMDC Submission for {MOCK_NMDC_STUDY['id']}"
        assert org_name == "NSF National Ecological Observatory Network"
        assert contact_email == "kthibault@battelleecology.org"
        assert contact_first == "Kate"
        assert contact_last == "Thibault"

    def test_set_bioproject(self, ncbi_submission_client):
        ncbi_submission_client.set_bioproject(
            title=MOCK_NMDC_STUDY["title"],
            project_id=MOCK_NMDC_STUDY["insdc_bioproject_identifiers"][0],
            description=MOCK_NMDC_STUDY["description"],
            data_type="metagenome",
            org="Test Org",
        )
        bioproject_xml = ET.tostring(
            ncbi_submission_client.root.find(".//Project"), "unicode"
        )
        assert (
            "National Ecological Observatory Network: soil metagenomes (DP1.10107.001)"
            in bioproject_xml
        )
        assert "bioproject:PRJNA1029061" in bioproject_xml
        assert (
            "This study contains the quality-controlled laboratory metadata and minimally processed sequence data from NEON's soil microbial shotgun metagenomics sequencing."
            in bioproject_xml
        )
        assert "metagenome" in bioproject_xml
        assert "Test Org" in bioproject_xml

    def test_set_biosample(self, ncbi_submission_client, nmdc_biosample, mocker):
        mocker.patch(
            "nmdc_runtime.site.export.ncbi_xml.load_mappings",
            return_value=(
                {
                    "analysis_type": "",
                    "biosample_categories": "",
                    "collection_date": "collection_date",
                    "conduc": "conduc",
                    "elev": "elev",
                    "env_broad_scale": "env_broad_scale",
                    "env_local_scale": "env_local_scale",
                    "env_medium": "env_medium",
                    "env_package": "env_package",
                    "geo_loc_name": "geo_loc_name",
                    "id": "",
                    "lat_lon": "lat_lon",
                    "name": "sample_name",
                    "part_of": "",
                    "samp_collec_device": "samp_collect_device",
                    "temp": "temp",
                    "type": "",
                },
                {
                    "analysis_type": "AnalysisTypeEnum",
                    "biosample_categories": "BiosampleCategoryEnum",
                    "collection_date": "TimestampValue",
                    "conduc": "QuantityValue",
                    "elev": "float",
                    "env_broad_scale": "ControlledIdentifiedTermValue",
                    "env_local_scale": "ControlledIdentifiedTermValue",
                    "env_medium": "ControlledIdentifiedTermValue",
                    "env_package": "TextValue",
                    "geo_loc_name": "TextValue",
                    "id": "uriorcurie",
                    "lat_lon": "GeolocationValue",
                    "name": "string",
                    "part_of": "Study",
                    "samp_collec_device": "string",
                    "temp": "QuantityValue",
                    "type": "string",
                },
            ),
        )
        ncbi_submission_client.set_biosample(
            organism_name=MOCK_NCBI_SUBMISSION_METADATA["ncbi_biosample_metadata"][
                "organism_name"
            ],
            org=MOCK_NCBI_SUBMISSION_METADATA["ncbi_submission_metadata"][
                "organization"
            ],
            bioproject_id=MOCK_NMDC_STUDY["insdc_bioproject_identifiers"][0],
            nmdc_biosamples=nmdc_biosample,
        )
        biosample_xml = ET.tostring(
            ncbi_submission_client.root.find(".//BioSample"), "unicode"
        )
        assert "E. coli" in biosample_xml
        assert "Test Org" in biosample_xml
        assert "PRJNA1029061" in biosample_xml

    def test_set_fastq(
        self,
        ncbi_submission_client,
        nmdc_biosample,
        data_objects_list,
        omics_processing_list,
        library_preparation_dict,
    ):
        biosample_data_objects = [
            {biosample["id"]: data_objects_list} for biosample in nmdc_biosample
        ]

        biosample_omics_processing = [
            {biosample["id"]: omics_processing_list} for biosample in nmdc_biosample
        ]

        biosample_library_preparation = [
            {biosample["id"]: library_preparation_dict} for biosample in nmdc_biosample
        ]

        ncbi_submission_client.set_fastq(
            biosample_data_objects=biosample_data_objects,
            bioproject_id=MOCK_NMDC_STUDY["insdc_bioproject_identifiers"][0],
            org="Test Org",
            nmdc_omics_processing=biosample_omics_processing,
            nmdc_biosamples=nmdc_biosample,
            nmdc_library_preparation=biosample_library_preparation,
        )

        action_elements = ncbi_submission_client.root.findall(".//Action")
        assert len(action_elements) == 1  # 1 SRA <Action> block

        for action_element in action_elements:
            action_xml = ET.tostring(action_element, "unicode")
            assert (
                "BMI_HVKNKBGX5_Tube347_R1.fastq.gz" in action_xml
                or "BMI_HVKNKBGX5_Tube347_R2.fastq.gz" in action_xml
            )
            assert "PRJNA1029061" in action_xml
            assert "nmdc:bsm-12-p9q5v236" in action_xml
            assert "Test Org" in action_xml
            # library Attributes in SRA <Action> block
            assert "ILLUMINA" in action_xml
            assert "NextSeq 550" in action_xml
            assert "METAGENOMIC" in action_xml
            assert "RANDOM" in action_xml
            assert "paired" in action_xml
            assert "ARIK.20150721.AMC.EPIPSAMMON.3" in action_xml
            assert "BMI_metagenomicsSequencingSOP_v1" in action_xml
            assert "sra-run-fastq" in action_xml

    def test_get_submission_xml(
        self,
        mocker,
        ncbi_submission_client,
        nmdc_biosample,
        data_objects_list,
        omics_processing_list,
        library_preparation_dict,
    ):
        mocker.patch(
            "nmdc_runtime.site.export.ncbi_xml.load_mappings",
            return_value=(
                {
                    "analysis_type": "",
                    "biosample_categories": "",
                    "collection_date": "collection_date",
                    "conduc": "conduc",
                    "elev": "elev",
                    "env_broad_scale": "env_broad_scale",
                    "env_local_scale": "env_local_scale",
                    "env_medium": "env_medium",
                    "env_package": "env_package",
                    "geo_loc_name": "geo_loc_name",
                    "id": "",
                    "lat_lon": "lat_lon",
                    "name": "sample_name",
                    "part_of": "",
                    "samp_collec_device": "samp_collect_device",
                    "temp": "temp",
                    "type": "",
                },
                {
                    "analysis_type": "AnalysisTypeEnum",
                    "biosample_categories": "BiosampleCategoryEnum",
                    "collection_date": "TimestampValue",
                    "conduc": "QuantityValue",
                    "elev": "float",
                    "env_broad_scale": "ControlledIdentifiedTermValue",
                    "env_local_scale": "ControlledIdentifiedTermValue",
                    "env_medium": "ControlledIdentifiedTermValue",
                    "env_package": "TextValue",
                    "geo_loc_name": "TextValue",
                    "id": "uriorcurie",
                    "lat_lon": "GeolocationValue",
                    "name": "string",
                    "part_of": "Study",
                    "samp_collec_device": "string",
                    "temp": "QuantityValue",
                    "type": "string",
                },
            ),
        )

        biosample_data_objects = [
            {biosample["id"]: data_objects_list} for biosample in nmdc_biosample
        ]

        biosample_omics_prcessing = [
            {biosample["id"]: omics_processing_list} for biosample in nmdc_biosample
        ]

        biosample_library_preparation = [
            {biosample["id"]: library_preparation_dict} for biosample in nmdc_biosample
        ]

        ncbi_submission_client.set_fastq(
            biosample_data_objects=biosample_data_objects,
            bioproject_id=MOCK_NMDC_STUDY["insdc_bioproject_identifiers"][0],
            org="Test Org",
            nmdc_omics_processing=biosample_omics_prcessing,
            nmdc_biosamples=nmdc_biosample,
            nmdc_library_preparation=biosample_library_preparation,
        )

        submission_xml = ncbi_submission_client.get_submission_xml(
            nmdc_biosample, [], biosample_data_objects, biosample_library_preparation
        )

        assert "nmdc:bsm-12-p9q5v236" in submission_xml
        assert "E. coli" in submission_xml
        assert "sediment" in submission_xml
        assert "USA: Colorado, Arikaree River" in submission_xml
        assert "2015-07-21T18:00Z" in submission_xml
        assert "National Microbiome Data Collaborative" in submission_xml
        assert (
            "National Ecological Observatory Network: soil metagenomes (DP1.10107.001)"
            in submission_xml
        )


class TestNCBIXMLUtils:
    def test_handle_quantity_value(self):
        assert (
            handle_quantity_value({"has_numeric_value": 10, "has_unit": "mg"})
            == "10 mg"
        )
        assert (
            handle_quantity_value(
                {
                    "has_maximum_numeric_value": 15,
                    "has_minimum_numeric_value": 5,
                    "has_unit": "kg",
                }
            )
            == "10 kg"
        )
        assert handle_quantity_value({"has_raw_value": "20 units"}) == "20 units"
        assert handle_quantity_value({}) == "Unknown format"

    def test_handle_text_value(self):
        assert handle_text_value({"has_raw_value": "Sample Text"}) == "Sample Text"
        assert handle_text_value({}) == "Unknown format"

    def test_handle_timestamp_value(self):
        assert handle_timestamp_value({"has_raw_value": "2021-01-01"}) == "2021-01-01"
        assert handle_timestamp_value({}) == "Unknown format"

    def test_handle_controlled_term_value(self):
        term_data = {"term": {"name": "Homo sapiens", "id": "NCBITaxon:9606"}}
        assert (
            handle_controlled_term_value(term_data) == "Homo sapiens [NCBITaxon:9606]"
        )
        assert (
            handle_controlled_term_value({"term": {"id": "NCBITaxon:9606"}})
            == "NCBITaxon:9606"
        )
        assert (
            handle_controlled_term_value({"term": {"name": "Homo sapiens"}})
            == "Homo sapiens"
        )
        assert (
            handle_controlled_term_value(
                {"has_raw_value": "Homo sapiens [NCBITaxon:9606]"}
            )
            == "Homo sapiens [NCBITaxon:9606]"
        )
        assert handle_controlled_term_value({}) == "Unknown format"

    def test_handle_controlled_identified_term_value(self):
        term_data = {"term": {"name": "Homo sapiens", "id": "NCBITaxon:9606"}}
        assert (
            handle_controlled_identified_term_value(term_data)
            == "Homo sapiens [NCBITaxon:9606]"
        )
        assert (
            handle_controlled_identified_term_value({"term": {"id": "NCBITaxon:9606"}})
            == "NCBITaxon:9606"
        )
        assert (
            handle_controlled_identified_term_value({"term": {"name": "Homo sapiens"}})
            == "Unknown format"
        )
        assert (
            handle_controlled_identified_term_value(
                {"has_raw_value": "Homo sapiens [NCBITaxon:9606]"}
            )
            == "Homo sapiens [NCBITaxon:9606]"
        )
        assert handle_controlled_identified_term_value({}) == "Unknown format"

    def test_handle_geolocation_value(self):
        assert (
            handle_geolocation_value({"latitude": 34.05, "longitude": -118.25})
            == "34.05 -118.25"
        )
        assert (
            handle_geolocation_value({"has_raw_value": "34.05, -118.25"})
            == "34.05, -118.25"
        )
        assert handle_geolocation_value({}) == "Unknown format"

    def test_handle_float_value(self):
        assert handle_float_value(10.1234) == "10.12"

    def test_handle_string_value(self):
        assert handle_string_value("Foo") == "Foo"

    def test_load_mappings(self, mocker):
        mock_tsv_content = (
            "nmdc_schema_class\tnmdc_schema_slot\tnmdc_schema_slot_range\tncbi_biosample_attribute_name\tstatic_value\tignore\n"
            "Biosample\tanalysis_type\tAnalysisTypeEnum\t\t\t\n"
            "Biosample\tbiosample_categories\tBiosampleCategoryEnum\t\t\t\n"
            "Biosample\tcollection_date\tTimestampValue\tcollection_date\t\t\n"
            "Biosample\tconduc\tQuantityValue\tconduc\t\t\n"
            "Biosample\telev\tfloat\telev\t\t\n"
            "Biosample\tenv_broad_scale\tControlledIdentifiedTermValue\tenv_broad_scale\t\t\n"
            "Biosample\tenv_local_scale\tControlledIdentifiedTermValue\tenv_local_scale\t\t\n"
            "Biosample\tenv_medium\tControlledIdentifiedTermValue\tenv_medium\t\t\n"
            "Biosample\tenv_package\tTextValue\tenv_package\t\t\n"
            "Biosample\tgeo_loc_name\tTextValue\tgeo_loc_name\t\t\n"
            "Biosample\tid\turiorcurie\t\t\t\n"
            "Biosample\tlat_lon\tGeolocationValue\tlat_lon\t\t\n"
            "Biosample\tname\tstring\tsample_name\t\t\n"
            "Biosample\tpart_of\tStudy\t\t\t\n"
            "Biosample\tsamp_collec_device\tstring\tsamp_collect_device\t\t\n"
            "Biosample\ttemp\tQuantityValue\ttemp\t\t\n"
            "Biosample\ttype\tstring\t\t\t\n"
        )

        mock_response = MagicMock()
        mock_response.text = mock_tsv_content
        mocker.patch("requests.get", return_value=mock_response)

        attribute_mappings, slot_range_mappings = load_mappings(
            "http://example.com/mappings.tsv"
        )

        expected_attribute_mappings = {
            "analysis_type": "analysis_type",
            "biosample_categories": "biosample_categories",
            "collection_date": "collection_date",
            "conduc": "conduc",
            "elev": "elev",
            "env_broad_scale": "env_broad_scale",
            "env_local_scale": "env_local_scale",
            "env_medium": "env_medium",
            "env_package": "env_package",
            "geo_loc_name": "geo_loc_name",
            "id": "id",
            "lat_lon": "lat_lon",
            "name": "sample_name",
            "part_of": "part_of",
            "samp_collec_device": "samp_collect_device",
            "temp": "temp",
            "type": "type",
        }

        expected_slot_range_mappings = {
            "analysis_type": "AnalysisTypeEnum",
            "biosample_categories": "BiosampleCategoryEnum",
            "collection_date": "TimestampValue",
            "conduc": "QuantityValue",
            "elev": "float",
            "env_broad_scale": "ControlledIdentifiedTermValue",
            "env_local_scale": "ControlledIdentifiedTermValue",
            "env_medium": "ControlledIdentifiedTermValue",
            "env_package": "TextValue",
            "geo_loc_name": "TextValue",
            "id": "uriorcurie",
            "lat_lon": "GeolocationValue",
            "name": "string",
            "part_of": "Study",
            "samp_collec_device": "string",
            "temp": "QuantityValue",
            "type": "string",
        }

        assert attribute_mappings == expected_attribute_mappings
        assert slot_range_mappings == expected_slot_range_mappings
