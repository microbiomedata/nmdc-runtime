from unittest.mock import MagicMock
import pytest
from requests.exceptions import HTTPError
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
from nmdc_runtime.site.export.nmdc_api_client import NMDCApiClient

MOCK_SUBMISSION_FIELDS = {
    "nmdc_study_id": "nmdc:sty-11-12345",
    "nmdc_ncbi_attribute_mapping_file_url": "http://example.com/mappings.tsv",
    "ncbi_submission_metadata": {
        "email": "user@example.com",
        "user": "testuser",
        "first": "Test",
        "last": "User",
        "organization": "Test Org",
    },
    "ncbi_bioproject_metadata": {
        "title": "Test Project",
        "project_id": "PRJNA12345",
        "description": "A test project",
        "data_type": "metagenome",
    },
    "ncbi_biosample_metadata": {
        "title": "Test Sample",
        "organism_name": "E. coli",
        "package": "Test Package",
    },
}


@pytest.fixture
def ncbi_submission_client():
    return NCBISubmissionXML(ncbi_submission_fields=MOCK_SUBMISSION_FIELDS)


@pytest.fixture
def nmdc_api_client():
    return NMDCApiClient(api_base_url="http://fakeapi.com/")


@pytest.fixture
def nmdc_biosample():
    return [
        {
            "analysis_type": ["metagenomics"],
            "biosample_categories": ["NEON"],
            "collection_date": {"has_raw_value": "2014-08-05T18:40Z"},
            "conduc": {"has_numeric_value": 567, "has_unit": "uS/cm"},
            "elev": 1178.7,
            "env_broad_scale": {
                "term": {"id": "ENVO:03605008", "name": "freshwater stream biome"}
            },
            "env_local_scale": {
                "term": {"id": "ENVO:03605007", "name": "freshwater stream"}
            },
            "env_medium": {"term": {"id": "ENVO:03605006", "name": "stream water"}},
            "env_package": {"has_raw_value": "water"},
            "geo_loc_name": {"has_raw_value": "USA: Colorado, Arikaree River"},
            "id": "nmdc:bsm-12-gnfpt483",
            "lat_lon": {"latitude": 39.758359, "longitude": -102.448595},
            "name": "ARIK.SS.20140805",
            "part_of": ["nmdc:sty-11-hht5sb92"],
            "samp_collec_device": "Grab",
            "temp": {"has_numeric_value": 20.1, "has_unit": "Cel"},
            "type": "nmdc:Biosample",
        }
    ]


class TestNCBISubmissionXML:
    def test_set_element(self, ncbi_submission_client):
        element = ncbi_submission_client.set_element("Test", "Hello", {"attr": "value"})
        assert element.tag == "Test"
        assert element.text == "Hello"
        assert element.attrib == {"attr": "value"}

    def test_set_description(self, ncbi_submission_client):
        ncbi_submission_client.set_description(
            MOCK_SUBMISSION_FIELDS["ncbi_submission_metadata"]["email"],
            MOCK_SUBMISSION_FIELDS["ncbi_submission_metadata"]["user"],
            MOCK_SUBMISSION_FIELDS["ncbi_submission_metadata"]["first"],
            MOCK_SUBMISSION_FIELDS["ncbi_submission_metadata"]["last"],
            MOCK_SUBMISSION_FIELDS["ncbi_submission_metadata"]["organization"],
        )
        description = ET.tostring(
            ncbi_submission_client.root.find("Description"), "unicode"
        )

        root = ET.fromstring(description)
        comment = root.find("Comment").text
        submitter = root.find("Submitter").attrib["user_name"]
        org_name = root.find("Organization/Name").text
        contact_email = root.find("Organization/Contact").attrib["email"]
        contact_first = root.find("Organization/Contact/Name/First").text
        contact_last = root.find("Organization/Contact/Name/Last").text

        assert comment == "NMDC Submission for nmdc:sty-11-12345"
        assert submitter == "testuser"
        assert org_name == "Test Org"
        assert contact_email == "user@example.com"
        assert contact_first == "Test"
        assert contact_last == "User"

    def test_set_bioproject(self, ncbi_submission_client):
        ncbi_submission_client.set_bioproject(
            title=MOCK_SUBMISSION_FIELDS["ncbi_bioproject_metadata"]["title"],
            project_id=MOCK_SUBMISSION_FIELDS["ncbi_bioproject_metadata"]["project_id"],
            description=MOCK_SUBMISSION_FIELDS["ncbi_bioproject_metadata"][
                "description"
            ],
            data_type=MOCK_SUBMISSION_FIELDS["ncbi_bioproject_metadata"]["data_type"],
            org=MOCK_SUBMISSION_FIELDS["ncbi_submission_metadata"]["organization"],
        )
        bioproject_xml = ET.tostring(
            ncbi_submission_client.root.find(".//Project"), "unicode"
        )
        assert "Test Project" in bioproject_xml
        assert "PRJNA12345" in bioproject_xml
        assert "A test project" in bioproject_xml
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
            organism_name=MOCK_SUBMISSION_FIELDS["ncbi_biosample_metadata"][
                "organism_name"
            ],
            package=MOCK_SUBMISSION_FIELDS["ncbi_biosample_metadata"]["package"],
            org=MOCK_SUBMISSION_FIELDS["ncbi_submission_metadata"]["organization"],
            nmdc_biosamples=nmdc_biosample,
        )
        biosample_xml = ET.tostring(
            ncbi_submission_client.root.find(".//BioSample"), "unicode"
        )
        assert "E. coli" in biosample_xml
        assert "Test Package" in biosample_xml
        assert "Test Org" in biosample_xml

    def test_get_submission_xml(self, mocker, ncbi_submission_client, nmdc_biosample):
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

        mocker.patch.object(
            NMDCApiClient, "get_biosamples_part_of_study", return_value=nmdc_biosample
        )

        submission_xml = ncbi_submission_client.get_submission_xml()

        assert "nmdc:bsm-12-gnfpt483" in submission_xml
        assert "E. coli" in submission_xml
        assert "stream water" in submission_xml
        assert "USA: Colorado, Arikaree River" in submission_xml
        assert "2014-08-05T18:40Z" in submission_xml
        assert "testuser" in submission_xml
        assert "Test Project" in submission_xml


class TestNMDCApiClient:
    def test_get_biosamples_part_of_study_success(self, mocker, nmdc_api_client):
        mock_response = mocker.MagicMock()
        mock_response.json.return_value = {
            "resources": [
                {"id": "nmdc:bsm-12-gnfpt483", "part_of": ["nmdc:sty-11-hht5sb92"]}
            ],
            "next_page_token": None,
        }
        mocker.patch("requests.get", return_value=mock_response)
        result = nmdc_api_client.get_biosamples_part_of_study("nmdc:sty-11-hht5sb92")
        assert result == [
            {"id": "nmdc:bsm-12-gnfpt483", "part_of": ["nmdc:sty-11-hht5sb92"]}
        ]

    def test_get_biosamples_part_of_study_failure(self, mocker, nmdc_api_client):
        mocker.patch("requests.get", side_effect=HTTPError("API Error"))
        with pytest.raises(HTTPError):
            nmdc_api_client.get_biosamples_part_of_study("nmdc:sty-11-hht5sb92")


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
            "Biosample\tgeo_loc_name\tQuantityValue\tgeo_loc_name\t\t\n"
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
            "geo_loc_name": "QuantityValue",
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
