from typing import Any, Callable, Generator
from unittest.mock import MagicMock
import pytest
import xml.etree.ElementTree as ET

from pytest_mock import MockerFixture

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
    # To satisfy referential integrity checks, omit this `part_of` reference
    # unless the corresponding study document is included.
    # "part_of": ["nmdc:sty-11-nxrz9m96"],
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
            "collection_date": {
                "has_raw_value": "2015-07-21T18:00Z",
                "type": "nmdc:TimestampValue",
            },
            "depth": {
                "has_maximum_numeric_value": 1,
                "has_minimum_numeric_value": 0,
                "has_unit": "m",
                "type": "nmdc:QuantityValue",
            },
            "elev": 1179.5,
            "env_broad_scale": {
                "term": {
                    "id": "ENVO:01000253",
                    "name": "freshwater river biome",
                    "type": "nmdc:OntologyClass",
                },
                "type": "nmdc:ControlledIdentifiedTermValue",
            },
            "env_local_scale": {
                "term": {
                    "id": "ENVO:03600094",
                    "name": "stream pool",
                    "type": "nmdc:OntologyClass",
                },
                "type": "nmdc:ControlledIdentifiedTermValue",
            },
            "env_medium": {
                "term": {
                    "id": "ENVO:03605004",
                    "name": "epipsammon",
                    "type": "nmdc:OntologyClass",
                },
                "type": "nmdc:ControlledIdentifiedTermValue",
            },
            "geo_loc_name": {
                "has_raw_value": "USA: Colorado, Arikaree River",
                "type": "nmdc:TextValue",
            },
            "id": "nmdc:bsm-12-p9q5v236",
            "lat_lon": {
                "latitude": 39.758206,
                "longitude": -102.447148,
                "type": "nmdc:GeolocationValue",
            },
            "name": "ARIK.20150721.AMC.EPIPSAMMON.3",
            "type": "nmdc:Biosample",
            "associated_studies": ["nmdc:sty-11-pzmd0x14"],
            "host_taxid": {
                "term": {"id": "NCBITaxon:9606", "name": "Homo sapiens"},
                "type": "nmdc:ControlledIdentifiedTermValue",
            },
            "env_package": {"has_raw_value": "soil", "type": "nmdc:TextValue"},
        }
    ]


@pytest.fixture
def nucleotide_sequencing_list():
    return [
        {
            "id": "nmdc:dgns-11-e01w1f21",
            "type": "nmdc:NucleotideSequencing",
            "name": "Benthic microbial communities - ARIK.20150721.AMC.EPIPSAMMON.3-DNA1",
            "has_input": ["nmdc:procsm-12-ehktny16"],
            "has_output": ["nmdc:dobj-11-8wjdvj33", "nmdc:dobj-11-0y3amn94"],
            "processing_institution": "Battelle",
            "analyte_category": "metagenome",
            "associated_studies": ["nmdc:sty-11-pzmd0x14"],
            "instrument_used": ["nmdc:inst-14-xz5tb342"],
            "ncbi_project_name": "PRJNA406976",
        }
    ]


@pytest.fixture
def data_objects_list():
    return [
        {
            "id": "nmdc:dobj-11-8wjdvj33",
            "type": "nmdc:DataObject",
            "name": "BMI_HVKNKBGX5_Tube347_srt_R1.fastq.gz",
            "description": "sequencing results for BMI_HVKNKBGX5_Tube347_srt_R1",
            "data_object_type": "Metagenome Raw Read 1",
            "md5_checksum": "98017c587ef4e6a8a54f8daa0925e4e1",
            "url": "https://storage.neonscience.org/neon-microbial-raw-seq-files/2023/BMI_HVKNKBGX5_srt_R1/BMI_HVKNKBGX5_Tube347_srt_R1.fastq.gz",
        },
        {
            "id": "nmdc:dobj-11-0y3amn94",
            "type": "nmdc:DataObject",
            "name": "BMI_HVKNKBGX5_Tube347_srt_R2.fastq.gz",
            "description": "sequencing results for BMI_HVKNKBGX5_Tube347_srt_R2",
            "data_object_type": "Metagenome Raw Read 2",
            "md5_checksum": "5358ce1da32bfad7c358c484cbf5075b",
            "url": "https://storage.neonscience.org/neon-microbial-raw-seq-files/2023/BMI_HVKNKBGX5_srt_R2/BMI_HVKNKBGX5_Tube347_srt_R2.fastq.gz",
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


@pytest.fixture
def mocked_instruments():
    return [
        {
            "id": "nmdc:inst-14-xz5tb342",
            "model": "nextseq_550",
            "name": "Illumina NextSeq 550",
            "vendor": "illumina",
            "type": "nmdc:Instrument",
        },
        {
            "id": "nmdc:inst-14-79zxap02",
            "model": "hiseq",
            "name": "Illumina HiSeq",
            "vendor": "illumina",
            "type": "nmdc:Instrument",
        },
    ]


class TestNCBISubmissionXML:
    def test_set_element(self, ncbi_submission_client: NCBISubmissionXML):
        element = ncbi_submission_client.set_element("Test", "Hello", {"attr": "value"})
        assert element.tag == "Test"
        assert element.text == "Hello"
        assert element.attrib == {"attr": "value"}

    def test_set_description(self, ncbi_submission_client: NCBISubmissionXML):
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

    def test_set_bioproject(self, ncbi_submission_client: NCBISubmissionXML):
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

    def test_set_biosample(
        self,
        ncbi_submission_client: NCBISubmissionXML,
        nmdc_biosample: list[dict[str, Any]],
        mocker: Callable[..., Generator[MockerFixture, None, None]],
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
        ncbi_submission_client: NCBISubmissionXML,
        nmdc_biosample: list[dict[str, Any]],
        data_objects_list: list[dict[str, str]],
        nucleotide_sequencing_list: list[dict[str, Any]],
        library_preparation_dict: dict[str, Any],
        mocked_instruments: list[dict[str, Any]],
    ):
        all_instruments = {
            instrument["id"]: {
                "vendor": instrument["vendor"],
                "model": instrument["model"],
            }
            for instrument in mocked_instruments
        }

        biosample_data_objects = [
            {biosample["id"]: data_objects_list} for biosample in nmdc_biosample
        ]

        biosample_nucleotide_sequencing = [
            {biosample["id"]: nucleotide_sequencing_list}
            for biosample in nmdc_biosample
        ]

        biosample_library_preparation = [
            {biosample["id"]: library_preparation_dict} for biosample in nmdc_biosample
        ]

        ncbi_submission_client.set_fastq(
            biosample_data_objects=biosample_data_objects,
            bioproject_id=MOCK_NMDC_STUDY["insdc_bioproject_identifiers"][0],
            org="Test Org",
            nmdc_nucleotide_sequencing=biosample_nucleotide_sequencing,
            nmdc_biosamples=nmdc_biosample,
            nmdc_library_preparation=biosample_library_preparation,
            all_instruments=all_instruments,
        )

        action_elements = ncbi_submission_client.root.findall(".//Action")
        assert len(action_elements) == 1  # 1 SRA <Action> block

        for action_element in action_elements:
            action_xml = ET.tostring(action_element, "unicode")
            assert (
                "BMI_HVKNKBGX5_Tube347_srt_R1.fastq.gz" in action_xml
                or "BMI_HVKNKBGX5_Tube347_srt_R2.fastq.gz" in action_xml
            )
            assert "PRJNA1029061" in action_xml
            assert "nmdc:bsm-12-p9q5v236" in action_xml
            assert "Test Org" in action_xml
            # library Attributes in SRA <Action> block
            assert "ILLUMINA" in action_xml
            assert "NextSeq 550" in action_xml
            # assert "METAGENOMIC" in action_xml
            assert "RANDOM" in action_xml
            assert "paired" in action_xml
            assert "ARIK.20150721.AMC.EPIPSAMMON.3" in action_xml
            assert "BMI_metagenomicsSequencingSOP_v1" in action_xml
            assert "sra-run-fastq" in action_xml

    def test_get_submission_xml(
        self,
        mocker: Callable[..., Generator[MockerFixture, None, None]],
        ncbi_submission_client: NCBISubmissionXML,
        nmdc_biosample: list[dict[str, Any]],
        data_objects_list: list[dict[str, str]],
        nucleotide_sequencing_list: list[dict[str, Any]],
        library_preparation_dict: dict[str, Any],
        mocked_instruments: list[dict[str, Any]],
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

        all_instruments = {
            instrument["id"]: {
                "vendor": instrument["vendor"],
                "model": instrument["model"],
            }
            for instrument in mocked_instruments
        }

        biosample_data_objects = [
            {biosample["id"]: data_objects_list} for biosample in nmdc_biosample
        ]

        biosample_nucleotide_sequencing = [
            {biosample["id"]: nucleotide_sequencing_list}
            for biosample in nmdc_biosample
        ]

        biosample_library_preparation = [
            {biosample["id"]: library_preparation_dict} for biosample in nmdc_biosample
        ]

        ncbi_submission_client.set_fastq(
            biosample_data_objects=biosample_data_objects,
            bioproject_id=MOCK_NMDC_STUDY["insdc_bioproject_identifiers"][0],
            org="Test Org",
            nmdc_nucleotide_sequencing=biosample_nucleotide_sequencing,
            nmdc_biosamples=nmdc_biosample,
            nmdc_library_preparation=biosample_library_preparation,
            all_instruments=all_instruments,
        )

        submission_xml = ncbi_submission_client.get_submission_xml(
            nmdc_biosample,
            biosample_nucleotide_sequencing,
            biosample_data_objects,
            biosample_library_preparation,
            all_instruments,
        )

        assert "nmdc:bsm-12-p9q5v236" in submission_xml
        assert "E. coli" in submission_xml
        assert "USA: Colorado, Arikaree River" in submission_xml
        assert "2015-07-21T18:00Z" in submission_xml
        assert "National Microbiome Data Collaborative" in submission_xml

    def test_get_submission_xml_filters_jgi_biosamples(
        self,
        mocker: Callable[..., Generator[MockerFixture, None, None]],
        ncbi_submission_client: NCBISubmissionXML,
        nmdc_biosample: list[dict[str, Any]],
        data_objects_list: list[dict[str, str]],
        nucleotide_sequencing_list: list[dict[str, Any]],
        library_preparation_dict: dict[str, Any],
        mocked_instruments: list[dict[str, Any]],
    ):
        mocker.patch(
            "nmdc_runtime.site.export.ncbi_xml.load_mappings",
            return_value=(
                {
                    "id": "",
                    "collection_date": "collection_date",
                    "geo_loc_name": "geo_loc_name",
                    "lat_lon": "lat_lon",
                    "name": "sample_name",
                },
                {
                    "id": "uriorcurie",
                    "collection_date": "TimestampValue",
                    "geo_loc_name": "TextValue",
                    "lat_lon": "GeolocationValue",
                    "name": "string",
                },
            ),
        )

        # Create two biosamples
        biosample1 = nmdc_biosample[0].copy()
        biosample1["id"] = "nmdc:bsm-12-p9q5v236"

        biosample2 = nmdc_biosample[0].copy()
        biosample2["id"] = "nmdc:bsm-12-jgitest"

        all_biosamples = [biosample1, biosample2]

        # Create nucleotide sequencing entries - one with JGI as processing_institution
        ntseq1 = nucleotide_sequencing_list[0].copy()
        ntseq1["processing_institution"] = "Battelle"

        ntseq2 = nucleotide_sequencing_list[0].copy()
        ntseq2["processing_institution"] = "JGI"  # This should be filtered out

        biosample_nucleotide_sequencing = [
            {biosample1["id"]: [ntseq1]},
            {biosample2["id"]: [ntseq2]},
        ]

        # Setup data objects and library prep
        biosample_data_objects = [
            {biosample1["id"]: data_objects_list},
            {biosample2["id"]: data_objects_list},
        ]

        biosample_library_preparation = [
            {biosample1["id"]: library_preparation_dict},
            {biosample2["id"]: library_preparation_dict},
        ]

        all_instruments = {
            instrument["id"]: {
                "vendor": instrument["vendor"],
                "model": instrument["model"],
            }
            for instrument in mocked_instruments
        }

        # Call get_submission_xml with both biosamples
        submission_xml = ncbi_submission_client.get_submission_xml(
            all_biosamples,
            biosample_nucleotide_sequencing,
            biosample_data_objects,
            biosample_library_preparation,
            all_instruments,
        )

        # Biosample 1 should be included
        assert "nmdc:bsm-12-p9q5v236" in submission_xml

        # Biosample 2 should be filtered out (JGI processing)
        assert "nmdc:bsm-12-jgitest" not in submission_xml

    def test_get_submission_xml_filters_biosamples_with_any_jgi_sequencing(
        self,
        mocker: Callable[..., Generator[MockerFixture, None, None]],
        ncbi_submission_client: NCBISubmissionXML,
        nmdc_biosample: list[dict[str, Any]],
        data_objects_list: list[dict[str, str]],
        nucleotide_sequencing_list: list[dict[str, Any]],
        library_preparation_dict: dict[str, Any],
        mocked_instruments: list[dict[str, Any]],
    ):
        mocker.patch(
            "nmdc_runtime.site.export.ncbi_xml.load_mappings",
            return_value=(
                {
                    "id": "",
                    "collection_date": "collection_date",
                    "geo_loc_name": "geo_loc_name",
                    "lat_lon": "lat_lon",
                    "name": "sample_name",
                },
                {
                    "id": "uriorcurie",
                    "collection_date": "TimestampValue",
                    "geo_loc_name": "TextValue",
                    "lat_lon": "GeolocationValue",
                    "name": "string",
                },
            ),
        )

        # Create a biosample with multiple sequencing activities
        biosample1 = nmdc_biosample[0].copy()
        biosample1["id"] = "nmdc:bsm-12-mixed"

        # Create nucleotide sequencing entries - with mixed processing institutions
        ntseq1 = nucleotide_sequencing_list[0].copy()
        ntseq1["id"] = "nmdc:ntseq-1"
        ntseq1["processing_institution"] = "Battelle"

        ntseq2 = nucleotide_sequencing_list[0].copy()
        ntseq2["id"] = "nmdc:ntseq-2"
        ntseq2["processing_institution"] = (
            "JGI"  # One JGI activity should exclude the biosample
        )

        ntseq3 = nucleotide_sequencing_list[0].copy()
        ntseq3["id"] = "nmdc:ntseq-3"
        ntseq3["processing_institution"] = "Other"

        # Put all sequencing activities in the same biosample
        biosample_nucleotide_sequencing = [
            {biosample1["id"]: [ntseq1, ntseq2, ntseq3]},
        ]

        # Setup data objects and library prep
        biosample_data_objects = [
            {biosample1["id"]: data_objects_list},
        ]

        biosample_library_preparation = [
            {biosample1["id"]: library_preparation_dict},
        ]

        all_instruments = {
            instrument["id"]: {
                "vendor": instrument["vendor"],
                "model": instrument["model"],
            }
            for instrument in mocked_instruments
        }

        # Call get_submission_xml with the biosample
        submission_xml = ncbi_submission_client.get_submission_xml(
            [biosample1],
            biosample_nucleotide_sequencing,
            biosample_data_objects,
            biosample_library_preparation,
            all_instruments,
        )

        # Biosample should be excluded because it has at least one JGI sequencing activity
        assert "nmdc:bsm-12-mixed" not in submission_xml

    def test_geo_loc_name_ascii_conversion(
        self,
        mocker: Callable[..., Generator[MockerFixture, None, None]],
        ncbi_submission_client: NCBISubmissionXML,
    ):
        mocker.patch(
            "nmdc_runtime.site.export.ncbi_xml.load_mappings",
            return_value=(
                {
                    "geo_loc_name": "geo_loc_name",
                    "id": "",
                    "name": "sample_name",
                    "env_broad_scale": "env_broad_scale",
                    "env_local_scale": "env_local_scale",
                    "env_medium": "env_medium",
                },
                {
                    "geo_loc_name": "TextValue",
                    "id": "uriorcurie",
                    "name": "string",
                    "env_broad_scale": "ControlledIdentifiedTermValue",
                    "env_local_scale": "ControlledIdentifiedTermValue",
                    "env_medium": "ControlledIdentifiedTermValue",
                },
            ),
        )

        # Create a biosample with non-ASCII characters in geo_loc_name
        test_biosample = [
            {
                "id": "nmdc:bsm-12-unicode-test",
                "name": "Test Biosample",
                "geo_loc_name": {
                    "has_raw_value": "USA: Alaska, Utqiaġvik",
                    "type": "nmdc:TextValue",
                },
                "env_broad_scale": {
                    "has_raw_value": "ENVO:00000446",
                    "type": "nmdc:ControlledTermValue",
                },
                "env_local_scale": {
                    "has_raw_value": "ENVO:00002030",
                    "type": "nmdc:ControlledTermValue",
                },
                "env_medium": {
                    "has_raw_value": "ENVO:00002007",
                    "type": "nmdc:ControlledTermValue",
                },
                "associated_studies": ["nmdc:sty-11-unicode-test"],
            }
        ]

        ncbi_submission_client.set_biosample(
            organism_name="Test Organism",
            org="Test Org",
            bioproject_id="PRJNA123456",
            nmdc_biosamples=test_biosample,
        )

        biosample_xml = ET.tostring(
            ncbi_submission_client.root.find(".//BioSample"), "unicode"
        )

        # Verify that non-ASCII characters are converted to closest ASCII equivalents using unidecode
        assert "USA: Alaska, Utqiagvik" in biosample_xml
        # Verify the original Unicode characters are not present
        assert "Utqiaġvik" not in biosample_xml


class TestNCBIXMLUtils:
    def test_handle_quantity_value(self):
        # Test numeric value with unit
        assert (
            handle_quantity_value({"has_numeric_value": 10, "has_unit": "mg"})
            == "10 mg"
        )
        # Test range value with unit
        assert (
            handle_quantity_value(
                {
                    "has_maximum_numeric_value": 15,
                    "has_minimum_numeric_value": 5,
                    "has_unit": "kg",
                }
            )
            == "5 - 15 kg"
        )
        # Test raw value
        assert handle_quantity_value({"has_raw_value": "20 units"}) == "20 units"
        # Test unknown format
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

    def test_load_mappings(
        self, mocker: Callable[..., Generator[MockerFixture, None, None]]
    ):
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

    def test_pooled_biosample_grouping(
        self,
        mocker: Callable[..., Generator[MockerFixture, None, None]],
        ncbi_submission_client: NCBISubmissionXML,
    ):
        mocker.patch(
            "nmdc_runtime.site.export.ncbi_xml.load_mappings",
            return_value=(
                {
                    "id": "",
                    "name": "sample_name",
                    "geo_loc_name": "geo_loc_name",
                    "collection_date": "collection_date",
                    "depth": "depth",
                    "elev": "elev",
                    "lat_lon": "lat_lon",
                    "env_broad_scale": "env_broad_scale",
                    "env_local_scale": "env_local_scale",
                    "env_medium": "env_medium",
                },
                {
                    "id": "uriorcurie",
                    "name": "string",
                    "geo_loc_name": "TextValue",
                    "collection_date": "TimestampValue",
                    "depth": "QuantityValue",
                    "elev": "float",
                    "lat_lon": "GeolocationValue",
                    "env_broad_scale": "ControlledIdentifiedTermValue",
                    "env_local_scale": "ControlledIdentifiedTermValue",
                    "env_medium": "ControlledIdentifiedTermValue",
                },
            ),
        )

        # Create test biosamples
        biosample1 = {
            "id": "nmdc:bsm-12-002hb858",
            "name": "Pooled Sample 1",
            "geo_loc_name": {
                "has_raw_value": "USA: Test Location",
                "type": "nmdc:TextValue",
            },
            "collection_date": {
                "has_raw_value": "2021-01-01",
                "type": "nmdc:TimestampValue",
            },
            "depth": {
                "has_numeric_value": 5,
                "has_unit": "m",
                "type": "nmdc:QuantityValue",
            },
            "elev": 100.5,
            "lat_lon": {
                "latitude": 40.0,
                "longitude": -120.0,
                "type": "nmdc:GeolocationValue",
            },
            "env_broad_scale": {
                "term": {"id": "ENVO:00000446", "name": "terrestrial biome"},
                "type": "nmdc:ControlledIdentifiedTermValue",
            },
            "env_local_scale": {
                "term": {"id": "ENVO:00002030", "name": "meadow"},
                "type": "nmdc:ControlledIdentifiedTermValue",
            },
            "env_medium": {
                "term": {"id": "ENVO:00002007", "name": "sediment"},
                "type": "nmdc:ControlledIdentifiedTermValue",
            },
        }

        biosample2 = {
            "id": "nmdc:bsm-12-938kxq31",
            "name": "Pooled Sample 2",
            "geo_loc_name": {
                "has_raw_value": "USA: Test Location",
                "type": "nmdc:TextValue",
            },
            "collection_date": {
                "has_raw_value": "2021-01-02",
                "type": "nmdc:TimestampValue",
            },
            "depth": {
                "has_numeric_value": 10,
                "has_unit": "m",
                "type": "nmdc:QuantityValue",
            },
            "elev": 100.5,
            "lat_lon": {
                "latitude": 40.0,
                "longitude": -120.0,
                "type": "nmdc:GeolocationValue",
            },
            "env_broad_scale": {
                "term": {"id": "ENVO:00000446", "name": "terrestrial biome"},
                "type": "nmdc:ControlledIdentifiedTermValue",
            },
            "env_local_scale": {
                "term": {"id": "ENVO:00002030", "name": "meadow"},
                "type": "nmdc:ControlledIdentifiedTermValue",
            },
            "env_medium": {
                "term": {"id": "ENVO:00002007", "name": "sediment"},
                "type": "nmdc:ControlledIdentifiedTermValue",
            },
        }

        biosample3 = {
            "id": "nmdc:bsm-12-s2ngn133",
            "name": "Individual Sample",
            "geo_loc_name": {
                "has_raw_value": "USA: Test Location 2",
                "type": "nmdc:TextValue",
            },
            "collection_date": {
                "has_raw_value": "2021-01-03",
                "type": "nmdc:TimestampValue",
            },
            "depth": {
                "has_numeric_value": 2,
                "has_unit": "m",
                "type": "nmdc:QuantityValue",
            },
            "elev": 200.0,
            "lat_lon": {
                "latitude": 41.0,
                "longitude": -121.0,
                "type": "nmdc:GeolocationValue",
            },
            "env_broad_scale": {
                "term": {"id": "ENVO:00000446", "name": "terrestrial biome"},
                "type": "nmdc:ControlledIdentifiedTermValue",
            },
            "env_local_scale": {
                "term": {"id": "ENVO:00002030", "name": "meadow"},
                "type": "nmdc:ControlledIdentifiedTermValue",
            },
            "env_medium": {
                "term": {"id": "ENVO:00002007", "name": "sediment"},
                "type": "nmdc:ControlledIdentifiedTermValue",
            },
        }

        pooled_biosamples_data = {
            "nmdc:bsm-12-002hb858": {
                "pooling_process_id": "nmdc:poolp-11-gznh3638",
                "processed_sample_id": "nmdc:procsm-11-dha8mw20",
                "processed_sample_name": "Aggregated Pool Sample",
                "pooled_biosample_ids": [
                    "nmdc:bsm-12-002hb858",
                    "nmdc:bsm-12-938kxq31",
                    "nmdc:bsm-12-s2ngn133",
                ],
                "aggregated_collection_date": "2021-01-01/2021-01-03",
                "aggregated_depth": "2 - 10 m",
            },
            "nmdc:bsm-12-938kxq31": {
                "pooling_process_id": "nmdc:poolp-11-gznh3638",
                "processed_sample_id": "nmdc:procsm-11-dha8mw20",
                "processed_sample_name": "Aggregated Pool Sample",
                "pooled_biosample_ids": [
                    "nmdc:bsm-12-002hb858",
                    "nmdc:bsm-12-938kxq31",
                    "nmdc:bsm-12-s2ngn133",
                ],
                "aggregated_collection_date": "2021-01-01/2021-01-03",
                "aggregated_depth": "2 - 10 m",
            },
            "nmdc:bsm-12-s2ngn133": {
                "pooling_process_id": "nmdc:poolp-11-gznh3638",
                "processed_sample_id": "nmdc:procsm-11-dha8mw20",
                "processed_sample_name": "Aggregated Pool Sample",
                "pooled_biosample_ids": [
                    "nmdc:bsm-12-002hb858",
                    "nmdc:bsm-12-938kxq31",
                    "nmdc:bsm-12-s2ngn133",
                ],
                "aggregated_collection_date": "2021-01-01/2021-01-03",
                "aggregated_depth": "2 - 10 m",
            },
        }

        ncbi_submission_client.set_biosample(
            organism_name="Test Organism",
            org="Test Org",
            bioproject_id="PRJNA123456",
            nmdc_biosamples=[biosample1, biosample2, biosample3],
            pooled_biosamples_data=pooled_biosamples_data,
        )

        # Should create 1 Action element: all three biosamples are pooled into a single pooling process
        action_elements = ncbi_submission_client.root.findall(".//Action")
        assert len(action_elements) == 1

        # Find the pooled sample action
        pooled_action = action_elements[0]
        pooled_xml = ET.tostring(pooled_action, "unicode")

        # Check pooled action content - should reference the processed sample ID from the pooling process
        assert "nmdc:procsm-11-dha8mw20" in pooled_xml
        assert "Aggregated Pool Sample" in pooled_xml
        assert "nmdc:poolp-11-gznh3638" in pooled_xml
        assert "2021-01-01/2021-01-03" in pooled_xml
        assert "2 - 10 m" in pooled_xml
        # Should contain all three biosample IDs in the pooled list
        assert "nmdc:bsm-12-002hb858" in pooled_xml
        assert "nmdc:bsm-12-938kxq31" in pooled_xml
        assert "nmdc:bsm-12-s2ngn133" in pooled_xml

    def test_elev_special_handling(
        self,
        mocker: Callable[..., Generator[MockerFixture, None, None]],
        ncbi_submission_client: NCBISubmissionXML,
        nmdc_biosample: list[dict[str, Any]],
    ):
        mocker.patch(
            "nmdc_runtime.site.export.ncbi_xml.load_mappings",
            return_value=(
                {"elev": "elev", "id": "", "name": "sample_name"},
                {"elev": "float", "id": "uriorcurie", "name": "string"},
            ),
        )

        ncbi_submission_client.set_biosample(
            organism_name="Test Organism",
            org="Test Org",
            bioproject_id="PRJNA123456",
            nmdc_biosamples=nmdc_biosample,
        )

        biosample_xml = ET.tostring(
            ncbi_submission_client.root.find(".//BioSample"), "unicode"
        )

        # Elevation should be converted to string with " m" suffix
        assert "1179.5 m" in biosample_xml

    def test_host_taxid_special_handling(
        self,
        mocker: Callable[..., Generator[MockerFixture, None, None]],
        ncbi_submission_client: NCBISubmissionXML,
        nmdc_biosample: list[dict[str, Any]],
    ):
        mocker.patch(
            "nmdc_runtime.site.export.ncbi_xml.load_mappings",
            return_value=(
                {"host_taxid": "host_taxid", "id": "", "name": "sample_name"},
                {
                    "host_taxid": "ControlledIdentifiedTermValue",
                    "id": "uriorcurie",
                    "name": "string",
                },
            ),
        )

        ncbi_submission_client.set_biosample(
            organism_name="Test Organism",
            org="Test Org",
            bioproject_id="PRJNA123456",
            nmdc_biosamples=nmdc_biosample,
        )

        biosample_xml = ET.tostring(
            ncbi_submission_client.root.find(".//BioSample"), "unicode"
        )

        # Should extract just the numeric part
        assert "9606" in biosample_xml

    def test_env_package_processing(
        self,
        mocker: Callable[..., Generator[MockerFixture, None, None]],
        ncbi_submission_client: NCBISubmissionXML,
        nmdc_biosample: list[dict[str, Any]],
    ):
        mocker.patch(
            "nmdc_runtime.site.export.ncbi_xml.load_mappings",
            return_value=(
                {"env_package": "env_package", "id": "", "name": "sample_name"},
                {"env_package": "TextValue", "id": "uriorcurie", "name": "string"},
            ),
        )

        ncbi_submission_client.set_biosample(
            organism_name="Test Organism",
            org="Test Org",
            bioproject_id="PRJNA123456",
            nmdc_biosamples=nmdc_biosample,
        )

        biosample_xml = ET.tostring(
            ncbi_submission_client.root.find(".//BioSample"), "unicode"
        )

        # Should be formatted as MIMS.me.{value}.6.0
        assert "MIMS.me.soil.6.0" in biosample_xml

    def test_fastq_file_filtering(
        self,
        mocker: Callable[..., Generator[MockerFixture, None, None]],
        ncbi_submission_client: NCBISubmissionXML,
        nmdc_biosample: list[dict[str, Any]],
        nucleotide_sequencing_list: list[dict[str, Any]],
        library_preparation_dict: dict[str, Any],
        mocked_instruments: list[dict[str, Any]],
    ):
        mocker.patch(
            "nmdc_runtime.site.export.ncbi_xml.load_mappings",
            return_value=(
                {
                    "analysis_type": "",
                    "biosample_categories": "",
                    "collection_date": "collection_date",
                    "depth": "depth",
                    "env_broad_scale": "env_broad_scale",
                    "env_local_scale": "env_local_scale",
                    "env_medium": "env_medium",
                    "geo_loc_name": "geo_loc_name",
                    "id": "",
                    "lat_lon": "lat_lon",
                    "name": "sample_name",
                    "type": "",
                },
                {
                    "analysis_type": "string",
                    "biosample_categories": "string",
                    "collection_date": "TimestampValue",
                    "depth": "QuantityValue",
                    "env_broad_scale": "ControlledTermValue",
                    "env_local_scale": "ControlledTermValue",
                    "env_medium": "ControlledTermValue",
                    "geo_loc_name": "TextValue",
                    "id": "uriorcurie",
                    "lat_lon": "GeolocationValue",
                    "name": "string",
                    "type": "string",
                },
            ),
        )
        # Create mixed data objects with and without acceptable extensions
        mixed_data_objects = [
            {
                "id": "nmdc:dobj-11-fastq1",
                "url": "https://example.com/test1.fastq.gz",
                "data_object_type": "Metagenome Raw Read 1",
            },
            {
                "id": "nmdc:dobj-11-fastq2",
                "url": "https://example.com/test2.fastq",
                "data_object_type": "Metagenome Raw Read 2",
            },
            {
                "id": "nmdc:dobj-11-other",
                "url": "https://example.com/test.bam",
                "data_object_type": "Alignment File",
            },
            {
                "id": "nmdc:dobj-11-txt",
                "url": "https://example.com/readme.txt",
                "data_object_type": "Text File",
            },
        ]

        all_instruments = {
            instrument["id"]: {
                "vendor": instrument["vendor"],
                "model": instrument["model"],
            }
            for instrument in mocked_instruments
        }

        biosample_data_objects = [
            {biosample["id"]: mixed_data_objects} for biosample in nmdc_biosample
        ]

        biosample_nucleotide_sequencing = [
            {biosample["id"]: nucleotide_sequencing_list}
            for biosample in nmdc_biosample
        ]

        biosample_library_preparation = [
            {biosample["id"]: library_preparation_dict} for biosample in nmdc_biosample
        ]

        # Call get_submission_xml which includes file filtering
        submission_xml = ncbi_submission_client.get_submission_xml(
            nmdc_biosample,
            biosample_nucleotide_sequencing,
            biosample_data_objects,
            biosample_library_preparation,
            all_instruments,
        )

        # Should include .fastq.gz and .fastq files
        assert "test1.fastq.gz" in submission_xml
        assert "test2.fastq" in submission_xml

        # Should exclude other file types
        assert "test.bam" not in submission_xml
        assert "readme.txt" not in submission_xml

    def test_pooled_sra_action_creation(
        self,
        ncbi_submission_client: NCBISubmissionXML,
        mocked_instruments: list[dict[str, Any]],
    ):
        # Create test data for pooled SRA action
        fastq_data_objects = [
            {
                "id": "nmdc:dobj-pooled-1",
                "url": "https://example.com/pooled_R1.fastq.gz",
                "data_object_type": "Metagenome Raw Read 1",
            },
            {
                "id": "nmdc:dobj-pooled-2",
                "url": "https://example.com/pooled_R2.fastq.gz",
                "data_object_type": "Metagenome Raw Read 2",
            },
        ]

        nucleotide_sequencing = [
            {
                "id": "nmdc:ntseq-pooled-1",
                "analyte_category": "metagenome",
                "instrument_used": ["nmdc:inst-14-xz5tb342"],
            }
        ]

        library_preparation = {"protocol_link": {"name": "Test Library Prep Protocol"}}

        all_instruments = {
            instrument["id"]: {
                "vendor": instrument["vendor"],
                "model": instrument["model"],
            }
            for instrument in mocked_instruments
        }

        pooled_biosamples_data = {
            "nmdc:bsm-12-002hb858": {
                "pooling_process_id": "nmdc:poolp-11-gznh3638",
                "processed_sample_id": "nmdc:procsm-11-dha8mw20",
                "processed_sample_name": "Pooled SRA Sample",
            },
            "nmdc:bsm-12-938kxq31": {
                "pooling_process_id": "nmdc:poolp-11-gznh3638",
                "processed_sample_id": "nmdc:procsm-11-dha8mw20",
                "processed_sample_name": "Pooled SRA Sample",
            },
        }

        biosample_data_objects = [
            {
                "nmdc:bsm-12-002hb858": fastq_data_objects,
                "nmdc:bsm-12-938kxq31": fastq_data_objects,
            }
        ]

        biosample_nucleotide_sequencing = [
            {
                "nmdc:bsm-12-002hb858": nucleotide_sequencing,
                "nmdc:bsm-12-938kxq31": nucleotide_sequencing,
            }
        ]

        biosample_library_preparation = [
            {
                "nmdc:bsm-12-002hb858": library_preparation,
                "nmdc:bsm-12-938kxq31": library_preparation,
            }
        ]

        ncbi_submission_client.set_fastq(
            biosample_data_objects=biosample_data_objects,
            bioproject_id="PRJNA123456",
            org="Test Org",
            nmdc_nucleotide_sequencing=biosample_nucleotide_sequencing,
            nmdc_biosamples=[],
            nmdc_library_preparation=biosample_library_preparation,
            all_instruments=all_instruments,
            pooled_biosamples_data=pooled_biosamples_data,
        )

        # Should create 1 SRA action for the pooled samples
        action_elements = ncbi_submission_client.root.findall(".//Action")
        assert len(action_elements) == 1

        action_xml = ET.tostring(action_elements[0], "unicode")

        # Should reference the processed sample, not individual biosamples
        assert "nmdc:procsm-11-dha8mw20" in action_xml
        assert "Pooled SRA Sample" in action_xml
        assert "pooled_R1.fastq.gz" in action_xml
        assert "pooled_R2.fastq.gz" in action_xml
        assert "paired" in action_xml  # Should detect paired reads from _R1/_R2 pattern
        assert "ILLUMINA" in action_xml
        assert "NextSeq 550" in action_xml
        assert "Test Library Prep Protocol" in action_xml

    def test_library_layout_detection_pooled(
        self,
        ncbi_submission_client: NCBISubmissionXML,
        mocked_instruments: list[dict[str, Any]],
    ):
        # Test single read detection for pooled samples
        single_read_data = [
            {
                "id": "nmdc:dobj-single",
                "url": "https://example.com/single_read.fastq.gz",
                "data_object_type": "Metagenome Raw Reads",
            }
        ]

        nucleotide_sequencing = [
            {
                "id": "nmdc:ntseq-single",
                "analyte_category": "metagenome",
                "instrument_used": ["nmdc:inst-14-xz5tb342"],
            }
        ]

        all_instruments = {
            instrument["id"]: {
                "vendor": instrument["vendor"],
                "model": instrument["model"],
            }
            for instrument in mocked_instruments
        }

        pooled_biosamples_data = {
            "nmdc:bsm-12-bcfa4694": {
                "pooling_process_id": "nmdc:poolp-11-gznh3638",
                "processed_sample_id": "nmdc:procsm-11-dha8mw20",
                "processed_sample_name": "Single Read Sample",
            }
        }

        ncbi_submission_client.set_fastq(
            biosample_data_objects=[{"nmdc:bsm-12-bcfa4694": single_read_data}],
            bioproject_id="PRJNA123456",
            org="Test Org",
            nmdc_nucleotide_sequencing=[
                {"nmdc:bsm-12-bcfa4694": nucleotide_sequencing}
            ],
            nmdc_biosamples=[],
            nmdc_library_preparation=[{"nmdc:bsm-12-bcfa4694": {}}],
            all_instruments=all_instruments,
            pooled_biosamples_data=pooled_biosamples_data,
        )

        action_xml = ET.tostring(
            ncbi_submission_client.root.find(".//Action"), "unicode"
        )

        # Should detect single read layout since no _R1/_R2 pattern
        assert "single" in action_xml
        assert "paired" not in action_xml

    def test_external_links_for_pooled_samples(
        self,
        mocker: Callable[..., Generator[MockerFixture, None, None]],
        ncbi_submission_client: NCBISubmissionXML,
    ):
        mocker.patch(
            "nmdc_runtime.site.export.ncbi_xml.load_mappings",
            return_value=(
                {"id": "", "name": "sample_name"},
                {"id": "uriorcurie", "name": "string"},
            ),
        )

        test_biosample = {
            "id": "nmdc:bsm-12-s2ngn133",
            "name": "External Link Test Sample",
        }

        pooled_biosamples_data = {
            "nmdc:bsm-12-s2ngn133": {
                "processed_sample_id": "nmdc:procsm-11-dha8mw20",
                "pooling_process_id": "nmdc:poolp-11-gznh3638",
            }
        }

        ncbi_submission_client.set_biosample(
            organism_name="Test Organism",
            org="Test Org",
            bioproject_id="PRJNA123456",
            nmdc_biosamples=[test_biosample],
            pooled_biosamples_data=pooled_biosamples_data,
        )

        biosample_xml = ET.tostring(
            ncbi_submission_client.root.find(".//BioSample"), "unicode"
        )

        # Should contain external links to processed sample and pooling process
        assert "https://bioregistry.io/nmdc:procsm-11-dha8mw20" in biosample_xml
        assert "https://bioregistry.io/nmdc:poolp-11-gznh3638" in biosample_xml
