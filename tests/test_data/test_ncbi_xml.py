from typing import Any, Callable, Generator
from unittest.mock import MagicMock
import pytest
import xml.etree.ElementTree as ET

from pytest_mock import MockerFixture

from nmdc_runtime.site.export.ncbi_xml import (
    NCBISubmissionXML,
    get_excluded_biosample_slots,
)
from nmdc_runtime.site.export.ncbi_xml_utils import (
    aggregate_carb_nitro_ratio,
    aggregate_range,
    check_pooling_for_biosamples,
    fetch_nucleotide_sequencing_ids_downstream,
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
            "applies_to_agent": {
                "name": "Hugh Cross",
                "email": "crossh@battelleecology.org",
                "orcid": "orcid:0000-0002-6745-9479",
            },
            "applied_roles": ["Methodology", "Data curation"],
        },
        {
            "applies_to_agent": {
                "name": "Samantha Weintraub-Leff",
                "email": "sweintraub@battelleecology.org",
                "orcid": "orcid:0000-0003-4789-5086",
            },
            "applied_roles": ["Methodology", "Data curation"],
        },
        {
            "applies_to_agent": {
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

    def test_get_excluded_biosample_slots(self):
        excluded = get_excluded_biosample_slots()

        # explicitly excluded slots
        assert "biosample_categories" in excluded
        assert "type" in excluded

        # all descendants of the `external_database_identifiers` grouping slot
        for slot in (
            "emsl_biosample_identifiers",
            "gold_biosample_identifiers",
            "igsn_biosample_identifiers",
            "img_identifiers",
            "insdc_biosample_identifiers",
            "neon_biosample_identifiers",
        ):
            assert slot in excluded

        # slots that must still be exported as attributes
        for slot in ("collection_date", "env_medium", "geo_loc_name", "name"):
            assert slot not in excluded

    @pytest.mark.parametrize("pooled", [False, True])
    def test_set_biosample_omits_excluded_slots(
        self,
        ncbi_submission_client: NCBISubmissionXML,
        mocker: Callable[..., Generator[MockerFixture, None, None]],
        pooled: bool,
    ):
        biosample = {
            "id": "nmdc:bsm-11-excl01",
            "type": "nmdc:Biosample",
            "name": "Sample with external identifiers",
            "biosample_categories": ["NEON"],
            "gold_biosample_identifiers": ["gold:Gb0123456"],
            "img_identifiers": ["img.taxon:3300012345"],
            "emsl_biosample_identifiers": ["emsl:12345"],
            "neon_biosample_identifiers": ["neon:ABBY_004-M-20170605-COMP"],
            "elev": 100.0,
            "env_package": {"has_raw_value": "soil", "type": "nmdc:TextValue"},
        }
        # Mapping file lists the identifier slots, so exclusion must come from code.
        slots = [
            "id",
            "type",
            "name",
            "biosample_categories",
            "gold_biosample_identifiers",
            "img_identifiers",
            "emsl_biosample_identifiers",
            "neon_biosample_identifiers",
            "elev",
            "env_package",
        ]
        mocker.patch(
            "nmdc_runtime.site.export.ncbi_xml.load_mappings",
            return_value=(
                {slot: slot for slot in slots},
                {slot: "string" for slot in slots} | {"elev": "float"},
            ),
        )

        pooled_biosamples_data = None
        if pooled:
            pooled_biosamples_data = {
                biosample["id"]: {
                    "pooling_process_id": "nmdc:poolp-11-excl01",
                    "processed_sample_id": "nmdc:procsm-11-excl01",
                    "pooled_biosample_ids": [biosample["id"]],
                }
            }

        ncbi_submission_client.set_biosample(
            organism_name="E. coli",
            org="Test Org",
            bioproject_id="PRJNA1029061",
            nmdc_biosamples=[biosample],
            pooled_biosamples_data=pooled_biosamples_data,
        )

        attribute_names = {
            attr.get("attribute_name")
            for attr in ncbi_submission_client.root.iter("Attribute")
        }
        assert attribute_names  # sanity: some attributes were emitted
        assert "elev" in attribute_names
        for slot in (
            "type",
            "biosample_categories",
            "gold_biosample_identifiers",
            "img_identifiers",
            "emsl_biosample_identifiers",
            "neon_biosample_identifiers",
        ):
            assert slot not in attribute_names

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
        root = ET.fromstring(submission_xml)
        contact_email = root.find("Description/Organization/Contact").attrib["email"]
        contact_first = root.find("Description/Organization/Contact/Name/First").text
        contact_last = root.find("Description/Organization/Contact/Name/Last").text
        assert contact_email == "kthibault@battelleecology.org"
        assert contact_first == "Kate"
        assert contact_last == "Thibault"

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

    def test_get_submission_xml_keeps_mixed_jgi_biosample_but_drops_jgi_run(
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
        ntseq2["processing_institution"] = "JGI"  # JGI deposits this run itself
        ntseq2["has_output"] = ["nmdc:dobj-11-jgi00001"]
        jgi_data_object = {
            "id": "nmdc:dobj-11-jgi00001",
            "type": "nmdc:DataObject",
            "name": "52834.4.466476.fastq.gz",
            "data_object_type": "Metagenome Raw Reads",
            "url": "https://data.microbiomedata.org/data/x/52834.4.466476.fastq.gz",
        }

        ntseq3 = nucleotide_sequencing_list[0].copy()
        ntseq3["id"] = "nmdc:ntseq-3"
        ntseq3["processing_institution"] = "Other"

        # Put all sequencing activities in the same biosample
        biosample_nucleotide_sequencing = [
            {biosample1["id"]: [ntseq1, ntseq2, ntseq3]},
        ]

        # Setup data objects and library prep
        biosample_data_objects = [
            {biosample1["id"]: data_objects_list + [jgi_data_object]},
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

        # The biosample was also sequenced by another lab, so it stays in the
        # submission; only JGI's run and its file are left out.
        assert "nmdc:bsm-12-mixed" in submission_xml
        assert "nmdc:ntseq-2" not in submission_xml
        assert "52834.4.466476.fastq.gz" not in submission_xml
        assert "BMI_HVKNKBGX5_Tube347_srt_R1.fastq.gz" in submission_xml

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
                "aggregated_values": {
                    "collection_date": "2021-01-01/2021-01-03",
                    "depth": "2 - 10 m",
                },
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
                "aggregated_values": {
                    "collection_date": "2021-01-01/2021-01-03",
                    "depth": "2 - 10 m",
                },
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
                "aggregated_values": {
                    "collection_date": "2021-01-01/2021-01-03",
                    "depth": "2 - 10 m",
                },
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


def _qv(value, unit):
    return {"has_numeric_value": value, "has_unit": unit, "type": "nmdc:QuantityValue"}


class TestPooledValueAggregation:
    def test_aggregate_range_quantity_values(self):
        assert aggregate_range([_qv(0, "m"), _qv(10, "m")]) == "0-10 m"

    def test_aggregate_range_expands_min_max_values(self):
        depth = {
            "has_minimum_numeric_value": 0,
            "has_maximum_numeric_value": 0.1,
            "has_unit": "m",
        }
        assert aggregate_range([depth, _qv(0.3, "m")]) == "0-0.3 m"

    def test_aggregate_range_strings_with_unit_text(self):
        values = [["0.75 g water/g dry soil"], ["0.6 g water/g dry soil"]]
        assert aggregate_range(values) == "0.6-0.75 g water/g dry soil"

    def test_aggregate_range_rejects_unparseable_value(self):
        assert aggregate_range([_qv(1, "m"), {"has_raw_value": "unknown"}]) is None

    def test_aggregate_carb_nitro_ratio_component_unit_mismatch_falls_back(self):
        biosamples = [
            {
                "org_carb": _qv(10, "g/kg"),
                "nitro": _qv(1000, "mg/kg"),
                "carb_nitro_ratio": _qv(10, ""),
            },
            {
                "org_carb": _qv(20, "g/kg"),
                "nitro": _qv(3000, "mg/kg"),
                "carb_nitro_ratio": _qv(6, ""),
            },
        ]
        assert aggregate_carb_nitro_ratio(biosamples) == "8"

    def test_check_pooling_for_biosamples_populates_aggregated_values(self):
        biosamples = [
            {"id": "nmdc:bsm-11-a", "ph": 6.0, "depth": _qv(0, "m")},
            {"id": "nmdc:bsm-11-b", "ph": 7.0, "depth": _qv(0.1, "m")},
            {"id": "nmdc:bsm-11-c", "ph": 5.0},
        ]
        material_processing_set = MagicMock()
        material_processing_set.find.return_value = [
            {
                "id": "nmdc:poolp-11-x",
                "type": "nmdc:Pooling",
                "has_input": ["nmdc:bsm-11-a", "nmdc:bsm-11-b"],
                "has_output": ["nmdc:procsm-11-x"],
            }
        ]

        result = check_pooling_for_biosamples(material_processing_set, biosamples)

        assert result["nmdc:bsm-11-c"] == {}
        pooling_info = result["nmdc:bsm-11-a"]
        assert pooling_info == result["nmdc:bsm-11-b"]
        assert pooling_info["processed_sample_id"] == "nmdc:procsm-11-x"
        assert pooling_info["aggregated_values"] == {"ph": "6.26", "depth": "0-0.1 m"}

    def test_pooled_biosample_xml_includes_aggregated_values(
        self,
        ncbi_submission_client: NCBISubmissionXML,
        mocker: Callable[..., Generator[MockerFixture, None, None]],
    ):
        slots = ["id", "name", "collection_date", "depth", "ph", "temp", "elev"]
        mocker.patch(
            "nmdc_runtime.site.export.ncbi_xml.load_mappings",
            return_value=(
                {slot: slot for slot in slots},
                {
                    "ph": "float",
                    "elev": "float",
                    "depth": "QuantityValue",
                    "temp": "QuantityValue",
                },
            ),
        )
        pooling_info = {
            "pooling_process_id": "nmdc:poolp-11-agg01",
            "processed_sample_id": "nmdc:procsm-11-agg01",
            "pooled_biosample_ids": ["nmdc:bsm-11-agg01", "nmdc:bsm-11-agg02"],
            "aggregated_values": {
                "collection_date": "2017-06-05/2017-06-06",
                "depth": "0-0.1 m",
                "ph": "6.26",
                "temp": "15-17 Cel",
            },
        }
        biosamples = [
            {
                "id": "nmdc:bsm-11-agg01",
                "name": "core 1",
                "ph": 6.0,
                "temp": _qv(15, "Cel"),
                "elev": 100.0,
            },
            {
                "id": "nmdc:bsm-11-agg02",
                "name": "core 2",
                "ph": 7.0,
                "temp": _qv(17, "Cel"),
                "elev": 100.0,
            },
        ]

        ncbi_submission_client.set_biosample(
            organism_name="E. coli",
            org="Test Org",
            bioproject_id="PRJNA1029061",
            nmdc_biosamples=biosamples,
            pooled_biosamples_data={bs["id"]: pooling_info for bs in biosamples},
        )

        attributes = {
            attr.get("attribute_name"): attr.text
            for attr in ncbi_submission_client.root.iter("Attribute")
        }
        assert attributes["ph"] == "6.26"
        assert attributes["temp"] == "15-17 Cel"
        assert attributes["depth"] == "0-0.1 m"
        assert attributes["collection_date"] == "2017-06-05/2017-06-06"
        # value taken from the first constituent must not leak through
        assert attributes["samp_pooling"] == "nmdc:bsm-11-agg01;nmdc:bsm-11-agg02"
        assert "name" not in attributes


def _alldocs_mock(docs):
    """Minimal alldocs stand-in: `find({"has_input": x})` returns docs whose has_input has x."""
    alldocs = MagicMock()
    alldocs.find.side_effect = lambda query: [
        d for d in docs if query["has_input"] in d.get("has_input", [])
    ]
    return alldocs


class TestPooledBiosampleWithIndividualSequencing:
    """A biosample can be sequenced on its own *and* as part of a pool (e.g. NEON
    soil cores with both a GEN and a COMP run). The individual run must get its
    own BioSample and SRA action rather than being folded into the pool."""

    POOL = "nmdc:poolp-11-dual01"
    POOL_PROCSM = "nmdc:procsm-11-pool01"
    BSM_A = "nmdc:bsm-11-dual0a"  # sequenced individually and in the pool
    BSM_B = "nmdc:bsm-11-dual0b"  # pooled only
    COMP_NTSEQ = "nmdc:dgns-11-comp01"
    GEN_NTSEQ = "nmdc:dgns-11-gen001"

    def _biosample(self, bsm_id, name):
        return {
            "id": bsm_id,
            "type": "nmdc:Biosample",
            "name": name,
            "env_package": {"has_raw_value": "soil", "type": "nmdc:TextValue"},
            "geo_loc_name": {"has_raw_value": "USA: Florida", "type": "nmdc:TextValue"},
        }

    def _dobj(self, dobj_id, filename, read):
        return {
            "id": dobj_id,
            "type": "nmdc:DataObject",
            "name": filename,
            "data_object_type": f"Metagenome Raw Read {read}",
            "url": f"https://storage.neonscience.org/x/{filename}",
        }

    def _ntseq(self, ntseq_id, name, has_input, has_output):
        return {
            "id": ntseq_id,
            "type": "nmdc:NucleotideSequencing",
            "name": name,
            "has_input": [has_input],
            "has_output": has_output,
            "processing_institution": "ANL",
            "analyte_category": "metagenome",
            "instrument_used": ["nmdc:inst-14-xz5tb342"],
        }

    def _inputs(self, with_pooled_ntseq_ids):
        comp_dobjs = [
            self._dobj("nmdc:dobj-11-comp0r1", "OSBS_006-comp_R1.fastq.gz", 1),
            self._dobj("nmdc:dobj-11-comp0r2", "OSBS_006-comp_R2.fastq.gz", 2),
        ]
        gen_dobjs = [
            self._dobj("nmdc:dobj-11-gen00r1", "OSBS_006-5-30-gen_R1.fastq.gz", 1),
            self._dobj("nmdc:dobj-11-gen00r2", "OSBS_006-5-30-gen_R2.fastq.gz", 2),
        ]
        comp_ntseq = self._ntseq(
            self.COMP_NTSEQ,
            "OSBS_006-M-20130726-COMP-DNA1",
            "nmdc:procsm-11-complib",
            [d["id"] for d in comp_dobjs],
        )
        gen_ntseq = self._ntseq(
            self.GEN_NTSEQ,
            "OSBS_006-M-5-30-20130726-GEN-DNA1",
            "nmdc:procsm-11-genlib0",
            [d["id"] for d in gen_dobjs],
        )
        biosamples = [
            self._biosample(self.BSM_A, "OSBS_006-M-5-30-20130726"),
            self._biosample(self.BSM_B, "OSBS_006-M-8-12-20130726"),
        ]
        # The downstream traversal reaches both runs from biosample A.
        biosample_data_objects = [
            {self.BSM_A: comp_dobjs + gen_dobjs},
            {self.BSM_B: comp_dobjs},
        ]
        biosample_ntseq = [
            {self.BSM_A: [comp_ntseq, gen_ntseq]},
            {self.BSM_B: [comp_ntseq]},
        ]
        pooling_info = {
            "pooling_process_id": self.POOL,
            "processed_sample_id": self.POOL_PROCSM,
            "processed_sample_name": "OSBS_006-M-20130726-COMP",
            "pooled_biosample_ids": [self.BSM_A, self.BSM_B],
            "aggregated_values": {},
        }
        if with_pooled_ntseq_ids:
            pooling_info["nucleotide_sequencing_ids"] = [self.COMP_NTSEQ]
        pooled_biosamples_data = {self.BSM_A: pooling_info, self.BSM_B: pooling_info}
        return (
            biosamples,
            biosample_ntseq,
            biosample_data_objects,
            pooled_biosamples_data,
        )

    @pytest.fixture(autouse=True)
    def _mappings(self, mocker):
        mocker.patch(
            "nmdc_runtime.site.export.ncbi_xml.load_mappings",
            return_value=(
                {"id": "", "name": "sample_name", "geo_loc_name": "geo_loc_name"},
                {"id": "uriorcurie", "name": "string", "geo_loc_name": "TextValue"},
            ),
        )

    @staticmethod
    def _sra_actions(root):
        out = {}
        for add_files in root.findall(".//Action/AddFiles"):
            identifier = add_files.find("Identifier/SPUID").text
            out[identifier] = {
                "files": sorted(f.get("file_path") for f in add_files.findall("File")),
                "biosample_refs": [
                    ref.find("RefId/SPUID").text
                    for ref in add_files.findall("AttributeRefId")
                    if ref.get("name") == "BioSample"
                ],
                "library_name": next(
                    a.text
                    for a in add_files.findall("Attribute")
                    if a.get("name") == "library_name"
                ),
            }
        return out

    @staticmethod
    def _biosample_spuids(root):
        return sorted(
            e.text
            for e in root.findall(
                ".//Action/AddData[@target_db='BioSample']/Identifier/SPUID"
            )
        )

    def test_individual_run_gets_its_own_biosample_and_sra_action(
        self, ncbi_submission_client: NCBISubmissionXML, mocked_instruments
    ):
        all_instruments = {
            i["id"]: {"vendor": i["vendor"], "model": i["model"]}
            for i in mocked_instruments
        }
        biosamples, ntseq, dobjs, pooled = self._inputs(with_pooled_ntseq_ids=True)

        xml_str = ncbi_submission_client.get_submission_xml(
            biosamples, ntseq, dobjs, [], all_instruments, pooled_biosamples_data=pooled
        )
        root = ET.fromstring(xml_str)

        # BioSamples: the pool, plus biosample A on its own. B is pooled only.
        assert self._biosample_spuids(root) == sorted([self.POOL_PROCSM, self.BSM_A])

        sra = self._sra_actions(root)
        assert set(sra) == {self.COMP_NTSEQ, self.GEN_NTSEQ}

        pooled_action = sra[self.COMP_NTSEQ]
        assert pooled_action["files"] == [
            "OSBS_006-comp_R1.fastq.gz",
            "OSBS_006-comp_R2.fastq.gz",
        ]
        assert pooled_action["biosample_refs"] == [self.POOL_PROCSM]
        assert pooled_action["library_name"] == "OSBS_006-M-20130726-COMP"

        individual_action = sra[self.GEN_NTSEQ]
        assert individual_action["files"] == [
            "OSBS_006-5-30-gen_R1.fastq.gz",
            "OSBS_006-5-30-gen_R2.fastq.gz",
        ]
        assert individual_action["biosample_refs"] == [self.BSM_A]
        assert individual_action["library_name"] == "OSBS_006-M-5-30-20130726"

    def test_without_pooled_run_ids_everything_is_folded_into_the_pool(
        self, ncbi_submission_client: NCBISubmissionXML, mocked_instruments
    ):
        """Previous behavior is preserved when pooling info carries no
        `nucleotide_sequencing_ids` (older callers / pooling data)."""
        all_instruments = {
            i["id"]: {"vendor": i["vendor"], "model": i["model"]}
            for i in mocked_instruments
        }
        biosamples, ntseq, dobjs, pooled = self._inputs(with_pooled_ntseq_ids=False)

        xml_str = ncbi_submission_client.get_submission_xml(
            biosamples, ntseq, dobjs, [], all_instruments, pooled_biosamples_data=pooled
        )
        root = ET.fromstring(xml_str)

        assert self._biosample_spuids(root) == [self.POOL_PROCSM]
        sra = self._sra_actions(root)
        assert len(sra) == 1
        (action,) = sra.values()
        assert len(action["files"]) == 4
        assert action["biosample_refs"] == [self.POOL_PROCSM]

    def test_check_pooling_for_biosamples_collects_pooled_run_ids(self):
        biosamples = [{"id": self.BSM_A}, {"id": self.BSM_B}]
        material_processing_set = MagicMock()
        material_processing_set.find.return_value = [
            {
                "id": self.POOL,
                "type": "nmdc:Pooling",
                "has_input": [self.BSM_A, self.BSM_B],
                "has_output": [self.POOL_PROCSM],
            }
        ]
        alldocs = _alldocs_mock(
            [
                # pooled path: procsm -> Extraction -> LibraryPrep -> COMP run
                {
                    "id": "nmdc:extrp-11-p",
                    "has_input": [self.POOL_PROCSM],
                    "has_output": ["nmdc:procsm-11-pext"],
                },
                {
                    "id": "nmdc:libprp-11-p",
                    "has_input": ["nmdc:procsm-11-pext"],
                    "has_output": ["nmdc:procsm-11-complib"],
                },
                {
                    "id": self.COMP_NTSEQ,
                    "has_input": ["nmdc:procsm-11-complib"],
                    "has_output": ["nmdc:dobj-11-comp0r1"],
                },
                # individual path from biosample A, must NOT be collected
                {
                    "id": "nmdc:extrp-11-a",
                    "has_input": [self.BSM_A],
                    "has_output": ["nmdc:procsm-11-aext"],
                },
                {
                    "id": self.GEN_NTSEQ,
                    "has_input": ["nmdc:procsm-11-aext"],
                    "has_output": ["nmdc:dobj-11-gen00r1"],
                },
                # the Pooling process itself
                {
                    "id": self.POOL,
                    "has_input": [self.BSM_A, self.BSM_B],
                    "has_output": [self.POOL_PROCSM],
                },
            ]
        )

        result = check_pooling_for_biosamples(
            material_processing_set, biosamples, alldocs
        )

        assert result[self.BSM_A]["nucleotide_sequencing_ids"] == [self.COMP_NTSEQ]
        assert result[self.BSM_B]["nucleotide_sequencing_ids"] == [self.COMP_NTSEQ]
        assert fetch_nucleotide_sequencing_ids_downstream(alldocs, self.BSM_A) == [
            self.GEN_NTSEQ,
            self.COMP_NTSEQ,
        ]

    def test_check_pooling_for_biosamples_without_alldocs_is_unchanged(self):
        material_processing_set = MagicMock()
        material_processing_set.find.return_value = [
            {
                "id": self.POOL,
                "type": "nmdc:Pooling",
                "has_input": [self.BSM_A],
                "has_output": [self.POOL_PROCSM],
            }
        ]
        result = check_pooling_for_biosamples(
            material_processing_set, [{"id": self.BSM_A}]
        )
        assert "nucleotide_sequencing_ids" not in result[self.BSM_A]


class TestBiosampleInMultiplePoolingProcesses:
    """A core can sit in two Pooling records (e.g. a duplicate import) where only one
    record's ProcessedSample was carried through to sequencing. The lookup must
    report the record that leads to runs, whichever order Mongo returns them in."""

    BSM = "nmdc:bsm-11-twopool1"
    LIVE_POOL = "nmdc:poolp-11-live0001"
    LIVE_PROCSM = "nmdc:procsm-11-live0001"
    DEAD_POOL = "nmdc:poolp-11-dead0001"
    DEAD_PROCSM = "nmdc:procsm-11-dead0001"
    COMP_NTSEQ = "nmdc:dgns-11-comp0002"

    def _pool(self, pool_id, procsm_id):
        return {
            "id": pool_id,
            "type": "nmdc:Pooling",
            "has_input": [self.BSM],
            "has_output": [procsm_id],
        }

    def _alldocs(self):
        # only the live pool's ProcessedSample leads to a sequencing run
        return _alldocs_mock(
            [
                {
                    "id": "nmdc:extrp-11-live001",
                    "has_input": [self.LIVE_PROCSM],
                    "has_output": ["nmdc:procsm-11-livelib"],
                },
                {
                    "id": self.COMP_NTSEQ,
                    "has_input": ["nmdc:procsm-11-livelib"],
                    "has_output": ["nmdc:dobj-11-comp0002"],
                },
            ]
        )

    @pytest.mark.parametrize("dead_pool_first", [True, False])
    def test_pool_with_downstream_runs_wins_regardless_of_order(self, dead_pool_first):
        live, dead = (
            self._pool(self.LIVE_POOL, self.LIVE_PROCSM),
            self._pool(self.DEAD_POOL, self.DEAD_PROCSM),
        )
        material_processing_set = MagicMock()
        material_processing_set.find.return_value = (
            [dead, live] if dead_pool_first else [live, dead]
        )

        result = check_pooling_for_biosamples(
            material_processing_set, [{"id": self.BSM}], self._alldocs()
        )

        info = result[self.BSM]
        assert info["pooling_process_id"] == self.LIVE_POOL
        assert info["processed_sample_id"] == self.LIVE_PROCSM
        assert info["nucleotide_sequencing_ids"] == [self.COMP_NTSEQ]

    def test_without_alldocs_last_pool_still_wins(self):
        """Runs are unknown without alldocs, so the previous last-wins rule holds."""
        material_processing_set = MagicMock()
        material_processing_set.find.return_value = [
            self._pool(self.LIVE_POOL, self.LIVE_PROCSM),
            self._pool(self.DEAD_POOL, self.DEAD_PROCSM),
        ]

        result = check_pooling_for_biosamples(
            material_processing_set, [{"id": self.BSM}]
        )

        assert result[self.BSM]["pooling_process_id"] == self.DEAD_POOL


class TestExistingInsdcAccessionReferences:
    """Samples already registered with INSDC (e.g. NEON's ENA ``SAMEA`` samples) must
    be referenced from SRA actions by accession, not by a SPUID that was never
    submitted under NMDC's namespace."""

    POOL = "nmdc:poolp-11-acc00001"
    POOL_PROCSM = "nmdc:procsm-11-acc00001"
    BSM_A = (
        "nmdc:bsm-11-acc0000a"  # own accession + pool accession, sequenced both ways
    )
    BSM_B = "nmdc:bsm-11-acc0000b"  # pool accession only
    BSM_C = "nmdc:bsm-11-acc0000c"  # pool accession only, but also sequenced on its own
    BSM_SOLO = "nmdc:bsm-11-acc0solo"  # not pooled, registered
    BSM_NEW = "nmdc:bsm-11-acc00new"  # not pooled, not registered
    POOL_ACC = "SAMEA104200253"
    A_ACC = "SAMEA104200272"
    SOLO_ACC = "SAMEA104200999"
    COMP_NTSEQ = "nmdc:dgns-11-acccomp1"
    GEN_A_NTSEQ = "nmdc:dgns-11-accgena1"
    GEN_C_NTSEQ = "nmdc:dgns-11-accgenc1"
    SOLO_NTSEQ = "nmdc:dgns-11-accsolo1"
    NEW_NTSEQ = "nmdc:dgns-11-accnew01"

    def _bsm(self, bsm_id, name, accessions):
        b = {
            "id": bsm_id,
            "type": "nmdc:Biosample",
            "name": name,
            "env_package": {"has_raw_value": "soil", "type": "nmdc:TextValue"},
        }
        if accessions:
            b["insdc_biosample_identifiers"] = [f"biosample:{a}" for a in accessions]
        return b

    def _dobj(self, dobj_id, filename, read):
        return {
            "id": dobj_id,
            "type": "nmdc:DataObject",
            "name": filename,
            "data_object_type": f"Metagenome Raw Read {read}",
            "url": f"https://storage.neonscience.org/x/{filename}",
        }

    def _ntseq(self, ntseq_id, name, has_output):
        return {
            "id": ntseq_id,
            "type": "nmdc:NucleotideSequencing",
            "name": name,
            "has_input": ["nmdc:procsm-11-x"],
            "has_output": has_output,
            "processing_institution": "ANL",
            "analyte_category": "metagenome",
            "instrument_used": ["nmdc:inst-14-xz5tb342"],
        }

    def _pooling_info(self):
        return {
            "pooling_process_id": self.POOL,
            "processed_sample_id": self.POOL_PROCSM,
            "processed_sample_name": "CPER_002-M-20130701-COMP",
            "pooled_biosample_ids": [self.BSM_A, self.BSM_B, self.BSM_C],
            "aggregated_values": {},
            "nucleotide_sequencing_ids": [self.COMP_NTSEQ],
        }

    @pytest.fixture(autouse=True)
    def _mappings(self, mocker):
        mocker.patch(
            "nmdc_runtime.site.export.ncbi_xml.load_mappings",
            return_value=(
                {"id": "", "name": "sample_name"},
                {"id": "uriorcurie", "name": "string"},
            ),
        )

    def test_resolve_existing_biosample_accessions(self):
        biosamples = [
            self._bsm(self.BSM_A, "A", [self.A_ACC, self.POOL_ACC]),
            self._bsm(self.BSM_B, "B", [self.POOL_ACC]),
            self._bsm(self.BSM_C, "C", [self.POOL_ACC]),
            self._bsm(self.BSM_SOLO, "solo", [self.SOLO_ACC]),
            self._bsm(self.BSM_NEW, "new", []),
        ]
        pooling = {
            b: self._pooling_info() for b in (self.BSM_A, self.BSM_B, self.BSM_C)
        }

        by_biosample, by_pool = (
            NCBISubmissionXML._resolve_existing_biosample_accessions(
                biosamples, pooling
            )
        )

        # the accession every constituent shares belongs to the composite
        assert by_pool == {self.POOL_PROCSM: self.POOL_ACC}
        # only accessions that are the biosample's own remain
        assert by_biosample == {self.BSM_A: self.A_ACC, self.BSM_SOLO: self.SOLO_ACC}

    def test_resolution_is_conservative_when_ambiguous(self):
        two_shared = [
            self._bsm(self.BSM_A, "A", ["SAMEA1", "SAMEA2"]),
            self._bsm(self.BSM_B, "B", ["SAMEA1", "SAMEA2"]),
        ]
        pooling = {b: self._pooling_info() for b in (self.BSM_A, self.BSM_B)}
        by_biosample, by_pool = (
            NCBISubmissionXML._resolve_existing_biosample_accessions(
                two_shared, pooling
            )
        )
        assert by_pool == {} and by_biosample == {}

    @staticmethod
    def _sra_refs(root):
        out = {}
        for add in root.findall(".//Action/AddFiles"):
            ident = add.find("Identifier/SPUID").text
            refs = [
                r.find("RefId")
                for r in add.findall("AttributeRefId")
                if r.get("name") == "BioSample"
            ]
            assert len(refs) == 1
            child = list(refs[0])[0]
            out[ident] = (child.tag, child.get("db"), child.text)
        return out

    def test_sra_actions_reference_registered_samples_by_accession(
        self, ncbi_submission_client: NCBISubmissionXML, mocked_instruments
    ):
        all_instruments = {
            i["id"]: {"vendor": i["vendor"], "model": i["model"]}
            for i in mocked_instruments
        }
        comp = [
            self._dobj("nmdc:dobj-11-acccomp1", "comp_R1.fastq.gz", 1),
            self._dobj("nmdc:dobj-11-acccomp2", "comp_R2.fastq.gz", 2),
        ]
        gen_a = [
            self._dobj("nmdc:dobj-11-accgena1", "genA_R1.fastq.gz", 1),
            self._dobj("nmdc:dobj-11-accgena2", "genA_R2.fastq.gz", 2),
        ]
        gen_c = [
            self._dobj("nmdc:dobj-11-accgenc1", "genC_R1.fastq.gz", 1),
            self._dobj("nmdc:dobj-11-accgenc2", "genC_R2.fastq.gz", 2),
        ]
        solo = [
            self._dobj("nmdc:dobj-11-accsolo1", "solo_R1.fastq.gz", 1),
            self._dobj("nmdc:dobj-11-accsolo2", "solo_R2.fastq.gz", 2),
        ]
        new = [
            self._dobj("nmdc:dobj-11-accnew01", "new_R1.fastq.gz", 1),
            self._dobj("nmdc:dobj-11-accnew02", "new_R2.fastq.gz", 2),
        ]
        comp_ntseq = self._ntseq(self.COMP_NTSEQ, "COMP", [d["id"] for d in comp])
        gen_a_ntseq = self._ntseq(self.GEN_A_NTSEQ, "GEN A", [d["id"] for d in gen_a])
        gen_c_ntseq = self._ntseq(self.GEN_C_NTSEQ, "GEN C", [d["id"] for d in gen_c])
        solo_ntseq = self._ntseq(self.SOLO_NTSEQ, "SOLO", [d["id"] for d in solo])
        new_ntseq = self._ntseq(self.NEW_NTSEQ, "NEW", [d["id"] for d in new])

        biosamples = [
            self._bsm(self.BSM_A, "CPER_002-M-38-5", [self.A_ACC, self.POOL_ACC]),
            self._bsm(self.BSM_B, "CPER_002-M-9-22", [self.POOL_ACC]),
            self._bsm(self.BSM_C, "CPER_002-M-22-37", [self.POOL_ACC]),
            self._bsm(self.BSM_SOLO, "SOLO", [self.SOLO_ACC]),
            self._bsm(self.BSM_NEW, "NEW", []),
        ]
        data_objects = [
            {self.BSM_A: comp + gen_a},
            {self.BSM_B: comp},
            {self.BSM_C: comp + gen_c},
            {self.BSM_SOLO: solo},
            {self.BSM_NEW: new},
        ]
        ntseq = [
            {self.BSM_A: [comp_ntseq, gen_a_ntseq]},
            {self.BSM_B: [comp_ntseq]},
            {self.BSM_C: [comp_ntseq, gen_c_ntseq]},
            {self.BSM_SOLO: [solo_ntseq]},
            {self.BSM_NEW: [new_ntseq]},
        ]
        pooling = {
            b: self._pooling_info() for b in (self.BSM_A, self.BSM_B, self.BSM_C)
        }

        xml_str = ncbi_submission_client.get_submission_xml(
            biosamples,
            ntseq,
            data_objects,
            [],
            all_instruments,
            pooled_biosamples_data=pooling,
        )
        root = ET.fromstring(xml_str)
        org = "National Microbiome Data Collaborative"

        biosample_blocks = sorted(
            e.text
            for e in root.findall(
                ".//Action/AddData[@target_db='BioSample']/Identifier/SPUID"
            )
        )
        # Registered samples get no BioSample block. C's only accession is the
        # composite's, so C itself is unregistered and must be submitted.
        assert biosample_blocks == sorted([self.BSM_C, self.BSM_NEW])

        refs = self._sra_refs(root)
        assert refs[self.COMP_NTSEQ] == ("PrimaryId", "BioSample", self.POOL_ACC)
        assert refs[self.GEN_A_NTSEQ] == ("PrimaryId", "BioSample", self.A_ACC)
        assert refs[self.SOLO_NTSEQ] == ("PrimaryId", "BioSample", self.SOLO_ACC)
        assert refs[self.GEN_C_NTSEQ] == ("SPUID", None, self.BSM_C)
        assert refs[self.NEW_NTSEQ] == ("SPUID", None, self.BSM_NEW)
        # every SPUID reference resolves to a BioSample block in this file
        for tag, _, value in refs.values():
            if tag == "SPUID":
                assert value in biosample_blocks
        assert org in xml_str


class TestUnsubmittableRunsAreSkipped:
    """A NucleotideSequencing record with ``insdc_experiment_identifiers`` already has
    an SRA experiment, and one with ``qc_status: fail`` must not be deposited. In
    both cases the exporter must not emit an SRA action (or the files) for it, and a
    biosample whose only run is such a run needs no BioSample block."""

    POOL = "nmdc:poolp-11-sub00001"
    POOL_PROCSM = "nmdc:procsm-11-sub00001"
    BSM_A = (
        "nmdc:bsm-11-sub0000a"  # pooled; also sequenced on its own (not yet submitted)
    )
    BSM_B = "nmdc:bsm-11-sub0000b"  # pooled only
    BSM_SOLO = "nmdc:bsm-11-sub0solo"  # not pooled, two runs: one submitted, one not
    POOL_ACC = "SAMEA104200253"
    COMP_NTSEQ = "nmdc:dgns-11-subcomp1"  # submitted at ENA
    GEN_A_NTSEQ = "nmdc:dgns-11-subgena1"  # not submitted
    SOLO_OLD_NTSEQ = "nmdc:dgns-11-subold01"  # submitted
    SOLO_NEW_NTSEQ = "nmdc:dgns-11-subnew01"  # not submitted
    SOLO_BAD_NTSEQ = "nmdc:dgns-11-subbad01"  # failed QC

    def _bsm(self, bsm_id, name, accessions=()):
        b = {
            "id": bsm_id,
            "type": "nmdc:Biosample",
            "name": name,
            "env_package": {"has_raw_value": "soil", "type": "nmdc:TextValue"},
        }
        if accessions:
            b["insdc_biosample_identifiers"] = [f"biosample:{a}" for a in accessions]
        return b

    def _dobj(self, dobj_id, filename, read):
        return {
            "id": dobj_id,
            "type": "nmdc:DataObject",
            "name": filename,
            "data_object_type": f"Metagenome Raw Read {read}",
            "url": f"https://storage.neonscience.org/x/{filename}",
        }

    def _ntseq(self, ntseq_id, name, has_output, submitted=False, qc_status=None):
        n = {
            "id": ntseq_id,
            "type": "nmdc:NucleotideSequencing",
            "name": name,
            "has_input": ["nmdc:procsm-11-x"],
            "has_output": has_output,
            "processing_institution": "Battelle",
            "analyte_category": "metagenome",
            "instrument_used": ["nmdc:inst-14-xz5tb342"],
            # a BioProject alone is NOT evidence that the run was deposited
            "insdc_bioproject_identifiers": ["bioproject:PRJEB22068"],
        }
        if submitted:
            n["insdc_experiment_identifiers"] = ["insdc.sra:ERX2133930"]
        if qc_status:
            n["qc_status"] = qc_status
        return n

    @pytest.fixture(autouse=True)
    def _mappings(self, mocker):
        mocker.patch(
            "nmdc_runtime.site.export.ncbi_xml.load_mappings",
            return_value=(
                {"id": "", "name": "sample_name"},
                {"id": "uriorcurie", "name": "string"},
            ),
        )

    def test_exclude_unsubmittable_runs_helper(self):
        submitted = self._ntseq("nmdc:dgns-11-s", "s", ["nmdc:dobj-11-s1"], True)
        failed = self._ntseq(
            "nmdc:dgns-11-f", "f", ["nmdc:dobj-11-f1"], qc_status="fail"
        )
        passed = self._ntseq(
            "nmdc:dgns-11-p", "p", ["nmdc:dobj-11-p1"], qc_status="pass"
        )
        kept = self._ntseq("nmdc:dgns-11-k", "k", ["nmdc:dobj-11-k1"])
        ntseq = [
            {self.BSM_A: [submitted, failed, passed, kept]},
            {self.BSM_B: [submitted, failed]},
        ]

        remaining, output_ids = NCBISubmissionXML._exclude_unsubmittable_runs(ntseq)

        # only "fail" is excluded; "pass" and an unset qc_status are kept
        assert remaining == [{self.BSM_A: [passed, kept]}]  # B had nothing left
        assert output_ids == {"nmdc:dobj-11-s1", "nmdc:dobj-11-f1"}

    def test_submitted_runs_produce_no_sra_action_and_no_files(
        self, ncbi_submission_client: NCBISubmissionXML, mocked_instruments
    ):
        all_instruments = {
            i["id"]: {"vendor": i["vendor"], "model": i["model"]}
            for i in mocked_instruments
        }
        comp = [
            self._dobj("nmdc:dobj-11-subcomp1", "comp_R1.fastq.gz", 1),
            self._dobj("nmdc:dobj-11-subcomp2", "comp_R2.fastq.gz", 2),
        ]
        gen_a = [
            self._dobj("nmdc:dobj-11-subgena1", "genA_R1.fastq.gz", 1),
            self._dobj("nmdc:dobj-11-subgena2", "genA_R2.fastq.gz", 2),
        ]
        old = [
            self._dobj("nmdc:dobj-11-subold01", "old_R1.fastq.gz", 1),
            self._dobj("nmdc:dobj-11-subold02", "old_R2.fastq.gz", 2),
        ]
        new = [
            self._dobj("nmdc:dobj-11-subnew01", "new_R1.fastq.gz", 1),
            self._dobj("nmdc:dobj-11-subnew02", "new_R2.fastq.gz", 2),
        ]
        bad = [
            self._dobj("nmdc:dobj-11-subbad01", "bad_R1.fastq.gz", 1),
            self._dobj("nmdc:dobj-11-subbad02", "bad_R2.fastq.gz", 2),
        ]
        comp_ntseq = self._ntseq(
            self.COMP_NTSEQ, "COMP", [d["id"] for d in comp], submitted=True
        )
        gen_a_ntseq = self._ntseq(self.GEN_A_NTSEQ, "GEN A", [d["id"] for d in gen_a])
        old_ntseq = self._ntseq(
            self.SOLO_OLD_NTSEQ, "OLD", [d["id"] for d in old], submitted=True
        )
        new_ntseq = self._ntseq(self.SOLO_NEW_NTSEQ, "NEW", [d["id"] for d in new])
        bad_ntseq = self._ntseq(
            self.SOLO_BAD_NTSEQ, "BAD", [d["id"] for d in bad], qc_status="fail"
        )

        pooling = {
            b: {
                "pooling_process_id": self.POOL,
                "processed_sample_id": self.POOL_PROCSM,
                "processed_sample_name": "OSBS_006-M-20130726-COMP",
                "pooled_biosample_ids": [self.BSM_A, self.BSM_B],
                "aggregated_values": {},
                "nucleotide_sequencing_ids": [self.COMP_NTSEQ],
            }
            for b in (self.BSM_A, self.BSM_B)
        }
        # A and B only carry the composite's accession; SOLO has none at all.
        biosamples = [
            self._bsm(self.BSM_A, "OSBS_006-M-5-30-20130726", [self.POOL_ACC]),
            self._bsm(self.BSM_B, "OSBS_006-M-8-12-20130726", [self.POOL_ACC]),
            self._bsm(self.BSM_SOLO, "SOLO"),
        ]
        data_objects = [
            {self.BSM_A: comp + gen_a},
            {self.BSM_B: comp},
            {self.BSM_SOLO: old + new + bad},
        ]
        ntseq = [
            {self.BSM_A: [comp_ntseq, gen_a_ntseq]},
            {self.BSM_B: [comp_ntseq]},
            {self.BSM_SOLO: [old_ntseq, new_ntseq, bad_ntseq]},
        ]

        xml_str = ncbi_submission_client.get_submission_xml(
            biosamples,
            ntseq,
            data_objects,
            [],
            all_instruments,
            pooled_biosamples_data=pooling,
        )
        root = ET.fromstring(xml_str)

        identifiers = sorted(
            a.find("Identifier/SPUID").text for a in root.findall(".//Action/AddFiles")
        )
        # the two ENA-deposited runs and the QC-failed run are gone; the two
        # unsubmitted, passing ones remain
        assert identifiers == sorted([self.GEN_A_NTSEQ, self.SOLO_NEW_NTSEQ])
        files = {f.get("file_path") for f in root.iter("File")}
        assert files == {
            "genA_R1.fastq.gz",
            "genA_R2.fastq.gz",
            "new_R1.fastq.gz",
            "new_R2.fastq.gz",
        }

        blocks = sorted(
            e.text
            for e in root.findall(
                ".//Action/AddData[@target_db='BioSample']/Identifier/SPUID"
            )
        )
        # A needs a BioSample for its individual run; the pool is registered already;
        # B has no run of its own in this submission; SOLO is unregistered.
        assert blocks == sorted([self.BSM_A, self.BSM_SOLO])
        for a in root.findall(".//Action/AddFiles"):
            ref = next(
                list(r.find("RefId"))[0]
                for r in a.findall("AttributeRefId")
                if r.get("name") == "BioSample"
            )
            assert ref.tag == "SPUID" and ref.text in blocks


class TestManifestAsSraIdentifier:
    """Replicate runs of one library (linked by a ``poolable_replicates`` Manifest on
    their DataObjects) are merged into one SRA action, which must be identified by
    the Manifest rather than by an arbitrary one of the run ids."""

    MANIFEST = "nmdc:manif-11-test0001"
    SOLO_MANIFEST = "nmdc:manif-11-test0002"
    POOL = "nmdc:poolp-11-mani0001"
    POOL_PROCSM = "nmdc:procsm-11-mani0001"
    BSM_A = "nmdc:bsm-11-mani000a"
    BSM_B = "nmdc:bsm-11-mani000b"
    RUN1 = "nmdc:dgns-11-manirun1"
    RUN2 = "nmdc:dgns-11-manirun2"
    SOLO = "nmdc:bsm-11-manisolo"
    SOLO_RUN1 = "nmdc:dgns-11-solorun1"
    SOLO_RUN2 = "nmdc:dgns-11-solorun2"

    def _dobj(self, dobj_id, filename, read, manifest=None):
        d = {
            "id": dobj_id,
            "type": "nmdc:DataObject",
            "name": filename,
            "data_object_type": f"Metagenome Raw Read {read}",
            "url": f"https://storage.neonscience.org/x/{filename}",
        }
        if manifest:
            d["in_manifest"] = [manifest]
        return d

    def _ntseq(self, ntseq_id, has_output):
        return {
            "id": ntseq_id,
            "type": "nmdc:NucleotideSequencing",
            "name": ntseq_id,
            "has_input": ["nmdc:procsm-11-x"],
            "has_output": has_output,
            "processing_institution": "Battelle",
            "analyte_category": "metagenome",
            "instrument_used": ["nmdc:inst-14-xz5tb342"],
        }

    def _bsm(self, bsm_id):
        return {
            "id": bsm_id,
            "type": "nmdc:Biosample",
            "name": bsm_id,
            "env_package": {"has_raw_value": "soil", "type": "nmdc:TextValue"},
        }

    @pytest.fixture(autouse=True)
    def _mappings(self, mocker):
        mocker.patch(
            "nmdc_runtime.site.export.ncbi_xml.load_mappings",
            return_value=(
                {"id": "", "name": "sample_name"},
                {"id": "uriorcurie", "name": "string"},
            ),
        )

    def test_shared_manifest_id_helper(self):
        f = NCBISubmissionXML._shared_manifest_id
        a = self._dobj("d1", "a_R1.fastq.gz", 1, self.MANIFEST)
        b = self._dobj("d2", "b_R1.fastq.gz", 1, self.MANIFEST)
        c = self._dobj("d3", "c_R1.fastq.gz", 1)  # no manifest
        d = self._dobj("d4", "d_R1.fastq.gz", 1, "nmdc:manif-11-other001")
        assert f([a, b]) == self.MANIFEST
        assert f([a]) == self.MANIFEST
        assert f([a, c]) is None  # not every file is in the manifest
        assert f([a, d]) is None  # two different manifests
        assert f([c]) is None
        assert f([]) is None

    def _run(self, client, mocked_instruments, with_manifest):
        all_instruments = {
            i["id"]: {"vendor": i["vendor"], "model": i["model"]}
            for i in mocked_instruments
        }
        m = self.MANIFEST if with_manifest else None
        sm = self.SOLO_MANIFEST if with_manifest else None
        run1 = [
            self._dobj("nmdc:dobj-11-m1r1", "fc1_R1.fastq.gz", 1, m),
            self._dobj("nmdc:dobj-11-m1r2", "fc1_R2.fastq.gz", 2, m),
        ]
        run2 = [
            self._dobj("nmdc:dobj-11-m2r1", "fc2_R1.fastq.gz", 1, m),
            self._dobj("nmdc:dobj-11-m2r2", "fc2_R2.fastq.gz", 2, m),
        ]
        solo1 = [
            self._dobj("nmdc:dobj-11-s1r1", "s1_R1.fastq.gz", 1, sm),
            self._dobj("nmdc:dobj-11-s1r2", "s1_R2.fastq.gz", 2, sm),
        ]
        solo2 = [
            self._dobj("nmdc:dobj-11-s2r1", "s2_R1.fastq.gz", 1, sm),
            self._dobj("nmdc:dobj-11-s2r2", "s2_R2.fastq.gz", 2, sm),
        ]
        pooling = {
            b: {
                "pooling_process_id": self.POOL,
                "processed_sample_id": self.POOL_PROCSM,
                "processed_sample_name": "POOL",
                "pooled_biosample_ids": [self.BSM_A, self.BSM_B],
                "aggregated_values": {},
                "nucleotide_sequencing_ids": [self.RUN1, self.RUN2],
            }
            for b in (self.BSM_A, self.BSM_B)
        }
        biosamples = [
            self._bsm(self.BSM_A),
            self._bsm(self.BSM_B),
            self._bsm(self.SOLO),
        ]
        data_objects = [
            {self.BSM_A: run1 + run2},
            {self.BSM_B: run1 + run2},
            {self.SOLO: solo1 + solo2},
        ]
        ntseq = [
            {
                self.BSM_A: [
                    self._ntseq(self.RUN1, [d["id"] for d in run1]),
                    self._ntseq(self.RUN2, [d["id"] for d in run2]),
                ]
            },
            {
                self.BSM_B: [
                    self._ntseq(self.RUN1, [d["id"] for d in run1]),
                    self._ntseq(self.RUN2, [d["id"] for d in run2]),
                ]
            },
            {
                self.SOLO: [
                    self._ntseq(self.SOLO_RUN1, [d["id"] for d in solo1]),
                    self._ntseq(self.SOLO_RUN2, [d["id"] for d in solo2]),
                ]
            },
        ]
        xml_str = client.get_submission_xml(
            biosamples,
            ntseq,
            data_objects,
            [],
            all_instruments,
            pooled_biosamples_data=pooling,
        )
        root = ET.fromstring(xml_str)
        return {
            a.find("Identifier/SPUID").text: sorted(
                f.get("file_path") for f in a.findall("File")
            )
            for a in root.findall(".//Action/AddFiles")
        }

    def test_replicate_runs_are_identified_by_their_manifest(
        self, ncbi_submission_client: NCBISubmissionXML, mocked_instruments
    ):
        actions = self._run(
            ncbi_submission_client, mocked_instruments, with_manifest=True
        )
        # pooled: both flowcells in one action named after the manifest
        assert actions[self.MANIFEST] == [
            "fc1_R1.fastq.gz",
            "fc1_R2.fastq.gz",
            "fc2_R1.fastq.gz",
            "fc2_R2.fastq.gz",
        ]
        assert self.RUN1 not in actions and self.RUN2 not in actions
        # the non-pooled biosample's replicate runs behave the same way
        assert actions[self.SOLO_MANIFEST] == [
            "s1_R1.fastq.gz",
            "s1_R2.fastq.gz",
            "s2_R1.fastq.gz",
            "s2_R2.fastq.gz",
        ]
        assert set(actions) == {self.MANIFEST, self.SOLO_MANIFEST}

    def test_without_a_manifest_the_first_run_id_is_used(
        self, ncbi_submission_client: NCBISubmissionXML, mocked_instruments
    ):
        actions = self._run(
            ncbi_submission_client, mocked_instruments, with_manifest=False
        )
        assert self.MANIFEST not in actions and self.SOLO_MANIFEST not in actions
        # falls back to one of the run ids (which one is the existing behaviour)
        (pooled_id,) = set(actions) & {self.RUN1, self.RUN2}
        assert actions[pooled_id] == [
            "fc1_R1.fastq.gz",
            "fc1_R2.fastq.gz",
            "fc2_R1.fastq.gz",
            "fc2_R2.fastq.gz",
        ]
        assert set(actions) & {self.SOLO_RUN1, self.SOLO_RUN2}


class TestPoolSequencedByJgiAndAnotherLab:
    """A pool sequenced by JGI *and* by another lab (NEON soil ONAQ 2021): JGI
    registered the BioSample and deposited its own run, so the other lab's run is
    submitted against JGI's BioSample accession and JGI's run is left out."""

    POOL = "nmdc:poolp-11-onaq0001"
    POOL_PROCSM = "nmdc:procsm-11-onaq0001"
    CORES = ["nmdc:bsm-11-onaq000a", "nmdc:bsm-11-onaq000b", "nmdc:bsm-11-onaq000c"]
    ACC = "SAMN37862670"
    BATTELLE_RUN = "nmdc:omprc-11-onaqbat1"
    JGI_RUN = "nmdc:omprc-11-onaqjgi1"

    @pytest.fixture(autouse=True)
    def _mappings(self, mocker):
        mocker.patch(
            "nmdc_runtime.site.export.ncbi_xml.load_mappings",
            return_value=(
                {"id": "", "name": "sample_name"},
                {"id": "uriorcurie", "name": "string"},
            ),
        )

    def test_other_labs_run_is_submitted_against_jgis_biosample(
        self, ncbi_submission_client: NCBISubmissionXML, mocked_instruments
    ):
        all_instruments = {
            i["id"]: {"vendor": i["vendor"], "model": i["model"]}
            for i in mocked_instruments
        }

        def dobj(i, name, t):
            return {
                "id": i,
                "type": "nmdc:DataObject",
                "name": name,
                "data_object_type": t,
                "url": f"https://x/{name}",
            }

        bat = [
            dobj(
                "nmdc:dobj-11-bat00r1",
                "BMI_21S_23_2075_mms_HHM55BGXM_R1.fastq.gz",
                "Metagenome Raw Read 1",
            ),
            dobj(
                "nmdc:dobj-11-bat00r2",
                "BMI_21S_23_2075_mms_HHM55BGXM_R2.fastq.gz",
                "Metagenome Raw Read 2",
            ),
        ]
        jgi = [
            dobj(
                "nmdc:dobj-11-jgi00001",
                "52834.4.466476.fastq.gz",
                "Metagenome Raw Reads",
            )
        ]

        def ntseq(i, inst, outs, **extra):
            return {
                "id": i,
                "type": "nmdc:NucleotideSequencing",
                "name": i,
                "has_input": ["nmdc:procsm-11-x"],
                "has_output": [d["id"] for d in outs],
                "processing_institution": inst,
                "analyte_category": "metagenome",
                "instrument_used": ["nmdc:inst-14-xz5tb342"],
                **extra,
            }

        bat_run = ntseq(self.BATTELLE_RUN, "Battelle", bat)
        jgi_run = ntseq(
            self.JGI_RUN, "JGI", jgi
        )  # no experiment id: institution alone must exclude it
        biosamples = [
            {
                "id": c,
                "type": "nmdc:Biosample",
                "name": c,
                "insdc_biosample_identifiers": [f"biosample:{self.ACC}"],
                "env_package": {"has_raw_value": "soil", "type": "nmdc:TextValue"},
            }
            for c in self.CORES
        ]
        pooling = {
            c: {
                "pooling_process_id": self.POOL,
                "processed_sample_id": self.POOL_PROCSM,
                "processed_sample_name": "ONAQ_002-M-20210524-COMP",
                "pooled_biosample_ids": self.CORES,
                "aggregated_values": {},
                "nucleotide_sequencing_ids": [self.BATTELLE_RUN, self.JGI_RUN],
            }
            for c in self.CORES
        }
        data_objects = [{c: bat + jgi} for c in self.CORES]
        ntseqs = [{c: [bat_run, jgi_run]} for c in self.CORES]

        xml_str = ncbi_submission_client.get_submission_xml(
            biosamples,
            ntseqs,
            data_objects,
            [],
            all_instruments,
            pooled_biosamples_data=pooling,
        )
        root = ET.fromstring(xml_str)

        # JGI registered the pool, so no BioSample block is written for it
        assert root.findall(".//Action/AddData[@target_db='BioSample']") == []
        actions = root.findall(".//Action/AddFiles")
        assert [a.find("Identifier/SPUID").text for a in actions] == [self.BATTELLE_RUN]
        (action,) = actions
        assert sorted(f.get("file_path") for f in action.findall("File")) == [
            "BMI_21S_23_2075_mms_HHM55BGXM_R1.fastq.gz",
            "BMI_21S_23_2075_mms_HHM55BGXM_R2.fastq.gz",
        ]
        ref = next(
            list(r.find("RefId"))[0]
            for r in action.findall("AttributeRefId")
            if r.get("name") == "BioSample"
        )
        assert (ref.tag, ref.get("db"), ref.text) == (
            "PrimaryId",
            "BioSample",
            self.ACC,
        )
        assert "52834.4.466476.fastq.gz" not in xml_str
