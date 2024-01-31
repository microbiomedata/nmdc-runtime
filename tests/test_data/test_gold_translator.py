import pytest

import random
from pathlib import Path

import yaml
from nmdc_schema import nmdc

from nmdc_runtime.site.translation.gold_translator import GoldStudyTranslator


def test_get_pi():
    translator = GoldStudyTranslator()

    # _get_pi should find the first PI listed
    pi_person_value = translator._get_pi(
        {
            "contacts": [
                {
                    "name": "Clifton P. Parker",
                    "email": "CliftonPParker@example.com",
                    "roles": ["co-PI"],
                },
                {"name": "Joan D. Berger", "email": "jdb@example.com", "roles": ["PI"]},
                {
                    "name": "Beth S. Hemphill",
                    "email": "bhemphill@example.com",
                    "roles": ["submitter", "co-PI"],
                },
                {
                    "name": "Randy T. Woolf",
                    "email": "RandyWoolf@example.com",
                    "roles": ["PI"],
                },
            ]
        }
    )
    assert pi_person_value is not None
    assert pi_person_value.name == "Joan D. Berger"
    assert pi_person_value.email == "jdb@example.com"

    # no PI in contacts, _get_pi should return None
    pi_person_value = translator._get_pi(
        {
            "contacts": [
                {
                    "name": "Beth S. Hemphill",
                    "email": "bhemphill@example.com",
                    "roles": ["submitter", "co-PI"],
                },
            ]
        }
    )
    assert pi_person_value is None


def test_get_mod_date():
    translator = GoldStudyTranslator()

    mod_date = translator._get_mod_date(
        {"modDate": "2023-03-02", "addDate": "2023-01-01"}
    )
    assert mod_date == "2023-03-02"

    mod_date = translator._get_mod_date({"modDate": None, "addDate": "2023-01-01"})
    assert mod_date == "2023-01-01"

    mod_date = translator._get_mod_date({"modDate": None, "addDate": None})
    assert mod_date is None


def test_get_insdc_biosample_identifiers():
    projects = [
        {
            "projectGoldId": "Gp0000001",
            "biosampleGoldId": "Gb0000001",
            "ncbiBioSampleAccession": None,
        },
        {
            "projectGoldId": "Gp0000002",
            "biosampleGoldId": "Gb0000001",
            "ncbiBioSampleAccession": "SAMN00000001",
        },
        {
            "projectGoldId": "Gp0000003",
            "biosampleGoldId": "Gb0000001",
            "ncbiBioSampleAccession": "SAMN00000002",
        },
        {
            "projectGoldId": "Gp0000004",
            "biosampleGoldId": "Gb0000002",
            "ncbiBioSampleAccession": "SAMN00000003",
        },
    ]
    translator = GoldStudyTranslator(projects=projects)

    insdc_biosample_identifiers = translator._get_insdc_biosample_identifiers(
        "Gb0000001"
    )
    assert insdc_biosample_identifiers == [
        "biosample:SAMN00000001",
        "biosample:SAMN00000002",
    ]

    insdc_biosample_identifiers = translator._get_insdc_biosample_identifiers(
        "Gb0000002"
    )
    assert insdc_biosample_identifiers == ["biosample:SAMN00000003"]

    insdc_biosample_identifiers = translator._get_insdc_biosample_identifiers(
        "Gb0000003"
    )
    assert insdc_biosample_identifiers == []


def test_get_samp_taxon_id():
    translator = GoldStudyTranslator()

    samp_taxon_id = translator._get_samp_taxon_id(
        {
            "ncbiTaxId": 449393,
            "ncbiTaxName": "freshwater metagenome",
        }
    )
    assert samp_taxon_id is not None
    assert samp_taxon_id.has_raw_value == "freshwater metagenome [NCBITaxon:449393]"

    samp_taxon_id = translator._get_samp_taxon_id(
        {
            "ncbiTaxId": None,
            "ncbiTaxName": "freshwater metagenome",
        }
    )
    assert samp_taxon_id is None

    samp_taxon_id = translator._get_samp_taxon_id(
        {
            "ncbiTaxId": 449393,
            "ncbiTaxName": None,
        }
    )
    assert samp_taxon_id is None


def test_get_samp_name():
    translator = GoldStudyTranslator()

    samp_name = translator._get_samp_name(
        {
            "biosampleName": "Freshwater microbial communities from Washington, USA - columbia_2019_sw_WHONDRS-S19S_0074",
        }
    )
    assert samp_name == "columbia_2019_sw_WHONDRS-S19S_0074"

    samp_name = translator._get_samp_name(
        {
            "biosampleName": "Freshwater microbial communities from Congo - River, Brazzaville, Congo - CONGO_065",
        }
    )
    assert samp_name == "CONGO_065"

    samp_name = translator._get_samp_name(
        {
            "biosampleName": "No hyphen",
        }
    )
    assert samp_name == "No hyphen"


def test_get_img_identifiers():
    analysis_projects = [
        {
            "apGoldId": "Ga0000001",
            "projects": ["Gp0000001"],
            "imgTaxonOid": 3300000001,
        },
        {
            "apGoldId": "Ga0000002",
            "projects": ["Gp0000002", "Gp0000003"],
            "imgTaxonOid": 3300000002,
        },
    ]
    projects = [
        {
            "projectGoldId": "Gp0000001",
            "biosampleGoldId": "Gb0000001",
        },
        {
            "projectGoldId": "Gp0000002",
            "biosampleGoldId": "Gb0000001",
        },
        {
            "projectGoldId": "Gp0000003",
            "biosampleGoldId": "Gb0000001",
        },
    ]
    translator = GoldStudyTranslator(
        projects=projects, analysis_projects=analysis_projects
    )

    img_identifiers = translator._get_img_identifiers("Gb0000001")
    assert "img.taxon:3300000001" in img_identifiers
    assert "img.taxon:3300000002" in img_identifiers


def test_get_collection_date():
    translator = GoldStudyTranslator()

    collection_date = translator._get_collection_date({"dateCollected": "2023-03-02"})
    assert collection_date is not None
    assert collection_date.has_raw_value == "2023-03-02"

    collection_date = translator._get_collection_date({"dateCollected": None})
    assert collection_date is None


def test_get_quantity_value():
    translator = GoldStudyTranslator()

    entity = {"arbitraryField": 7}
    value = translator._get_quantity_value(entity, "arbitraryField")
    assert value is not None
    assert value.has_raw_value == "7"
    assert value.has_numeric_value == 7.0
    assert value.has_unit is None

    entity = {"arbitraryField": 0}
    value = translator._get_quantity_value(entity, "arbitraryField", unit="meters")
    assert value is not None
    assert value.has_raw_value == "0"
    assert value.has_numeric_value == 0.0
    assert value.has_unit == "meters"

    entity = {"arbitraryField": 8}
    value = translator._get_quantity_value(entity, "arbitraryField", unit="meters")
    assert value is not None
    assert value.has_raw_value == "8"
    assert value.has_numeric_value == 8.0
    assert value.has_unit == "meters"

    entity = {"arbitraryField": None}
    value = translator._get_quantity_value(entity, "arbitraryField", unit="meters")
    assert value is None

    entity = {"arbitraryField1": 1, "arbitraryField2": 10}
    value = translator._get_quantity_value(
        entity, ("arbitraryField1", "arbitraryField2"), unit="meters"
    )
    assert value is not None
    assert value.has_minimum_numeric_value == 1.0
    assert value.has_maximum_numeric_value == 10.0
    assert value.has_raw_value is None
    assert value.has_numeric_value is None
    assert value.has_unit == "meters"


def test_get_text_value():
    translator = GoldStudyTranslator()

    entity = {"arbitraryField": "hello"}
    value = translator._get_text_value(entity, "arbitraryField")
    assert value is not None
    assert value.has_raw_value == "hello"

    entity = {"arbitraryField": None}
    value = translator._get_text_value(entity, "arbitraryField")
    assert value is None


def test_get_controlled_term_value():
    translator = GoldStudyTranslator()

    entity = {"arbitraryField": "hello"}
    value = translator._get_controlled_term_value(entity, "arbitraryField")
    assert value is not None
    assert value.has_raw_value == "hello"

    entity = {"arbitraryField": None}
    value = translator._get_controlled_term_value(entity, "arbitraryField")
    assert value is None


def test_get_env_term_value():
    translator = GoldStudyTranslator()

    entity = {"arbitraryField": {"id": "ENVO_00000446", "label": "terrestrial biome"}}
    env_term = translator._get_env_term_value(entity, "arbitraryField")
    assert env_term is not None
    assert env_term.has_raw_value == "ENVO_00000446"
    assert env_term.term.id == "ENVO:00000446"
    assert env_term.term.name == "terrestrial biome"

    entity = {
        "arbitraryField": {
            "id": "ENVO_00000446",
        }
    }
    env_term = translator._get_env_term_value(entity, "arbitraryField")
    assert env_term is not None
    assert env_term.has_raw_value == "ENVO_00000446"
    assert env_term.term.id == "ENVO:00000446"
    assert env_term.term.name is None

    entity = {"arbitraryField": {"label": "terrestrial biome"}}
    env_term = translator._get_env_term_value(entity, "arbitraryField")
    assert env_term is None

    entity = {"arbitraryField": None}
    env_term = translator._get_env_term_value(entity, "arbitraryField")
    assert env_term is None


def test_get_lat_lon():
    translator = GoldStudyTranslator()

    lat_lon = translator._get_lat_lon(
        {
            "latitude": 45.553,
            "longitude": -122.392,
        }
    )
    assert lat_lon is not None
    assert lat_lon.has_raw_value == "45.553 -122.392"
    assert lat_lon.latitude == 45.553
    assert lat_lon.longitude == -122.392

    lat_lon = translator._get_lat_lon(
        {
            "latitude": None,
            "longitude": -122.392,
        }
    )
    assert lat_lon is None

    lat_lon = translator._get_lat_lon(
        {
            "latitude": 45.553,
            "longitude": None,
        }
    )
    assert lat_lon is None


def test_get_instrument_name():
    translator = GoldStudyTranslator()

    instrument_name = translator._get_instrument_name(
        {
            "seqMethod": ["Illumina NextSeq 550", "Illumina NextSeq 3000"],
        }
    )
    assert instrument_name == "Illumina NextSeq 550"

    instrument_name = translator._get_instrument_name(
        {
            "seqMethod": [],
        }
    )
    assert instrument_name is None

    instrument_name = translator._get_instrument_name({"seqMethod": None})
    assert instrument_name is None


def test_get_processing_institution():
    translator = GoldStudyTranslator()

    processing_institution = translator._get_processing_institution(
        {
            "sequencingCenters": [
                "Battelle Memorial Institute",
                "Joint Genome Institute",
                "Environmental Molecular Sciences Laboratory",
            ],
        }
    )
    assert processing_institution is not None
    assert processing_institution.code.text == "JGI"

    processing_institution = translator._get_processing_institution(
        {
            "sequencingCenters": [
                "Battelle Memorial Institute",
                "Environmental Molecular Sciences Laboratory" "Joint Genome Institute",
            ],
        }
    )
    assert processing_institution is not None
    assert processing_institution.code.text == "EMSL"

    processing_institution = translator._get_processing_institution(
        {
            "sequencingCenters": ["University of California, San Diego"],
        }
    )
    assert processing_institution is not None
    assert processing_institution.code.text == "UCSD"

    processing_institution = translator._get_processing_institution(
        {
            "sequencingCenters": ["Battelle Memorial Institute"],
        }
    )
    assert processing_institution is None

    processing_institution = translator._get_processing_institution(
        {
            "sequencingCenters": [],
        }
    )
    assert processing_institution is None


def test_get_field_site_name():
    translator = GoldStudyTranslator()

    field_site_name = translator._get_field_site_name(
        {
            "biosampleName": "Freshwater microbial communities from Georgia, United States - altamaha_2019_sw_WHONDRS-S19S_0010"
        }
    )
    assert field_site_name == "altamaha_2019_sw_WHONDRS-S19S_0010"

    field_site_name = translator._get_field_site_name(
        {
            "biosampleName": "Freshwater microbial communities from Congo - River, Brazzaville, Congo - CONGO_063"
        }
    )
    assert field_site_name == "CONGO_063"

    field_site_name = translator._get_field_site_name(
        {
            "biosampleName": "Freshwater microbial communities from Mackenzie River, Tsiigehtchic, Northwest Territories, Canada - Mackenzie 2004-4"
        }
    )
    assert field_site_name == "Mackenzie"


@pytest.mark.xfail(reason="ValueError: term must be supplied")
def test_get_dataset(test_minter):
    random.seed(0)
    with open(Path(__file__).parent / "test_gold_translator_data.yaml") as f:
        test_datasets = yaml.safe_load_all(f)

        for test_data in test_datasets:
            translator = GoldStudyTranslator(
                **test_data["input"], id_minter=test_minter
            )

            expected = nmdc.Database(**test_data["output"])
            actual = translator.get_database()

            assert actual == expected
