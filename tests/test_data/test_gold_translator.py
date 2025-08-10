import collections
import pandas as pd
import pytest

import random
from pathlib import Path

import yaml
from nmdc_schema import nmdc

from nmdc_runtime.site.translation.gold_translator import GoldStudyTranslator

mock_gold_nmdc_instrument_map_df = pd.DataFrame(
    {
        "GOLD SeqMethod": [
            "Illumina HiSeq",
            "Illumina HiSeq 2500",
            "Illumina HiSeq 2500-1TB",
            "Illumina HiSeq 2500-Rapid",
            "Illumina NextSeq 550",
            "Illumina NovaSeq",
            "Illumina NovaSeq 6000",
            "Illumina NovaSeq S2",
            "Illumina NovaSeq S4",
            "Illumina NovaSeq SP",
        ],
        "NMDC instrument_set id": [
            "nmdc:inst-14-79zxap02",
            "nmdc:inst-14-nn4b6k72",
            "nmdc:inst-14-nn4b6k72",
            "nmdc:inst-14-nn4b6k72",
            "nmdc:inst-14-xz5tb342",
            "nmdc:inst-14-xx07be40",
            "nmdc:inst-14-mr4r2w09",
            "nmdc:inst-14-mr4r2w09",
            "nmdc:inst-14-mr4r2w09",
            "nmdc:inst-14-mr4r2w09",
        ],
    }
)


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
                    "type": "nmdc:PersonValue",
                },
                {
                    "name": "Joan D. Berger",
                    "email": "jdb@example.com",
                    "roles": ["PI"],
                    "type": "nmdc:PersonValue",
                },
                {
                    "name": "Beth S. Hemphill",
                    "email": "bhemphill@example.com",
                    "roles": ["submitter", "co-PI"],
                    "type": "nmdc:PersonValue",
                },
                {
                    "name": "Randy T. Woolf",
                    "email": "RandyWoolf@example.com",
                    "roles": ["PI"],
                    "type": "nmdc:PersonValue",
                },
            ]
        }
    )
    assert pi_person_value is not None
    assert pi_person_value.name == "Joan D. Berger"
    assert pi_person_value.email == "jdb@example.com"
    assert pi_person_value.type == "nmdc:PersonValue"

    # no PI in contacts, _get_pi should return None
    pi_person_value = translator._get_pi(
        {
            "contacts": [
                {
                    "name": "Beth S. Hemphill",
                    "email": "bhemphill@example.com",
                    "roles": ["submitter", "co-PI"],
                    "type": "nmdc:PersonValue",
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

    translator._projects_by_id = {p["projectGoldId"]: p for p in projects}
    translator._project_ids_by_biosample_id = collections.defaultdict(list)
    for project in projects:
        translator._project_ids_by_biosample_id[project["biosampleGoldId"]].append(
            project["projectGoldId"]
        )

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

    translator._projects_by_id = {p["projectGoldId"]: p for p in projects}
    translator._analysis_projects_by_id = {
        ap["apGoldId"]: ap for ap in analysis_projects
    }

    translator._analysis_project_ids_by_biosample_id = collections.defaultdict(list)
    for analysis_project in analysis_projects:
        for project_id in analysis_project["projects"]:
            if project_id in translator._projects_by_id:
                biosample_id = translator._projects_by_id[project_id]["biosampleGoldId"]
                translator._analysis_project_ids_by_biosample_id[biosample_id].append(
                    analysis_project["apGoldId"]
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
    with pytest.raises(ValueError, match="has_unit must be supplied"):
        # If the raw data does not have a parseable unit, and we don't supply one, an error should
        # be raised.
        translator._get_quantity_value(entity, "arbitraryField")

    entity = {"arbitraryField": "5.5 kPa"}
    value = translator._get_quantity_value(entity, "arbitraryField")
    assert value is not None
    assert value.has_raw_value == "5.5 kPa"
    assert value.has_numeric_value == 5.5
    assert value.has_unit == nmdc.UnitEnum("kPa")
    assert value.type == "nmdc:QuantityValue"

    entity = {"arbitraryField": 0}
    value = translator._get_quantity_value(entity, "arbitraryField", unit="m")
    assert value is not None
    assert value.has_raw_value == "0"
    assert value.has_numeric_value == 0.0
    assert value.has_unit == nmdc.UnitEnum("m")
    assert value.type == "nmdc:QuantityValue"

    entity = {"arbitraryField": 8}
    value = translator._get_quantity_value(entity, "arbitraryField", unit="m")
    assert value is not None
    assert value.has_raw_value == "8"
    assert value.has_numeric_value == 8.0
    assert value.has_unit == nmdc.UnitEnum("m")
    assert value.type == "nmdc:QuantityValue"

    entity = {"arbitraryField": None}
    value = translator._get_quantity_value(entity, "arbitraryField", unit="m")
    assert value is None

    entity = {"arbitraryField1": 1, "arbitraryField2": 10}
    value = translator._get_quantity_value(
        entity, ("arbitraryField1", "arbitraryField2"), unit="m"
    )
    assert value is not None
    assert value.has_minimum_numeric_value == 1.0
    assert value.has_maximum_numeric_value == 10.0
    assert value.has_raw_value is None
    assert value.has_numeric_value is None
    assert value.has_unit == nmdc.UnitEnum("m")
    assert value.type == "nmdc:QuantityValue"


def test_get_text_value():
    translator = GoldStudyTranslator()

    entity = {"arbitraryField": "hello"}
    value = translator._get_text_value(entity, "arbitraryField")
    assert value is not None
    assert value.has_raw_value == "hello"

    entity = {"arbitraryField": None}
    value = translator._get_text_value(entity, "arbitraryField")
    assert value is None


# TODO: Determine if value.type should be "nmdc:ControlledIdentifiedTermValue" or "nmdc:ControlledTermValue"
def test_get_controlled_term_value():
    translator = GoldStudyTranslator()

    entity = {"arbitraryField": "hello"}
    value = translator._get_controlled_term_value(entity, "arbitraryField")
    assert value is not None
    assert value.has_raw_value == "hello"
    # assert value.type == "nmdc:ControlledIdentifiedTermValue"
    assert value.type == "nmdc:ControlledTermValue"

    entity = {"arbitraryField": None}
    value = translator._get_controlled_term_value(entity, "arbitraryField")
    assert value is None
    # value.type should not exist is value is None
    # assert value.type == "nmdc:ControlledIdentifiedTermValue"


def test_get_env_term_value():
    translator = GoldStudyTranslator()

    entity = {
        "arbitraryField": {
            "id": "ENVO_00000446",
            "label": "terrestrial biome",
            "type": "nmdc:OntologyClass",
        }
    }
    env_term = translator._get_env_term_value(entity, "arbitraryField")
    assert env_term is not None
    assert env_term.has_raw_value == "ENVO_00000446"
    assert env_term.term.id == "ENVO:00000446"
    assert env_term.term.name == "terrestrial biome"
    assert env_term.term.type == "nmdc:OntologyClass"

    entity = {
        "arbitraryField": {
            "id": "ENVO_00000446",
            "type": "nmdc:OntologyClass",
        }
    }
    env_term = translator._get_env_term_value(entity, "arbitraryField")
    assert env_term is not None
    assert env_term.has_raw_value == "ENVO_00000446"
    assert env_term.term.id == "ENVO:00000446"
    assert env_term.term.name is None
    assert env_term.term.type == "nmdc:OntologyClass"

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
            "type": "nmdc:GeolocationValue",
        }
    )
    assert lat_lon is not None
    assert lat_lon.has_raw_value == "45.553 -122.392"
    assert lat_lon.latitude == 45.553
    assert lat_lon.longitude == -122.392
    assert lat_lon.type == "nmdc:GeolocationValue"

    lat_lon = translator._get_lat_lon(
        {
            "latitude": None,
            "longitude": -122.392,
            "type": "nmdc:GeolocationValue",
        }
    )
    assert lat_lon is None

    lat_lon = translator._get_lat_lon(
        {
            "latitude": 45.553,
            "longitude": None,
            "type": "nmdc:GeolocationValue",
        }
    )
    assert lat_lon is None


def test_get_instrument():
    translator = GoldStudyTranslator(
        gold_nmdc_instrument_map_df=mock_gold_nmdc_instrument_map_df
    )

    instrument_id = translator._get_instrument(
        {
            "seqMethod": ["Illumina NextSeq 550"],
        }
    )
    assert instrument_id == "nmdc:inst-14-xz5tb342"

    instrument_id = translator._get_instrument(
        {
            "seqMethod": [],
        }
    )
    assert instrument_id is None

    instrument_id = translator._get_instrument({"seqMethod": None})
    assert instrument_id is None


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


def test_include_field_site_info(test_minter):
    study = {"studyGoldId": "Gs0000001", "studyName": "Test Study"}
    biosamples = [
        {
            "biosampleGoldId": "Gb0000001",
            "biosampleName": "Test study - site_1_sample_001",
            "projects": [{"sequencingStrategy": "Metagenome"}],
            "envoBroadScale": {
                "id": "ENVO_01000253",
                "label": "freshwater river biome",
            },
            "envoLocalScale": {"id": "ENVO_00000022", "label": "river"},
            "envoMedium": {"id": "ENVO_00002006", "label": "water"},
        },
        {
            "biosampleGoldId": "Gb0000002",
            "biosampleName": "Test study - site_2_sample_002",
            "projects": [{"sequencingStrategy": "Metagenome"}],
            "envoBroadScale": {
                "id": "ENVO_01000253",
                "label": "freshwater river biome",
            },
            "envoLocalScale": {"id": "ENVO_00000022", "label": "river"},
            "envoMedium": {"id": "ENVO_00002006", "label": "water"},
        },
    ]

    projects = [
        {
            "projectGoldId": "Gp0000001",
            "biosampleGoldId": "Gb0000001",
            "sequencingStrategy": "Metagenome",
        },
        {
            "projectGoldId": "Gp0000002",
            "biosampleGoldId": "Gb0000002",
            "sequencingStrategy": "Metagenome",
        },
    ]

    # include_field_site_info=True has been enabled (set to `True`)
    translator_with_sites = GoldStudyTranslator(
        study=study,
        biosamples=biosamples,
        projects=projects,
        include_field_site_info=True,
        id_minter=test_minter,
    )

    # include_field_site_info=False has been enabled (set to `False`)
    translator_without_sites = GoldStudyTranslator(
        study=study,
        biosamples=biosamples,
        projects=projects,
        include_field_site_info=False,
        id_minter=test_minter,
    )

    # check that the include_field_site_info class variables are set correctly
    assert translator_with_sites.include_field_site_info is True
    assert translator_without_sites.include_field_site_info is False

    db_with_sites = translator_with_sites.get_database()
    db_without_sites = translator_without_sites.get_database()

    # if include_field_site_info has been enabled (True), field_research_site_set should not be
    # None, and should be populated with the correct number of nmdc:FieldResearchSite objects
    assert db_with_sites.field_research_site_set is not None
    assert (
        len(db_with_sites.field_research_site_set) == 2
    )  # two unique field sites: "site_1_sample_001" and "site_2_sample_002"

    field_site_names = {site.name for site in db_with_sites.field_research_site_set}
    assert "site_1_sample_001" in field_site_names
    assert "site_2_sample_002" in field_site_names

    for site in db_with_sites.field_research_site_set:
        assert site.type == "nmdc:FieldResearchSite"
        assert site.id.startswith("nmdc:")

    # if include_field_site_info has been disabled (False), field_research_site_set should be None
    # or empty, and no nmdc:FieldResearchSite objects should be created
    assert (
        db_without_sites.field_research_site_set is None
        or len(db_without_sites.field_research_site_set) == 0
    )

    # biosamples should have `collected_from` slot set when field sites are included
    for biosample in db_with_sites.biosample_set:
        assert biosample.collected_from is not None

    # biosamples should have `collected_from` slot to None when field research sites are
    # not included
    for biosample in db_without_sites.biosample_set:
        assert biosample.collected_from is None


def test_enable_biosample_filtering():
    biosamples = [
        {
            "biosampleGoldId": "Gb0000001",
            "projects": [{"sequencingStrategy": "Metagenome"}],  # valid
        },
        {
            "biosampleGoldId": "Gb0000002",
            "projects": [{"sequencingStrategy": "Amplicon"}],  # invalid
        },
        {
            "biosampleGoldId": "Gb0000003",
            "projects": [{"sequencingStrategy": "Metatranscriptome"}],  # valid
        },
    ]

    projects = [
        {
            "projectGoldId": "Gp0000001",
            "biosampleGoldId": "Gb0000001",
            "sequencingStrategy": "Metagenome",
        },
        {
            "projectGoldId": "Gp0000002",
            "biosampleGoldId": "Gb0000002",
            "sequencingStrategy": "Amplicon",
        },
        {
            "projectGoldId": "Gp0000003",
            "biosampleGoldId": "Gb0000003",
            "sequencingStrategy": "Metatranscriptome",
        },
    ]

    # enable_biosample_filtering is set to True
    translator_with_filtering = GoldStudyTranslator(
        biosamples=biosamples, projects=projects, enable_biosample_filtering=True
    )

    # enable_biosample_filtering is set to False
    translator_without_filtering = GoldStudyTranslator(
        biosamples=biosamples, projects=projects, enable_biosample_filtering=False
    )

    assert translator_with_filtering.enable_biosample_filtering is True
    assert translator_without_filtering.enable_biosample_filtering is False

    # when enable_biosample_filtering is True, only valid biosamples should be included
    assert len(translator_with_filtering.biosamples) == 2
    valid_biosample_ids = {
        bs["biosampleGoldId"] for bs in translator_with_filtering.biosamples
    }
    assert "Gb0000001" in valid_biosample_ids  # Metagenome
    assert "Gb0000002" not in valid_biosample_ids  # Amplicon (filtered out)
    assert "Gb0000003" in valid_biosample_ids  # Metatranscriptome

    # when enable_biosample_filtering is False, all biosamples should be included
    assert len(translator_without_filtering.biosamples) == 3  # All biosamples
    all_biosample_ids = {
        bs["biosampleGoldId"] for bs in translator_without_filtering.biosamples
    }
    assert "Gb0000001" in all_biosample_ids
    assert "Gb0000002" in all_biosample_ids  # Amplicon (not filtered)
    assert "Gb0000003" in all_biosample_ids


def test_enable_biosample_filtering_affects_projects():
    biosamples = [
        {
            "biosampleGoldId": "Gb0000001",
            "projects": [{"sequencingStrategy": "Metagenome"}],  # Valid
        },
        {
            "biosampleGoldId": "Gb0000002",
            "projects": [{"sequencingStrategy": "Amplicon"}],  # Invalid
        },
    ]

    projects = [
        {
            "projectGoldId": "Gp0000001",
            "biosampleGoldId": "Gb0000001",
            "sequencingStrategy": "Metagenome",
        },
        {
            "projectGoldId": "Gp0000002",
            "biosampleGoldId": "Gb0000002",
            "sequencingStrategy": "Amplicon",
        },
    ]

    # with biosample filtering enabled
    translator_with_filtering = GoldStudyTranslator(
        biosamples=biosamples, projects=projects, enable_biosample_filtering=True
    )

    # with biosample filtering disabled
    translator_without_filtering = GoldStudyTranslator(
        biosamples=biosamples, projects=projects, enable_biosample_filtering=False
    )

    # with biosample filtering enabled, only valid projects should remain
    assert len(translator_with_filtering.projects) == 1
    assert translator_with_filtering.projects[0]["projectGoldId"] == "Gp0000001"

    # with biosample filtering disabled, both projects should remain
    # (though the invalid one may still be filtered by project validation)
    # Note: projects are still subject to `_is_valid_project()` filtering regardless
    # of enable_biosample_filtering
    assert (
        len(translator_without_filtering.projects) == 1
    )  # still filtered by `_is_valid_project()`


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
