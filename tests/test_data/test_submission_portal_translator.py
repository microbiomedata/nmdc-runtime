import datetime
from pathlib import Path
import random

import yaml
from linkml_runtime.dumpers import json_dumper
import pytest
from nmdc_schema.nmdc import (
    InstrumentModelEnum,
    InstrumentVendorEnum,
    Database,
)

from nmdc_runtime.site.translation.submission_portal_translator import (
    SubmissionPortalTranslator,
)

from nmdc_runtime.util import validate_json
from tests.conftest import get_mongo_test_db


def _mock_soil_data(sample_name: str, analysis_type: list[str] = ["metagenomics"]):
    return {
        "elev": 1325,
        "depth": "0 - 0.1",
        "lat_lon": "43 -120",
        "samp_name": sample_name,
        "env_medium": "bulk soil [ENVO:00005802]",
        "store_cond": "frozen",
        "geo_loc_name": "USA: Fake, Fake",
        "growth_facil": "field",
        "analysis_type": analysis_type,
        "collection_date": "2025-01-01",
        "env_broad_scale": "mixed forest biome [ENVO:01000198]",
        "env_local_scale": "area of evergreen forest [ENVO:01000843]",
        "samp_store_temp": "-80 Cel",
    }


def test_get_pi():
    translator = SubmissionPortalTranslator()
    pi_person_value = translator._get_pi(
        {
            "studyForm": {
                "piName": "Maria D. McDonald",
                "piEmail": "MariaDMcDonald@example.edu",
                "piOrcid": "",
            }
        }
    )
    assert pi_person_value is not None
    assert pi_person_value.name == "Maria D. McDonald"
    assert pi_person_value.email == "MariaDMcDonald@example.edu"

    pi_person_value = translator._get_pi({})
    assert pi_person_value is None

    translator = SubmissionPortalTranslator(
        study_pi_image_url="http://www.example.org/image.jpg"
    )
    pi_person_value = translator._get_pi(
        {
            "studyForm": {
                "piName": "Maria D. McDonald",
                "piEmail": "MariaDMcDonald@example.edu",
                "piOrcid": "0000-0000-0000-0001",
            }
        }
    )
    assert pi_person_value is not None
    assert pi_person_value.name == "Maria D. McDonald"
    assert pi_person_value.email == "MariaDMcDonald@example.edu"
    assert pi_person_value.orcid == "0000-0000-0000-0001"
    assert pi_person_value.profile_image_url == "http://www.example.org/image.jpg"


def test_get_has_credit_associations():
    translator = SubmissionPortalTranslator()
    credit_associations = translator._get_has_credit_associations(
        {
            "studyForm": {
                "contributors": [
                    {
                        "name": "Brenda Patterson",
                        "orcid": "1234",
                        "roles": ["Conceptualization", "Data curation"],
                    },
                    {
                        "name": "Lee F. Dukes",
                        "orcid": "5678",
                        "roles": ["Data curation"],
                    },
                ]
            }
        }
    )
    assert credit_associations is not None
    assert len(credit_associations) == 2
    assert credit_associations[0].applies_to_person is not None
    assert credit_associations[0].applies_to_person.name == "Brenda Patterson"
    assert credit_associations[0].applies_to_person.orcid == "1234"
    assert credit_associations[0].applied_roles is not None
    assert len(credit_associations[0].applied_roles) == 2
    assert credit_associations[0].applied_roles[0].code.text == "Conceptualization"


def test_get_quantity_value():
    translator = SubmissionPortalTranslator()

    qv = translator._get_quantity_value("3.5")
    assert qv is not None
    assert qv.has_raw_value == "3.5"
    assert qv.has_numeric_value == 3.5
    assert qv.has_minimum_numeric_value is None
    assert qv.has_maximum_numeric_value is None
    assert qv.has_unit is None

    qv = translator._get_quantity_value("0-.1")
    assert qv is not None
    assert qv.has_raw_value == "0-.1"
    assert qv.has_numeric_value is None
    assert qv.has_minimum_numeric_value == 0
    assert qv.has_maximum_numeric_value == 0.1
    assert qv.has_unit is None

    qv = translator._get_quantity_value("1.2 ppm")
    assert qv is not None
    assert qv.has_raw_value == "1.2 ppm"
    assert qv.has_numeric_value == 1.2
    assert qv.has_minimum_numeric_value is None
    assert qv.has_maximum_numeric_value is None
    assert qv.has_unit == "ppm"

    qv = translator._get_quantity_value("98.6F")
    assert qv is not None
    assert qv.has_raw_value == "98.6F"
    assert qv.has_numeric_value == 98.6
    assert qv.has_minimum_numeric_value is None
    assert qv.has_maximum_numeric_value is None
    assert qv.has_unit == "F"

    qv = translator._get_quantity_value("-80", unit="C")
    assert qv is not None
    assert qv.has_raw_value == "-80"
    assert qv.has_numeric_value == -80
    assert qv.has_minimum_numeric_value is None
    assert qv.has_maximum_numeric_value is None
    assert qv.has_unit == "C"

    qv = translator._get_quantity_value("-90 - -100m", unit="meter")
    assert qv is not None
    assert qv.has_raw_value == "-90 - -100m"
    assert qv.has_numeric_value is None
    assert qv.has_minimum_numeric_value == -100
    assert qv.has_maximum_numeric_value == -90
    assert qv.has_unit == "meter"


def test_get_gold_study_identifiers():
    translator = SubmissionPortalTranslator()

    gold_ids = translator._get_gold_study_identifiers(
        {"studyForm": {"GOLDStudyId": "Gs000000"}}
    )
    assert gold_ids is not None
    assert len(gold_ids) == 1
    assert gold_ids[0] == "gold:Gs000000"

    gold_ids = translator._get_gold_study_identifiers(
        {"studyForm": {"GOLDStudyId": ""}}
    )
    assert gold_ids is None


def test_get_controlled_identified_term_value():
    translator = SubmissionPortalTranslator()

    value = translator._get_controlled_identified_term_value(None)
    assert value is None

    value = translator._get_controlled_identified_term_value("")
    assert value is None

    value = translator._get_controlled_identified_term_value("term")
    assert value is None

    value = translator._get_controlled_identified_term_value("____term [id:00001]")
    assert value is not None
    assert value.has_raw_value == "____term [id:00001]"
    assert value.term.id == "id:00001"
    assert value.term.name == "term"


def test_get_controlled_term_value():
    translator = SubmissionPortalTranslator()

    value = translator._get_controlled_term_value(None)
    assert value is None

    value = translator._get_controlled_term_value("")
    assert value is None

    value = translator._get_controlled_term_value("term")
    assert value is not None
    assert value.has_raw_value == "term"
    assert value.term is None

    value = translator._get_controlled_term_value("____term [id:00001]")
    assert value is not None
    assert value.has_raw_value == "____term [id:00001]"
    assert value.term.id == "id:00001"
    assert value.term.name == "term"


def test_get_geolocation_value():
    translator = SubmissionPortalTranslator()

    value = translator._get_geolocation_value("0 0")
    assert value is not None
    assert value.has_raw_value == "0 0"
    assert value.latitude == 0
    assert value.longitude == 0

    value = translator._get_geolocation_value("-3.903895 -38.560507")
    assert value is not None
    assert value.has_raw_value == "-3.903895 -38.560507"
    assert value.latitude == -3.903895
    assert value.longitude == -38.560507

    # latitude > 90 not allowed
    value = translator._get_geolocation_value("93.903895 -38.560507")
    assert value is None

    value = translator._get_geolocation_value("180")
    assert value is None


def test_get_float():
    translator = SubmissionPortalTranslator()

    value = translator._get_float("-3.5332")
    assert value == -3.5332

    value = translator._get_float("")
    assert value is None

    value = translator._get_float(None)
    assert value is None


def test_get_from():
    translator = SubmissionPortalTranslator()

    metadata = {
        "one": {
            "two": {"three": "  three  ", "empty": ""},
            "four": ["one", "  two  ", "three"],
            "empty": [""],
            "some_empty": ["one", "", "three"],
        }
    }
    assert translator._get_from(metadata, "one") == metadata["one"]
    assert translator._get_from(metadata, ["one", "two", "three"]) == "three"
    assert translator._get_from(metadata, ["one", "two", "empty"]) is None
    assert translator._get_from(metadata, "two") is None
    assert translator._get_from(metadata, ["one", "four"]) == ["one", "two", "three"]
    assert translator._get_from(metadata, ["one", "empty"]) is None
    assert translator._get_from(metadata, ["one", "some_empty"]) == ["one", "three"]


def test_url_and_md5_lengths(test_minter):
    """Test that _get_data_objects_from_fields enforces the same number of urls and md5s"""
    translator = SubmissionPortalTranslator(
        id_minter=test_minter,
    )
    sample_data = {
        "model": "hiseq_1500",
        "samp_name": "001",
        "read_1_url": "http://example.com/001-1.fastq",
        "read_2_url": "http://example.com/001-2.fastq",
        "analysis_type": ["metagenomics"],
        "read_1_md5_checksum": "b1946ac92492d2347c6235b4d2611184",
        "read_2_md5_checksum": "94baaad4d1347ec6e15ae35c88ee8bc8",
    }
    data_objects, manifest = translator._get_data_objects_from_fields(
        sample_data, "read_1_url", "read_1_md5_checksum", name_suffix=""
    )
    assert len(data_objects) == 1
    assert manifest is None

    sample_data["read_1_url"] = (
        "http://example.com/001-1a.fastq; http://example.com/001-1b.fastq"
    )
    with pytest.raises(ValueError, match="read_1_url"):
        translator._get_data_objects_from_fields(
            sample_data, "read_1_url", "read_1_md5_checksum", name_suffix=""
        )

    sample_data["read_1_md5_checksum"] = (
        "b1946ac92492d2347c6235b4d2611184; 94baaad4d1347ec6e15ae35c88ee8bc8"
    )
    data_objects, manifest = translator._get_data_objects_from_fields(
        sample_data, "read_1_url", "read_1_md5_checksum", name_suffix=""
    )
    assert len(data_objects) == 2
    assert manifest is not None
    assert all(do.in_manifest == [manifest.id] for do in data_objects)


def test_instruments(test_minter):
    """Test that get_database reuses known instruments when possible and creates new ones when needed."""
    known_instruments = {
        "hiseq_2500": "nmdc:inst-14-nn4b6k72",
    }
    metadata_submission = {
        "metadata_submission": {
            "packageName": ["soil"],
            "templates": ["soil", "data_mg_interleaved"],
            "studyForm": {
                "studyName": "asdfasdf",
                "piEmail": "fake@fake.com",
            },
            "sampleData": {
                "soil_data": [
                    _mock_soil_data("001"),
                    _mock_soil_data("002"),
                ],
                "metagenome_sequencing_interleaved_data": [
                    {
                        "model": "hiseq_1500",
                        "samp_name": "001",
                        "interleaved_url": "http://example.com/001-1.fastq",
                        "analysis_type": ["metagenomics"],
                        "interleaved_checksum": "b1946ac92492d2347c6235b4d2611184",
                    },
                    {
                        "model": "hiseq_2500",
                        "samp_name": "002",
                        "interleaved_url": "http://example.com/002-1.fastq",
                        "analysis_type": [
                            "metagenomics",
                        ],
                        "interleaved_checksum": "916f4c31aaa35d6b867dae9a7f54270d",
                    },
                ],
            },
        },
    }
    translator = SubmissionPortalTranslator(
        id_minter=test_minter,
        illumina_instrument_mapping=known_instruments,
        metadata_submission=metadata_submission,
        study_category="research_study",
    )
    database = translator.get_database()

    # One instrument should be created for the hiseq_1500
    assert len(database.instrument_set) == 1
    assert database.instrument_set[0].model == InstrumentModelEnum(
        InstrumentModelEnum.hiseq_1500
    )
    assert database.instrument_set[0].vendor == InstrumentVendorEnum(
        InstrumentVendorEnum.illumina
    )

    # Both the new ID and the known ID should be used by nucleotide sequencing instances
    minted_instrument_id = database.instrument_set[0].id
    known_instrument_id = known_instruments["hiseq_2500"]
    assert len(database.data_generation_set) == 2
    instruments_used = {dg.instrument_used[0] for dg in database.data_generation_set}
    assert instruments_used == {minted_instrument_id, known_instrument_id}


@pytest.mark.parametrize(
    "data_file_base",
    ["plant_air_jgi", "nucleotide_sequencing_mapping", "sequencing_data"],
)
def test_get_dataset(test_minter, monkeypatch, data_file_base):
    # OmicsProcess objects have an add_date and a mod_date slot that are populated with the
    # current date. In order to compare with a static expected output we need to patch
    # the datetime.now() call to return a predefined date.
    the_time = datetime.datetime(2023, 10, 17, 9, 0, 0)

    class FrozenDatetime(datetime.datetime):
        @classmethod
        def now(cls, **kwargs):
            return the_time

    monkeypatch.setattr(
        "nmdc_runtime.site.translation.submission_portal_translator.datetime",
        FrozenDatetime,
    )

    mongo_db = get_mongo_test_db()
    data_path = Path(__file__).parent / "data"
    with open(data_path / f"{data_file_base}_input.yaml") as f:
        translator_inputs = yaml.safe_load(f)

    with open(data_path / f"{data_file_base}_output.yaml") as f:
        expected_output = yaml.safe_load(f)

    # Use mocked known instruments. The ability to test that the translator creates new instrument
    # instances is tested elsewhere.
    instrument_mapping = {
        "hiseq_1500": "nmdc:inst-00-00000001",
        "nextseq": "nmdc:inst-00-00000002",
    }

    # Reset the random number seed here so that fake IDs generated by the `test_minter`
    # fixture are stable across test runs
    random.seed(0)
    translator = SubmissionPortalTranslator(
        **translator_inputs,
        id_minter=test_minter,
        illumina_instrument_mapping=instrument_mapping,
        study_category="research_study",
    )

    expected = Database(**expected_output)
    actual = translator.get_database()
    assert actual == expected

    validation_result = validate_json(json_dumper.to_dict(actual), mongo_db)
    assert validation_result == {"result": "All Okay!"}
