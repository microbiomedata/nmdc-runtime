import datetime
from pathlib import Path
import random

import yaml
from linkml_runtime.dumpers import json_dumper

from nmdc_runtime.site.translation.submission_portal_translator import (
    SubmissionPortalTranslator,
)
from nmdc_schema import nmdc

from nmdc_runtime.util import validate_json
from tests.conftest import get_mongo_test_db


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


def test_get_doi():
    translator = SubmissionPortalTranslator()
    doi = translator._get_doi({"contextForm": {"datasetDoi": "1234"}})
    assert doi is not None
    assert doi == [
        nmdc.Doi(doi_value="doi:1234", doi_category=nmdc.DoiCategoryEnum.dataset_doi)
    ]

    doi = translator._get_doi({"contextForm": {"datasetDoi": ""}})
    assert doi is None

    doi = translator._get_doi({"contextForm": {}})
    assert doi is None

    translator = SubmissionPortalTranslator(
        study_doi_provider="kbase", study_doi_category="award_doi"
    )
    doi = translator._get_doi({"contextForm": {"datasetDoi": "5678"}})
    assert doi is not None
    assert doi == [
        nmdc.Doi(
            doi_value="doi:5678",
            doi_provider=nmdc.DoiProviderEnum.kbase,
            doi_category=nmdc.DoiCategoryEnum.award_doi,
        )
    ]


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
        {"multiOmicsForm": {"GOLDStudyId": "Gs000000"}}
    )
    assert gold_ids is not None
    assert len(gold_ids) == 1
    assert gold_ids[0] == "GOLD:Gs000000"

    gold_ids = translator._get_gold_study_identifiers(
        {"multiOmicsForm": {"GOLDStudyId": ""}}
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


def test_get_dataset(test_minter, monkeypatch):
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
    with open(
        Path(__file__).parent / "test_submission_portal_translator_data.yaml"
    ) as f:
        test_datasets = yaml.safe_load_all(f)

        for test_data in test_datasets:
            # Reset the random number seed here so that fake IDs generated by the `test_minter`
            # fixture are stable across test runs
            random.seed(0)
            translator = SubmissionPortalTranslator(
                **test_data["input"],
                id_minter=test_minter,
                study_category="research_study",
                study_doi_provider="jgi",
            )

            expected = nmdc.Database(**test_data["output"])
            actual = translator.get_database()
            assert actual == expected

            validation_result = validate_json(json_dumper.to_dict(actual), mongo_db)
            assert validation_result == {"result": "All Okay!"}
