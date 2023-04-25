import json
from pathlib import Path
from nmdc_runtime.api.endpoints.metadata import _validate_json
from nmdc_runtime.site.translation.submission_portal_translator import SubmissionPortalTranslator
from linkml_runtime.dumpers import json_dumper
import requests


def test_get_pi():
    translator = SubmissionPortalTranslator()
    pi_person_value = translator._get_pi({
        "studyForm": {
            "piName": "Maria D. McDonald",
            "piEmail": "MariaDMcDonald@example.edu",
            "piOrcid": "",
        }
    })
    assert pi_person_value is not None
    assert pi_person_value.name == "Maria D. McDonald"
    assert pi_person_value.email == "MariaDMcDonald@example.edu"

    pi_person_value = translator._get_pi({})
    assert pi_person_value is None


def test_get_doi():
    translator = SubmissionPortalTranslator()
    doi = translator._get_doi({
        "contextForm": {
            "datasetDoi": "1234"
        }
    })
    assert doi is not None
    assert doi.has_raw_value == "1234"

    doi = translator._get_doi({
        "contextForm": {
            "datasetDoi": ""
        }
    })
    assert doi is None

    doi = translator._get_doi({
        "contextForm": { }
    })
    assert doi is None


def test_get_has_credit_associations():
    translator = SubmissionPortalTranslator()
    credit_associations = translator._get_has_credit_associations({
        "studyForm": {
            "contributors": [
                {
                    "name": "Brenda Patterson",
                    "orcid": "1234",
                    "roles": ["Conceptualization", "Data curation"]
                },
                {
                    "name": "Lee F. Dukes",
                    "orcid": "5678",
                    "roles": ["Data curation"]
                }
            ]
        }
    })
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


def test_get_dataset(test_minter):
    translator = SubmissionPortalTranslator(id_minter=test_minter)
    assert translator.get_database()
