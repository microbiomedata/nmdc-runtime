from nmdc_runtime.site.translation.submission_portal_translator import SubmissionPortalTranslator


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


def test_get_dataset():
    translator = SubmissionPortalTranslator()
    assert translator.get_database()
