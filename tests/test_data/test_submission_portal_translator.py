import datetime
from decimal import Decimal
from pathlib import Path
import random

import yaml
from linkml_runtime.dumpers import json_dumper
import pytest
from linkml_runtime.linkml_model import SlotDefinition
from nmdc_schema.nmdc import (
    InstrumentModelEnum,
    InstrumentVendorEnum,
    Database,
    FileTypeEnum,
    DoiProviderEnum,
    DoiCategoryEnum,
    Doi,
    UnitEnum,
    Study,
)

from nmdc_runtime.site.translation.submission_portal_translator import (
    SubmissionPortalTranslator,
)

from nmdc_runtime.api.db.mongo import validate_json
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
            "study_form": {
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
            "study_form": {
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


def test_get_study_dois():
    translator = SubmissionPortalTranslator()
    dois = translator._get_study_dois(
        {
            "study_form": {
                "dataDois": [
                    {
                        "provider": "emsl",
                        "value": "10.12345/6789",
                    },
                ],
                "publicationDois": [
                    {
                        "provider": None,
                        "value": "10.54321/9876",
                    },
                    {
                        "provider": "zenodo",
                        "value": "10.99999/abcd",
                    }
                ],
            },
        }
    )
    assert dois is not None
    assert len(dois) == 3

    assert dois[0].doi_value == "doi:10.12345/6789"
    assert dois[0].doi_provider == DoiProviderEnum("emsl")
    assert dois[0].type == "nmdc:Doi"
    assert dois[0].doi_category == DoiCategoryEnum("dataset_doi")

    assert dois[1].doi_value == "doi:10.54321/9876"
    assert dois[1].doi_provider is None
    assert dois[1].type == "nmdc:Doi"
    assert dois[1].doi_category == DoiCategoryEnum("publication_doi")

    assert dois[2].doi_value == "doi:10.99999/abcd"
    assert dois[2].doi_provider == DoiProviderEnum("zenodo")
    assert dois[2].type == "nmdc:Doi"
    assert dois[2].doi_category == DoiCategoryEnum("publication_doi")

    empty_doi = translator._get_study_dois({})
    assert empty_doi is None


def test_get_sample_set_dois():
    translator = SubmissionPortalTranslator()
    sample_set_dois = translator._get_sample_set_dois(
        {
            "multi_omics_form": {
                "awardDois": [
                    {
                        "provider": "jgi",
                        "value": "doi:10.11121314/15161718",
                    },
                ]
            },
        }
    )
    assert sample_set_dois is not None
    assert len(sample_set_dois) == 1

    assert sample_set_dois[0].doi_value == "doi:10.11121314/15161718"
    assert sample_set_dois[0].doi_provider == DoiProviderEnum("jgi")
    assert sample_set_dois[0].type == "nmdc:Doi"
    assert sample_set_dois[0].doi_category == DoiCategoryEnum("award_doi")

    empty_sample_set_doi = translator._get_sample_set_dois({})
    assert empty_sample_set_doi is None


def test_get_has_credit_associations():
    translator = SubmissionPortalTranslator()
    credit_associations = translator._get_has_credit_associations(
        {
            "study_form": {
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

    mock_slot = SlotDefinition(
        name="mock_slot",
    )

    with pytest.raises(ValueError, match="has_unit must be supplied"):
        translator._get_quantity_value("3.5", mock_slot)

    qv = translator._get_quantity_value("0-.1 m", mock_slot)
    assert qv is not None
    assert qv.has_raw_value == "0-.1 m"
    assert qv.has_numeric_value is None
    assert qv.has_minimum_numeric_value == Decimal("0")
    assert qv.has_maximum_numeric_value == Decimal("0.1")
    assert qv.has_unit == UnitEnum("m")

    qv = translator._get_quantity_value("1.2 [ppm]", mock_slot)
    assert qv is not None
    assert qv.has_raw_value == "1.2 [ppm]"
    assert qv.has_numeric_value == Decimal("1.2")
    assert qv.has_minimum_numeric_value is None
    assert qv.has_maximum_numeric_value is None
    assert qv.has_unit == UnitEnum("[ppm]")

    qv = translator._get_quantity_value("98.6Cel", mock_slot)
    assert qv is not None
    assert qv.has_raw_value == "98.6Cel"
    assert qv.has_numeric_value == Decimal("98.6")
    assert qv.has_minimum_numeric_value is None
    assert qv.has_maximum_numeric_value is None
    assert qv.has_unit == UnitEnum("Cel")

    qv = translator._get_quantity_value("-80", mock_slot, unit="Cel")
    assert qv is not None
    assert qv.has_raw_value == "-80"
    assert qv.has_numeric_value == Decimal("-80")
    assert qv.has_minimum_numeric_value is None
    assert qv.has_maximum_numeric_value is None
    assert qv.has_unit == UnitEnum("Cel")

    qv = translator._get_quantity_value("-90 - -100m", mock_slot, unit="m")
    assert qv is not None
    assert qv.has_raw_value == "-90 - -100m"
    assert qv.has_numeric_value is None
    assert qv.has_minimum_numeric_value == Decimal("-100")
    assert qv.has_maximum_numeric_value == Decimal("-90")
    assert qv.has_unit == UnitEnum("m")


def test_get_quantity_value_with_storage_units():
    translator = SubmissionPortalTranslator()

    # A slot with a single unit in storage_units annotation should use that unit
    single_unit_slot = SlotDefinition(
        name="single_unit_slot",
        annotations=[
            {
                "tag": "storage_units",
                "value": "Cel",
            }
        ],
    )
    qv = translator._get_quantity_value(-80, single_unit_slot)
    assert qv is not None
    assert qv.has_raw_value == "-80"
    assert qv.has_numeric_value == Decimal("-80")
    assert qv.has_unit == UnitEnum("Cel")

    # A slot with multiple units in storage_units annotation should not assume a unit
    multi_unit_slot = SlotDefinition(
        name="multi_unit_slot",
        annotations=[
            {
                "tag": "storage_units",
                "value": "g|mg|kg",
            }
        ],
    )
    qv = translator._get_quantity_value("0.5 kg", multi_unit_slot)
    assert qv is not None
    assert qv.has_raw_value == "0.5 kg"
    assert qv.has_numeric_value == Decimal("0.5")
    assert qv.has_unit == UnitEnum("kg")

    with pytest.raises(ValueError, match="has_unit must be supplied"):
        translator._get_quantity_value("3.5", multi_unit_slot)


def test_get_gold_study_identifiers():
    translator = SubmissionPortalTranslator()

    gold_ids = translator._get_gold_study_identifiers(
        {"study_form": {"GOLDStudyId": "Gs000000"}}
    )
    assert gold_ids is not None
    assert len(gold_ids) == 1
    assert gold_ids[0] == "gold:Gs000000"

    gold_ids = translator._get_gold_study_identifiers(
        {"study_form": {"GOLDStudyId": ""}}
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
        sample_data,
        url_field_name="read_1_url",
        md5_checksum_field_name="read_1_md5_checksum",
        nucleotide_sequencing_id="nmdc:dgns-00-00000000",
        data_object_type=FileTypeEnum("Metagenome Raw Reads"),
    )
    assert len(data_objects) == 1
    assert manifest is None

    sample_data["read_1_url"] = (
        "http://example.com/001-1a.fastq; http://example.com/001-1b.fastq"
    )
    with pytest.raises(ValueError, match="read_1_url"):
        translator._get_data_objects_from_fields(
            sample_data,
            url_field_name="read_1_url",
            md5_checksum_field_name="read_1_md5_checksum",
            nucleotide_sequencing_id="nmdc:dgns-00-00000000",
            data_object_type=FileTypeEnum("Metagenome Raw Reads"),
        )

    sample_data["read_1_md5_checksum"] = (
        "b1946ac92492d2347c6235b4d2611184; 94baaad4d1347ec6e15ae35c88ee8bc8"
    )
    data_objects, manifest = translator._get_data_objects_from_fields(
        sample_data,
        url_field_name="read_1_url",
        md5_checksum_field_name="read_1_md5_checksum",
        nucleotide_sequencing_id="nmdc:dgns-00-00000000",
        data_object_type=FileTypeEnum("Metagenome Raw Reads"),
    )
    assert len(data_objects) == 2
    assert manifest is not None
    assert all(do.in_manifest == [manifest.id] for do in data_objects)


def test_instruments(test_minter):
    """Test that get_database reuses known instruments when possible and creates new ones when needed."""
    known_instruments = {
        "hiseq_2500": "nmdc:inst-14-nn4b6k72",
    }
    submission = {
        "packageName": ["soil"],
        "templates": ["soil", "data_mg_interleaved"],
        "study_form": {
            "studyName": "asdfasdf",
            "piEmail": "fake@fake.com",
        },
    }
    sample_set = {
        "sample_data": {
            "data": {
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
        metadata_submission=submission,
        sample_set=sample_set,
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


def test_existing_study_only_uses_sample_set_updates(test_minter):
    existing_study = Study(
        id="nmdc:sty-00-00000000",
        type="nmdc:Study",
        study_category="research_study",
        name="Existing study name",
        title="Existing study title",
        description="Existing study description",
        funding_sources=["Existing award"],
        associated_dois=[
            Doi(
                doi_value="doi:10.0000/existing",
                doi_provider=DoiProviderEnum("emsl"),
                doi_category=DoiCategoryEnum("dataset_doi"),
                type="nmdc:Doi",
            ),
        ],
        emsl_project_identifiers=["emsl.project:existing"],
        jgi_portal_study_identifiers=["jgi.proposal:existing"],
    )
    metadata_submission = {
        "id": "00000000-0000-0000-0000-000000000000",
        "study_form": {
            # In reality, the Submission Portal UI should prevent this form from being updated in
            # the case where a new sample set is being added to an existing study. But if somehow
            # the form is updated, the translator should ignore the updates.
            "studyName": "Incoming study name",
            "description": "Incoming study description",
            "fundingSources": ["Incoming award"],
            "dataDois": [
                {
                    "provider": "emsl",
                    "value": "10.1111/from-study-form",
                },
            ],
        },
    }
    sample_set = {
        "multi_omics_form": {
            "awardDois": [
                {
                    "provider": "jgi",
                    "value": "10.2222/from-sample-set",
                },
            ],
            "JGIStudyId": "123456",
            "studyNumber": "78910",
        },
    }
    translator = SubmissionPortalTranslator(
        metadata_submission=metadata_submission,
        sample_set=sample_set,
        existing_study=existing_study,
        id_minter=test_minter,
        study_category="research_study",
    )

    database = translator.get_database()

    assert len(database.study_set) == 1
    study = database.study_set[0]

    # Assert that the study-level information did not change.
    assert study.id == existing_study.id
    assert study.name == "Existing study name"
    assert study.title == "Existing study title"
    assert study.description == "Existing study description"
    assert study.funding_sources == ["Existing award"]

    # Assert that information from the sample set was added to the study.
    assert study.associated_dois is not None
    assert [doi.doi_value for doi in study.associated_dois] == [
        "doi:10.0000/existing",
        "doi:10.2222/from-sample-set",
    ]
    assert [doi.doi_category for doi in study.associated_dois] == [
        DoiCategoryEnum("dataset_doi"),
        DoiCategoryEnum("award_doi"),
    ]
    assert study.emsl_project_identifiers == [
        "emsl.project:existing",
        "emsl.project:78910",
    ]
    assert study.jgi_portal_study_identifiers == [
        "jgi.proposal:existing",
        "jgi.proposal:123456",
    ]


def test_existing_study_sample_set_updates_are_deduplicated(test_minter):
    existing_study = Study(
        id="nmdc:sty-00-00000000",
        type="nmdc:Study",
        study_category="research_study",
        name="Existing study name",
        title="Existing study title",
        description="Existing study description",
        associated_dois=[
            Doi(
                doi_value="doi:10.2222/from-sample-set",
                doi_provider=DoiProviderEnum("jgi"),
                doi_category=DoiCategoryEnum("award_doi"),
                type="nmdc:Doi",
            ),
        ],
        emsl_project_identifiers=["emsl.project:78910"],
        jgi_portal_study_identifiers=["jgi.proposal:123456"],
    )
    metadata_submission = {
        "id": "00000000-0000-0000-0000-000000000000",
    }
    sample_set = {
        "multi_omics_form": {
            "awardDois": [
                {
                    "provider": "jgi",
                    "value": "10.2222/from-sample-set",
                },
            ],
            "JGIStudyId": "123456",
            "studyNumber": "78910",
        },
    }
    translator = SubmissionPortalTranslator(
        metadata_submission=metadata_submission,
        sample_set=sample_set,
        existing_study=existing_study,
        id_minter=test_minter,
        study_category="research_study",
    )

    database = translator.get_database()

    study = database.study_set[0]
    assert study.associated_dois is not None
    assert [doi.doi_value for doi in study.associated_dois] == [
        "doi:10.2222/from-sample-set",
    ]
    assert study.emsl_project_identifiers == ["emsl.project:78910"]
    assert study.jgi_portal_study_identifiers == ["jgi.proposal:123456"]


@pytest.mark.parametrize(
    "data_file_base",
    [
        "isolate_only",
        "isolate_soil",
        "isolate_soil_jgi",
        "nucleotide_sequencing_mapping",
        "plant_air_jgi",
        "sequencing_data",
        "soil_sample_link",
    ],
)
def test_get_database(test_minter, monkeypatch, data_file_base):
    # ProvenanceMetadata objects have an add_date and a mod_date slot that are populated with the
    # current datetime. In order to compare with a static expected output we need to patch
    # the datetime.now() call to return a predefined datetime.
    the_time = datetime.datetime(2023, 10, 17, 9, 0, 0)

    class FrozenDatetime(datetime.datetime):
        @classmethod
        def now(cls, **kwargs):
            # If tz was provided, make the_time timezone aware with that tz, otherwise return a
            # naive datetime. We could hardcode the timezone in the_time, but this makes the test
            # a little more robust by allowing the translator to attempt to use naive datetimes.
            # This would cause the validation at the end of this test to fail.
            if "tz" in kwargs:
                return the_time.replace(tzinfo=kwargs["tz"])
            return the_time

    def mock_version(package_name):
        if package_name == "nmdc_runtime":
            return "999.9.9"
        raise ValueError(f"Unexpected package name: {package_name}")

    monkeypatch.setattr("nmdc_runtime.site.translation.translator.datetime", FrozenDatetime)
    monkeypatch.setattr("nmdc_runtime.site.translation.translator.version", mock_version)

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
    expected_as_dict = json_dumper.to_dict(expected)
    actual = translator.get_database()
    actual_as_dict = json_dumper.to_dict(actual)
    assert actual_as_dict == expected_as_dict

    validation_result = validate_json(actual_as_dict, mongo_db)
    assert validation_result == {"result": "All Okay!"}


def test_parse_sample_link():
    translator = SubmissionPortalTranslator()

    parsed = translator._parse_sample_link("")
    assert parsed is None

    parsed = translator._parse_sample_link("invalid syntax")
    assert parsed is None

    parsed = translator._parse_sample_link("Pooling:sample1, sample2")
    assert parsed == ("Pooling", ["sample1", "sample2"])


def test_set_study_images():
    study = Study(
        id="nmdc:study-00-00000000", type="nmdc:Study", study_category="research_study"
    )

    SubmissionPortalTranslator.set_study_images(
        study,
        pi_image_url="http://www.example.org/pi_image.jpg",
        primary_study_image_url="http://www.example.org/primary_study_image.jpg",
        study_images_url=[
            "http://www.example.org/study_image1.jpg",
            "http://www.example.org/study_image2.jpg",
        ],
    )

    assert (
        study.principal_investigator.profile_image_url
        == "http://www.example.org/pi_image.jpg"
    )
    assert study.study_image is not None
    assert len(study.study_image) == 3
    assert study.study_image[0].url == "http://www.example.org/primary_study_image.jpg"
    assert study.study_image[0].display_order == 0
    assert study.study_image[1].url == "http://www.example.org/study_image1.jpg"
    assert study.study_image[1].display_order == 1
    assert study.study_image[2].url == "http://www.example.org/study_image2.jpg"
    assert study.study_image[2].display_order == 2
