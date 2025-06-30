import pytest
import requests_mock

from dagster import build_op_context

from nmdc_runtime.site.resources import gold_api_client_resource
from nmdc_runtime.site.ops import (
    get_gold_study_pipeline_inputs,
    gold_analysis_projects_by_study,
    gold_biosamples_by_study,
    gold_projects_by_study,
    gold_study,
)


@pytest.fixture
def client_config():
    return {
        "base_url": "http://example.com/gold",
        "username": "username",
        "password": "password",
    }


@pytest.fixture
def op_context(client_config):
    return build_op_context(
        resources={
            "gold_api_client": gold_api_client_resource.configured(client_config)
        },
        op_config={
            "study_id": "Gs0149396",
            "study_type": "research_study",
            "gold_nmdc_instrument_mapping_file_url": "https://raw.githubusercontent.com/microbiomedata/nmdc-schema/refs/heads/main/assets/misc/gold_seqMethod_to_nmdc_instrument_set.tsv",
            "include_field_site_info": False,
        },
    )


def test_gold_biosamples_by_study(client_config, op_context):
    with requests_mock.mock() as mock:
        mock.get(
            f'{client_config["base_url"]}/biosamples',
            json=[{"biosampleGoldId": "Gb123456789"}],
        )

        # mock the /projects endpoint
        mock.get(
            f'{client_config["base_url"]}/projects',
            json=[
                {
                    "biosampleGoldId": "Gb123456789",
                    "projectGoldId": "Gp123456789",
                    "sequencingStrategy": "Metagenome",
                }
            ],
        )

        (study_id, _, _, _) = get_gold_study_pipeline_inputs(op_context)
        gold_biosamples_by_study(op_context, study_id)

        assert (
            len(mock.request_history) == 2
        )  # one for /biosamples and one for /projects
        assert mock.last_request.qs["studygoldid"] == ["gs0149396"]
        assert mock.last_request.headers["Authorization"].startswith("Basic ")


def test_gold_projects_by_study(client_config, op_context):
    with requests_mock.mock() as mock:
        mock.get(
            f'{client_config["base_url"]}/projects',
            json=[{"projectGoldId": "Gp123456789"}],
        )

        (study_id, _, _, _) = get_gold_study_pipeline_inputs(op_context)
        gold_projects_by_study(op_context, study_id)

        assert len(mock.request_history) == 1
        assert mock.last_request.qs["studygoldid"] == ["gs0149396"]
        assert mock.last_request.headers["Authorization"].startswith("Basic ")


def test_gold_analysis_projects_by_study(client_config, op_context):
    with requests_mock.mock() as mock:
        mock.get(
            f'{client_config["base_url"]}/analysis_projects',
            json=[{"apGoldId": "Ga0499994"}],
        )

        (study_id, _, _, _) = get_gold_study_pipeline_inputs(op_context)
        gold_analysis_projects_by_study(op_context, study_id)

        assert len(mock.request_history) == 1
        assert mock.last_request.qs["studygoldid"] == ["gs0149396"]
        assert mock.last_request.headers["Authorization"].startswith("Basic ")


def test_gold_study(client_config, op_context):
    with requests_mock.mock() as mock:
        mock.get(
            f'{client_config["base_url"]}/studies', json=[{"studyGoldId": "Gs0149396"}]
        )

        (study_id, _, _, _) = get_gold_study_pipeline_inputs(op_context)
        gold_study(op_context, study_id)

        assert len(mock.request_history) == 1
        assert mock.last_request.qs["studygoldid"] == ["gs0149396"]
        assert mock.last_request.headers["Authorization"].startswith("Basic ")
