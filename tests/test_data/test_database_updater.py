import pytest

import pandas as pd

from unittest.mock import MagicMock, patch

from nmdc_runtime.site.repair.database_updater import DatabaseUpdater


@pytest.fixture
def test_setup(test_minter):
    mock_runtime_api_user_client = MagicMock()
    mock_runtime_api_site_client = MagicMock()
    mock_gold_api_client = MagicMock()

    study_id = "nmdc:sty-11-e4yb9z58"
    mock_gold_nmdc_instrument_map_df = pd.DataFrame(
        {
            "GOLD SeqMethod": [
                "Illumina HiSeq",
                "Illumina HiSeq 2500-1TB",
            ],
            "NMDC instrument_set id": [
                "nmdc:inst-14-79zxap02",
                "nmdc:inst-14-nn4b6k72",
            ],
        }
    )

    mint_id_mock = MagicMock()
    mint_id_mock.json.return_value = test_minter("nmdc:NucleotideSequencing", 1)
    mock_runtime_api_site_client.mint_id.return_value = mint_id_mock

    database_updater = DatabaseUpdater(
        runtime_api_user_client=mock_runtime_api_user_client,
        runtime_api_site_client=mock_runtime_api_site_client,
        gold_api_client=mock_gold_api_client,
        study_id=study_id,
        gold_nmdc_instrument_map_df=mock_gold_nmdc_instrument_map_df,
    )

    return {
        "runtime_api_user_client": mock_runtime_api_user_client,
        "runtime_api_site_client": mock_runtime_api_site_client,
        "gold_api_client": mock_gold_api_client,
        "database_updater": database_updater,
        "study_id": study_id,
    }


@patch("nmdc_runtime.site.repair.database_updater.GoldStudyTranslator")
def test_create_missing_dg_records(MockGoldStudyTranslator, test_setup):
    mock_runtime_api_user_client = test_setup["runtime_api_user_client"]
    mock_runtime_api_site_client = test_setup["runtime_api_site_client"]
    mock_gold_api_client = test_setup["gold_api_client"]
    database_updater = test_setup["database_updater"]

    mock_runtime_api_user_client.get_biosamples_for_study.return_value = [
        {
            "id": "nmdc:bsm-11-q59jb831",
            "gold_biosample_identifiers": ["gold:Gb0150488"],
        }
    ]

    mock_gold_api_client.fetch_biosample_by_biosample_id.return_value = [
        {
            "biosampleGoldId": "Gb0150488",
            "biosampleName": "Switchgrass phyllosphere microbial communities",
            "projects": [
                {
                    "projectGoldId": "Gp0208640",
                    "biosampleGoldId": "Gb0150488",
                    "sequencingStrategy": "Metagenome",
                }
            ],
        }
    ]

    mock_gold_api_client.fetch_projects_by_biosample.return_value = [
        {
            "projectGoldId": "Gp0208640",
            "biosampleGoldId": "Gb0150488",
            "sequencingStrategy": "Metagenome",
        }
    ]

    MockGoldStudyTranslator.return_value.biosamples = [
        {"biosampleGoldId": "Gb0150488", "projects": [{"projectGoldId": "Gp0208640"}]}
    ]
    MockGoldStudyTranslator.return_value.projects = [{"projectGoldId": "Gp0208640"}]

    MockGoldStudyTranslator.return_value._translate_nucleotide_sequencing.return_value = MagicMock(
        id="nmdc:dgns-00-12345678",
        biosample_id="nmdc:bsm-11-q59jb831",
    )

    database = database_updater.create_missing_dg_records()

    assert len(database.data_generation_set) > 0
    assert database.data_generation_set[0].id.startswith("nmdc:dgns-00-")
    assert database.data_generation_set[0].biosample_id == "nmdc:bsm-11-q59jb831"

    mock_runtime_api_user_client.get_biosamples_for_study.assert_called_once_with(
        test_setup["study_id"]
    )
    mock_gold_api_client.fetch_biosample_by_biosample_id.assert_called_once_with(
        "gold:Gb0150488"
    )
    mock_gold_api_client.fetch_projects_by_biosample.assert_called_once_with(
        "gold:Gb0150488"
    )
    mock_runtime_api_site_client.mint_id.assert_called_once_with(
        "nmdc:NucleotideSequencing", 1
    )
