import pytest

import pandas as pd

from unittest.mock import MagicMock, patch

from nmdc_runtime.site.repair.database_updater import DatabaseUpdater


@pytest.fixture
def mock_clients():
    runtime_api_user_client = MagicMock()
    runtime_api_site_client = MagicMock()
    gold_api_client = MagicMock()

    gold_nmdc_instrument_map_df = pd.DataFrame(
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

    return {
        "runtime_api_user_client": runtime_api_user_client,
        "runtime_api_site_client": runtime_api_site_client,
        "gold_api_client": gold_api_client,
        "gold_nmdc_instrument_map_df": gold_nmdc_instrument_map_df,
    }


@pytest.fixture
def db_updater(mock_clients):
    study_id = "nmdc:sty-11-abcdefx"
    return DatabaseUpdater(
        runtime_api_user_client=mock_clients["runtime_api_user_client"],
        runtime_api_site_client=mock_clients["runtime_api_site_client"],
        gold_api_client=mock_clients["gold_api_client"],
        study_id=study_id,
        gold_nmdc_instrument_map_df=mock_clients["gold_nmdc_instrument_map_df"],
    )


@patch("nmdc_runtime.site.repair.database_updater.GoldStudyTranslator")
def test_generate_data_generation_set_records_from_gold_api_for_study(
    MockGoldStudyTranslator, db_updater, mock_clients, test_minter
):
    mock_runtime_api_user_client = mock_clients["runtime_api_user_client"]
    mock_runtime_api_site_client = mock_clients["runtime_api_site_client"]
    mock_gold_api_client = mock_clients["gold_api_client"]

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
    mint_id_mock = MagicMock()
    mint_id_mock.json.return_value = test_minter("nmdc:NucleotideSequencing", 1)
    mock_runtime_api_site_client.mint_id.return_value = mint_id_mock

    database = db_updater.generate_data_generation_set_records_from_gold_api_for_study()

    assert database is not None
    assert len(database.data_generation_set) > 0
    assert database.data_generation_set[0].id.startswith("nmdc:dgns-00-")
    assert database.data_generation_set[0].biosample_id == "nmdc:bsm-11-q59jb831"

    mock_runtime_api_user_client.get_biosamples_for_study.assert_called_once()
    mock_gold_api_client.fetch_biosample_by_biosample_id.assert_called_once_with(
        "gold:Gb0150488"
    )
    mock_gold_api_client.fetch_projects_by_biosample.assert_called_once_with(
        "gold:Gb0150488"
    )
    mock_runtime_api_site_client.mint_id.assert_called_once_with(
        "nmdc:NucleotideSequencing", 1
    )


@patch("nmdc_runtime.site.repair.database_updater.GoldStudyTranslator")
def test_generate_biosample_set_from_gold_api_for_study(
    MockGoldStudyTranslator, db_updater, mock_clients, test_minter
):
    mock_runtime_api_user_client = mock_clients["runtime_api_user_client"]
    mock_runtime_api_site_client = mock_clients["runtime_api_site_client"]
    mock_gold_api_client = mock_clients["gold_api_client"]

    mock_runtime_api_user_client.get_biosamples_for_study.return_value = [
        {
            "id": "nmdc:bsm-11-39qcw250",
            "gold_biosample_identifiers": ["gold:Gb0240101"],
        },
        {
            "id": "nmdc:bsm-11-j9v5kc93",
            "gold_biosample_identifiers": ["gold:Gb0240022"],
        },
    ]

    mock_runtime_api_user_client.get_study.return_value = [
        {
            "id": "nmdc:sty-11-abcdefx",
            "gold_study_identifiers": ["gold:Gs0154244"],
        }
    ]

    mock_gold_api_client.fetch_biosamples_by_study.return_value = [
        {
            "biosampleGoldId": "Gb0240101",
            "biosampleName": "Terrestrial rangeland soil microbial communities from Oregon, Union County, USA - Myrold59.soil.5.NRCS0096",
            "projects": [
                {
                    "projectGoldId": "Gp0619228",
                    "sequencingStrategy": "Targeted Gene Survey",
                    "projectStatus": "incomplete",
                },
                {
                    "projectGoldId": "Gp0452727",
                    "sequencingStrategy": "Metagenome",
                    "projectStatus": "Permanent Draft",
                },
            ],
        },
        {
            "biosampleGoldId": "Gb0240022",
            "biosampleName": "Subpolar coniferous forest soil microbial communities from Alaska, Fairbanks, USA - Stegen36.permafrost.t15.4m",
            "projects": [
                {
                    "projectGoldId": "Gp0619320",
                    "sequencingStrategy": "Targeted Gene Survey",
                    "projectStatus": "incomplete",
                },
                {
                    "projectGoldId": "Gp0452648",
                    "sequencingStrategy": "Metagenome",
                    "projectStatus": "Permanent Draft",
                },
            ],
        },
        {
            "biosampleGoldId": "Gb0316050",
            "biosampleName": "Shipworm tissue microbial communities from Massachusetts Bay, MA, USA - 13114.distel.72.s015",
            "projects": [
                {
                    "projectGoldId": "Gp0619559",
                    "sequencingStrategy": "Targeted Gene Survey",
                    "projectStatus": "incomplete",
                },
                {
                    "projectGoldId": "Gp0619144",
                    "sequencingStrategy": "Metagenome",
                    "projectStatus": "Permanent Draft",
                },
            ],
        },
        {
            "biosampleGoldId": "Gb0316049",
            "biosampleName": "Shipworm tissue microbial communities from Massachusetts Bay, MA, USA - 13114.distel.72.s014",
            "projects": [
                {
                    "projectGoldId": "Gp0619378",
                    "sequencingStrategy": "Targeted Gene Survey",
                    "projectStatus": "incomplete",
                },
                {
                    "projectGoldId": "Gp0619143",
                    "sequencingStrategy": "Metagenome",
                    "projectStatus": "incomplete",
                },
            ],
        },
    ]

    mint_id_mock = MagicMock()
    mint_id_mock.json.return_value = test_minter("nmdc:Biosample", 2)
    mock_runtime_api_site_client.mint_id.return_value = mint_id_mock

    MockGoldStudyTranslator.return_value.biosamples = [
        {
            "biosampleGoldId": "Gb0316050",
            "biosampleName": "Shipworm tissue microbial communities from Massachusetts Bay, MA, USA - 13114.distel.72.s015",
        },
        {
            "biosampleGoldId": "Gb0316049",
            "biosampleName": "Shipworm tissue microbial communities from Massachusetts Bay, MA, USA - 13114.distel.72.s014",
        },
    ]

    database = db_updater.generate_biosample_set_from_gold_api_for_study()

    assert database is not None
    assert len(database.biosample_set) == 2

    mock_runtime_api_user_client.get_biosamples_for_study.assert_called_once()
    mock_runtime_api_user_client.get_study.assert_called_once_with(
        "nmdc:sty-11-abcdefx"
    )
    mock_gold_api_client.fetch_biosamples_by_study.assert_called_once_with("Gs0154244")
    mock_runtime_api_site_client.mint_id.assert_called_once_with("nmdc:Biosample", 2)
