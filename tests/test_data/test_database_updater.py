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


def test_queries_run_script_to_update_insdc_identifiers(db_updater, mock_clients):
    mock_runtime_api_user_client = mock_clients["runtime_api_user_client"]
    mock_gold_api_client = mock_clients["gold_api_client"]

    # Setup test data for biosamples
    mock_runtime_api_user_client.get_biosamples_for_study.return_value = [
        {
            "id": "nmdc:bsm-11-12345678",
            "gold_biosample_identifiers": ["gold:Gb0111111"],
            # No existing insdc_biosample_identifiers
        },
        {
            "id": "nmdc:bsm-11-87654321",
            "gold_biosample_identifiers": ["gold:Gb0222222"],
            "insdc_biosample_identifiers": [
                "biosample:SAMN11111111"
            ],  # Existing biosample identifier
        },
        {
            "id": "nmdc:bsm-11-abcdefgh",
            "gold_biosample_identifiers": ["gold:Gb0333333"],
            # No NCBI accession will be found for this one
        },
        {
            # This one has no gold_biosample_identifiers
            "id": "nmdc:bsm-11-ijklmnop",
        },
    ]

    # Setup test data for data generation records
    mock_runtime_api_user_client.get_data_generation_records_for_study.return_value = [
        {
            "id": "nmdc:dgns-00-aaaa1111",
            "gold_sequencing_project_identifiers": ["gold:Gp0111111"],
            # No existing insdc_bioproject_identifiers
        },
        {
            "id": "nmdc:dgns-00-bbbb2222",
            "gold_sequencing_project_identifiers": ["gold:Gp0222222", "gold:Gp0222223"],
            "insdc_bioproject_identifiers": [
                "bioproject:PRJNA11111111"
            ],  # Existing bioproject identifier
        },
        {
            "id": "nmdc:dgns-00-cccc3333",
            "gold_sequencing_project_identifiers": ["gold:Gp0333333"],
            # No NCBI accession will be found for this one
        },
        {
            # This one has no gold project identifiers
            "id": "nmdc:dgns-00-dddd4444",
        },
    ]

    # Configure mock for fetch_projects_by_biosample to return different data for different inputs
    def mock_fetch_projects(biosample_id):
        if biosample_id == "Gb0111111":
            return [
                {
                    "projectGoldId": "Gp0111111",
                    "ncbiBioSampleAccession": "SAMN22222222",
                    "ncbiBioProjectAccession": "PRJNA22222222",
                }
            ]
        elif biosample_id == "Gb0222222":
            return [
                {
                    "projectGoldId": "Gp0222222",
                    "ncbiBioSampleAccession": "SAMN33333333",
                    "ncbiBioProjectAccession": "PRJNA33333333",
                },
                {
                    "projectGoldId": "Gp0222223",
                    "ncbiBioSampleAccession": "SAMN33333334",  # Second accession for the same biosample
                    "ncbiBioProjectAccession": "PRJNA33333334",  # Second bioproject for the same biosample
                },
            ]
        elif biosample_id == "Gb0333333":
            return [
                {
                    "projectGoldId": "Gp0333333",
                    # No ncbiBioSampleAccession field
                    # No ncbiBioProjectAccession field
                }
            ]
        else:
            return []

    mock_gold_api_client.fetch_projects_by_biosample.side_effect = mock_fetch_projects

    # Run the method
    result = db_updater.queries_run_script_to_update_insdc_identifiers()

    # Assertions for the structure of the result
    assert result is not None
    assert isinstance(result, list)
    assert len(result) == 2

    # Extract biosample and data_generation updates
    biosample_update_item = next(
        item for item in result if item["update"] == "biosample_set"
    )
    data_generation_update_item = next(
        item for item in result if item["update"] == "data_generation_set"
    )

    # Verify biosample updates
    biosample_updates = biosample_update_item["updates"]
    assert (
        len(biosample_updates) == 2
    )  # 2 updates: one for each biosample with new identifiers

    # Group updates by biosample ID
    biosample_updates_by_id = {}
    for update in biosample_updates:
        biosample_id = update["q"]["id"]
        if biosample_id not in biosample_updates_by_id:
            biosample_updates_by_id[biosample_id] = []
        biosample_updates_by_id[biosample_id].append(update)

    # Check updates for first biosample (no existing identifiers)
    assert "nmdc:bsm-11-12345678" in biosample_updates_by_id
    first_biosample_update = biosample_updates_by_id["nmdc:bsm-11-12345678"][0]
    assert first_biosample_update["u"]["$set"]["insdc_biosample_identifiers"] == [
        "biosample:SAMN22222222"
    ]

    # Check updates for second biosample (existing identifiers to be merged)
    assert "nmdc:bsm-11-87654321" in biosample_updates_by_id
    second_biosample_update = biosample_updates_by_id["nmdc:bsm-11-87654321"][0]
    updated_biosample_identifiers = set(
        second_biosample_update["u"]["$set"]["insdc_biosample_identifiers"]
    )
    assert "biosample:SAMN11111111" in updated_biosample_identifiers
    assert "biosample:SAMN33333333" in updated_biosample_identifiers
    assert "biosample:SAMN33333334" in updated_biosample_identifiers
    assert len(updated_biosample_identifiers) == 3

    # Check that 3rd and 4th biosamples don't have updates
    assert "nmdc:bsm-11-abcdefgh" not in biosample_updates_by_id
    assert "nmdc:bsm-11-ijklmnop" not in biosample_updates_by_id

    # Verify data_generation updates
    data_generation_updates = data_generation_update_item["updates"]
    assert (
        len(data_generation_updates) == 2
    )  # 2 updates: one for each data_generation with new identifiers

    # Group updates by data_generation ID
    data_generation_updates_by_id = {}
    for update in data_generation_updates:
        data_generation_id = update["q"]["id"]
        if data_generation_id not in data_generation_updates_by_id:
            data_generation_updates_by_id[data_generation_id] = []
        data_generation_updates_by_id[data_generation_id].append(update)

    # Check updates for first data_generation (no existing identifiers)
    assert "nmdc:dgns-00-aaaa1111" in data_generation_updates_by_id
    first_data_generation_update = data_generation_updates_by_id[
        "nmdc:dgns-00-aaaa1111"
    ][0]
    assert first_data_generation_update["u"]["$set"][
        "insdc_bioproject_identifiers"
    ] == ["bioproject:PRJNA22222222"]

    # Check updates for second data_generation (existing identifiers to be merged)
    assert "nmdc:dgns-00-bbbb2222" in data_generation_updates_by_id
    second_data_generation_update = data_generation_updates_by_id[
        "nmdc:dgns-00-bbbb2222"
    ][0]
    updated_bioproject_identifiers = set(
        second_data_generation_update["u"]["$set"]["insdc_bioproject_identifiers"]
    )
    assert "bioproject:PRJNA11111111" in updated_bioproject_identifiers
    assert "bioproject:PRJNA33333333" in updated_bioproject_identifiers
    assert "bioproject:PRJNA33333334" in updated_bioproject_identifiers
    assert len(updated_bioproject_identifiers) == 3

    # Check that 3rd and 4th data_generation records don't have updates
    assert "nmdc:dgns-00-cccc3333" not in data_generation_updates_by_id
    assert "nmdc:dgns-00-dddd4444" not in data_generation_updates_by_id

    # Verify calls to the necessary methods
    mock_runtime_api_user_client.get_biosamples_for_study.assert_called_once()
    mock_runtime_api_user_client.get_data_generation_records_for_study.assert_called_once()
    assert (
        mock_gold_api_client.fetch_projects_by_biosample.call_count == 3
    )  # Called for the first 3 biosamples that have gold_ids
