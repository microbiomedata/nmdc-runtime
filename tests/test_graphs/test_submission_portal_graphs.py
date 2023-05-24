import requests_mock

from nmdc_runtime.site.graphs import (
    translate_metadata_submission_to_nmdc_schema_database,
)
from nmdc_runtime.site.repository import resource_defs


MOCK_PORTAL_API_BASE = "http://www.example.com/nmdc-portal-api"
MOCK_PORTAL_SUBMISSION_ID = "test-submission-id"
MOCK_PORTAL_SUBMISSION = {
    "id": MOCK_PORTAL_SUBMISSION_ID,
    "metadata_submission": {
        "packageName": "plant-associated",
        "contextForm": {"datasetDoi": "10.12345/10.12345/00000000"},
        "templates": ["plant-associated"],
        "studyForm": {
            "studyName": "A test submission",
            "piName": "Test Testerson",
            "piEmail": "test.testerson@example.com",
            "piOrcid": "0000-0000-0000-0000",
            "linkOutWebpage": ["http://www.example.com/submission-test"],
            "description": "This is a test submission",
            "contributors": [
                {
                    "name": "Test Testerson",
                    "orcid": "0000-0000-0000-0000",
                    "roles": [
                        "Principal Investigator",
                    ],
                },
            ],
            "multiOmicsForm": {
                "alternativeNames": [],
                "studyNumber": "",
                "GOLDStudyId": "",
                "JGIStudyId": "",
                "NCBIBioProjectId": "",
            },
        },
        "sampleData": {
            "plant_associated_data": [
                {
                    "elev": "286",
                    "depth": "0",
                    "lat_lon": "42.39 -85.37",
                    "ecosystem": "Environmental",
                    "samp_name": "G5R1_MAIN_09MAY2016",
                    "env_medium": "plant-associated biome [ENVO:01001001]",
                    "env_package": "soil",
                    "geo_loc_name": "USA: Kellogg Biological Station, Michigan",
                    "growth_facil": "field",
                    "analysis_type": [
                        "metagenomics",
                    ],
                    "source_mat_id": "UUID:e8ed34cc-32f4-4fc5-9b9f-c2699e43163c",
                    "ecosystem_type": "Plant-associated",
                    "collection_date": "2016-05-09",
                    "env_broad_scale": "agricultural biome [ENVO:01001442]",
                    "env_local_scale": "phyllosphere biome [ENVO:01001442]",
                    "samp_store_temp": "-80 Celsius",
                    "ecosystem_subtype": "Leaf",
                    "ecosystem_category": "Terrestrial",
                    "specific_ecosystem": "Phyllosphere",
                }
            ]
        },
    },
}


def test_translate_metadata_submission_to_nmdc_schema_database():
    """Smoke test for translate_metadata_submission_to_nmdc_schema_database job"""

    job = translate_metadata_submission_to_nmdc_schema_database.to_job(
        resource_defs=resource_defs
    )
    run_config = {
        "ops": {
            "export_json_to_drs": {
                "config": {"username": "test"},
            },
            "fetch_nmdc_portal_submission_by_id": {
                "config": {
                    "submission_id": MOCK_PORTAL_SUBMISSION_ID,
                }
            },
        },
        "resources": {
            "mongo": {
                "config": {
                    "dbname": {"env": "MONGO_DBNAME"},
                    "host": {"env": "MONGO_HOST"},
                    "password": {"env": "MONGO_PASSWORD"},
                    "username": {"env": "MONGO_USERNAME"},
                },
            },
            "nmdc_portal_api_client": {
                "config": {
                    "base_url": MOCK_PORTAL_API_BASE,
                    "session_cookie": "xyz",
                }
            },
            "runtime_api_site_client": {
                "config": {
                    "base_url": {"env": "API_HOST"},
                    "client_id": {"env": "API_SITE_CLIENT_ID"},
                    "client_secret": {"env": "API_SITE_CLIENT_SECRET"},
                    "site_id": {"env": "API_SITE_ID"},
                }
            },
            "runtime_api_user_client": {
                "config": {
                    "base_url": {"env": "API_HOST"},
                    "password": {"env": "API_ADMIN_PASS"},
                    "username": {"env": "API_ADMIN_USER"},
                }
            },
        },
    }

    with requests_mock.mock(real_http=True) as mock:
        mock.get(
            f"{MOCK_PORTAL_API_BASE}/api/metadata_submission/{MOCK_PORTAL_SUBMISSION_ID}",
            json=MOCK_PORTAL_SUBMISSION,
        )

        result = job.execute_in_process(run_config=run_config)

    assert result.success
