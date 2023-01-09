import nmdc_schema.nmdc as nmdc

from nmdc_runtime.site.resources import GoldApiClient
from nmdc_runtime.site.etl.util import get_pi_dict

from dagster import op, graph, OpExecutionContext


@op(
    required_resource_keys=["gold_api_client", "nmdc_database"],
    config_schema={"study_id": str},
)
def compute_nmdc_study_set(context: OpExecutionContext) -> nmdc.Database:
    client: GoldApiClient = context.resources.gold_api_client
    study_data = client.fetch_study(context.op_config["study_id"])
    nmdc_database: nmdc.Database = context.resources.nmdc_database

    pi_dict = get_pi_dict(study_data)

    nmdc_database.study_set.append(
        nmdc.Study(
            id="gold:" + study_data["studyGoldId"],
            description=study_data["description"],
            title=study_data["studyName"],
            GOLD_study_identifiers="gold:" + study_data["studyGoldId"],
            principal_investigator=nmdc.PersonValue(
                has_raw_value=pi_dict["name"],
                name=pi_dict["name"],
                email=pi_dict["email"],
            ),
            type="nmdc:Study",
        )
    )

    return nmdc_database


@graph
def gold_nmdc_etl():
    nmdc_database = compute_nmdc_study_set()
    return nmdc_database
