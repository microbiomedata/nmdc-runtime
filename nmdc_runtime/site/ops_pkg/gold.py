"""
Dagster ops related to GOLD.

Note: These were extracted from a 1900-line file at `nmdc_runtime/site/ops.py` during a refactor.
"""

from typing import Tuple

import pandas as pd
from dagster import Any, OpExecutionContext, Out, op
from nmdc_schema import nmdc

from nmdc_runtime.site.repair.database_updater import DatabaseUpdater
from nmdc_runtime.site.resources import (
    GoldApiClient,
    RuntimeApiSiteClient,
    RuntimeApiUserClient,
)
from nmdc_runtime.site.translation.gold_translator import GoldStudyTranslator


@op(
    config_schema={
        "study_id": str,
        "study_type": str,
        "gold_nmdc_instrument_mapping_file_url": str,
        "include_field_site_info": bool,
        "enable_biosample_filtering": bool,
    },
    out={
        "study_id": Out(str),
        "study_type": Out(str),
        "gold_nmdc_instrument_mapping_file_url": Out(str),
        "include_field_site_info": Out(bool),
        "enable_biosample_filtering": Out(bool),
    },
)
def get_gold_study_pipeline_inputs(
    context: OpExecutionContext,
) -> Tuple[str, str, str, bool, bool]:
    return (
        context.op_config["study_id"],
        context.op_config["study_type"],
        context.op_config["gold_nmdc_instrument_mapping_file_url"],
        context.op_config["include_field_site_info"],
        context.op_config["enable_biosample_filtering"],
    )


@op(required_resource_keys={"gold_api_client"})
def gold_biosamples_by_study(
    context: OpExecutionContext, study_id: str
) -> list[dict[str, Any]]:
    client: GoldApiClient = context.resources.gold_api_client
    return client.fetch_biosamples_by_study(study_id)


@op(required_resource_keys={"gold_api_client"})
def gold_projects_by_study(
    context: OpExecutionContext, study_id: str
) -> list[dict[str, Any]]:
    client: GoldApiClient = context.resources.gold_api_client
    return client.fetch_projects_by_study(study_id)


@op(required_resource_keys={"gold_api_client"})
def gold_analysis_projects_by_study(
    context: OpExecutionContext, study_id: str
) -> list[dict[str, Any]]:
    client: GoldApiClient = context.resources.gold_api_client
    return client.fetch_analysis_projects_by_study(study_id)


@op(required_resource_keys={"gold_api_client"})
def gold_study(context: OpExecutionContext, study_id: str) -> dict[str, Any] | None:
    client: GoldApiClient = context.resources.gold_api_client
    return client.fetch_study(study_id)


@op(required_resource_keys={"runtime_api_site_client"})
def nmdc_schema_database_from_gold_study(
    context: OpExecutionContext,
    study: dict[str, Any],
    study_type: str,
    projects: list[dict[str, Any]],
    biosamples: list[dict[str, Any]],
    analysis_projects: list[dict[str, Any]],
    gold_nmdc_instrument_map_df: pd.DataFrame,
    include_field_site_info: bool,
    enable_biosample_filtering: bool,
) -> nmdc.Database:
    client: RuntimeApiSiteClient = context.resources.runtime_api_site_client

    def id_minter(*args, **kwargs):
        response = client.mint_id(*args, **kwargs)
        return response.json()

    translator = GoldStudyTranslator(
        study,
        study_type,
        biosamples,
        projects,
        analysis_projects,
        gold_nmdc_instrument_map_df,
        include_field_site_info,
        enable_biosample_filtering,
        id_minter=id_minter,
    )
    database = translator.get_database()
    return database

@op(
    config_schema={
        "nmdc_study_id": str,
        "gold_nmdc_instrument_mapping_file_url": str,
        "include_field_site_info": bool,
        "enable_biosample_filtering": bool,
    },
    out={
        "nmdc_study_id": Out(str),
        "gold_nmdc_instrument_mapping_file_url": Out(str),
        "include_field_site_info": Out(bool),
        "enable_biosample_filtering": Out(bool),
    },
)
def get_database_updater_inputs(
    context: OpExecutionContext,
) -> Tuple[str, str, bool, bool]:
    return (
        context.op_config["nmdc_study_id"],
        context.op_config["gold_nmdc_instrument_mapping_file_url"],
        context.op_config["include_field_site_info"],
        context.op_config["enable_biosample_filtering"],
    )

@op(
    required_resource_keys={
        "runtime_api_user_client",
        "runtime_api_site_client",
        "gold_api_client",
    }
)
def generate_data_generation_set_post_biosample_ingest(
    context: OpExecutionContext,
    nmdc_study_id: str,
    gold_nmdc_instrument_map_df: pd.DataFrame,
    include_field_site_info: bool,
    enable_biosample_filtering: bool,
) -> nmdc.Database:
    runtime_api_user_client: RuntimeApiUserClient = (
        context.resources.runtime_api_user_client
    )
    runtime_api_site_client: RuntimeApiSiteClient = (
        context.resources.runtime_api_site_client
    )
    gold_api_client: GoldApiClient = context.resources.gold_api_client

    database_updater = DatabaseUpdater(
        runtime_api_user_client,
        runtime_api_site_client,
        gold_api_client,
        nmdc_study_id,
        gold_nmdc_instrument_map_df,
        include_field_site_info,
        enable_biosample_filtering,
    )
    database = (
        database_updater.generate_data_generation_set_records_from_gold_api_for_study()
    )

    return database

@op(
    required_resource_keys={
        "runtime_api_user_client",
        "runtime_api_site_client",
        "gold_api_client",
    }
)
def generate_biosample_set_for_nmdc_study_from_gold(
    context: OpExecutionContext,
    nmdc_study_id: str,
    gold_nmdc_instrument_map_df: pd.DataFrame,
    include_field_site_info: bool = False,
    enable_biosample_filtering: bool = False,
) -> nmdc.Database:
    runtime_api_user_client: RuntimeApiUserClient = (
        context.resources.runtime_api_user_client
    )
    runtime_api_site_client: RuntimeApiSiteClient = (
        context.resources.runtime_api_site_client
    )
    gold_api_client: GoldApiClient = context.resources.gold_api_client

    database_updater = DatabaseUpdater(
        runtime_api_user_client,
        runtime_api_site_client,
        gold_api_client,
        nmdc_study_id,
        gold_nmdc_instrument_map_df,
        include_field_site_info,
        enable_biosample_filtering,
    )
    database = database_updater.generate_biosample_set_from_gold_api_for_study()

    return database


@op(
    required_resource_keys={
        "runtime_api_user_client",
        "runtime_api_site_client",
        "gold_api_client",
    },
    out=Out(Any),
)
def run_script_to_update_insdc_biosample_identifiers(
    context: OpExecutionContext,
    nmdc_study_id: str,
    gold_nmdc_instrument_map_df: pd.DataFrame,
    include_field_site_info: bool,
    enable_biosample_filtering: bool,
):
    """Generates a MongoDB update script to add INSDC biosample identifiers to biosamples.

    This op uses the DatabaseUpdater to generate a script that can be used to update biosample
    records with INSDC identifiers obtained from GOLD.

    Args:
        context: The execution context
        nmdc_study_id: The NMDC study ID for which to generate the update script
        gold_nmdc_instrument_map_df: A dataframe mapping GOLD instrument IDs to NMDC instrument set records

    Returns:
        A dictionary or list of dictionaries containing the MongoDB update script(s)
    """
    runtime_api_user_client: RuntimeApiUserClient = (
        context.resources.runtime_api_user_client
    )
    runtime_api_site_client: RuntimeApiSiteClient = (
        context.resources.runtime_api_site_client
    )
    gold_api_client: GoldApiClient = context.resources.gold_api_client

    database_updater = DatabaseUpdater(
        runtime_api_user_client,
        runtime_api_site_client,
        gold_api_client,
        nmdc_study_id,
        gold_nmdc_instrument_map_df,
        include_field_site_info,
        enable_biosample_filtering,
    )
    update_script = database_updater.queries_run_script_to_update_insdc_identifiers()

    if isinstance(update_script, list):
        total_updates = sum(len(item.get("updates", [])) for item in update_script)
    else:
        total_updates = len(update_script.get("updates", []))
    context.log.info(
        f"Generated update script for study {nmdc_study_id} with {total_updates} updates"
    )

    return update_script