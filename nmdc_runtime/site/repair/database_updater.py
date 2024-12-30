from functools import lru_cache
import pandas as pd
from nmdc_runtime.site.resources import (
    RuntimeApiUserClient,
    RuntimeApiSiteClient,
    GoldApiClient,
)
from nmdc_runtime.site.translation.gold_translator import GoldStudyTranslator
from nmdc_schema import nmdc


class DatabaseUpdater:
    def __init__(
        self,
        runtime_api_user_client: RuntimeApiUserClient,
        runtime_api_site_client: RuntimeApiSiteClient,
        gold_api_client: GoldApiClient,
        study_id: str,
        gold_nmdc_instrument_map_df: pd.DataFrame = pd.DataFrame(),
    ):
        self.runtime_api_user_client = runtime_api_user_client
        self.runtime_api_site_client = runtime_api_site_client
        self.gold_api_client = gold_api_client
        self.study_id = study_id
        self.gold_nmdc_instrument_map_df = gold_nmdc_instrument_map_df

    @lru_cache
    def create_missing_dg_records(self):
        database = nmdc.Database()

        biosample_set = self.runtime_api_user_client.get_biosamples_for_study(
            self.study_id
        )

        all_gold_biosamples = []
        all_gold_projects = []
        for biosample in biosample_set:
            gold_biosample_identifiers = biosample.get("gold_biosample_identifiers")
            if gold_biosample_identifiers:
                gold_biosample_id = gold_biosample_identifiers[0]
                gold_biosample = self.gold_api_client.fetch_biosample_by_biosample_id(
                    gold_biosample_id
                )[0]
                gold_projects = self.gold_api_client.fetch_projects_by_biosample(
                    gold_biosample_id
                )
                gold_biosample["projects"] = gold_projects
                all_gold_biosamples.append(gold_biosample)
                all_gold_projects.extend(gold_projects)

        gold_study_translator = GoldStudyTranslator(
            biosamples=all_gold_biosamples,
            projects=all_gold_projects,
            gold_nmdc_instrument_map_df=self.gold_nmdc_instrument_map_df,
        )

        filtered_biosamples = gold_study_translator.biosamples
        filtered_projects = gold_study_translator.projects

        gold_project_ids = [project["projectGoldId"] for project in filtered_projects]
        nmdc_nucleotide_sequencing_ids = self.runtime_api_site_client.mint_id(
            "nmdc:NucleotideSequencing", len(gold_project_ids)
        ).json()
        gold_project_to_nmdc_nucleotide_sequencing_ids = dict(
            zip(gold_project_ids, nmdc_nucleotide_sequencing_ids)
        )

        gold_to_nmdc_biosample_ids = {
            biosample["gold_biosample_identifiers"][0].replace("gold:", ""): biosample[
                "id"
            ]
            for biosample in biosample_set
            if "gold_biosample_identifiers" in biosample
            and biosample["gold_biosample_identifiers"]
        }

        database.data_generation_set = []
        for project in filtered_projects:
            biosample_gold_id = next(
                (
                    biosample["biosampleGoldId"]
                    for biosample in filtered_biosamples
                    if any(
                        p["projectGoldId"] == project["projectGoldId"]
                        for p in biosample.get("projects", [])
                    )
                ),
                None,
            )

            if biosample_gold_id:
                nmdc_biosample_id = gold_to_nmdc_biosample_ids.get(biosample_gold_id)
                if nmdc_biosample_id:
                    database.data_generation_set.append(
                        gold_study_translator._translate_nucleotide_sequencing(
                            project,
                            nmdc_nucleotide_sequencing_id=gold_project_to_nmdc_nucleotide_sequencing_ids[
                                project["projectGoldId"]
                            ],
                            nmdc_biosample_id=nmdc_biosample_id,
                            nmdc_study_id=self.study_id,
                        )
                    )

        return database
