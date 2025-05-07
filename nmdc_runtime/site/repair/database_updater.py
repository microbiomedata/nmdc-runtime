from functools import lru_cache
from typing import Any, Dict, List
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
        """This class serves as an API for repairing connections in the database by
        adding records that are essentially missing "links"/"connections". As we identify
        common use cases for adding missing records to the database, we can
        add helper methods to this class.

        :param runtime_api_user_client: An object of RuntimeApiUserClient which can be
        used to retrieve instance records from the NMDC database.
        :param runtime_api_site_client: An object of RuntimeApiSiteClient which can be
        used to mint new IDs for the repaired records that need to be added into the NMDC database.
        :param gold_api_client: An object of GoldApiClient which can be used to retrieve
        records from GOLD via the GOLD API.
        :param study_id: NMDC study ID for which the missing records need to be added.
        :param gold_nmdc_instrument_map_df: A dataframe originally stored as a TSV mapping file in the
        NMDC schema repo, which maps GOLD instrument IDs to IDs of NMDC instrument_set records.
        """
        self.runtime_api_user_client = runtime_api_user_client
        self.runtime_api_site_client = runtime_api_site_client
        self.gold_api_client = gold_api_client
        self.study_id = study_id
        self.gold_nmdc_instrument_map_df = gold_nmdc_instrument_map_df

    @lru_cache
    def _fetch_gold_biosample(self, gold_biosample_id: str) -> List[Dict[str, Any]]:
        """Fetch response from GOLD /biosamples API for a given biosample id.

        :param gold_biosample_id: GOLD biosample ID.
        :return: Dictionary containing the response from the GOLD /biosamples API.
        """
        return self.gold_api_client.fetch_biosample_by_biosample_id(gold_biosample_id)

    @lru_cache
    def _fetch_gold_projects(self, gold_biosample_id: str):
        """Fetch response from GOLD /projects API for a given biosample id.

        :param gold_biosample_id: GOLD biosample ID
        :return: Dictionary containing the response from the GOLD /projects API.
        """
        return self.gold_api_client.fetch_projects_by_biosample(gold_biosample_id)

    def generate_data_generation_set_records_from_gold_api_for_study(
        self,
    ) -> nmdc.Database:
        """This method creates missing data generation records for a given study in the NMDC database using
        metadata from GOLD. The way the logic works is, it first fetches all the biosamples associated
        with the study from the NMDC database. Then, it fetches all the biosample and project data data
        associated with the individual biosamples from the GOLD API using the NMDC-GOLD biosample id
        mappings on the "gold_biosample_identifiers" key/slot. We use the GoldStudyTranslator class
        to mint the required number of `nmdc:DataGeneration` (`nmdc:NucleotideSequencing`) records based
        on the number of GOLD sequencing projects, and then reimplement only the part of logic from that
        class which is responsible for making data_generation_set records.

        :return: An instance of `nmdc:Database` object which is JSON-ified and rendered on the frontend.
        """
        database = nmdc.Database()

        biosample_set = self.runtime_api_user_client.get_biosamples_for_study(
            self.study_id
        )

        all_gold_biosamples = []
        all_gold_projects = []
        for biosample in biosample_set:
            gold_biosample_identifiers = biosample.get("gold_biosample_identifiers")
            if gold_biosample_identifiers:
                for gold_biosample_id in gold_biosample_identifiers:
                    gold_biosample = self._fetch_gold_biosample(gold_biosample_id)[0]
                    gold_projects = self._fetch_gold_projects(gold_biosample_id)
                    gold_biosample["projects"] = gold_projects

                    all_gold_biosamples.append(gold_biosample)
                    all_gold_projects.extend(gold_projects)

        gold_study_translator = GoldStudyTranslator(
            biosamples=all_gold_biosamples,
            projects=all_gold_projects,
            gold_nmdc_instrument_map_df=self.gold_nmdc_instrument_map_df,
        )

        # The GoldStudyTranslator class has some pre-processing logic which filters out
        # invalid biosamples and projects (based on `sequencingStrategy`, `projectStatus`, etc.)
        filtered_biosamples = gold_study_translator.biosamples
        filtered_projects = gold_study_translator.projects

        gold_project_ids = [project["projectGoldId"] for project in filtered_projects]
        nmdc_nucleotide_sequencing_ids = self.runtime_api_site_client.mint_id(
            "nmdc:NucleotideSequencing", len(gold_project_ids)
        ).json()
        gold_project_to_nmdc_nucleotide_sequencing_ids = dict(
            zip(gold_project_ids, nmdc_nucleotide_sequencing_ids)
        )

        gold_to_nmdc_biosample_ids = {}

        for biosample in biosample_set:
            gold_ids = biosample.get("gold_biosample_identifiers", [])
            for gold_id in gold_ids:
                gold_id_stripped = gold_id.replace("gold:", "")
                gold_to_nmdc_biosample_ids[gold_id_stripped] = biosample["id"]

        database.data_generation_set = []
        # Similar to the logic in GoldStudyTranslator, the number of nmdc:NucleotideSequencing records
        # created is based on the number of GOLD sequencing projects
        for project in filtered_projects:
            # map the projectGoldId to the NMDC biosample ID
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

    def generate_biosample_set_from_gold_api_for_study(self) -> nmdc.Database:
        """This method creates biosample_set records for a given study in the NMDC database using
        metadata from GOLD. The logic works by first fetching the biosampleGoldId values of all
        biosamples associated with the study. Then, it fetches the list of all biosamples associated
        with the GOLD study using the GOLD API. There's pre-processing logic in the GoldStudyTranslator
        to filter out biosamples based on `sequencingStrategy` and `projectStatus`. On this list of
        filtered biosamples, we compute a "set difference" (conceptually) between the list of
        filtered samples and ones that are already in the NMDC database, i.e., we ignore biosamples
        that are already present in the database, and continue on to create biosample_set records for
        those that do not have records in the database already.

        :return: An instance of `nmdc:Database` object which is JSON-ified and rendered on the frontend.
        """
        database = nmdc.Database()

        # get a list of all biosamples associated with a given NMDC study id
        biosample_set = self.runtime_api_user_client.get_biosamples_for_study(
            self.study_id
        )

        # get a list of GOLD biosample ids (`biosampleGoldId` values) by iterating
        # over all the biosample_set records retrieved using the above logic
        nmdc_gold_ids = set()
        for biosample in biosample_set:
            gold_ids = biosample.get("gold_biosample_identifiers", [])
            for gold_id in gold_ids:
                nmdc_gold_ids.add(gold_id.replace("gold:", ""))

        # retrieve GOLD study id by looking at the `gold_study_identifiers` key/slot
        # on the NMDC study record
        nmdc_study = self.runtime_api_user_client.get_study(self.study_id)[0]
        gold_study_id = nmdc_study.get("gold_study_identifiers", [])[0].replace(
            "gold:", ""
        )

        # use the GOLD study id to fetch all biosample records associated with the study
        gold_biosamples_for_study = self.gold_api_client.fetch_biosamples_by_study(
            gold_study_id
        )

        # part of the code where we are (conceptually) computing a set difference between
        # the list of filtered samples and ones that are already in the NMDC database
        missing_gold_biosamples = [
            gbs
            for gbs in gold_biosamples_for_study
            if gbs.get("biosampleGoldId") not in nmdc_gold_ids
        ]

        # use the GOLD study id to fetch all sequencing project records associated with the study
        gold_sequencing_projects_for_study = (
            self.gold_api_client.fetch_projects_by_study(gold_study_id)
        )

        # use the GOLD study id to fetch all analysis project records associated with the study
        gold_analysis_projects_for_study = (
            self.gold_api_client.fetch_analysis_projects_by_study(gold_study_id)
        )

        gold_study_translator = GoldStudyTranslator(
            biosamples=missing_gold_biosamples,
            projects=gold_sequencing_projects_for_study,
            analysis_projects=gold_analysis_projects_for_study,
            gold_nmdc_instrument_map_df=self.gold_nmdc_instrument_map_df,
        )

        translated_biosamples = gold_study_translator.biosamples

        # mint new NMDC biosample IDs for the "missing" biosamples
        gold_biosample_ids = [
            biosample["biosampleGoldId"] for biosample in translated_biosamples
        ]
        nmdc_biosample_ids = self.runtime_api_site_client.mint_id(
            "nmdc:Biosample", len(translated_biosamples)
        ).json()
        gold_to_nmdc_biosample_ids = dict(zip(gold_biosample_ids, nmdc_biosample_ids))

        database.biosample_set = [
            gold_study_translator._translate_biosample(
                biosample,
                nmdc_biosample_id=gold_to_nmdc_biosample_ids[
                    biosample["biosampleGoldId"]
                ],
                nmdc_study_id=self.study_id,
                nmdc_field_site_id=None,
            )
            for biosample in translated_biosamples
        ]

        return database

    def queries_run_script_to_update_insdc_biosample_identifiers(
        self,
    ) -> Dict[str, Any]:
        """This method creates a MongoDB update script to add INSDC biosample identifiers to biosamples
        based on data from GOLD. It gets all the biosamples associated with a study ID, fetches their
        gold_biosample_identifiers, then uses the fetch_projects_by_biosample() method in the gold_api_client
        to get the "ncbiBioSampleAccession" values for each biosample. It also adds "ncbiBioProjectAccession"
        values to the insdc_bioproject_identifiers field.

        :return: A dictionary containing the MongoDB update script with updates for each biosample.
        """
        # Get all biosamples associated with the study
        biosample_set = self.runtime_api_user_client.get_biosamples_for_study(
            self.study_id
        )

        updates = []

        # Process each biosample
        for biosample in biosample_set:
            gold_biosample_identifiers = biosample.get("gold_biosample_identifiers", [])
            if not gold_biosample_identifiers:
                continue

            biosample_id = biosample.get("id")
            if not biosample_id:
                continue

            insdc_biosample_identifiers = []
            insdc_bioproject_identifiers = []

            # Process each GOLD identifier for this biosample
            for gold_biosample_id in gold_biosample_identifiers:
                # Strip 'gold:' prefix if present
                normalized_id = gold_biosample_id.replace("gold:", "")

                # Fetch projects associated with this biosample from GOLD API
                gold_projects = self.gold_api_client.fetch_projects_by_biosample(
                    normalized_id
                )

                # Look for ncbiBioSampleAccession and ncbiBioProjectAccession in the projects
                for project in gold_projects:
                    ncbi_biosample_accession = project.get("ncbiBioSampleAccession")
                    if ncbi_biosample_accession and ncbi_biosample_accession.strip():
                        insdc_biosample_identifiers.append(ncbi_biosample_accession)

                    ncbi_bioproject_accession = project.get("ncbiBioProjectAccession")
                    if ncbi_bioproject_accession and ncbi_bioproject_accession.strip():
                        insdc_bioproject_identifiers.append(ncbi_bioproject_accession)

            # Process updates for insdc_biosample_identifiers
            if insdc_biosample_identifiers:
                existing_insdc_biosample_identifiers = biosample.get(
                    "insdc_biosample_identifiers", []
                )
                new_insdc_biosample_identifiers = list(
                    set(insdc_biosample_identifiers)
                    - set(existing_insdc_biosample_identifiers)
                )

                if new_insdc_biosample_identifiers:
                    # Add biosample: prefix to all identifiers
                    prefixed_new_biosample_identifiers = [
                        f"biosample:{id}" for id in new_insdc_biosample_identifiers
                    ]

                    # If biosample already has insdc_biosample_identifiers, append to it
                    if existing_insdc_biosample_identifiers:
                        all_biosample_identifiers = list(
                            set(
                                existing_insdc_biosample_identifiers
                                + prefixed_new_biosample_identifiers
                            )
                        )
                        updates.append(
                            {
                                "q": {"id": biosample_id},
                                "u": {
                                    "$set": {
                                        "insdc_biosample_identifiers": all_biosample_identifiers
                                    }
                                },
                            }
                        )
                    # Otherwise, create a new field
                    else:
                        updates.append(
                            {
                                "q": {"id": biosample_id},
                                "u": {
                                    "$set": {
                                        "insdc_biosample_identifiers": prefixed_new_biosample_identifiers
                                    }
                                },
                            }
                        )

            # Process updates for insdc_bioproject_identifiers
            if insdc_bioproject_identifiers:
                existing_insdc_bioproject_identifiers = biosample.get(
                    "insdc_bioproject_identifiers", []
                )
                new_insdc_bioproject_identifiers = list(
                    set(insdc_bioproject_identifiers)
                    - set(existing_insdc_bioproject_identifiers)
                )

                if new_insdc_bioproject_identifiers:
                    # Add bioproject: prefix to all identifiers
                    prefixed_new_bioproject_identifiers = [
                        f"bioproject:{id}" for id in new_insdc_bioproject_identifiers
                    ]

                    # If biosample already has insdc_bioproject_identifiers, append to it
                    if existing_insdc_bioproject_identifiers:
                        all_bioproject_identifiers = list(
                            set(
                                existing_insdc_bioproject_identifiers
                                + prefixed_new_bioproject_identifiers
                            )
                        )
                        updates.append(
                            {
                                "q": {"id": biosample_id},
                                "u": {
                                    "$set": {
                                        "insdc_bioproject_identifiers": all_bioproject_identifiers
                                    }
                                },
                            }
                        )
                    # Otherwise, create a new field
                    else:
                        updates.append(
                            {
                                "q": {"id": biosample_id},
                                "u": {
                                    "$set": {
                                        "insdc_bioproject_identifiers": prefixed_new_bioproject_identifiers
                                    }
                                },
                            }
                        )

        # Return the formatted script
        return {"update": "biosample_set", "updates": updates}
