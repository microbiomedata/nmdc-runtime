from typing import List

import pandas as pd

from nmdc_schema import nmdc
from nmdc_runtime.site.translation.translator import Translator


class NeonDataTranslator(Translator):
    def __init__(self, mms_data: dict, sls_data: dict, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        neon_mms_data_tables = (
            "mms_metagenomeDnaExtraction",
            "mms_metagenomeSequencing",
        )

        neon_sls_data_tables = (
            "sls_metagenomicsPooling",
            "sls_soilCoreCollection",
        )

        if all(k in mms_data for k in neon_mms_data_tables):
            self.mms_metagenome_dna_extraction_df = mms_data[
                "mms_metagenomeDnaExtraction"
            ]
            self.mms_metagenome_sequencing_df = mms_data["mms_metagenomeSequencing"]
        else:
            raise ValueError(
                f"You are missing one of the MMS tables: {neon_mms_data_tables}"
            )

        if all(k in sls_data for k in neon_sls_data_tables):
            self.sls_metagenomics_pooling_df = sls_data["sls_metagenomicsPooling"]
            self.sls_soil_core_collection_df = sls_data["sls_soilCoreCollection"]
        else:
            raise ValueError(
                f"You are missing one of the SLS tables: {neon_sls_data_tables}"
            )

        neon_envo_mappings_file = "https://raw.githubusercontent.com/microbiomedata/nmdc-schema/main/assets/neon_mixs_env_triad_mappings/neon-nlcd-local-broad-mappings.tsv"
        self.neon_envo_terms_df = pd.read_csv(neon_envo_mappings_file, delimiter="\t")

    def _translate_biosample(
        self, neon_id: str, nmdc_id: str, biosample_row: pd.DataFrame
    ) -> List[nmdc.Biosample]:
        return nmdc.Biosample(
            id=nmdc_id,
            part_of="nmdc:sty-11-34xj1150",
            env_broad_scale=nmdc.ControlledIdentifiedTermValue(
                term=nmdc.OntologyClass(id="ENVO:00000446", name="terrestrial biome"),
            ),
            env_local_scale=nmdc.ControlledIdentifiedTermValue(
                term=nmdc.OntologyClass(
                    id=biosample_row["envo_id"].values[0],
                    name=biosample_row["envo_label"].values[0],
                )
            ),
            env_medium=nmdc.ControlledIdentifiedTermValue(
                term=nmdc.OntologyClass(id="ENVO:00001998", name="soil")
            ),
            samp_name=neon_id,
            lat_lon=nmdc.GeolocationValue(
                latitude=nmdc.DecimalDegree(biosample_row["decimalLatitude"].values[0]),
                longitude=nmdc.DecimalDegree(
                    biosample_row["decimalLongitude"].values[0]
                ),
            ),
            elev=nmdc.Float(biosample_row["elevation"].values[0]),
            collection_date=nmdc.TimestampValue(
                has_raw_value=biosample_row["collectDate"].values[0]
            ),
            temp=nmdc.QuantityValue(
                has_raw_value=biosample_row["soilTemp"].values[0],
                has_numeric_value=biosample_row["soilTemp"].values[0],
                has_unit="degree celcius",
            ),
            depth=nmdc.QuantityValue(
                has_minimum_numeric_value=biosample_row["sampleTopDepth"].values[0],
                has_maximum_numeric_value=biosample_row["sampleBottomDepth"].values[0],
            ),
            samp_collec_device=biosample_row["soilSamplingDevice"].values[0],
            soil_horizon=f"{biosample_row['horizon'].values[0]} horizon",
            analysis_type=biosample_row["sequenceAnalysisType"].values[0]
            if "sequenceAnalysisType" in biosample_row
            and not biosample_row["sequenceAnalysisType"].isnull().all()
            else None,
        )

    def _translate_planned_processes(self) -> List[nmdc.PlannedProcess]:
        pass

    def _translate_omics_processing(self) -> nmdc.OmicsProcessing:
        pass

    def get_database(self) -> nmdc.Database:
        database = nmdc.Database()

        # join between mms_metagenomeDnaExtraction and mms_metagenomeSequencing tables
        mms_extraction_sequencing_merged_df = pd.merge(
            self.mms_metagenome_dna_extraction_df,
            self.mms_metagenome_sequencing_df["dnaSampleID"],
            on="dnaSampleID",
            how="left",
        )

        # join between sls_metagenomicsPooling and merged dna extraction
        # and metagenome sequencing tables
        mms_sls_pooling_merged_df = pd.merge(
            self.sls_metagenomics_pooling_df,
            mms_extraction_sequencing_merged_df[
                ["genomicsSampleID", "dnaSampleID", "sequenceAnalysisType"]
            ],
            on="genomicsSampleID",
            how="left",
        )
        mms_sls_pooling_merged_df = mms_sls_pooling_merged_df[
            mms_sls_pooling_merged_df["dnaSampleID"].notna()
        ]

        # explode genomicsPooledIDList column on "|" and duplicate rows
        # for each of the split values
        mms_sls_pooling_exploded_df = pd.DataFrame(
            mms_sls_pooling_merged_df["genomicsPooledIDList"].str.split("|").tolist(),
            index=mms_sls_pooling_merged_df["dnaSampleID"],
        ).stack()
        mms_sls_pooling_exploded_df = mms_sls_pooling_exploded_df.reset_index()[
            [0, "dnaSampleID"]
        ]
        mms_sls_pooling_exploded_df.columns = ["sampleID", "dnaSampleID"]

        # join based on sampleID in sls_soilCoreCollection table
        soil_biosamples_df = pd.merge(
            self.sls_soil_core_collection_df,
            mms_sls_pooling_exploded_df[["sampleID", "dnaSampleID"]],
            on="sampleID",
            how="left",
        )
        self.soil_biosamples_df = soil_biosamples_df[
            soil_biosamples_df["dnaSampleID"].notna()
        ]

        # determining MIXS ENVO local scale
        self.soil_biosamples_df = pd.merge(
            self.soil_biosamples_df,
            self.neon_envo_terms_df[
                ["neon_nlcd_value", "envo_id", "envo_label", "env_local_scale"]
            ],
            left_on="nlcdClass",
            right_on="neon_nlcd_value",
            how="left",
        )

        neon_biosample_ids = self.soil_biosamples_df["sampleID"]
        nmdc_biosample_ids = self._id_minter("nmdc:Biosample", len(neon_biosample_ids))
        neon_to_nmdc_biosample_ids = dict(zip(neon_biosample_ids, nmdc_biosample_ids))

        for neon_id, nmdc_id in neon_to_nmdc_biosample_ids.items():
            biosample_row = self.soil_biosamples_df[
                self.soil_biosamples_df["sampleID"] == neon_id
            ]

            biosample_row["sequenceAnalysisType"] = mms_sls_pooling_merged_df[
                mms_sls_pooling_merged_df["genomicsPooledIDList"].str.contains(neon_id)
            ]["sequenceAnalysisType"]

            database.biosample_set.append(
                self._translate_biosample(neon_id, nmdc_id, biosample_row)
            )

        return database
