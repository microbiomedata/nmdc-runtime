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

        if all(k in mms_data for k in neon_mms_data_tables) in mms_data:
            self.mms_metagenome_dna_extraction_df = mms_data[
                "mms_metagenomeDnaExtraction"
            ]
            self.mms_metagenome_sequencing_df = mms_data["mms_metagenomeSequencing"]
        else:
            raise ValueError(
                f"You are missing one of the MMS tables: {neon_mms_data_tables}"
            )

        if all(k in sls_data for k in neon_sls_data_tables) in sls_data:
            self.sls_metagenomics_pooling_df = sls_data["sls_metagenomicsPooling"]
            self.sls_soil_core_collection_df = sls_data["sls_soilCoreCollection"]
        else:
            raise ValueError(
                f"You are missing one of the SLS tables: {neon_sls_data_tables}"
            )

    def _translate_biosample(self) -> nmdc.Biosample:
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
            mms_extraction_sequencing_merged_df[["genomicsSampleID", "dnaSampleID"]],
            on="genomicsSampleID",
            how="left",
        )
        mms_sls_pooling_merged_df = mms_sls_pooling_merged_df[
            mms_sls_pooling_merged_df["dnaSampleID"].notna()
        ]

        # explode genomicsPooledIDList column on "|" and duplicate rows
        # for each of the split values
        mms_sls_pooling_exploded_df = pd.DataFrame(
            mms_sls_pooling_merged_df.genomicsPooledIDList.str.split("|").tolist(),
            index=mms_sls_pooling_merged_df.dnaSampleID,
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

        neon_biosample_ids = self.soil_biosamples_df["sampleID"]
        nmdc_biosample_ids = self._id_minter("nmdc:Biosample", len(neon_biosample_ids))
        neon_to_nmdc_biosample_ids = dict(zip(neon_biosample_ids, nmdc_biosample_ids))

        return database
