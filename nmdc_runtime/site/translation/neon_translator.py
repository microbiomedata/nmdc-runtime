import pandas as pd

from nmdc_schema import nmdc
from nmdc_runtime.site.translation.translator import Translator


class NeonDataTranslator(Translator):
    def __init__(self, data: dict, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        neon_data_tables = (
            "mms_metagenomeDnaExtraction",
            "mms_metagenomeSequencing",
            "sls_metagenomicsPooling",
            "sls_soilCoreCollection",
        )

        if all(k in data for k in neon_data_tables) in data:
            self.mms_metagenome_dna_extraction_df = data["mms_metagenomeDnaExtraction"]
            self.mms_metagenome_sequencing_df = data["mms_metagenomeSequencing"]
            self.sls_metagenomics_pooling_df = data["sls_metagenomicsPooling"]
            self.sls_soil_core_collection_df = data["sls_soilCoreCollection"]
        else:
            raise ValueError(
                f"You are missing one of the following tables: {neon_data_tables}"
            )

    def _translate_biosample(self) -> nmdc.Biosample:
        pass

    def _translate_omics_processing(self) -> nmdc.OmicsProcessing:
        pass

    def get_database(self) -> nmdc.Database:
        database = nmdc.Database()

        # join between mms_metagenomeDnaExtraction and mms_metagenomeSequencing tables
        mms_extraction_sequencing_merged_df = pd.merge(
            self.mms_metagenome_sequencing_df,
            self.mms_metagenome_dna_extraction_df["dnaSampleID"],
            on="dnaSampleID",
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
            ["genomicsPooledIDList", "dnaSampleID"]
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
