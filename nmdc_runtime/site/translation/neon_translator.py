import sqlite3

from typing import List

import pandas as pd

from nmdc_schema import nmdc
from nmdc_runtime.site.translation.translator import Translator


class NeonDataTranslator(Translator):
    def __init__(self, mms_data: dict, sls_data: dict, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.conn = sqlite3.connect("neon.db")

        neon_mms_data_tables = (
            "mms_metagenomeDnaExtraction",
            "mms_metagenomeSequencing",
        )

        neon_sls_data_tables = (
            "sls_metagenomicsPooling",
            "sls_soilCoreCollection",
            "sls_soilChemistry",
            "sls_soilMoisture",
            "sls_soilpH",
        )

        neon_ntr_data_tables = (
            "ntr_externalLab",
            "ntr_internalLab"
        )

        if all(k in mms_data for k in neon_mms_data_tables):
            mms_data["mms_metagenomeDnaExtraction"].to_sql(
                "mms_metagenomeDnaExtraction",
                self.conn,
                if_exists="replace",
                index=False,
            )
            mms_data["mms_metagenomeSequencing"].to_sql(
                "mms_metagenomeSequencing", self.conn, if_exists="replace", index=False
            )
        else:
            raise ValueError(
                f"You are missing one of the MMS tables: {neon_mms_data_tables}"
            )

        if all(k in sls_data for k in neon_sls_data_tables):
            sls_data["sls_metagenomicsPooling"].to_sql(
                "sls_metagenomicsPooling", self.conn, if_exists="replace", index=False
            )
            sls_data["sls_soilCoreCollection"].to_sql(
                "sls_soilCoreCollection", self.conn, if_exists="replace", index=False
            )
            sls_data["sls_soilChemistry"].to_sql(
                "sls_soilChemistry", self.conn, if_exists="replace", index=False
            )
            sls_data["sls_soilMoisture"].to_sql(
                "sls_soilMoisture", self.conn, if_exists="replace", index=False
            )
            sls_data["sls_soilpH"].to_sql(
                "sls_soilpH", self.conn, if_exists="replace", index=False
            )
        else:
            raise ValueError(
                f"You are missing one of the SLS tables: {neon_sls_data_tables}"
            )
        
        if all(k in neon_ntr_data_tables for k in neon_ntr_data_tables):
            sls_data["ntr_externalLab"].to_sql(
                "ntr_externalLab", self.conn, if_exists="replace", index=False
            )
            sls_data["ntr_internalLab"].to_sql(
                "ntr_internalLab", self.conn, if_exists="replace", index=False
            )
        else:
            raise ValueError(
                f"You are missing one of the SLS tables: {neon_sls_data_tables}"
            )

        neon_envo_mappings_file = "https://raw.githubusercontent.com/microbiomedata/nmdc-schema/main/assets/neon_mixs_env_triad_mappings/neon-nlcd-local-broad-mappings.tsv"
        neon_envo_terms = pd.read_csv(neon_envo_mappings_file, delimiter="\t")
        neon_envo_terms.to_sql(
            "neonEnvoTerms", self.conn, if_exists="replace", index=False
        )

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
                ),
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
            )
            if not biosample_row["decimalLatitude"].isna().any()
            else None,
            elev=nmdc.Float(biosample_row["elevation"].values[0])
            if not biosample_row["elevation"].isna().any()
            else None,
            collection_date=nmdc.TimestampValue(
                has_raw_value=biosample_row["collectDate"].values[0]
            )
            if not biosample_row["collectDate"].isna().any()
            else None,
            temp=nmdc.QuantityValue(
                has_raw_value=biosample_row["soilTemp"].values[0],
                has_numeric_value=biosample_row["soilTemp"].values[0],
                has_unit="degree celsius",
            )
            if not biosample_row["soilTemp"].isna().any()
            else None,
            depth=nmdc.QuantityValue(
                has_minimum_numeric_value=biosample_row["sampleTopDepth"].values[0]
                if not biosample_row["sampleTopDepth"].isna().any()
                else None,
                has_maximum_numeric_value=biosample_row["sampleBottomDepth"].values[0]
                if not biosample_row["sampleBottomDepth"].isna().any()
                else None,
            ),
            samp_collec_device=biosample_row["soilSamplingDevice"].values[0]
            if not biosample_row["soilSamplingDevice"].isna().any()
            else None,
            soil_horizon=f"{biosample_row['horizon'].values[0]} horizon"
            if not biosample_row["horizon"].isna().any()
            else None,
            analysis_type=biosample_row["sequenceAnalysisType"].values[0]
            if not biosample_row["sequenceAnalysisType"].isna().any()
            else None,
            env_package=nmdc.TextValue(
                has_raw_value=biosample_row["sampleType"].values[0]
            )
            if not biosample_row["sampleType"].isna().any()
            else None,
            nitro=nmdc.QuantityValue(
                has_raw_value=biosample_row["nitrogenPercent"].values[0],
                has_numeric_value=biosample_row["nitrogenPercent"].values[0],
            )
            if not biosample_row["nitrogenPercent"].isna().any()
            else None,
            org_carb=nmdc.QuantityValue(
                has_raw_value=biosample_row["organicCPercent"].values[0],
                has_numeric_value=biosample_row["organicCPercent"].values[0],
            )
            if not biosample_row["organicCPercent"].isna().any()
            else None,
            carb_nitro_ratio=nmdc.QuantityValue(
                has_raw_value=biosample_row["CNratio"].values[0],
                has_numeric_value=biosample_row["CNratio"].values[0],
            )
            if not biosample_row["CNratio"].isna().any()
            else None,
            ph_meth=nmdc.TextValue(
                has_raw_value=biosample_row["samplingProtocolVersion"].values[0]
            )
            if not biosample_row["samplingProtocolVersion"].isna().any()
            else None,
            ph=nmdc.Double(biosample_row["soilInWaterpH"].values[0])
            if not biosample_row["soilInWaterpH"].isna().any()
            else None,
            water_content=[biosample_row["soilMoisture"].values[0]]
            if not biosample_row["soilMoisture"].isna().any()
            else None,
            ammonium_nitrogen=nmdc.QuantityValue(
                has_raw_value=biosample_row["kclAmmoniumNConc"].values[0],
                has_numeric_value=biosample_row["kclAmmoniumNConc"].values[0],
            )
            if not biosample_row["kclAmmoniumNConc"].isna().any()
            else None,
            tot_nitro_content=nmdc.QuantityValue(
                has_raw_value=biosample_row["kclNitrateNitriteNConc"].values[0],
                has_numeric_value=biosample_row["kclNitrateNitriteNConc"].values[0],
            )
            if not biosample_row["kclNitrateNitriteNConc"].isna().any()
            else None,
        )

    def _translate_pooling_process(self) -> List[nmdc.PlannedProcess]:
        pass

    def _translate_extraction_process(self) -> List[nmdc.PlannedProcess]:
        pass

    def _translate_omics_processing(self) -> nmdc.OmicsProcessing:
        pass

    def get_database(self) -> nmdc.Database:
        database = nmdc.Database()

        # Joining sls_metagenomicsPooling and merged tables
        query = """
        SELECT sls_metagenomicsPooling.genomicsPooledIDList, sls_metagenomicsPooling.genomicsSampleID, merged.dnaSampleID, merged.sequenceAnalysisType
        FROM sls_metagenomicsPooling
        LEFT JOIN (
            SELECT mms_metagenomeDnaExtraction.dnaSampleID, mms_metagenomeDnaExtraction.genomicsSampleID, mms_metagenomeDnaExtraction.sequenceAnalysisType
            FROM mms_metagenomeSequencing
            LEFT JOIN mms_metagenomeDnaExtraction ON mms_metagenomeDnaExtraction.dnaSampleID = mms_metagenomeSequencing.dnaSampleID
        ) AS merged ON sls_metagenomicsPooling.genomicsSampleID = merged.genomicsSampleID
        """
        mms_sls_pooling_merged = pd.read_sql_query(query, self.conn)
        mms_sls_pooling_merged.to_sql(
            "mms_sls_pooling_merged", self.conn, if_exists="replace", index=False
        )

        # for each of the split values
        query = """
            WITH RECURSIVE split_values(sampleID, remaining_values, genomicsPooledIDList, dnaSampleID) AS (
                SELECT
                    CASE
                        WHEN instr(genomicsPooledIDList, '|') > 0 THEN substr(genomicsPooledIDList, 1, instr(genomicsPooledIDList, '|') - 1)
                        ELSE genomicsPooledIDList
                    END AS sampleID,
                    CASE
                        WHEN instr(genomicsPooledIDList, '|') > 0 THEN substr(genomicsPooledIDList, instr(genomicsPooledIDList, '|') + 1)
                        ELSE NULL
                    END AS remaining_values,
                    genomicsPooledIDList,
                    dnaSampleID
                FROM mms_sls_pooling_merged
                WHERE genomicsPooledIDList IS NOT NULL
                
                UNION ALL
                
                SELECT
                    CASE
                        WHEN instr(remaining_values, '|') > 0 THEN substr(remaining_values, 1, instr(remaining_values, '|') - 1)
                        ELSE remaining_values
                    END AS sampleID,
                    CASE
                        WHEN instr(remaining_values, '|') > 0 THEN substr(remaining_values, instr(remaining_values, '|') + 1)
                        ELSE NULL
                    END AS remaining_values,
                    genomicsPooledIDList,
                    dnaSampleID
                FROM split_values
                WHERE remaining_values IS NOT NULL
            )
            SELECT split_values.sampleID, split_values.genomicsPooledIDList, split_values.dnaSampleID, mms_sls_pooling_merged.sequenceAnalysisType
            FROM split_values
            LEFT JOIN mms_sls_pooling_merged ON split_values.dnaSampleID = mms_sls_pooling_merged.dnaSampleID
            """
        mms_sls_pooling_exploded = pd.read_sql_query(query, self.conn)
        mms_sls_pooling_exploded.to_sql(
            "mms_sls_pooling_exploded", self.conn, if_exists="replace", index=False
        )

        # Joining sls_soilCoreCollection and mms_sls_pooling_exploded tables
        query = """
        SELECT *
        FROM sls_soilCoreCollection
        LEFT JOIN mms_sls_pooling_exploded ON sls_soilCoreCollection.sampleID = mms_sls_pooling_exploded.sampleID
        WHERE sls_soilCoreCollection.sampleID IS NOT NULL
        """
        soil_biosamples = pd.read_sql_query(query, self.conn)

        soil_biosamples = soil_biosamples[soil_biosamples["dnaSampleID"].notna()]
        soil_biosamples = soil_biosamples.loc[
            :, ~soil_biosamples.columns.duplicated()
        ].copy()
        soil_biosamples.to_sql(
            "soil_biosamples", self.conn, if_exists="replace", index=False
        )

        query = """
        SELECT sc.sampleType, sc.nitrogenPercent, sc.organicCPercent, sc.CNratio, sc.analyticalRepNumber, sb.*
        FROM soil_biosamples sb
        LEFT JOIN sls_soilChemistry sc ON sb.sampleID = sc.sampleID
        """
        soil_biosamples_chemical = pd.read_sql_query(query, self.conn)
        soil_biosamples_chemical.to_sql(
            "soil_biosamples_chemical", self.conn, if_exists="replace", index=False
        )

        query = """
        SELECT sp.soilInWaterpH, sbc.*
        FROM soil_biosamples_chemical sbc
        LEFT JOIN sls_soilpH sp ON sbc.sampleID = sp.sampleID
        """
        soil_biosamples_ph = pd.read_sql_query(query, self.conn)
        soil_biosamples_ph.to_sql(
            "soil_biosamples_ph", self.conn, if_exists="replace", index=False
        )

        query = """
        SELECT sm.soilMoisture, sbp.*
        FROM soil_biosamples_ph sbp
        LEFT JOIN sls_soilMoisture sm ON sbp.sampleID = sm.sampleID
        """
        soil_biosamples_combined = pd.read_sql_query(query, self.conn)
        soil_biosamples_combined.to_sql(
            "soil_biosamples_combined", self.conn, if_exists="replace", index=False
        )
        
        query = """
            SELECT soil_biosamples_combined.*, ntr_externalLab.kclAmmoniumNConc, ntr_externalLab.kclNitrateNitriteNConc
            FROM soil_biosamples_combined
            LEFT JOIN ntr_internalLab ON soil_biosamples_combined.sampleID = ntr_internalLab.sampleID
            LEFT JOIN ntr_externalLab ON soil_biosamples_combined.sampleID = ntr_externalLab.sampleID
        """
        soil_biosamples_combined_ntr = pd.read_sql_query(query, self.conn)
        soil_biosamples_combined_ntr.to_sql(
            "soil_biosamples_combined_ntr", self.conn, if_exists="replace", index=False
        )

        query = """
            SELECT sbcbn.*, net.neon_nlcd_value, net.envo_id, net.envo_label, net.env_local_scale
            FROM soil_biosamples_combined_ntr sbcbn
            LEFT JOIN neonEnvoTerms net ON sbcbn.nlcdClass = net.neon_nlcd_value
        """
        soil_biosamples_envo = pd.read_sql_query(query, self.conn)
        soil_biosamples_envo.to_sql(
            "soil_biosamples_envo", self.conn, if_exists="replace", index=False
        )

        neon_biosample_ids = soil_biosamples_envo["sampleID"]
        nmdc_biosample_ids = self._id_minter("nmdc:Biosample", len(neon_biosample_ids))
        neon_to_nmdc_biosample_ids = dict(zip(neon_biosample_ids, nmdc_biosample_ids))

        for neon_id, nmdc_id in neon_to_nmdc_biosample_ids.items():
            biosample_row = soil_biosamples_envo[
                soil_biosamples_envo["sampleID"] == neon_id
            ]

            database.biosample_set.append(
                self._translate_biosample(neon_id, nmdc_id, biosample_row)
            )

        return database
