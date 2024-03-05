import re
import sqlite3
from typing import List

import pandas as pd

from nmdc_schema import nmdc
from nmdc_runtime.site.translation.translator import Translator
from nmdc_runtime.site.util import get_basename
from nmdc_runtime.site.translation.neon_utils import (
    _get_value_or_none,
    _create_controlled_identified_term_value,
    _create_controlled_term_value,
    _create_geolocation_value,
    _create_quantity_value,
    _create_timestamp_value,
    _create_text_value,
    _create_double_value,
)


class NeonSoilDataTranslator(Translator):
    def __init__(
        self,
        mms_data: dict,
        sls_data: dict,
        neon_envo_mappings_file: pd.DataFrame,
        neon_raw_data_file_mappings_file: pd.DataFrame,
        *args,
        **kwargs,
    ) -> None:
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
            "ntr_externalLab",
            "ntr_internalLab",
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
                f"You are missing one of the metagenomic microbe soil tables: {neon_mms_data_tables}"
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
            sls_data["ntr_externalLab"].to_sql(
                "ntr_externalLab", self.conn, if_exists="replace", index=False
            )
            sls_data["ntr_internalLab"].to_sql(
                "ntr_internalLab", self.conn, if_exists="replace", index=False
            )
        else:
            raise ValueError(
                f"You are missing one of the soil periodic tables: {neon_sls_data_tables}"
            )

        neon_envo_mappings_file.to_sql(
            "neonEnvoTerms", self.conn, if_exists="replace", index=False
        )

        self.neon_raw_data_file_mappings_df = neon_raw_data_file_mappings_file
        self.neon_raw_data_file_mappings_df.to_sql(
            "neonRawDataFile", self.conn, if_exists="replace", index=False
        )

    def _translate_biosample(
        self, neon_id: str, nmdc_id: str, biosample_row: pd.DataFrame
    ) -> nmdc.Biosample:
        """
        Translate a row from the biosamples DataFrame into an nmdc Biosample object.
        This method takes in a DataFrame with all the biosample metadata in biosample_row,
        `nmdc_id`, which is the NMDC minted biosample id corresponding to a NEON biosample id
        in `neon_id`.

        :param neon_id: Neon biosample id, which is typically the value from `sampleID` column.
        :param nmdc_id: NMDC minted biosample id, corresponding to `neon_id`.
        :param biosample_row: DataFrame with biosample metadata.
        :return: nmdc Biosample object with all the biosample metadata including correctly
        specified ids.
        """
        return nmdc.Biosample(
            id=nmdc_id,
            part_of="nmdc:sty-11-34xj1150",
            env_broad_scale=_create_controlled_identified_term_value(
                "ENVO:00000446", "terrestrial biome"
            ),
            env_local_scale=_create_controlled_identified_term_value(
                biosample_row["envo_id"].values[0],
                biosample_row["envo_label"].values[0],
            ),
            env_medium=_create_controlled_identified_term_value(
                "ENVO:00001998", "soil"
            ),
            name=neon_id,
            lat_lon=_create_geolocation_value(
                biosample_row["decimalLatitude"].values[0],
                biosample_row["decimalLongitude"].values[0],
            ),
            elev=nmdc.Float(biosample_row["elevation"].values[0]),
            collection_date=_create_timestamp_value(
                biosample_row["collectDate"].values[0]
            ),
            temp=_create_quantity_value(biosample_row["soilTemp"].values[0], "Celsius"),
            depth=nmdc.QuantityValue(
                has_minimum_numeric_value=_get_value_or_none(
                    biosample_row, "sampleTopDepth"
                ),
                has_maximum_numeric_value=_get_value_or_none(
                    biosample_row, "sampleBottomDepth"
                ),
                has_unit="m",
            ),
            samp_collec_device=_get_value_or_none(biosample_row, "soilSamplingDevice"),
            soil_horizon=_get_value_or_none(biosample_row, "horizon"),
            analysis_type=_get_value_or_none(biosample_row, "sequenceAnalysisType"),
            env_package=_create_text_value(biosample_row["sampleType"].values[0]),
            nitro=_create_quantity_value(
                biosample_row["nitrogenPercent"].values[0], "percent"
            ),
            org_carb=_create_quantity_value(
                biosample_row["organicCPercent"].values[0], "percent"
            ),
            carb_nitro_ratio=_create_quantity_value(
                biosample_row["CNratio"].values[0], None
            ),
            ph=_create_double_value(biosample_row["soilInWaterpH"].values[0]),
            water_content=(
                [f"{biosample_row['soilMoisture'].values[0]} g of water/g of dry soil"]
                if not biosample_row["soilMoisture"].isna().any()
                else None
            ),
            ammonium_nitrogen=_create_quantity_value(
                biosample_row["kclAmmoniumNConc"].values[0], "mg/L"
            ),
            tot_nitro_content=_create_quantity_value(
                biosample_row["kclNitrateNitriteNConc"].values[0], "mg/L"
            ),
            type="nmdc:Biosample",
        )

    def _translate_pooling_process(
        self,
        nmdc_id: str,
        processed_sample_id: str,
        bsm_input_values_list: List[str],
        pooling_row: pd.DataFrame,
    ) -> nmdc.Pooling:
        """
        Create an nmdc Pooling process. The input to a Pooling process is a list of biosamples,
        and the output of the process is a nmdc ProcessedSample. The metadata related to Pooling
        process is in pooling_row.

        :param nmdc_id: Minted NMDC Pooling process id.
        :param processed_sample_id: Minted NMDC ProcessedSample id that is output from Pooling.
        :param bsm_input_values_list: List of biosample ids that are input to Pooling.
        :param pooling_row: DataFrame containing Pooling metadata.
        :return: nmdc Pooling object.
        """
        return nmdc.Pooling(
            id=nmdc_id,
            has_output=processed_sample_id,
            has_input=bsm_input_values_list,
            start_date=_get_value_or_none(pooling_row, "startDate"),
            end_date=_get_value_or_none(pooling_row, "collectDate"),
        )

    def _translate_processed_sample(
        self, processed_sample_id: str, sample_id: str
    ) -> nmdc.ProcessedSample:
        """
        Create an nmdc ProcessedSample. ProcessedSample is typically the output of a PlannedProcess
        like Pooling, Extraction, LibraryPreparation, etc. We are using this to create a
        reference for the nmdc minted ProcessedSample ids in `processed_sample_set`. We are
        associating the minted ids with the name of the sample it is coming from which can be
        a value from either the `genomicsSampleID` column or from the `dnaSampleID` column.

        :param processed_sample_id: NMDC minted ProcessedSampleID.
        :param sample_id: Value from `genomicsSampleID` or `dnaSampleID` column.
        :return: ProcessedSample objects to be stored in `processed_sample_set`.
        """
        return nmdc.ProcessedSample(id=processed_sample_id, name=sample_id)

    def _translate_data_object(
        self, do_id: str, url: str, do_type: str, checksum: str
    ) -> nmdc.DataObject:
        """Create nmdc DataObject which is the output of an OmicsProcessing process. This
        object mainly contains information about the sequencing file that was generated as
        the result of running a Bioinformatics workflow on a certain ProcessedSample, which
        is the result of a LibraryPreparation process.

        :param do_id: NMDC minted DataObject id.
        :param url: URL of zipped FASTQ file on NEON file server. Retrieved from file provided
        by Hugh Cross at NEON.
        :param do_type: Indicate whether it is FASTQ for Read 1 or Read 2 (paired end sequencing).
        :param checksum: Checksum value for FASTQ in zip file, once again provided by Hugh Cross
        at NEON.
        :return: DataObject with all the sequencing file metadata.
        """
        file_name = get_basename(url)
        basename = file_name.split(".", 1)[0]

        return nmdc.DataObject(
            id=do_id,
            name=file_name,
            url=url,
            description=f"sequencing results for {basename}",
            type="nmdc:DataObject",
            md5_checksum=checksum,
            data_object_type=do_type,
        )

    def _translate_extraction_process(
        self,
        extraction_id: str,
        extraction_input: str,
        processed_sample_id: str,
        extraction_row: pd.DataFrame,
    ) -> nmdc.Extraction:
        """
        Create an nmdc Extraction process, which is a process to model the DNA extraction in
        a metagenome sequencing experiment. The input to an Extraction process is the
        output from a Pooling process.

        :param extraction_id: Minted id for Extraction process.
        :param extraction_input: Input to an Extraction process is the output from a Pooling process.
        :param processed_sample_id: Output of Extraction process is a ProcessedSample.
        :param extraction_row: DataFrame with Extraction process metadata.
        :return: Extraction process object.
        """
        processing_institution = None
        laboratory_name = _get_value_or_none(extraction_row, "laboratoryName")
        if laboratory_name is not None:
            if re.search("Battelle", laboratory_name, re.IGNORECASE):
                processing_institution = "Battelle"
            elif re.search("Argonne", laboratory_name, re.IGNORECASE):
                processing_institution = "ANL"

        return nmdc.Extraction(
            id=extraction_id,
            has_input=extraction_input,
            has_output=processed_sample_id,
            start_date=_get_value_or_none(extraction_row, "collectDate"),
            end_date=_get_value_or_none(extraction_row, "processedDate"),
            input_mass=_create_quantity_value(
                _get_value_or_none(extraction_row, "sampleMass"), "g"
            ),
            quality_control_report=nmdc.QualityControlReport(
                status=_get_value_or_none(extraction_row, "qaqcStatus")
            ),
            processing_institution=processing_institution,
        )

    def _translate_library_preparation(
        self,
        library_preparation_id: str,
        library_preparation_input: str,
        processed_sample_id: str,
        library_preparation_row: pd.DataFrame,
    ):
        """
        Create LibraryPreparation process object. The input to LibraryPreparation process
        is the output ProcessedSample from an Extraction process. The output of LibraryPreparation
        process is fed as input to an OmicsProcessing object.

        :param library_preparation_id: Minted id for LibraryPreparation process.
        :param library_preparation_input: Input to LibraryPreparation process is output from
        Extraction process.
        :param processed_sample_id: Minted ProcessedSample id which is output of LibraryPreparation
        is also input to OmicsProcessing.
        :param library_preparation_row: Metadata required to populate LibraryPreparation.
        :return: Object that using LibraryPreparation process model.
        """
        processing_institution = None
        laboratory_name = _get_value_or_none(library_preparation_row, "laboratoryName")
        if laboratory_name is not None:
            if re.search("Battelle", laboratory_name, re.IGNORECASE):
                processing_institution = "Battelle"
            elif re.search("Argonne", laboratory_name, re.IGNORECASE):
                processing_institution = "ANL"

        return nmdc.LibraryPreparation(
            id=library_preparation_id,
            has_input=library_preparation_input,
            has_output=processed_sample_id,
            start_date=_get_value_or_none(library_preparation_row, "collectDate"),
            end_date=_get_value_or_none(library_preparation_row, "processedDate"),
            processing_institution=processing_institution,
        )

    def _translate_omics_processing(
        self,
        omics_processing_id: str,
        processed_sample_id: str,
        raw_data_file_data: str,
        omics_processing_row: pd.DataFrame,
    ) -> nmdc.OmicsProcessing:
        """Create nmdc OmicsProcessing object. This class typically models the run of a
        Bioinformatics workflow on sequence data from a biosample. The input to an OmicsProcessing
        process is the output from a LibraryPreparation process, and the output of OmicsProcessing
        is a DataObject which has the FASTQ sequence file URLs embedded in them.

        :param omics_processing_id: Minted id for an OmicsProcessing process.
        :param processed_sample_id: ProcessedSample that is the output of LibraryPreparation.
        :param raw_data_file_data: R1/R2 DataObjects which have links to workflow processed output
        files embedded in them.
        :param omics_processing_row: DataFrame with metadata for an OmicsProcessing workflow
        process/run.
        :return: OmicsProcessing object that models a Bioinformatics workflow process/run.
        """
        processing_institution = None
        sequencing_facility = _get_value_or_none(
            omics_processing_row, "sequencingFacilityID"
        )
        if sequencing_facility is not None:
            if re.search("Battelle", sequencing_facility, re.IGNORECASE):
                processing_institution = "Battelle"
            elif re.search("Argonne", sequencing_facility, re.IGNORECASE):
                processing_institution = "ANL"

        return nmdc.OmicsProcessing(
            id=omics_processing_id,
            has_input=processed_sample_id,
            has_output=raw_data_file_data,
            processing_institution=processing_institution,
            ncbi_project_name=_get_value_or_none(omics_processing_row, "ncbiProjectID"),
            omics_type=_create_controlled_term_value(
                omics_processing_row["investigation_type"].values[0]
            ),
            instrument_name=f"{_get_value_or_none(omics_processing_row, 'sequencingMethod')} {_get_value_or_none(omics_processing_row, 'instrument_model')}",
            part_of="nmdc:sty-11-34xj1150",
            name=f"Terrestrial soil microbial communities - {_get_value_or_none(omics_processing_row, 'dnaSampleID')}",
            type="nmdc:OmicsProcessing",
        )

    def get_database(self) -> nmdc.Database:
        """The main entry point method that combines/uses all the above main (Biosample, Pooling, etc.)
        nmdc object creation methods as well as the nmdc type (QuantityValue, GeolocationValue, etc.)
        creation methods, to make an nmdc Database object. It populates multiple sets in the Mongo database -
            * `biosample_set`: uses `_translate_biosample()`
            * `pooling_set`: uses `_translate_pooling_process()`
            * `extraction_set`: uses `_translate_extraction_process()`
            * `library_preparation_set`: uses `_translate_library_preparation()`
            * `omics_processing_set`: uses `_translate_omics_processing()`
            * `processed_sample_set`: uses `_translate_processed_sample()`
            * `data_object_set`: uses `_translate_data_object()`
        The core Biosample information is in the `sls_soilCoreCollection` table. However, we
        want to restrict ourselves to only those biosamples that have been sequenced and QC'd, so we
        only get those samples that are in the `mms_metagenomeSequencing` (and `mms_metagenomeDnaExtraction`)
        tables. A lot of these samples are pooled samples, so we look in the `sls_metagenomicsPooling`
        to get all individual biosamples. Then we look through various other tables that contain metadata like
        biogeochemical measurement values (in `sls_soilChemistry`, `sls_soilWaterpH`, etc.) and more.

        Note: You will see a lot of SQLite queries being run in the body of this method. That is because
        after evaluation, the running time overhead for pandas was very high, and the pipeline was simply
        taking too long to run.
        """
        database = nmdc.Database()

        # Joining sls_metagenomicsPooling and merged tables
        query = """
                SELECT 
                sls_metagenomicsPooling.genomicsPooledIDList, 
                sls_metagenomicsPooling.genomicsSampleID, 
                merged.dnaSampleID, 
                merged.sequenceAnalysisType,
                merged.laboratoryName,
                merged.collectDate,
                merged.processedDate,
                merged.sampleMaterial,
                merged.sampleMass,
                merged.nucleicAcidConcentration,
                merged.qaqcStatus
            FROM sls_metagenomicsPooling
            LEFT JOIN (
                SELECT 
                    mms_metagenomeDnaExtraction.dnaSampleID, 
                    mms_metagenomeDnaExtraction.genomicsSampleID, 
                    mms_metagenomeDnaExtraction.sequenceAnalysisType,
                    mms_metagenomeDnaExtraction.laboratoryName,
                    mms_metagenomeDnaExtraction.collectDate,
                    mms_metagenomeDnaExtraction.processedDate,
                    mms_metagenomeDnaExtraction.sampleMaterial,
                    mms_metagenomeDnaExtraction.sampleMass,
                    mms_metagenomeDnaExtraction.nucleicAcidConcentration,
                    mms_metagenomeDnaExtraction.qaqcStatus
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
                WITH RECURSIVE split_values(sampleID, remaining_values, genomicsPooledIDList, dnaSampleID, genomicsSampleID, laboratoryName, collectDate, processedDate, sampleMaterial, sampleMass, nucleicAcidConcentration, qaqcStatus) AS (
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
                        dnaSampleID,
                        genomicsSampleID,
                        laboratoryName,
                        collectDate,
                        processedDate,
                        sampleMaterial,
                        sampleMass,
                        nucleicAcidConcentration,
                        qaqcStatus
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
                        dnaSampleID,
                        genomicsSampleID,
                        laboratoryName,
                        collectDate,
                        processedDate,
                        sampleMaterial,
                        sampleMass,
                        nucleicAcidConcentration,
                        qaqcStatus
                    FROM split_values
                    WHERE remaining_values IS NOT NULL
                )
                SELECT
                    split_values.sampleID,
                    split_values.genomicsPooledIDList,
                    split_values.dnaSampleID,
                    split_values.laboratoryName,
                    split_values.collectDate,
                    split_values.processedDate,
                    split_values.sampleMaterial,
                    split_values.sampleMass,
                    split_values.nucleicAcidConcentration,
                    split_values.qaqcStatus,
                    mms_sls_pooling_merged.sequenceAnalysisType,
                    mms_sls_pooling_merged.genomicsSampleID
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

        query = """
            SELECT dnaSampleID, startDate, collectDate, GROUP_CONCAT(sampleID, '|') AS sampleIDs
            FROM soil_biosamples_envo
            GROUP BY dnaSampleID
        """
        mg_pooling_table = pd.read_sql_query(query, self.conn)
        pooling_ids_dict = (
            mg_pooling_table.set_index("dnaSampleID")["sampleIDs"]
            .str.split("|")
            .to_dict()
        )

        query = """
            SELECT dnaSampleID, GROUP_CONCAT(rawDataFilePath, '|') AS rawDataFilePaths
            FROM neonRawDataFile
            GROUP BY dnaSampleID
        """
        neon_raw_data_files = pd.read_sql_query(query, self.conn)
        neon_raw_data_files_dict = (
            neon_raw_data_files.set_index("dnaSampleID")["rawDataFilePaths"]
            .str.split("|")
            .to_dict()
        )

        query = """
            SELECT dnaSampleID, genomicsSampleID, collectDate, laboratoryName, processedDate, sampleMass, qaqcStatus
            FROM soil_biosamples_envo
            GROUP BY genomicsSampleID
        """
        extraction_table = pd.read_sql_query(query, self.conn)

        query = """
            SELECT 
                mms_metagenomeDnaExtraction.dnaSampleID, 
                mms_metagenomeDnaExtraction.genomicsSampleID, 
                mms_metagenomeDnaExtraction.sequenceAnalysisType,
                mms_metagenomeDnaExtraction.laboratoryName,
                mms_metagenomeDnaExtraction.collectDate,
                mms_metagenomeDnaExtraction.processedDate,
                mms_metagenomeSequencing.sequencingFacilityID,
                mms_metagenomeSequencing.ncbiProjectID,
                mms_metagenomeSequencing.investigation_type,
                mms_metagenomeSequencing.sequencingMethod,
                mms_metagenomeSequencing.instrument_model
            FROM mms_metagenomeSequencing 
            LEFT JOIN mms_metagenomeDnaExtraction ON mms_metagenomeDnaExtraction.dnaSampleID = mms_metagenomeSequencing.dnaSampleID
        """
        library_preparation_table = pd.read_sql_query(query, self.conn)
        omics_processing_table = pd.read_sql_query(query, self.conn)

        nmdc_pooling_ids = self._id_minter("nmdc:Pooling", len(pooling_ids_dict))
        neon_to_nmdc_pooling_ids = dict(
            zip(list(pooling_ids_dict.keys()), nmdc_pooling_ids)
        )

        nmdc_processed_sample_ids = self._id_minter(
            "nmdc:ProcessedSample", len(pooling_ids_dict)
        )
        pooling_processed_sample_ids = dict(
            zip(list(pooling_ids_dict.keys()), nmdc_processed_sample_ids)
        )

        extraction_ids = extraction_table["genomicsSampleID"]
        nmdc_extraction_ids = self._id_minter("nmdc:Extraction", len(extraction_ids))
        nmdc_extraction_processed_sample_ids = self._id_minter(
            "nmdc:ProcessedSample", len(nmdc_extraction_ids)
        )
        neon_to_nmdc_extraction_ids = dict(zip(extraction_ids, nmdc_extraction_ids))
        neon_to_nmdc_extraction_processed_sample_ids = dict(
            zip(extraction_ids, nmdc_extraction_processed_sample_ids)
        )

        library_prepration_ids = library_preparation_table["dnaSampleID"]
        nmdc_library_prepration_ids = self._id_minter(
            "nmdc:LibraryPreparation", len(library_prepration_ids)
        )

        nmdc_library_preparation_processed_sample_ids = self._id_minter(
            "nmdc:ProcessedSample", len(nmdc_library_prepration_ids)
        )
        neon_to_nmdc_library_prepration_ids = dict(
            zip(library_prepration_ids, nmdc_library_prepration_ids)
        )
        neon_to_nmdc_library_preparation_processed_sample_ids = dict(
            zip(library_prepration_ids, nmdc_library_preparation_processed_sample_ids)
        )

        omics_processing_ids = omics_processing_table["dnaSampleID"]
        nmdc_omics_processing_ids = self._id_minter(
            "nmdc:OmicsProcessing", len(omics_processing_ids)
        )
        neon_to_nmdc_omics_processing_ids = dict(
            zip(omics_processing_ids, nmdc_omics_processing_ids)
        )

        neon_raw_data_file_mappings_df = self.neon_raw_data_file_mappings_df
        neon_raw_file_paths = neon_raw_data_file_mappings_df["rawDataFilePath"]
        nmdc_data_object_ids = self._id_minter(
            "nmdc:DataObject", len(neon_raw_file_paths)
        )
        neon_to_nmdc_data_object_ids = dict(
            zip(neon_raw_file_paths, nmdc_data_object_ids)
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

        for dna_sample_id, bsm_sample_ids in pooling_ids_dict.items():
            pooling_process_id = neon_to_nmdc_pooling_ids[dna_sample_id]
            processed_sample_id = pooling_processed_sample_ids[dna_sample_id]

            bsm_values_list = [
                neon_to_nmdc_biosample_ids[key]
                for key in bsm_sample_ids
                if key in neon_to_nmdc_biosample_ids
            ]
            bsm_values_list = list(set(bsm_values_list))

            pooling_row = mg_pooling_table[
                mg_pooling_table["dnaSampleID"] == dna_sample_id
            ]

            # if the number of biosamples that are input to a pooling process
            # is one or less, then ignore it and go straight to extraction
            if len(bsm_values_list) > 1:
                database.pooling_set.append(
                    self._translate_pooling_process(
                        pooling_process_id,
                        processed_sample_id,
                        bsm_values_list,
                        pooling_row,
                    )
                )

                database.processed_sample_set.append(
                    self._translate_processed_sample(processed_sample_id, dna_sample_id)
                )

        for genomics_sample_id, extraction_id in neon_to_nmdc_extraction_ids.items():
            processed_sample_id = neon_to_nmdc_extraction_processed_sample_ids[
                genomics_sample_id
            ]
            dna_sample_input = extraction_table[
                extraction_table["genomicsSampleID"] == genomics_sample_id
            ]["dnaSampleID"].values[0]
            extraction_input = pooling_processed_sample_ids[dna_sample_input]

            extraction_row = extraction_table[
                extraction_table["genomicsSampleID"] == genomics_sample_id
            ]

            genomics_pooled_id_list = mms_sls_pooling_merged[
                mms_sls_pooling_merged["genomicsSampleID"] == genomics_sample_id
            ]["genomicsPooledIDList"].values[0]

            # handler for creating extraction process records
            # for both pooled and non-pooled samples
            if "|" in genomics_pooled_id_list:
                database.extraction_set.append(
                    self._translate_extraction_process(
                        extraction_id,
                        extraction_input,
                        processed_sample_id,
                        extraction_row,
                    )
                )

                database.processed_sample_set.append(
                    self._translate_processed_sample(
                        processed_sample_id, genomics_sample_id
                    )
                )
            else:
                neon_biosample_id = soil_biosamples_envo[
                    soil_biosamples_envo["sampleID"] == genomics_pooled_id_list
                ]["sampleID"].values[0]

                extraction_input = neon_to_nmdc_biosample_ids[neon_biosample_id]

                database.extraction_set.append(
                    self._translate_extraction_process(
                        extraction_id,
                        extraction_input,
                        processed_sample_id,
                        extraction_row,
                    )
                )

        for (
            dna_sample_id,
            library_preparation_id,
        ) in neon_to_nmdc_library_prepration_ids.items():
            processed_sample_id = neon_to_nmdc_library_preparation_processed_sample_ids[
                dna_sample_id
            ]

            omics_processing_id = neon_to_nmdc_omics_processing_ids[dna_sample_id]

            genomics_sample_id = library_preparation_table[
                library_preparation_table["dnaSampleID"] == dna_sample_id
            ]["genomicsSampleID"].values[0]

            if genomics_sample_id in neon_to_nmdc_extraction_processed_sample_ids:
                library_preparation_input = (
                    neon_to_nmdc_extraction_processed_sample_ids[genomics_sample_id]
                )

                library_preparation_row = library_preparation_table[
                    library_preparation_table["dnaSampleID"] == dna_sample_id
                ]

                database.library_preparation_set.append(
                    self._translate_library_preparation(
                        library_preparation_id,
                        library_preparation_input,
                        processed_sample_id,
                        library_preparation_row,
                    )
                )

                database.processed_sample_set.append(
                    self._translate_processed_sample(processed_sample_id, dna_sample_id)
                )

                has_output = None
                has_output_do_ids = []

                if dna_sample_id in neon_raw_data_files_dict:
                    has_output = neon_raw_data_files_dict[dna_sample_id]
                    for item in has_output:
                        if item in neon_to_nmdc_data_object_ids:
                            has_output_do_ids.append(neon_to_nmdc_data_object_ids[item])

                    database.omics_processing_set.append(
                        self._translate_omics_processing(
                            omics_processing_id,
                            processed_sample_id,
                            has_output_do_ids,
                            library_preparation_row,
                        )
                    )

        for raw_file_path, nmdc_data_object_id in neon_to_nmdc_data_object_ids.items():
            checksum = None
            do_type = None

            checksum = neon_raw_data_file_mappings_df[
                neon_raw_data_file_mappings_df["rawDataFilePath"] == raw_file_path
            ]["checkSum"].values[0]
            if "_R1.fastq.gz" in raw_file_path:
                do_type = "Metagenome Raw Read 1"
            elif "_R2.fastq.gz" in raw_file_path:
                do_type = "Metagenome Raw Read 2"

            database.data_object_set.append(
                self._translate_data_object(
                    nmdc_data_object_id, raw_file_path, do_type, checksum
                )
            )

        return database
