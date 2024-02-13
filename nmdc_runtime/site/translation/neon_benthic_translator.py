import re
import sqlite3

import pandas as pd
import requests_cache

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
)


BENTHIC_BROAD_SCALE_MAPPINGS = {
    "stream": {"term_id": "ENVO:01000253", "term_name": "freshwater river biome"}
}

BENTHIC_LOCAL_SCALE_MAPPINGS = {
    "pool": {"term_id": "ENVO:03600094", "term_name": "stream pool"},
    "run": {"term_id": "ENVO:03600095", "term_name": "stream run"},
    "step pool": {"term_id": "ENVO:03600096", "term_name": "step pool"},
    "riffle": {"term_id": "ENVO:00000148", "term_name": "riffle"},
    "stepPool": {"term_id": "ENVO:03600096", "term_name": "step pool"},
}

BENTHIC_ENV_MEDIUM_MAPPINGS = {
    "plant-associated": {
        "term_id": "ENVO:01001057",
        "term_name": "environment associated with a plant part or small plant",
    },
    "sediment": {"term_id": "ENVO:00002007", "term_name": "sediment"},
    "biofilm": {"term_id": "ENVO:00002034", "term_name": "biofilm"},
}


class NeonBenthicDataTranslator(Translator):
    def __init__(
        self,
        benthic_data: dict,
        site_code_mapping: dict,
        neon_envo_mappings_file: pd.DataFrame,
        neon_raw_data_file_mappings_file: pd.DataFrame,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)

        self.conn = sqlite3.connect("neon.db")
        requests_cache.install_cache("neon_api_cache")

        neon_amb_data_tables = (
            "mms_benthicMetagenomeSequencing",
            "mms_benthicMetagenomeDnaExtraction",
            "amb_fieldParent",
        )

        if all(k in benthic_data for k in neon_amb_data_tables):
            benthic_data["mms_benthicMetagenomeSequencing"].to_sql(
                "mms_benthicMetagenomeSequencing",
                self.conn,
                if_exists="replace",
                index=False,
            )
            benthic_data["mms_benthicMetagenomeDnaExtraction"].to_sql(
                "mms_benthicMetagenomeDnaExtraction",
                self.conn,
                if_exists="replace",
                index=False,
            )
            benthic_data["amb_fieldParent"].to_sql(
                "amb_fieldParent", self.conn, if_exists="replace", index=False
            )
        else:
            raise ValueError(
                f"You are missing one of the aquatic benthic microbiome tables: {neon_amb_data_tables}"
            )

        neon_envo_mappings_file.to_sql(
            "neonEnvoTerms", self.conn, if_exists="replace", index=False
        )

        self.neon_raw_data_file_mappings_df = neon_raw_data_file_mappings_file
        self.neon_raw_data_file_mappings_df.to_sql(
            "neonRawDataFile", self.conn, if_exists="replace", index=False
        )

        self.site_code_mapping = site_code_mapping

    def _translate_biosample(
        self, neon_id: str, nmdc_id: str, biosample_row: pd.DataFrame
    ) -> nmdc.Biosample:
        return nmdc.Biosample(
            id=nmdc_id,
            part_of="nmdc:sty-11-pzmd0x14",
            env_broad_scale=_create_controlled_identified_term_value(
                BENTHIC_BROAD_SCALE_MAPPINGS.get(
                    biosample_row["aquaticSiteType"].values[0]
                ).get("term_id"),
                BENTHIC_BROAD_SCALE_MAPPINGS.get(
                    biosample_row["aquaticSiteType"].values[0]
                ).get("term_name"),
            ),
            env_local_scale=_create_controlled_identified_term_value(
                BENTHIC_LOCAL_SCALE_MAPPINGS.get(
                    biosample_row["habitatType"].values[0]
                ).get("term_id"),
                BENTHIC_LOCAL_SCALE_MAPPINGS.get(
                    biosample_row["habitatType"].values[0]
                ).get("term_name"),
            ),
            env_medium=_create_controlled_identified_term_value(
                BENTHIC_ENV_MEDIUM_MAPPINGS.get(
                    biosample_row["sampleMaterial"].values[0]
                ).get("term_id"),
                BENTHIC_ENV_MEDIUM_MAPPINGS.get(
                    biosample_row["sampleMaterial"].values[0]
                ).get("term_name"),
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
            samp_size=_create_quantity_value(
                biosample_row["fieldSampleVolume"].values[0], "mL"
            ),
            geo_loc_name=_create_text_value(
                self.site_code_mapping[biosample_row["siteID"].values[0]]
                if biosample_row["siteID"].values[0]
                else None
            ),
            type="nmdc:Biosample",
            analysis_type="metagenomics",
            biosample_categories="NEON",
            depth=nmdc.QuantityValue(
                has_minimum_numeric_value=nmdc.Float("0"),
                has_maximum_numeric_value=nmdc.Float("1"),
                has_unit="meters",
            ),
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

    def get_database(self):
        database = nmdc.Database()

        query = """
            SELECT
                merged.laboratoryName,
                merged.sequencingFacilityID,
                merged.processedDate,
                merged.dnaSampleID,
                merged.dnaSampleCode,
                merged.internalLabID,
                merged.instrument_model,
                merged.sequencingMethod,
                merged.investigation_type,
                merged.qaqcStatus,
                merged.ncbiProjectID,
                merged.genomicsSampleID,
                merged.sequenceAnalysisType,
                merged.sampleMass,
                merged.nucleicAcidConcentration,
                afp.aquaticSiteType,
                afp.habitatType,
                afp.sampleMaterial,
                afp.geneticSampleID,
                afp.elevation,
                afp.fieldSampleVolume,
                afp.decimalLatitude,
                afp.decimalLongitude,
                afp.siteID,
                afp.sampleID,
                afp.collectDate
            FROM 
                (
                    SELECT
                        bs.collectDate,
                        bs.laboratoryName,
                        bs.sequencingFacilityID,
                        bs.processedDate,
                        bs.dnaSampleID,
                        bs.dnaSampleCode,
                        bs.internalLabID,
                        bs.instrument_model,
                        bs.sequencingMethod,
                        bs.investigation_type,
                        bs.qaqcStatus,
                        bs.ncbiProjectID,
                        bd.genomicsSampleID,
                        bd.sequenceAnalysisType,
                        bd.sampleMass,
                        bd.nucleicAcidConcentration
                    FROM 
                        mms_benthicMetagenomeSequencing AS bs
                    JOIN 
                        mms_benthicMetagenomeDnaExtraction AS bd
                    ON 
                        bs.dnaSampleID = bd.dnaSampleID
                ) AS merged
            LEFT JOIN amb_fieldParent AS afp
            ON
                merged.genomicsSampleID = afp.geneticSampleID
        """
        benthic_samples = pd.read_sql_query(query, self.conn)
        benthic_samples.to_sql(
            "benthicSamples", self.conn, if_exists="replace", index=False
        )

        neon_biosample_ids = benthic_samples["sampleID"]
        nmdc_biosample_ids = self._id_minter("nmdc:Biosample", len(neon_biosample_ids))
        neon_to_nmdc_biosample_ids = dict(zip(neon_biosample_ids, nmdc_biosample_ids))

        neon_extraction_ids = benthic_samples["sampleID"]
        nmdc_extraction_ids = self._id_minter(
            "nmdc:Extraction", len(neon_extraction_ids)
        )
        neon_to_nmdc_extraction_ids = dict(
            zip(neon_extraction_ids, nmdc_extraction_ids)
        )

        neon_extraction_processed_ids = benthic_samples["sampleID"]
        nmdc_extraction_processed_ids = self._id_minter(
            "nmdc:ProcessedSample", len(neon_extraction_processed_ids)
        )
        neon_to_nmdc_extraction_processed_ids = dict(
            zip(neon_extraction_processed_ids, nmdc_extraction_processed_ids)
        )

        neon_lib_prep_ids = benthic_samples["sampleID"]
        nmdc_lib_prep_ids = self._id_minter(
            "nmdc:LibraryPreparation", len(neon_lib_prep_ids)
        )
        neon_to_nmdc_lib_prep_ids = dict(zip(neon_lib_prep_ids, nmdc_lib_prep_ids))

        neon_lib_prep_processed_ids = benthic_samples["sampleID"]
        nmdc_lib_prep_processed_ids = self._id_minter(
            "nmdc:ProcessedSample", len(neon_lib_prep_processed_ids)
        )
        neon_to_nmdc_lib_prep_processed_ids = dict(
            zip(neon_lib_prep_processed_ids, nmdc_lib_prep_processed_ids)
        )

        neon_omprc_ids = benthic_samples["sampleID"]
        nmdc_omprc_ids = self._id_minter("nmdc:OmicsProcessing", len(neon_omprc_ids))
        neon_to_nmdc_omprc_ids = dict(zip(neon_omprc_ids, nmdc_omprc_ids))

        neon_raw_data_file_mappings_df = self.neon_raw_data_file_mappings_df
        neon_raw_file_paths = neon_raw_data_file_mappings_df["rawDataFilePath"]
        nmdc_data_object_ids = self._id_minter(
            "nmdc:DataObject", len(neon_raw_file_paths)
        )
        neon_to_nmdc_data_object_ids = dict(
            zip(neon_raw_file_paths, nmdc_data_object_ids)
        )

        for neon_id, nmdc_id in neon_to_nmdc_biosample_ids.items():
            biosample_row = benthic_samples[benthic_samples["sampleID"] == neon_id]

            database.biosample_set.append(
                self._translate_biosample(neon_id, nmdc_id, biosample_row)
            )

        for neon_id, nmdc_id in neon_to_nmdc_extraction_ids.items():
            extraction_row = benthic_samples[benthic_samples["sampleID"] == neon_id]

            extraction_input = neon_to_nmdc_biosample_ids.get(neon_id)
            processed_sample_id = neon_to_nmdc_extraction_processed_ids.get(neon_id)

            if extraction_input is not None and processed_sample_id is not None:
                database.extraction_set.append(
                    self._translate_extraction_process(
                        nmdc_id,
                        extraction_input,
                        processed_sample_id,
                        extraction_row,
                    )
                )

                genomics_sample_id = _get_value_or_none(
                    extraction_row, "genomicsSampleID"
                )

                database.processed_sample_set.append(
                    self._translate_processed_sample(
                        processed_sample_id,
                        f"Extracted DNA from {genomics_sample_id}",
                    )
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
        filtered_neon_raw_data_files_dict = {
            key: value
            for key, value in neon_raw_data_files_dict.items()
            if len(value) <= 2
        }

        for neon_id, nmdc_id in neon_to_nmdc_lib_prep_ids.items():
            lib_prep_row = benthic_samples[benthic_samples["sampleID"] == neon_id]

            lib_prep_input = neon_to_nmdc_extraction_processed_ids.get(neon_id)
            processed_sample_id = neon_to_nmdc_lib_prep_processed_ids.get(neon_id)

            if lib_prep_input is not None and processed_sample_id is not None:
                database.library_preparation_set.append(
                    self._translate_library_preparation(
                        nmdc_id,
                        lib_prep_input,
                        processed_sample_id,
                        lib_prep_row,
                    )
                )

                dna_sample_id = _get_value_or_none(lib_prep_row, "dnaSampleID")

                database.processed_sample_set.append(
                    self._translate_processed_sample(
                        processed_sample_id,
                        f"Library preparation for {dna_sample_id}",
                    )
                )

                has_output = None
                has_output_do_ids = []

                if dna_sample_id in filtered_neon_raw_data_files_dict:
                    has_output = filtered_neon_raw_data_files_dict[dna_sample_id]
                    for item in has_output:
                        if item in neon_to_nmdc_data_object_ids:
                            has_output_do_ids.append(neon_to_nmdc_data_object_ids[item])

                        checksum = None
                        do_type = None

                        checksum = neon_raw_data_file_mappings_df[
                            neon_raw_data_file_mappings_df["rawDataFilePath"] == item
                        ]["checkSum"].values[0]
                        if "_R1.fastq.gz" in item:
                            do_type = "Metagenome Raw Read 1"
                        elif "_R2.fastq.gz" in item:
                            do_type = "Metagenome Raw Read 2"

                        database.data_object_set.append(
                            self._translate_data_object(
                                neon_to_nmdc_data_object_ids.get(item),
                                item,
                                do_type,
                                checksum,
                            )
                        )

                    database.omics_processing_set.append(
                        self._translate_omics_processing(
                            neon_to_nmdc_omprc_ids.get(neon_id),
                            processed_sample_id,
                            has_output_do_ids,
                            lib_prep_row,
                        )
                    )

        return database
