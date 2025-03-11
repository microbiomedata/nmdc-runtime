import re
import sqlite3
from typing import Optional, Union

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
        neon_nmdc_instrument_map_df: pd.DataFrame = pd.DataFrame(),
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
            "mms_benthicRawDataFiles",  # <--- ensure this is present
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
            benthic_data["mms_benthicRawDataFiles"].to_sql(
                "mms_benthicRawDataFiles",
                self.conn,
                if_exists="replace",
                index=False,
            )
        else:
            raise ValueError(
                f"You are missing one of the aquatic benthic microbiome tables: {neon_amb_data_tables}"
            )

        neon_envo_mappings_file.to_sql(
            "neonEnvoTerms", self.conn, if_exists="replace", index=False
        )

        self.neon_raw_data_file_mappings_df = benthic_data["mms_benthicRawDataFiles"]

        self.site_code_mapping = site_code_mapping

        self.neon_nmdc_instrument_map_df = neon_nmdc_instrument_map_df

    def _translate_manifest(self, manifest_id: str) -> nmdc.Manifest:
        return nmdc.Manifest(
            id=manifest_id,
            manifest_category=nmdc.ManifestCategoryEnum.poolable_replicates,
            type="nmdc:Manifest",
        )

    def _translate_biosample(
        self, neon_id: str, nmdc_id: str, biosample_row: pd.DataFrame
    ) -> nmdc.Biosample:
        return nmdc.Biosample(
            id=nmdc_id,
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
                has_unit="m",
                type="nmdc:QuantityValue",
            ),
            associated_studies=["nmdc:sty-11-pzmd0x14"],
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
            qc_status=_get_value_or_none(extraction_row, "qaqcStatus"),
            processing_institution=processing_institution,
            type="nmdc:Extraction",
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
        process is fed as input to an NucleotideSequencing object.

        :param library_preparation_id: Minted id for LibraryPreparation process.
        :param library_preparation_input: Input to LibraryPreparation process is output from
        Extraction process.
        :param processed_sample_id: Minted ProcessedSample id which is output of LibraryPreparation
        is also input to NucleotideSequencing.
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
            type="nmdc:LibraryPreparation",
        )

    def _get_instrument_id(self, instrument_model: Union[str | None]) -> str:
        if not instrument_model:
            raise ValueError(
                f"instrument_model '{instrument_model}' could not be found in the NEON-NMDC instrument mapping TSV file."
            )

        df = self.neon_nmdc_instrument_map_df
        matching_row = df[
            df["NEON sequencingMethod"].str.contains(instrument_model, case=False)
        ]

        if not matching_row.empty:
            nmdc_instrument_id = matching_row["NMDC instrument_set id"].values[0]
            return nmdc_instrument_id

    def _translate_nucleotide_sequencing(
        self,
        nucleotide_sequencing_id: str,
        processed_sample_id: str,
        raw_data_file_data: str,
        nucleotide_sequencing_row: pd.DataFrame,
    ):
        """Create nmdc NucleotideSequencing object. This class typically models the run of a
        Bioinformatics workflow on sequence data from a biosample. The input to an NucleotideSequencing
        process is the output from a LibraryPreparation process, and the output of NucleotideSequencing
        is a DataObject which has the FASTQ sequence file URLs embedded in them.

        :param nucleotide_sequencing_id: Minted id for an NucleotideSequencing process.
        :param processed_sample_id: ProcessedSample that is the output of LibraryPreparation.
        :param raw_data_file_data: R1/R2 DataObjects which have links to workflow processed output
        files embedded in them.
        :param nucleotide_sequencing_row: DataFrame with metadata for an NucleotideSequencing workflow
        process/run.
        :return: NucleotideSequencing object that models a Bioinformatics workflow process/run.
        """
        processing_institution = None
        sequencing_facility = _get_value_or_none(
            nucleotide_sequencing_row, "sequencingFacilityID"
        )
        if sequencing_facility is not None:
            if re.search("Battelle", sequencing_facility, re.IGNORECASE):
                processing_institution = "Battelle"
            elif re.search("Argonne", sequencing_facility, re.IGNORECASE):
                processing_institution = "ANL"

        return nmdc.NucleotideSequencing(
            id=nucleotide_sequencing_id,
            has_input=processed_sample_id,
            has_output=raw_data_file_data,
            processing_institution=processing_institution,
            ncbi_project_name=_get_value_or_none(
                nucleotide_sequencing_row, "ncbiProjectID"
            ),
            instrument_used=self._get_instrument_id(
                _get_value_or_none(nucleotide_sequencing_row, "instrument_model")
            ),
            name=f"Benthic microbial communities - {_get_value_or_none(nucleotide_sequencing_row, 'dnaSampleID')}",
            type="nmdc:NucleotideSequencing",
            associated_studies=["nmdc:sty-11-pzmd0x14"],
            analyte_category="metagenome",
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
        return nmdc.ProcessedSample(
            id=processed_sample_id, name=sample_id, type="nmdc:ProcessedSample"
        )

    def _translate_data_object(
        self, do_id: str, url: str, do_type: str, manifest_id: str
    ) -> nmdc.DataObject:
        """Create nmdc DataObject which is the output of a NucleotideSequencing process. This
        object mainly contains information about the sequencing file that was generated as
        the result of running a Bioinformatics workflow on a certain ProcessedSample, which
        is the result of a LibraryPreparation process.

        :param do_id: NMDC minted DataObject id.
        :param url: URL of zipped FASTQ file on NEON file server. Retrieved from file provided
        by Hugh Cross at NEON.
        :param do_type: Indicate whether it is FASTQ for Read 1 or Read 2 (paired end sequencing).
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
            data_object_type=do_type,
            in_manifest=manifest_id,
        )

    def get_database(self) -> nmdc.Database:
        database = nmdc.Database()

        join_query = """
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
            FROM (
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
                FROM mms_benthicMetagenomeSequencing AS bs
                JOIN mms_benthicMetagenomeDnaExtraction AS bd
                ON bs.dnaSampleID = bd.dnaSampleID
            ) AS merged
            LEFT JOIN amb_fieldParent AS afp
            ON merged.genomicsSampleID = afp.geneticSampleID
        """
        benthic_samples = pd.read_sql_query(join_query, self.conn)
        benthic_samples.to_sql(
            "benthicSamples", self.conn, if_exists="replace", index=False
        )

        sample_ids = benthic_samples["sampleID"]
        nmdc_biosample_ids = self._id_minter("nmdc:Biosample", len(sample_ids))
        neon_to_nmdc_biosample_ids = dict(zip(sample_ids, nmdc_biosample_ids))

        nmdc_extraction_ids = self._id_minter("nmdc:Extraction", len(sample_ids))
        neon_to_nmdc_extraction_ids = dict(zip(sample_ids, nmdc_extraction_ids))

        nmdc_extraction_processed_ids = self._id_minter(
            "nmdc:ProcessedSample", len(sample_ids)
        )
        neon_to_nmdc_extraction_processed_ids = dict(
            zip(sample_ids, nmdc_extraction_processed_ids)
        )

        nmdc_libprep_ids = self._id_minter("nmdc:LibraryPreparation", len(sample_ids))
        neon_to_nmdc_libprep_ids = dict(zip(sample_ids, nmdc_libprep_ids))

        nmdc_libprep_processed_ids = self._id_minter(
            "nmdc:ProcessedSample", len(sample_ids)
        )
        neon_to_nmdc_libprep_processed_ids = dict(
            zip(sample_ids, nmdc_libprep_processed_ids)
        )

        nmdc_ntseq_ids = self._id_minter("nmdc:NucleotideSequencing", len(sample_ids))
        neon_to_nmdc_ntseq_ids = dict(zip(sample_ids, nmdc_ntseq_ids))

        raw_df = self.neon_raw_data_file_mappings_df
        raw_file_paths = raw_df["rawDataFilePath"]
        dataobject_ids = self._id_minter("nmdc:DataObject", len(raw_file_paths))
        neon_to_nmdc_dataobject_ids = dict(zip(raw_file_paths, dataobject_ids))

        for neon_id, biosample_id in neon_to_nmdc_biosample_ids.items():
            row = benthic_samples[benthic_samples["sampleID"] == neon_id]
            if row.empty:
                continue

            # Example of how you might call _translate_biosample:
            database.biosample_set.append(
                self._translate_biosample(neon_id, biosample_id, row)
            )

        for neon_id, extraction_id in neon_to_nmdc_extraction_ids.items():
            row = benthic_samples[benthic_samples["sampleID"] == neon_id]
            if row.empty:
                continue

            biosample_id = neon_to_nmdc_biosample_ids.get(neon_id)
            extraction_ps_id = neon_to_nmdc_extraction_processed_ids.get(neon_id)

            if biosample_id and extraction_ps_id:
                database.material_processing_set.append(
                    self._translate_extraction_process(
                        extraction_id, biosample_id, extraction_ps_id, row
                    )
                )
                genomics_sample_id = _get_value_or_none(row, "genomicsSampleID")
                database.processed_sample_set.append(
                    self._translate_processed_sample(
                        extraction_ps_id,
                        f"Extracted DNA from {genomics_sample_id}",
                    )
                )

        query2 = """
            SELECT dnaSampleID, GROUP_CONCAT(rawDataFilePath, '|') AS rawDataFilePaths
            FROM mms_benthicRawDataFiles
            GROUP BY dnaSampleID
        """
        raw_data_files_df = pd.read_sql_query(query2, self.conn)
        dna_files_dict = (
            raw_data_files_df.set_index("dnaSampleID")["rawDataFilePaths"]
            .str.split("|")
            .to_dict()
        )

        dna_sample_to_manifest_id: dict[str, str] = {}

        for neon_id, libprep_id in neon_to_nmdc_libprep_ids.items():
            row = benthic_samples[benthic_samples["sampleID"] == neon_id]
            if row.empty:
                continue

            extr_ps_id = neon_to_nmdc_extraction_processed_ids.get(neon_id)
            libprep_ps_id = neon_to_nmdc_libprep_processed_ids.get(neon_id)
            if not extr_ps_id or not libprep_ps_id:
                continue

            database.material_processing_set.append(
                self._translate_library_preparation(
                    libprep_id, extr_ps_id, libprep_ps_id, row
                )
            )

            dna_sample_id = _get_value_or_none(row, "dnaSampleID")
            database.processed_sample_set.append(
                self._translate_processed_sample(
                    libprep_ps_id,
                    f"Library preparation for {dna_sample_id}",
                )
            )

            filepaths_for_dna: list[str] = dna_files_dict.get(dna_sample_id, [])
            if not filepaths_for_dna:
                # no raw files => skip
                ntseq_id = neon_to_nmdc_ntseq_ids.get(neon_id)
                if ntseq_id:
                    continue
                continue

            # If multiple => we create a Manifest
            manifest_id: Optional[str] = None
            if len(filepaths_for_dna) > 2:
                if dna_sample_id not in dna_sample_to_manifest_id:
                    new_man_id = self._id_minter("nmdc:Manifest", 1)[0]
                    dna_sample_to_manifest_id[dna_sample_id] = new_man_id
                    database.manifest_set.append(self._translate_manifest(new_man_id))
                manifest_id = dna_sample_to_manifest_id[dna_sample_id]

            has_input_value = self.samp_procsm_dict.get(neon_id)
            if not has_input_value:
                continue

            dataobject_ids_for_run: list[str] = []
            for fp in filepaths_for_dna:
                if fp not in neon_to_nmdc_dataobject_ids:
                    continue
                do_id = neon_to_nmdc_dataobject_ids[fp]

                do_type = None
                if "_R1.fastq.gz" in fp:
                    do_type = "Metagenome Raw Read 1"
                elif "_R2.fastq.gz" in fp:
                    do_type = "Metagenome Raw Read 2"

                database.data_object_set.append(
                    self._translate_data_object(
                        do_id=do_id,
                        url=fp,
                        do_type=do_type,
                        manifest_id=manifest_id,
                    )
                )
                dataobject_ids_for_run.append(do_id)

            ntseq_id = neon_to_nmdc_ntseq_ids.get(neon_id)
            if ntseq_id:
                database.data_generation_set.append(
                    self._translate_nucleotide_sequencing(
                        ntseq_id,
                        has_input_value,  # <--- from self.samp_procsm_dict
                        dataobject_ids_for_run,
                        row,
                    )
                )

        return database
