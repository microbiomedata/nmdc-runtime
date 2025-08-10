from io import StringIO
import pytest
from nmdc_runtime.site.translation.neon_benthic_translator import (
    NeonBenthicDataTranslator,
)
import pandas as pd


# Mock data for testing
benthic_data = {
    "mms_benthicMetagenomeSequencing": pd.DataFrame(
        [
            {
                "uid": "409b85ed-9f92-4948-a29a-e1bbcad0a3bf",
                "domainID": "D13",
                "siteID": "WLOU",
                "namedLocation": "WLOU.AOS.reach",
                "collectDate": "2018-07-26T15:51Z",
                "laboratoryName": "Battelle Applied Genomics",
                "sequencingFacilityID": "Battelle Memorial Institute",
                "processedDate": "2019-03-01",
                "dnaSampleID": "WLOU.20180726.AMC.EPILITHON.1-DNA1",
                "dnaSampleCode": "",
                "internalLabID": "BMI_AquaticPlate6WellA5",
                "barcodeSequence": "",
                "instrument_model": "NextSeq550",
                "sequencingMethod": "Illumina",
                "investigation_type": "metagenome",
                "sequencingConcentration": 0.3,
                "labPrepMethod": "BMI_metagenomicsSequencingSOP_v3",
                "sequencingProtocol": "BMI_metagenomicsSequencingSOP_v3",
                "illuminaAdapterKit": "Kapa Dual Index Kit",
                "illuminaIndex1": "CGCTCATT",
                "illuminaIndex2": "ACGTCCTG",
                "sequencerRunID": "HWVWKBGX7",
                "qaqcStatus": "Pass",
                "sampleTotalReadNumber": 8809468,
                "sampleFilteredReadNumber": 2952793.0,
                "averageFilteredReadQuality": 34.1,
                "ambiguousBasesNumber": 10359.0,
                "ncbiProjectID": "PRJNA406976",
                "processedSeqFileName": "BMI_AquaticPlate6WellA5_mms.fastq.zip",
                "analyzedBy": "hZbYwvE98vBBnHMG2IzsBA==",
                "remarks": "",
                "dataQF": "",
            }
        ]
    ),
    "mms_benthicMetagenomeDnaExtraction": pd.DataFrame(
        [
            {
                "uid": "0110830f-3b71-4361-8f37-0747f2c29d95",
                "domainID": "D13",
                "siteID": "WLOU",
                "namedLocation": "WLOU.AOS.reach",
                "laboratoryName": "Battelle Applied Genomics",
                "collectDate": "2018-07-26T15:51Z",
                "processedDate": "2019-02-12",
                "genomicsSampleID": "WLOU.20180726.AMC.EPILITHON.1",
                "genomicsSampleCode": "B00000010911",
                "deprecatedVialID": "",
                "dnaSampleID": "WLOU.20180726.AMC.EPILITHON.1-DNA1",
                "dnaSampleCode": "",
                "internalLabID": "BMI_AquaticPlate6WellA5",
                "sequenceAnalysisType": "marker gene and metagenomics",
                "testMethod": "BMI_dnaExtractionSOP_v1",
                "sampleMaterial": "biofilm",
                "sampleMass": None,  # No value provided in the data for sampleMass
                "samplePercent": 100.0,
                "nucleicAcidConcentration": 4.7,
                "nucleicAcidQuantMethod": "Quantus",
                "nucleicAcidPurity": None,  # No value provided in the data for nucleicAcidPurity
                "nucleicAcidRange": None,  # No value provided in the data for nucleicAcidRange
                "dnaPooledStatus": "N",
                "qaqcStatus": "Pass",
                "dnaProcessedBy": "hZbYwvE98vCkIvnhsCYWaA==",
                "remarks": "",
                "dataQF": "",
            }
        ]
    ),
    "amb_fieldParent": pd.DataFrame(
        [
            {
                "uid": "7434ec98-d4f4-49ec-a670-73446abc57b8",
                "domainID": "D13",
                "siteID": "WLOU",
                "namedLocation": "WLOU.AOS.reach",
                "aquaticSiteType": "stream",
                "decimalLatitude": 39.891366,
                "decimalLongitude": -105.915395,
                "coordinateUncertainty": 1000.0,
                "elevation": 2908.0,
                "elevationUncertainty": 1000.0,
                "geodeticDatum": "WGS84",
                "collectDate": "2018-07-26T15:51Z",
                "eventID": "WLOU.20180726",
                "sampleID": "WLOU.20180726.AMC.EPILITHON.1",
                "sampleCode": "",
                "samplingImpractical": "",
                "habitatType": "riffle",
                "sampleNumber": 1,
                "aquMicrobeType": "epilithon",
                "aquMicrobeScrubArea": 0.000802,
                "samplingProtocolVersion": "NEON.DOC.003044vC",
                "substratumSizeClass": "cobble",
                "fieldSampleVolume": 125.0,
                "remarks": "",
                "collectedBy": "rlehrter@battelleecology.org",
                "recordedBy": "Jswenson@battelleecology.org",
                "archiveSampleCond": "",
                "archiveID": "WLOU.20180726.AMC.EPILITHON.1",
                "archiveSampleCode": "B00000010896",
                "archiveFilteredSampleVolume": 25.0,
                "geneticSampleCond": "",
                "geneticSampleID": "WLOU.20180726.AMC.EPILITHON.1",
                "geneticSampleCode": "B00000010911",
                "geneticFilteredSampleVolume": 25.0,
                "metagenomicSampleCond": "",
                "metagenomicsCollected": "",
                "metagenomicSampleID": "",
                "metagenomicSampleCode": "",
                "metagenomicFilteredSampleVolume": "",
                "sampleMaterial": "biofilm",
                "labSampleMedium": "filter",
                "dataQF": "",
            }
        ]
    ),
    "mms_benthicRawDataFiles": pd.DataFrame(
        [
            {
                "uid": "74cfedfb-b369-43f2-81e8-035dadaabd34",
                "domainID": "D13",
                "siteID": "WLOU",
                "namedLocation": "WLOU.AOS.reach",
                "laboratoryName": "Battelle Applied Genomics",
                "sequencingFacilityID": "Battelle Memorial Institute",
                "setDate": "2018-07-26T15:51Z",
                "collectDate": "2018-07-26T15:51Z",
                "sequencerRunID": "HWVWKBGX7",
                "dnaSampleID": "WLOU.20180726.AMC.EPILITHON.1-DNA1",
                "dnaSampleCode": "LV7005092900",
                "internalLabID": "BMI_AquaticPlate6WellA5",
                "rawDataFileName": "BMI_HWVWKBGX7_AquaticPlate6WellA5_R2.fastq.gz",
                "rawDataFileDescription": "R2 metagenomic archive of fastq files",
                "rawDataFilePath": "https://storage.neonscience.org/neon-microbial-raw-seq-files/2023/BMI_HWVWKBGX7_mms_R2/BMI_HWVWKBGX7_AquaticPlate6WellA5_R2.fastq.gz",
                "remarks": "",
                "dataQF": "",
            },
            {
                "uid": "6dfc7444-3878-4db1-85da-b4430f52a023",
                "domainID": "D13",
                "siteID": "WLOU",
                "namedLocation": "WLOU.AOS.reach",
                "laboratoryName": "Battelle Applied Genomics",
                "sequencingFacilityID": "Battelle Memorial Institute",
                "setDate": "2018-07-26T15:51Z",
                "collectDate": "2018-07-26T15:51Z",
                "sequencerRunID": "HWVWKBGX7",
                "dnaSampleID": "WLOU.20180726.AMC.EPILITHON.1-DNA1",
                "dnaSampleCode": "LV7005092900",
                "internalLabID": "BMI_AquaticPlate6WellA5",
                "rawDataFileName": "BMI_HWVWKBGX7_AquaticPlate6WellA5_R1.fastq.gz",
                "rawDataFileDescription": "R1 metagenomic archive of fastq files",
                "rawDataFilePath": "https://storage.neonscience.org/neon-microbial-raw-seq-files/2023/BMI_HWVWKBGX7_mms_R1/BMI_HWVWKBGX7_AquaticPlate6WellA5_R1.fastq.gz",
                "remarks": "",
                "dataQF": "",
            },
        ]
    ),
}


def neon_envo_mappings_file():
    tsv_data = """neon_nlcd_value\tmrlc_edomvd_before_hyphen\tmrlc_edomv\tenvo_alt_id\tenvo_id\tenvo_label\tenv_local_scale\tsubCLassOf and part of path to biome\tother justification\tbiome_label\tbiome_id\tenv_broad_scale
deciduousForest\tDeciduous Forest\t41\tNLCD:41\tENVO:01000816\tarea of deciduous forest\tarea of deciduous forest [ENVO:01000816]\t --subCLassOf-->terretrial environmental zone--part of-->\t\tterrestrial biome\tENVO:00000448\tterrestrial biome [ENVO:00000448]"""

    return pd.read_csv(StringIO(tsv_data), delimiter="\t")


def neon_raw_data_file_mappings_file():
    tsv_data_dna = """dnaSampleID\tsequencerRunID\tinternalLabID\trawDataFileName\trawDataFileDescription\trawDataFilePath\tcheckSum
WLOU.20180726.AMC.EPILITHON.1-DNA1\tHWVWKBGX7\tAquaticPlate6WellA5\tBMI_HWVWKBGX7_AquaticPlate6WellA5_R2.fastq.gz\tR2 metagenomic archive of fastq files\thttps://storage.neonscience.org/neon-microbial-raw-seq-files/2023/BMI_HWVWKBGX7_mms_R2/BMI_HWVWKBGX7_AquaticPlate6WellA5_R2.fastq.gz\t16c11600c77818979b11a05ce7899d6c
WLOU.20180726.AMC.EPILITHON.1-DNA1\tHWVWKBGX7\tAquaticPlate6WellA5\tBMI_HWVWKBGX7_AquaticPlate6WellA5_R1.fastq.gz\tR1 metagenomic archive of fastq files\thttps://storage.neonscience.org/neon-microbial-raw-seq-files/2023/BMI_HWVWKBGX7_mms_R1/BMI_HWVWKBGX7_AquaticPlate6WellA5_R1.fastq.gz\t378052f3aeb3d587e3f94588247e7bda"""

    return pd.read_csv(StringIO(tsv_data_dna), delimiter="\t")


def site_code_mapping():
    return {"WLOU": "USA: Colorado, West St Louis Creek"}


mock_gold_nmdc_instrument_map_df = pd.DataFrame(
    {
        "NEON sequencingMethod": [
            "NextSeq550",
            "Illumina HiSeq",
        ],
        "NMDC instrument_set id": [
            "nmdc:inst-14-xz5tb342",
            "nmdc:inst-14-79zxap02",
        ],
    }
)


class TestNeonBenthicDataTranslator:
    @pytest.fixture
    def translator(self, test_minter):
        return NeonBenthicDataTranslator(
            benthic_data=benthic_data,
            site_code_mapping=site_code_mapping(),
            neon_envo_mappings_file=neon_envo_mappings_file(),
            neon_raw_data_file_mappings_file=neon_raw_data_file_mappings_file(),
            neon_nmdc_instrument_map_df=mock_gold_nmdc_instrument_map_df,
            id_minter=test_minter,
        )

    def test_get_database(self, translator):
        """Full end-to-end test for get_database() method in NeonBenthicDataTranslator.
        This test checks that the objects created for the various classes connected in
        the MaterialEntity/PlannedProcess bipartite graph represented in the schema have
        the correct inputs and outputs (`has_input`, `has_output`) between them.
        """
        translator.samp_procsm_dict = {
            "WLOU.20180726.AMC.EPILITHON.1": "nmdc:procsm-11-x1y2z3"
        }

        database = translator.get_database()

        assert len(database.biosample_set) == 1
        assert len(database.material_processing_set) == 2
        assert len(database.data_generation_set) == 1
        assert len(database.processed_sample_set) == 2
        assert len(database.data_object_set) == 2

        biosample_list = database.biosample_set
        biosample = biosample_list[0]
        assert biosample.name == "WLOU.20180726.AMC.EPILITHON.1"

        extraction_list = [
            proc
            for proc in database.material_processing_set
            if proc.type == "nmdc:Extraction"
        ]
        library_prep_list = [
            proc
            for proc in database.material_processing_set
            if proc.type == "nmdc:LibraryPreparation"
        ]
        ntseq_list = [
            proc
            for proc in database.data_generation_set
            if proc.type == "nmdc:NucleotideSequencing"
        ]

        assert len(extraction_list) == 1
        assert len(library_prep_list) == 1
        assert len(ntseq_list) == 1

        extraction = extraction_list[0]
        libprep = library_prep_list[0]
        ntseq = ntseq_list[0]

        ext_input_list = extraction.has_input
        ext_output_list = extraction.has_output
        assert len(ext_input_list) == 1
        assert len(ext_output_list) == 1

        biosample_ids = [b.id for b in database.biosample_set]
        assert ext_input_list[0] in biosample_ids

        processed_sample_ids = [ps.id for ps in database.processed_sample_set]
        assert ext_output_list[0] in processed_sample_ids

        lp_input_list = libprep.has_input
        lp_output_list = libprep.has_output
        assert len(lp_input_list) == 1
        assert len(lp_output_list) == 1

        assert lp_input_list == ext_output_list
        assert lp_output_list[0] in processed_sample_ids

        ntseq_input_list = ntseq.has_input
        ntseq_output_list = ntseq.has_output

        assert len(ntseq_input_list) == 1
        assert ntseq_input_list[0] == "nmdc:procsm-11-x1y2z3"

        assert len(ntseq_output_list) == 2
        data_object_ids = [obj.id for obj in database.data_object_set]
        for do_id in ntseq_output_list:
            assert do_id in data_object_ids

        for do_id in ntseq_output_list:
            matching_dobj = [x for x in database.data_object_set if x.id == do_id]
            assert len(matching_dobj) == 1
            dobj = matching_dobj[0]
            assert dobj.type == "nmdc:DataObject"
            assert dobj.name in [
                "BMI_HWVWKBGX7_AquaticPlate6WellA5_R1.fastq.gz",
                "BMI_HWVWKBGX7_AquaticPlate6WellA5_R2.fastq.gz",
            ]
