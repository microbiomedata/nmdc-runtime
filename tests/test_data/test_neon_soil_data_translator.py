from io import StringIO
import pytest
from nmdc_runtime.site.translation.neon_soil_translator import NeonSoilDataTranslator
from nmdc_runtime.site.translation.neon_utils import (
    _create_controlled_identified_term_value,
    _create_controlled_term_value,
    _create_timestamp_value,
    _get_value_or_none,
)
import pandas as pd

# Mock data for testing
mms_data = {
    "mms_metagenomeDnaExtraction": pd.DataFrame(
        [
            {
                "uid": "cad15c09-898b-4885-b131-142650b75a76",
                "domainID": "D02",
                "siteID": "BLAN",
                "namedLocation": "BLAN_005.basePlot.bgc",
                "laboratoryName": "Battelle Applied Genomics",
                "collectDate": "2020-07-13T15:54Z",
                "processedDate": "2021-08-19",
                "genomicsSampleID": "BLAN_005-M-20200713-COMP",
                "deprecatedVialID": "",
                "dnaSampleID": "BLAN_005-M-20200713-COMP-DNA1",
                "internalLabID": "20S_08_0661",
                "sequenceAnalysisType": "metagenomics",
                "testMethod": "BMI_dnaExtractionSOP_v5",
                "sampleMaterial": "soil",
                "sampleMass": 0.25,
                "nucleicAcidConcentration": 21.0,
                "nucleicAcidQuantMethod": "Quantus",
                "nucleicAcidPurity": "",
                "nucleicAcidRange": "",
                "dnaPooledStatus": "N",
                "qaqcStatus": "Pass",
                "dnaProcessedBy": "rxnG2CiGqmvoVyhy2f/SOg==",
                "remarks": "",
                "dataQF": "",
            }
        ]
    ),
    "mms_metagenomeSequencing": pd.DataFrame(
        [
            {
                "uid": "d5164c16-dc0e-4ea4-a0e4-233255118da8",
                "domainID": "D02",
                "siteID": "BLAN",
                "namedLocation": "BLAN_005.basePlot.bgc",
                "collectDate": "2020-07-13T15:54Z",
                "laboratoryName": "Battelle Applied Genomics",
                "sequencingFacilityID": "Battelle Memorial Institute",
                "processedDate": "2021-09-27",
                "dnaSampleID": "BLAN_005-M-20200713-COMP-DNA1",
                "internalLabID": "20S_08_0661",
                "barcodeSequence": "",
                "instrument_model": "NextSeq550",
                "sequencingMethod": "Illumina",
                "investigation_type": "metagenome",
                "sequencingConcentration": 1.2,
                "labPrepMethod": "BMI_metagenomicsSequencingSOP_v3",
                "sequencingProtocol": "BMI_metagenomicsSequencingSOP_v3",
                "illuminaAdapterKit": "Kapa Dual Index Kit",
                "illuminaIndex1": "TCGCTAGA",
                "illuminaIndex2": "GCAGTACT",
                "sequencerRunID": "HVT2HBGXJ",
                "qaqcStatus": "Pass",
                "sampleTotalReadNumber": 8870472,
                "sampleFilteredReadNumber": 8117460.0,
                "ncbiProjectID": "PRJNA406974",
                "analyzedBy": "hZbYwvE98vC74xef62GeRA==",
                "remarks": "",
                "dataQF": "",
            }
        ]
    ),
}

sls_data = {
    "sls_metagenomicsPooling": pd.DataFrame(
        [
            {
                "uid": "1b9386c3-f28c-4757-96a7-066a32aa1c0e",
                "domainID": "D02",
                "siteID": "BLAN",
                "plotID": "BLAN_005",
                "namedLocation": "BLAN_005.basePlot.bgc",
                "startDate": "2020-07-13T14:34Z",
                "collectDate": "2020-07-13T15:54Z",
                "eventID": "BLAN.peakGreenness.2020",
                "samplingProtocolVersion": "NEON.DOC.014048vN",
                "genomicsPooledIDList": "BLAN_005-M-8-0-20200713|BLAN_005-M-1-39.5-20200713|BLAN_005-M-32-35-20200713",
                "genomicsSampleID": "BLAN_005-M-20200713-COMP",
                "genomicsSampleCode": "B00000051899",
                "sampleCondition": "OK",
                "processedBy": "",
                "remarks": "",
                "genomicsDataQF": "",
            }
        ]
    ),
    "sls_soilCoreCollection": pd.DataFrame(
        [
            {
                "uid": "8e490914-0efa-42e2-a10f-4e3a7c429dcf",
                "domainID": "D02",
                "siteID": "BLAN",
                "plotID": "BLAN_005",
                "namedLocation": "BLAN_005.basePlot.bgc",
                "plotType": "distributed",
                "nlcdClass": "deciduousForest",
                "subplotID": 21.0,
                "coreCoordinateX": 8.0,
                "coreCoordinateY": 0.0,
                "geodeticDatum": "WGS84",
                "decimalLatitude": 39.087456,
                "decimalLongitude": -77.971942,
                "coordinateUncertainty": 20.1,
                "elevation": 137.4,
                "elevationUncertainty": 0.21,
                "samplingProtocolVersion": "NEON.DOC.014048vN",
                "startDate": "2020-07-13T14:34Z",
                "collectDate": "2020-07-13T14:34Z",
                "sampleTiming": "peakGreenness",
                "biophysicalCriteria": "OK - no known exceptions",
                "eventID": "BLAN.peakGreenness.2020",
                "standingWaterDepth": 0.0,
                "nTransBoutType": "tInitial",
                "boutType": "microbesBiomassBGC",
                "samplingImpractical": "OK",
                "incubationMethod": "no incubation",
                "incubationCondition": "",
                "sampleID": "BLAN_005-M-8-0-20200713",
                "sampleCode": "",
                "toxicodendronPossible": "N",
                "horizon": "M",
                "horizonDetails": "OK",
                "soilTemp": 20.3,
                "litterDepth": 3.0,
                "sampleTopDepth": 0.0,
                "sampleBottomDepth": 29.5,
                "sampleExtent": "obstruction",
                "soilSamplingDevice": "corer",
                "soilCoreCount": 1.0,
                "geneticSampleID": "BLAN_005-M-8-0-20200713-GEN",
                "geneticSampleCode": "B00000051898",
                "geneticSampleCondition": "OK",
                "geneticSamplePrepMethod": "",
                "geneticArchiveSample1ID": "BLAN_005-M-8-0-20200713-GA1",
                "geneticArchiveSample1Code": "C00000015996",
                "geneticArchiveSample2ID": "BLAN_005-M-8-0-20200713-GA2",
                "geneticArchiveSample2Code": "C00000015997",
                "geneticArchiveSample3ID": "BLAN_005-M-8-0-20200713-GA3",
                "geneticArchiveSample3Code": "C00000015998",
                "geneticArchiveSample4ID": "BLAN_005-M-8-0-20200713-GA4",
                "geneticArchiveSample4Code": "C00000015999",
                "geneticArchiveSample5ID": "BLAN_005-M-8-0-20200713-GA5",
                "geneticArchiveSample5Code": "C00000016000",
                "geneticArchiveSamplePrepMethod": "",
                "geneticArchiveContainer": "",
                "biomassID": "BLAN_005-M-8-0-20200713-BM",
                "biomassCode": "B00000051775",
                "biomassSampleCondition": "OK",
                "remarks": "",
                "collectedBy": "0000-0002-1718-8623",
                "dataQF": "",
            },
            {
                "uid": "76f0c596-ad54-4ed8-b016-61287332916e",
                "domainID": "D02",
                "siteID": "BLAN",
                "plotID": "BLAN_005",
                "namedLocation": "BLAN_005.basePlot.bgc",
                "plotType": "distributed",
                "nlcdClass": "deciduousForest",
                "subplotID": 41.0,
                "coreCoordinateX": 32.0,
                "coreCoordinateY": 35.0,
                "geodeticDatum": "WGS84",
                "decimalLatitude": 39.087456,
                "decimalLongitude": -77.971942,
                "coordinateUncertainty": 20.1,
                "elevation": 137.4,
                "elevationUncertainty": 0.21,
                "samplingProtocolVersion": "NEON.DOC.014048vN",
                "startDate": "2020-07-13T15:54Z",
                "collectDate": "2020-07-13T15:54Z",
                "sampleTiming": "peakGreenness",
                "biophysicalCriteria": "OK - no known exceptions",
                "eventID": "BLAN.peakGreenness.2020",
                "standingWaterDepth": 0.0,
                "nTransBoutType": "tInitial",
                "boutType": "microbesBiomassBGC",
                "samplingImpractical": "OK",
                "incubationMethod": "no incubation",
                "incubationCondition": "",
                "sampleID": "BLAN_005-M-32-35-20200713",
                "sampleCode": "",
                "toxicodendronPossible": "N",
                "horizon": "M",
                "horizonDetails": "OK",
                "soilTemp": 21.0,
                "litterDepth": 2.0,
                "sampleTopDepth": 0.0,
                "sampleBottomDepth": 27.5,
                "sampleExtent": "maximum",
                "soilSamplingDevice": "corer",
                "soilCoreCount": 1.0,
                "geneticSampleID": "BLAN_005-M-32-35-20200713-GEN",
                "geneticSampleCode": "B00000051896",
                "geneticSampleCondition": "OK",
                "geneticSamplePrepMethod": "",
                "geneticArchiveSample1ID": "BLAN_005-M-32-35-20200713-GA1",
                "geneticArchiveSample1Code": "C00000015986",
                "geneticArchiveSample2ID": "BLAN_005-M-32-35-20200713-GA2",
                "geneticArchiveSample2Code": "C00000015987",
                "geneticArchiveSample3ID": "BLAN_005-M-32-35-20200713-GA3",
                "geneticArchiveSample3Code": "C00000015988",
                "geneticArchiveSample4ID": "BLAN_005-M-32-35-20200713-GA4",
                "geneticArchiveSample4Code": "C00000015989",
                "geneticArchiveSample5ID": "BLAN_005-M-32-35-20200713-GA5",
                "geneticArchiveSample5Code": "C00000015990",
                "geneticArchiveSamplePrepMethod": "",
                "geneticArchiveContainer": "",
                "biomassID": "BLAN_005-M-32-35-20200713-BM",
                "biomassCode": "B00000051774",
                "biomassSampleCondition": "OK",
                "remarks": "",
                "collectedBy": "0000-0002-1718-8623",
                "dataQF": "",
            },
            {
                "uid": "272efff4-d419-4935-992f-a89c53fa3fe7",
                "domainID": "D02",
                "siteID": "BLAN",
                "plotID": "BLAN_005",
                "namedLocation": "BLAN_005.basePlot.bgc",
                "plotType": "distributed",
                "nlcdClass": "deciduousForest",
                "subplotID": 39.0,
                "coreCoordinateX": 1.0,
                "coreCoordinateY": 39.5,
                "geodeticDatum": "WGS84",
                "decimalLatitude": 39.087456,
                "decimalLongitude": -77.971942,
                "coordinateUncertainty": 20.1,
                "elevation": 137.4,
                "elevationUncertainty": 0.21,
                "samplingProtocolVersion": "NEON.DOC.014048vN",
                "startDate": "2020-07-13T15:03Z",
                "collectDate": "2020-07-13T15:03Z",
                "sampleTiming": "peakGreenness",
                "biophysicalCriteria": "OK - no known exceptions",
                "eventID": "BLAN.peakGreenness.2020",
                "standingWaterDepth": 0.0,
                "nTransBoutType": "tInitial",
                "boutType": "microbesBiomassBGC",
                "samplingImpractical": "OK",
                "incubationMethod": "no incubation",
                "incubationCondition": "",
                "sampleID": "BLAN_005-M-1-39.5-20200713",
                "sampleCode": "",
                "toxicodendronPossible": "N",
                "horizon": "M",
                "horizonDetails": "OK",
                "soilTemp": 20.3,
                "litterDepth": 1.0,
                "sampleTopDepth": 0.0,
                "sampleBottomDepth": 26.7,
                "sampleExtent": "maximum",
                "soilSamplingDevice": "corer",
                "soilCoreCount": 1.0,
                "geneticSampleID": "BLAN_005-M-1-39.5-20200713-GEN",
                "geneticSampleCode": "B00000051897",
                "geneticSampleCondition": "OK",
                "geneticSamplePrepMethod": "",
                "geneticArchiveSample1ID": "BLAN_005-M-1-39.5-20200713-GA1",
                "geneticArchiveSample1Code": "C00000015991",
                "geneticArchiveSample2ID": "BLAN_005-M-1-39.5-20200713-GA2",
                "geneticArchiveSample2Code": "C00000015992",
                "geneticArchiveSample3ID": "BLAN_005-M-1-39.5-20200713-GA3",
                "geneticArchiveSample3Code": "C00000015993",
                "geneticArchiveSample4ID": "BLAN_005-M-1-39.5-20200713-GA4",
                "geneticArchiveSample4Code": "C00000015994",
                "geneticArchiveSample5ID": "BLAN_005-M-1-39.5-20200713-GA5",
                "geneticArchiveSample5Code": "C00000015995",
                "geneticArchiveSamplePrepMethod": "",
                "geneticArchiveContainer": "",
                "biomassID": "BLAN_005-M-1-39.5-20200713-BM",
                "biomassCode": "B00000051786",
                "biomassSampleCondition": "OK",
                "remarks": "",
                "collectedBy": "0000-0002-1718-8623",
                "dataQF": "",
            },
        ]
    ),
    "sls_soilChemistry": pd.DataFrame(
        [
            {
                "uid": "f149c247-ea5b-4d15-a030-75e5c2830dfd",
                "analysisDate": "2021-03-03",
                "namedLocation": "BLAN_005.basePlot.bgc",
                "domainID": "D02",
                "siteID": "BLAN",
                "plotID": "BLAN_005",
                "plotType": "distributed",
                "startDate": "2020-07-13T14:34Z",
                "collectDate": "2020-07-13T14:34Z",
                "sampleID": "BLAN_005-M-8-0-20200713",
                "sampleCode": "",
                "cnSampleID": "BLAN_005-M-8-0-20200713-CN",
                "cnSampleCode": "A00000161637",
                "sampleType": "soil",
                "acidTreatment": "Y",
                "co2Trapped": "N",
                "d15N": -26.7,
                "organicd13C": None,
                "nitrogenPercent": 1.36,
                "organicCPercent": None,
                "CNratio": None,
                "cnIsotopeQF": "OK",
                "cnPercentQF": "OK",
                "isotopeAccuracyQF": "OK",
                "percentAccuracyQF": "OK",
                "analyticalRepNumber": 1.0,
                "remarks": "",
                "laboratoryName": "University of Wyoming Stable Isotope Facility",
                "testMethod": "008 EA-IRMS organic d13C & C%",
                "instrument": "Costech 4010 Elemental Analyzer with Costech Zero Blank Autosampler plus Thermo Finnigan Delta Plus XP with Finnigan Conflo III Interface",
                "analyzedBy": "rHBrhUbTwVSLtfc5tc9S18/eJGkjVJIj",
                "reviewedBy": "L/6ZjMlRllRQ+sQbi+7sFw==",
                "dataQF": "",
            },
            {
                "uid": "463441dd-a04d-4f79-8191-60467cb08fd7",
                "analysisDate": "2021-03-03",
                "namedLocation": "BLAN_005.basePlot.bgc",
                "domainID": "D02",
                "siteID": "BLAN",
                "plotID": "BLAN_005",
                "plotType": "distributed",
                "startDate": "2020-07-13T15:54Z",
                "collectDate": "2020-07-13T15:54Z",
                "sampleID": "BLAN_005-M-32-35-20200713",
                "sampleCode": "",
                "cnSampleID": "BLAN_005-M-32-35-20200713-CN",
                "cnSampleCode": "A00000161635",
                "sampleType": "soil",
                "acidTreatment": "Y",
                "co2Trapped": "N",
                "d15N": -26.2,
                "organicd13C": None,
                "nitrogenPercent": 1.78,
                "organicCPercent": None,
                "CNratio": None,
                "cnIsotopeQF": "OK",
                "cnPercentQF": "OK",
                "isotopeAccuracyQF": "OK",
                "percentAccuracyQF": "OK",
                "analyticalRepNumber": 1.0,
                "remarks": "",
                "laboratoryName": "University of Wyoming Stable Isotope Facility",
                "testMethod": "008 EA-IRMS organic d13C & C%",
                "instrument": "Costech 4010 Elemental Analyzer with Costech Zero Blank Autosampler plus Thermo Finnigan Delta Plus XP with Finnigan Conflo III Interface",
                "analyzedBy": "rHBrhUbTwVSLtfc5tc9S18/eJGkjVJIj",
                "reviewedBy": "L/6ZjMlRllRQ+sQbi+7sFw==",
                "dataQF": "",
            },
            {
                "uid": "563b265a-19c2-422e-a446-dce9cce8f857",
                "analysisDate": "2021-03-03",
                "namedLocation": "BLAN_005.basePlot.bgc",
                "domainID": "D02",
                "siteID": "BLAN",
                "plotID": "BLAN_005",
                "plotType": "distributed",
                "startDate": "2020-07-13T15:03Z",
                "collectDate": "2020-07-13T15:03Z",
                "sampleID": "BLAN_005-M-1-39.5-20200713",
                "sampleCode": "",
                "cnSampleID": "BLAN_005-M-1-39.5-20200713-CN",
                "cnSampleCode": "A00000161636",
                "sampleType": "soil",
                "acidTreatment": "Y",
                "co2Trapped": "N",
                "d15N": -27.3,
                "organicd13C": None,
                "nitrogenPercent": 1.44,
                "organicCPercent": None,
                "CNratio": None,
                "cnIsotopeQF": "OK",
                "cnPercentQF": "OK",
                "isotopeAccuracyQF": "OK",
                "percentAccuracyQF": "OK",
                "analyticalRepNumber": 1.0,
                "remarks": "",
                "laboratoryName": "University of Wyoming Stable Isotope Facility",
                "testMethod": "008 EA-IRMS organic d13C & C%",
                "instrument": "Costech 4010 Elemental Analyzer with Costech Zero Blank Autosampler plus Thermo Finnigan Delta Plus XP with Finnigan Conflo III Interface",
                "analyzedBy": "rHBrhUbTwVSLtfc5tc9S18/eJGkjVJIj",
                "reviewedBy": "L/6ZjMlRllRQ+sQbi+7sFw==",
                "dataQF": "",
            },
        ]
    ),
    "sls_soilMoisture": pd.DataFrame(
        [
            {
                "uid": "4d4345cc-b6f8-467c-93d7-8206f0f1f8b4",
                "domainID": "D02",
                "siteID": "BLAN",
                "plotID": "BLAN_005",
                "namedLocation": "BLAN_005.basePlot.bgc",
                "startDate": "2020-07-13T15:03Z",
                "collectDate": "2020-07-13T15:03Z",
                "sampleID": "BLAN_005-M-1-39.5-20200713",
                "sampleCode": "",
                "moistureSampleID": "BLAN_005-M-1-39.5-20200713-SM",
                "samplingProtocolVersion": "NEON.DOC.014048vN",
                "horizon": "M",
                "ovenStartDate": "2020-07-13T22:00Z",
                "ovenEndDate": "2020-07-20T20:00Z",
                "boatMass": 2.17,
                "freshMassBoatMass": 12.2,
                "dryMassBoatMass": 11.209,
                "soilMoisture": 0.11,
                "dryMassFraction": 0.901,
                "sampleCondition": "",
                "smRemarks": "",
                "smMeasuredBy": "",
                "smDataQF": "0000-0001-9247-047X",
            },
            {
                "uid": "b03b1475-f653-4822-9033-97da2fc7f56e",
                "domainID": "D02",
                "siteID": "BLAN",
                "plotID": "BLAN_005",
                "namedLocation": "BLAN_005.basePlot.bgc",
                "startDate": "2020-07-13T15:54Z",
                "collectDate": "2020-07-13T15:54Z",
                "sampleID": "BLAN_005-M-32-35-20200713",
                "sampleCode": "",
                "moistureSampleID": "BLAN_005-M-32-35-20200713-SM",
                "samplingProtocolVersion": "NEON.DOC.014048vN",
                "horizon": "M",
                "ovenStartDate": "2020-07-13T22:00Z",
                "ovenEndDate": "2020-07-20T20:00Z",
                "boatMass": 2.15,
                "freshMassBoatMass": 12.15,
                "dryMassBoatMass": 11.077,
                "soilMoisture": 0.12,
                "dryMassFraction": 0.893,
                "sampleCondition": "",
                "smRemarks": "",
                "smMeasuredBy": "",
                "smDataQF": "0000-0001-9247-047X",
            },
            {
                "uid": "72761eb9-21fb-4ae8-8e7a-96007c654e7e",
                "domainID": "D02",
                "siteID": "BLAN",
                "plotID": "BLAN_005",
                "namedLocation": "BLAN_005.basePlot.bgc",
                "startDate": "2020-07-13T14:34Z",
                "collectDate": "2020-07-13T14:34Z",
                "sampleID": "BLAN_005-M-8-0-20200713",
                "sampleCode": "",
                "moistureSampleID": "BLAN_005-M-8-0-20200713-SM",
                "samplingProtocolVersion": "NEON.DOC.014048vN",
                "horizon": "M",
                "ovenStartDate": "2020-07-13T22:00Z",
                "ovenEndDate": "2020-07-20T20:00Z",
                "boatMass": 2.17,
                "freshMassBoatMass": 12.32,
                "dryMassBoatMass": 11.116,
                "soilMoisture": 0.135,
                "dryMassFraction": 0.881,
                "sampleCondition": "",
                "smRemarks": "",
                "smMeasuredBy": "",
                "smDataQF": "0000-0001-9247-047X",
            },
        ]
    ),
    "sls_soilpH": pd.DataFrame(
        [
            {
                "uid": "4d4345cc-b6f8-467c-93d7-8206f0f1f8b4",
                "domainID": "D02",
                "siteID": "BLAN",
                "plotID": "BLAN_005",
                "namedLocation": "BLAN_005.basePlot.bgc",
                "startDate": "2020-07-13T15:03Z",
                "collectDate": "2020-07-13T15:03Z",
                "sampleID": "BLAN_005-M-1-39.5-20200713",
                "sampleCode": "",
                "moistureSampleID": "BLAN_005-M-1-39.5-20200713-SM",
                "samplingProtocolVersion": "NEON.DOC.014048vN",
                "horizon": "M",
                "ovenStartDate": "2020-07-13T22:00Z",
                "ovenEndDate": "2020-07-20T20:00Z",
                "boatMass": 2.17,
                "freshMassBoatMass": 12.2,
                "dryMassBoatMass": 11.209,
                "soilMoisture": 0.11,
                "dryMassFraction": 0.901,
                "sampleCondition": "",
                "smRemarks": "",
                "smMeasuredBy": "",
                "smDataQF": "0000-0001-9247-047X",
                "soilInWaterpH": 4.6,
            },
            {
                "uid": "b03b1475-f653-4822-9033-97da2fc7f56e",
                "domainID": "D02",
                "siteID": "BLAN",
                "plotID": "BLAN_005",
                "namedLocation": "BLAN_005.basePlot.bgc",
                "startDate": "2020-07-13T15:54Z",
                "collectDate": "2020-07-13T15:54Z",
                "sampleID": "BLAN_005-M-32-35-20200713",
                "sampleCode": "",
                "moistureSampleID": "BLAN_005-M-32-35-20200713-SM",
                "samplingProtocolVersion": "NEON.DOC.014048vN",
                "horizon": "M",
                "ovenStartDate": "2020-07-13T22:00Z",
                "ovenEndDate": "2020-07-20T20:00Z",
                "boatMass": 2.15,
                "freshMassBoatMass": 12.15,
                "dryMassBoatMass": 11.077,
                "soilMoisture": 0.12,
                "dryMassFraction": 0.893,
                "sampleCondition": "",
                "smRemarks": "",
                "smMeasuredBy": "",
                "smDataQF": "0000-0001-9247-047X",
            },
            {
                "uid": "72761eb9-21fb-4ae8-8e7a-96007c654e7e",
                "domainID": "D02",
                "siteID": "BLAN",
                "plotID": "BLAN_005",
                "namedLocation": "BLAN_005.basePlot.bgc",
                "startDate": "2020-07-13T14:34Z",
                "collectDate": "2020-07-13T14:34Z",
                "sampleID": "BLAN_005-M-8-0-20200713",
                "sampleCode": "",
                "moistureSampleID": "BLAN_005-M-8-0-20200713-SM",
                "samplingProtocolVersion": "NEON.DOC.014048vN",
                "horizon": "M",
                "ovenStartDate": "2020-07-13T22:00Z",
                "ovenEndDate": "2020-07-20T20:00Z",
                "boatMass": 2.17,
                "freshMassBoatMass": 12.32,
                "dryMassBoatMass": 11.116,
                "soilMoisture": 0.135,
                "dryMassFraction": 0.881,
                "sampleCondition": "",
                "smRemarks": "",
                "smMeasuredBy": "",
                "smDataQF": "0000-0001-9247-047X",
            },
        ]
    ),
    "ntr_externalLab": pd.DataFrame(
        [
            {
                "uid": "7c7dc710-a8fc-43cb-a7dc-ce03d548785f",
                "namedLocation": "BLAN_005.basePlot.bgc",
                "domainID": "D02",
                "siteID": "BLAN",
                "plotID": "BLAN_005",
                "startDate": "2020-07-13T14:34Z",
                "collectDate": "2020-07-13T14:34Z",
                "sampleID": "BLAN_005-M-8-0-20200713",
                "sampleCode": "",
                "kclSampleID": "BLAN_005-M-8-0-20200713-KCL",
                "kclSampleCode": "B00000052054",
                "sampleCondition": "ok",
                "ammoniumNAnalysisDate": "2020-08-20",
                "kclAmmoniumNConc": 0.151,
                "ammoniumNRepNum": 1.0,
                "ammoniumNQF": "OK",
                "ammoniumNRemarks": "",
                "nitrateNitriteNAnalysisDate": "2020-08-20",
                "kclNitrateNitriteNConc": 0.069,
                "nitrateNitriteNRepNum": 1.0,
                "nitrateNitriteNQF": "OK",
                "nitrateNitriteNRemarks": "",
                "laboratoryName": "University of Florida Wetland Biogeochemistry Laboratory",
                "ammoniumNMethod": "WBL-AN-020",
                "ammoniumNInstrument": "Bran+Luebbe AutoAnalyzer II",
                "ammoniumNAnalyzedBy": "/RBmT99ty+tXsqfXzuuuWg==",
                "ammoniumNReviewedBy": "8kr+qfmeu1FQ7vvHWE7F/A==",
                "nitrateNitriteNMethod": "WBL-AN-008",
                "nitrateNitriteNInstrument": "Bran+Luebbe AutoAnalyzer 3",
                "nitrateNitriteNAnalyzedBy": "/RBmT99ty+tXsqfXzuuuWg==",
                "nitrateNitriteNReviewedBy": "8kr+qfmeu1FQ7vvHWE7F/A==",
                "dataQF": "",
            }
        ],
        {
            "uid": "8bf209e6-ac85-4d52-ad60-f7b8d347471a",
            "namedLocation": "BLAN_005.basePlot.bgc",
            "domainID": "D02",
            "siteID": "BLAN",
            "plotID": "BLAN_005",
            "startDate": "2020-07-13T15:54Z",
            "collectDate": "2020-07-13T15:54Z",
            "sampleID": "BLAN_005-M-32-35-20200713",
            "sampleCode": "",
            "kclSampleID": "BLAN_005-M-32-35-20200713-KCL",
            "kclSampleCode": "B00000052056",
            "sampleCondition": "ok",
            "ammoniumNAnalysisDate": "2020-08-20",
            "kclAmmoniumNConc": 0.18,
            "ammoniumNRepNum": 1.0,
            "ammoniumNQF": "OK",
            "ammoniumNRemarks": "",
            "nitrateNitriteNAnalysisDate": "2020-08-20",
            "kclNitrateNitriteNConc": 0.068,
            "nitrateNitriteNRepNum": 1.0,
            "nitrateNitriteNQF": "OK",
            "nitrateNitriteNRemarks": "",
            "laboratoryName": "University of Florida Wetland Biogeochemistry Laboratory",
            "ammoniumNMethod": "WBL-AN-020",
            "ammoniumNInstrument": "Bran+Luebbe AutoAnalyzer II",
            "ammoniumNAnalyzedBy": "/RBmT99ty+tXsqfXzuuuWg==",
            "ammoniumNReviewedBy": "8kr+qfmeu1FQ7vvHWE7F/A==",
            "nitrateNitriteNMethod": "WBL-AN-008",
            "nitrateNitriteNInstrument": "Bran+Luebbe AutoAnalyzer 3",
            "nitrateNitriteNAnalyzedBy": "/RBmT99ty+tXsqfXzuuuWg==",
            "nitrateNitriteNReviewedBy": "8kr+qfmeu1FQ7vvHWE7F/A==",
            "dataQF": "",
        },
        {
            "uid": "6107535a-4e68-4314-b179-450a89c56db0",
            "namedLocation": "BLAN_005.basePlot.bgc",
            "domainID": "D02",
            "siteID": "BLAN",
            "plotID": "BLAN_005",
            "startDate": "2020-07-13T15:03Z",
            "collectDate": "2020-07-13T15:03Z",
            "sampleID": "BLAN_005-M-1-39.5-20200713",
            "sampleCode": "",
            "kclSampleID": "BLAN_005-M-1-39.5-20200713-KCL",
            "kclSampleCode": "B00000052055",
            "sampleCondition": "ok",
            "ammoniumNAnalysisDate": "2020-08-20",
            "kclAmmoniumNConc": 0.344,
            "ammoniumNRepNum": 1.0,
            "ammoniumNQF": "OK",
            "ammoniumNRemarks": "",
            "nitrateNitriteNAnalysisDate": "2020-08-20",
            "kclNitrateNitriteNConc": 0.026,
            "nitrateNitriteNRepNum": 1.0,
            "nitrateNitriteNQF": "OK",
            "nitrateNitriteNRemarks": "",
            "laboratoryName": "University of Florida Wetland Biogeochemistry Laboratory",
            "ammoniumNMethod": "WBL-AN-020",
            "ammoniumNInstrument": "Bran+Luebbe AutoAnalyzer II",
            "ammoniumNAnalyzedBy": "/RBmT99ty+tXsqfXzuuuWg==",
            "ammoniumNReviewedBy": "8kr+qfmeu1FQ7vvHWE7F/A==",
            "nitrateNitriteNMethod": "WBL-AN-008",
            "nitrateNitriteNInstrument": "Bran+Luebbe AutoAnalyzer 3",
            "nitrateNitriteNAnalyzedBy": "/RBmT99ty+tXsqfXzuuuWg==",
            "nitrateNitriteNReviewedBy": "8kr+qfmeu1FQ7vvHWE7F/A==",
            "dataQF": "",
        },
    ),
    "ntr_internalLab": pd.DataFrame(
        [
            {
                "uid": "5239ab0d-c4a0-4d2d-ae71-8bcf920e5257",
                "namedLocation": "BLAN_005.basePlot.bgc",
                "domainID": "D02",
                "siteID": "BLAN",
                "plotID": "BLAN_005",
                "plotType": "distributed",
                "startDate": "2020-07-13T15:03Z",
                "collectDate": "2020-07-13T15:03Z",
                "samplingProtocolVersion": "NEON.DOC.014048vN",
                "sampleID": "BLAN_005-M-1-39.5-20200713",
                "sampleCode": "",
                "kclSampleID": "BLAN_005-M-1-39.5-20200713-KCL",
                "kclSampleCode": "B00000052055",
                "incubationPairID": "BLAN_005-M-1-39.5",
                "nTransBoutType": "tInitial",
                "incubationLength": 0.0,
                "extractionStartDate": "2020-07-14T13:54Z",
                "kclReferenceID": "BLAN-20200714-BRef1",
                "soilFreshMass": 10.03,
                "kclVolume": 100.0,
                "extractionEndDate": "2020-07-14T16Z",
                "sampleCondition": "OK",
                "remarks": "",
                "processedBy": "0000-0001-7914-5891",
                "recordedBy": "0000-0001-9247-047X",
                "dataQF": "",
                "kclBlank1ID": "",
                "kclBlank1Code": "",
                "kclBlank2ID": "",
                "kclBlank2Code": "",
                "kclBlank3ID": "",
                "kclBlank3Code": "",
            },
            {
                "uid": "9aff593e-cec6-47c3-abce-a62be9f3e669",
                "namedLocation": "BLAN_005.basePlot.bgc",
                "domainID": "D02",
                "siteID": "BLAN",
                "plotID": "BLAN_005",
                "plotType": "distributed",
                "startDate": "2020-07-13T15:54Z",
                "collectDate": "2020-07-13T15:54Z",
                "samplingProtocolVersion": "NEON.DOC.014048vN",
                "sampleID": "BLAN_005-M-32-35-20200713",
                "sampleCode": "",
                "kclSampleID": "BLAN_005-M-32-35-20200713-KCL",
                "kclSampleCode": "B00000052056",
                "incubationPairID": "BLAN_005-M-32-35",
                "nTransBoutType": "tInitial",
                "incubationLength": 0.0,
                "extractionStartDate": "2020-07-14T13:56Z",
                "kclReferenceID": "BLAN-20200714-BRef1",
                "soilFreshMass": 10.01,
                "kclVolume": 100.0,
                "extractionEndDate": "2020-07-14T16Z",
                "sampleCondition": "OK",
                "remarks": "",
                "processedBy": "0000-0001-7914-5891",
                "recordedBy": "0000-0001-9247-047X",
                "dataQF": "",
                "kclBlank1ID": "",
                "kclBlank1Code": "",
                "kclBlank2ID": "",
                "kclBlank2Code": "",
                "kclBlank3ID": "",
                "kclBlank3Code": "",
            },
            {
                "uid": "dc1bbcf3-43a8-45c8-8de2-20ba04c14d47",
                "namedLocation": "BLAN_005.basePlot.bgc",
                "domainID": "D02",
                "siteID": "BLAN",
                "plotID": "BLAN_005",
                "plotType": "distributed",
                "startDate": "2020-07-13T14:34Z",
                "collectDate": "2020-07-13T14:34Z",
                "samplingProtocolVersion": "NEON.DOC.014048vN",
                "sampleID": "BLAN_005-M-8-0-20200713",
                "sampleCode": "",
                "kclSampleID": "BLAN_005-M-8-0-20200713-KCL",
                "kclSampleCode": "B00000052054",
                "incubationPairID": "BLAN_005-M-8-0",
                "nTransBoutType": "tInitial",
                "incubationLength": 0.0,
                "extractionStartDate": "2020-07-14T13:55Z",
                "kclReferenceID": "BLAN-20200714-BRef1",
                "soilFreshMass": 10.03,
                "kclVolume": 100.0,
                "extractionEndDate": "2020-07-14T16Z",
                "sampleCondition": "OK",
                "remarks": "",
                "processedBy": "0000-0001-7914-5891",
                "recordedBy": "0000-0001-9247-047X",
                "dataQF": "",
                "kclBlank1ID": "",
                "kclBlank1Code": "",
                "kclBlank2ID": "",
                "kclBlank2Code": "",
                "kclBlank3ID": "",
                "kclBlank3Code": "",
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
BLAN_005-M-20200713-COMP-DNA1\tHVT2HBGXJ\t20S_08_0661\tBMI_HVT2HBGXJ_20S_08_0661_R1.fastq.gz\tR1 metagenomic archive of fastq files\thttps://storage.neonscience.org/neon-microbial-raw-seq-files/2021/BMI_HVT2HBGXJ_mms_R1/BMI_HVT2HBGXJ_20S_08_0661_R1.fastq.gz\t8b5794e91b1e79e02f1a3e7ef53a73b3
BLAN_005-M-20200713-COMP-DNA1\tHVT2HBGXJ\t20S_08_0661\tBMI_HVT2HBGXJ_20S_08_0661_R2.fastq.gz\tR2 metagenomic archive of fastq files\thttps://storage.neonscience.org/neon-microbial-raw-seq-files/2021/BMI_HVT2HBGXJ_mms_R2/BMI_HVT2HBGXJ_20S_08_0661_R2.fastq.gz\t44dc66147143a6eb1e806defa7f3706e"""

    return pd.read_csv(StringIO(tsv_data_dna), delimiter="\t")


class TestNeonDataTranslator:
    @pytest.fixture
    def translator(self, test_minter):
        return NeonSoilDataTranslator(mms_data=mms_data, 
                                      sls_data=sls_data, 
                                      neon_envo_mappings_file=neon_envo_mappings_file(), 
                                      neon_raw_data_file_mappings_file=neon_raw_data_file_mappings_file(), 
                                      id_minter=test_minter
                                    )

    def test_missing_mms_table(self, test_minter):
        # Test behavior when mms data is missing a table
        with pytest.raises(
            ValueError, match="missing one of the metagenomic microbe soil tables"
        ):
            NeonSoilDataTranslator({}, sls_data, neon_envo_mappings_file(), neon_raw_data_file_mappings_file(), id_minter=test_minter)

    def test_missing_sls_table(self, test_minter):
        # Test behavior when sls data is missing a table
        with pytest.raises(ValueError, match="missing one of the soil periodic tables"):
            NeonSoilDataTranslator(mms_data, {}, neon_envo_mappings_file(), neon_raw_data_file_mappings_file(), id_minter=test_minter)

    def test_get_value_or_none(self):
        # use one biosample record to test this method
        test_biosample = sls_data["sls_soilCoreCollection"][
            sls_data["sls_soilCoreCollection"]["sampleID"] == "BLAN_005-M-8-0-20200713"
        ]

        # specific handler for horizon slot
        expected_horizon = "M horizon"
        actual_horizon = _get_value_or_none(test_biosample, "horizon")

        assert expected_horizon == actual_horizon

        # specific handler for depth slot
        expected_minimum_depth = 0.0
        actual_minimum_depth = _get_value_or_none(test_biosample, "sampleTopDepth")
        assert expected_minimum_depth == actual_minimum_depth

        expected_maximum_depth = 0.295
        actual_maximum_depth = _get_value_or_none(test_biosample, "sampleBottomDepth")
        assert expected_maximum_depth == actual_maximum_depth

        expected_sample_id = "BLAN_005-M-8-0-20200713"
        actual_sample_id = _get_value_or_none(test_biosample, "sampleID")
        assert expected_sample_id == actual_sample_id

        # test get_value_or_none() with invalid column
        expected_result = None
        actual_result = _get_value_or_none(test_biosample, "invalid_column")
        assert expected_result == actual_result

    def test_create_controlled_identified_term_value(self):
        env_broad_scale = _create_controlled_identified_term_value(
            "ENVO:00000446", "terrestrial biome"
        )
        assert env_broad_scale.term.id == "ENVO:00000446"
        assert env_broad_scale.term.name == "terrestrial biome"

    def test_create_controlled_term_value(self):
        env_package = _create_controlled_term_value("soil")
        assert env_package.has_raw_value == "soil"

    def test_create_timestamp_value_with_valid_args(self):
        collect_date = _create_timestamp_value("2020-07-13T14:34Z")
        assert collect_date.has_raw_value == "2020-07-13T14:34Z"

    @pytest.mark.xfail(reason="AttributeError: module 'nmdc_schema.nmdc' has no attribute 'QualityControlReport'")
    def test_get_database(self, translator):
        database = translator.get_database()

        # verify lengths of all collections in database
        assert len(database.biosample_set) == 3
        assert len(database.pooling_set) == 1
        assert len(database.extraction_set) == 1
        assert len(database.library_preparation_set) == 1
        assert len(database.omics_processing_set) == 1
        assert len(database.processed_sample_set) == 3

        # verify contents of biosample_set
        biosample_list = database.biosample_set
        expected_biosample_names = [
            "BLAN_005-M-8-0-20200713",
            "BLAN_005-M-32-35-20200713",
            "BLAN_005-M-1-39.5-20200713",
        ]
        for biosample in biosample_list:
            actual_biosample_name = biosample["name"]
            assert actual_biosample_name in expected_biosample_names

        # verify contents of omics_processing_set
        omics_processing_list = database.omics_processing_set
        expected_omics_processing = [
            "Terrestrial soil microbial communities - BLAN_005-M-20200713-COMP-DNA1"
        ]
        for omics_processing in omics_processing_list:
            actual_omics_processing = omics_processing["name"]

            assert actual_omics_processing in expected_omics_processing

        # input to a Pooling is a Biosample
        pooling_process_list = database.pooling_set
        extraction_list = database.extraction_set
        library_preparation_list = database.library_preparation_set
        omics_processing_list = database.omics_processing_set

        expected_input = [bsm["id"] for bsm in biosample_list]
        for pooling_process in pooling_process_list:
            pooling_output = pooling_process.has_output
            pooling_input = pooling_process.has_input
            assert sorted(expected_input) == sorted(pooling_input)

            # output of a Pooling is input to Extraction
            for extraction in extraction_list:
                extraction_input = extraction.has_input
                extraction_output = extraction.has_output
                assert extraction_input == pooling_output

                # output of Extraction is input to Library Preparation
                for lib_prep in library_preparation_list:
                    lib_prep_input = lib_prep.has_input
                    lib_prep_output = lib_prep.has_output
                    assert lib_prep_input == extraction_output

                    # output of Library Preparation is input to OmicsProcessing
                    for omics_processing in omics_processing_list:
                        omics_processing_input = omics_processing.has_input
                        assert omics_processing_input == lib_prep_output
