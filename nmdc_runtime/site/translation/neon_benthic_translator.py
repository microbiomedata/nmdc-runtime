import math
import sqlite3
from typing import Union, List

import pandas as pd
import requests
import requests_cache

from nmdc_schema import nmdc
from nmdc_runtime.site.translation.translator import Translator
from nmdc_runtime.site.util import get_basename


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
    def __init__(self, benthic_data: dict, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.conn = sqlite3.connect("neon.db")
        requests_cache.install_cache("neon_api_cache", expire_after=3600)

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

        neon_envo_mappings_file = "https://raw.githubusercontent.com/microbiomedata/nmdc-schema/main/assets/neon_mixs_env_triad_mappings/neon-nlcd-local-broad-mappings.tsv"
        neon_envo_terms = pd.read_csv(neon_envo_mappings_file, delimiter="\t")
        neon_envo_terms.to_sql(
            "neonEnvoTerms", self.conn, if_exists="replace", index=False
        )

    def get_site_by_code(self, site_code: str) -> str:
        site_response = requests.get(
            f"https://data.neonscience.org/api/v0/sites/{site_code}"
        )
        if site_response.status_code == 200:
            site_response = site_response.json()
            return f"USA: {site_response['data']['stateName']}, {site_response['data']['siteName']}".replace(
                " NEON", ""
            )

    def _get_value_or_none(
        self, data: pd.DataFrame, column_name: str
    ) -> Union[str, float, None]:
        """
        Get the value from the specified column in the data DataFrame.
        If the column value is NaN, return None. However, there are handlers
        for a select set of columns - horizon, qaqcStatus, sampleTopDepth,
        and sampleBottomDepth.

        :param data: DataFrame to read the column value from.
        :return: Either a string, float or None depending on the column/column values.
        """
        if column_name in data and not data[column_name].isna().any():
            if column_name == "horizon":
                return f"{data[column_name].values[0]} horizon"
            elif column_name == "qaqcStatus":
                return data[column_name].values[0].lower()
            elif column_name == "sampleTopDepth":
                return float(data[column_name].values[0]) / 100
            elif column_name == "sampleBottomDepth":
                return float(data[column_name].values[0]) / 100
            else:
                return data[column_name].values[0]

        return None

    def _create_controlled_identified_term_value(
        self, id: str = None, name: str = None
    ) -> nmdc.ControlledIdentifiedTermValue:
        """
        Create a ControlledIdentifiedTermValue object with the specified id and name.

        :param id: CURIE (with defined prefix expansion) or full URI of term.
        :param name: Name of term.
        :return: ControlledIdentifiedTermValue with mandatorily specified value for `id`.
        """
        if id is None:
            return None
        return nmdc.ControlledIdentifiedTermValue(
            term=nmdc.OntologyClass(id=id, name=name)
        )

    def _create_controlled_term_value(
        self, name: str = None
    ) -> nmdc.ControlledTermValue:
        """
        Create a ControlledIdentifiedTermValue object with the specified id and name.

        :param name: Name of term. This may or may not have an `id` associated with it,
        hence the decision to record it in `has_raw_value` meaning, record as it is
        in the data source.
        :return: ControlledTermValue object with name in `has_raw_value`.
        """
        if name is None:
            return None
        return nmdc.ControlledTermValue(has_raw_value=name)

    def _create_timestamp_value(self, value: str = None) -> nmdc.TimestampValue:
        """
        Create a TimestampValue object with the specified value.

        :param value: Timestamp value recorded in ISO-8601 format.
        Example: 2021-07-07T20:14Z.
        :return: ISO-8601 timestamp wrapped in TimestampValue object.
        """
        if value is None:
            return None
        return nmdc.TimestampValue(has_raw_value=value)

    def _create_quantity_value(
        self, numeric_value: Union[str, int, float] = None, unit: str = None
    ) -> nmdc.QuantityValue:
        """
        Create a QuantityValue object with the specified numeric value and unit.

        :param numeric_value: Numeric value from a dataframe column that typically
        records numerical values.
        :param unit: Unit corresponding to the numeric value. Example: biogeochemical
        measurement value like organic Carbon Nitrogen ratio.
        :return: Numeric value and unit stored together in nested QuantityValue object.
        """
        if numeric_value is None or math.isnan(numeric_value):
            return None
        return nmdc.QuantityValue(has_numeric_value=float(numeric_value), has_unit=unit)

    def _create_text_value(self, value: str = None) -> nmdc.TextValue:
        """
        Create a TextValue object with the specified value.

        :param value: column that we expect to primarily have text values.
        :return: Text wrapped in TextValue object.
        """
        if value is None:
            return None
        return nmdc.TextValue(has_raw_value=value)

    def _create_double_value(self, value: str = None) -> nmdc.Double:
        """
        Create a Double object with the specified value.

        :param value: Values from a column which typically records numeric
        (double) values like pH.
        :return: String (possibly) cast/converted to nmdc Double object.
        """
        if value is None or math.isnan(value):
            return None
        return nmdc.Double(value)

    def _create_geolocation_value(
        self, latitude: str = None, longitude: str = None
    ) -> nmdc.GeolocationValue:
        """
        Create a GeolocationValue object with latitude and longitude from the
        biosample DataFrame. Takes in values from the NEON API table with
        latitude (decimalLatitude) and longitude (decimalLongitude) values and
        puts it in the respective slots in the GeolocationValue class object.

        :param latitude: Value corresponding to `decimalLatitude` column.
        :param longitude: Value corresponding to `decimalLongitude` column.
        :return: Latitude and Longitude values wrapped in nmdc GeolocationValue
        object.
        """
        if (
            latitude is None
            or math.isnan(latitude)
            or longitude is None
            or math.isnan(longitude)
        ):
            return None

        return nmdc.GeolocationValue(
            latitude=nmdc.DecimalDegree(latitude),
            longitude=nmdc.DecimalDegree(longitude),
        )

    def _translate_biosample(
        self, neon_id: str, nmdc_id: str, biosample_row: pd.DataFrame
    ) -> nmdc.Biosample:
        return nmdc.Biosample(
            id=nmdc_id,
            part_of="nmdc:sty-11-34xj1150",
            env_broad_scale=self._create_controlled_identified_term_value(
                BENTHIC_BROAD_SCALE_MAPPINGS.get(
                    biosample_row["aquaticSiteType"].values[0]
                ).get("term_id"),
                BENTHIC_BROAD_SCALE_MAPPINGS.get(
                    biosample_row["aquaticSiteType"].values[0]
                ).get("term_name"),
            ),
            env_local_scale=self._create_controlled_identified_term_value(
                BENTHIC_LOCAL_SCALE_MAPPINGS.get(
                    biosample_row["habitatType"].values[0]
                ).get("term_id"),
                BENTHIC_LOCAL_SCALE_MAPPINGS.get(
                    biosample_row["habitatType"].values[0]
                ).get("term_name"),
            ),
            env_medium=self._create_controlled_identified_term_value(
                BENTHIC_ENV_MEDIUM_MAPPINGS.get(
                    biosample_row["sampleMaterial"].values[0]
                ).get("term_id"),
                BENTHIC_ENV_MEDIUM_MAPPINGS.get(
                    biosample_row["sampleMaterial"].values[0]
                ).get("term_name"),
            ),
            name=neon_id,
            lat_lon=self._create_geolocation_value(
                biosample_row["decimalLatitude"].values[0],
                biosample_row["decimalLongitude"].values[0],
            ),
            elev=nmdc.Float(biosample_row["elevation"].values[0]),
            collection_date=self._create_timestamp_value(
                biosample_row["collectDate"].values[0]
            ),
            samp_size=self._create_quantity_value(
                biosample_row["fieldSampleVolume"].values[0], "mL"
            ),
            geo_loc_name=self._create_text_value(
                self.get_site_by_code(biosample_row["siteID"].values[0])
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

    def get_database(self):
        database = nmdc.Database()

        query = """
            SELECT
                merged.collectDate,
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
                afp.sampleID
            FROM amb_fieldParent AS afp
            LEFT JOIN
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
            ON
                merged.genomicsSampleID = afp.geneticSampleID
        """
        benthic_samples = pd.read_sql_query(query, self.conn)
        benthic_samples.to_sql(
            "benthicSamples", self.conn, if_exists="replace", index=False
        )

        sql_query = """
        WITH CTE AS (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY dnaSampleID, genomicsSampleID, sampleID ORDER BY ROWID) AS rn
            FROM
                benthicSamples
        )
        SELECT
            *
        FROM
            CTE
        WHERE
            rn = 1;
        """
        benthic_samples_filtered = pd.read_sql_query(sql_query, self.conn)

        benthic_samples_filtered = benthic_samples_filtered[
            benthic_samples_filtered["sampleID"].notna()
            & (benthic_samples_filtered["sampleID"] != "")
        ]

        neon_biosample_ids = benthic_samples_filtered["sampleID"]
        nmdc_biosample_ids = self._id_minter("nmdc:Biosample", len(neon_biosample_ids))
        neon_to_nmdc_biosample_ids = dict(zip(neon_biosample_ids, nmdc_biosample_ids))

        for neon_id, nmdc_id in neon_to_nmdc_biosample_ids.items():
            biosample_row = benthic_samples_filtered[
                benthic_samples_filtered["sampleID"] == neon_id
            ]

            database.biosample_set.append(
                self._translate_biosample(neon_id, nmdc_id, biosample_row)
            )

        return database
