import math
from typing import Union

import pandas as pd
from nmdc_schema import nmdc


def _get_value_or_none(data: pd.DataFrame, column_name: str) -> Union[str, float, None]:
    """
    Get the value from the specified column in the data DataFrame.
    If the column value is NaN, return None. However, there are handlers
    for a select set of columns - horizon, qaqcStatus, sampleTopDepth,
    and sampleBottomDepth.

    :param data: DataFrame to read the column value from.
    :return: Either a string, float or None depending on the column/column values.
    """
    if (
        column_name in data
        and not data[column_name].isna().any()
        and not data[column_name].empty
    ):
        if column_name == "horizon":
            return f"{data[column_name].values[0]} horizon"
        elif (
            column_name == "qaqcStatus"
            or column_name == "extrQaqcStatus"
            or column_name == "seqQaqcStatus"
        ):
            return data[column_name].values[0].lower()
        elif column_name == "sampleTopDepth":
            return float(data[column_name].values[0]) / 100
        elif column_name == "sampleBottomDepth":
            return float(data[column_name].values[0]) / 100
        else:
            return data[column_name].values[0]

    return None


def _create_controlled_identified_term_value(
    id: str = None, name: str = None
) -> nmdc.ControlledIdentifiedTermValue:
    """
    Create a ControlledIdentifiedTermValue object with the specified id and name.

    :param id: CURIE (with defined prefix expansion) or full URI of term.
    :param name: Name of term.
    :return: ControlledIdentifiedTermValue with mandatorily specified value for `id`.
    """
    if id is None or name is None:
        return None
    return nmdc.ControlledIdentifiedTermValue(term=nmdc.OntologyClass(id=id, name=name))


def _create_controlled_term_value(name: str = None) -> nmdc.ControlledTermValue:
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


def _create_timestamp_value(value: str = None) -> nmdc.TimestampValue:
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
    numeric_value: Union[str, int, float] = None, unit: str = None
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


def _create_text_value(value: str = None) -> nmdc.TextValue:
    """
    Create a TextValue object with the specified value.

    :param value: column that we expect to primarily have text values.
    :return: Text wrapped in TextValue object.
    """
    if value is None:
        return None
    return nmdc.TextValue(has_raw_value=value)


def _create_double_value(value: str = None) -> nmdc.Double:
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
    latitude: str = None, longitude: str = None
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
