## author: Bill Duncan
## summary: Contains methods for extracting data for the NMDC ETL pipeline.

## system level modules
import pandas as pds


def extract_table(merged_df, table_name):
    df = unpivot_dataframe(merged_df[merged_df.nmdc_data_source == table_name])
    return df


def unpivot_dataframe(
    df,
    index="nmdc_record_id",
    columns="attribute",
    value="value",
    splice=["nmdc_record_id", "attribute", "value"],
):
    ## reshape eav structure to row-column structure
    ## see: https://www.journaldev.com/33398/pandas-melt-unmelt-pivot-function
    if len(splice) > 0:
        df = df[splice].pivot(index=index, columns=columns)
    else:
        df = df.pivot(index=index, columns=columns)

    if len(df) > 0:
        df = df[value].reset_index()  # drop value hierarchical index
    if len(df) > 0:
        df = df.where(pds.notnull(df), None)  # replace an NaN values with None
    df.columns.name = None  # remove column name attribute

    return df
