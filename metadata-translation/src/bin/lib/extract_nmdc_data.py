## author: Bill Duncan
## summary: Contians methods for extracting data for the NMDC ETL pipeline.

## add ./lib directory to sys.path so that local modules can be found
import os, sys

sys.path.append(os.path.abspath("."))
# print(sys.path)

## system level modules
import pandas as pds
import jq
import jsonasobj
import json
import zipfile
import yaml
from yaml import CLoader as Loader, CDumper as Dumper
from dotted_dict import DottedDict
from collections import namedtuple


def extract_table (merged_df, table_name):
    df = unpivot_dataframe(merged_df[merged_df.nmdc_data_source == table_name])
    return df


def unpivot_dataframe (df, index='nmdc_record_id', columns='attribute', value='value',
                       splice=['nmdc_record_id', 'attribute', 'value']):
    ## reshape eav structure to row-column structure
    ## see: https://www.journaldev.com/33398/pandas-melt-unmelt-pivot-function
    if len(splice) > 0:
        df = df[splice].pivot(index=index, columns=columns)
    else:
        df = df.pivot(index=index, columns=columns)
    
    if len(df) > 0: df = df[value].reset_index()  # drop value hierarchical index
    if len(df) > 0: df = df.where(pds.notnull(df), None) # replace an NaN values with None
    df.columns.name = None  # remove column name attribute
    
    return df