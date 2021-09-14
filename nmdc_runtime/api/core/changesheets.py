import copy

import pandas as pd
from pandas._typing import FilePathOrBuffer
from toolz import assoc_in


def load_changesheet(filename: FilePathOrBuffer, sep="\t", dtype="string") -> pd.DataFrame:
    # load dataframe replacing NaN with ''
    df = pd.read_csv(filename, sep=sep).fillna("")

    # add a group id column but copy only iris (has ":" in it)
    try:
        df["group_id"] = df["id"].map(lambda x: x if ":" in x else "")
    except KeyError:
        raise ValueError("Changesheet lacks 'id' column.")

    # fill in blank group ids
    for i in range(len(df)):
        if len(str(df.loc[i, "group_id"]).strip()) < 1:
            df.loc[i, "group_id"] = df.loc[i - 1, "group_id"]

    return df


def try_fetching_schema_for_id(id_):
    """
    TODO: Check that ID is a valid NMDC resource ID, and return the
      resource schema so that requested attribute paths to update can be checked against schema.
      Raise an exception if the ID is not valid.
    """
    return "study"  # returns a string right now because unused.


def check_attribute_path(schema, attribute_path):
    """
    TODO: Check that attribute path is valid given the resource schema. Raise exception if not.
    """
    return True


def update_data(
    data: dict,
    df_change: pd.DataFrame,
    separator="/",
    print_data=False,
    print_update=False,
) -> dict:
    # make a copy of the data for testing purposes
    new_data = copy.deepcopy(data)

    if print_data:
        print(new_data)

    schema = try_fetching_schema_for_id(data["id"])

    # the grouped dataframes may have indexes that don't
    # line with the row number, so reset the index
    df_change = df_change.reset_index(drop=True)

    for i in range(len(df_change)):
        attribute_path = df_change.loc[i, "attribute"].split(separator)
        check_attribute_path(schema, attribute_path)
        new_val = df_change.loc[i, "value"]
        new_data = assoc_in(new_data, attribute_path, new_val)

    if print_update:
        print(new_data)

    return new_data
