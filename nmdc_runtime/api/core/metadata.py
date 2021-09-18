import copy
import pandas as pds
from pandas._typing import FilePathOrBuffer
from toolz import assoc_in, merge


def load_changesheet(
    filename: FilePathOrBuffer, sep="\t", path_separator="/"
) -> pds.DataFrame:
    # load dataframe replacing NaN with ''
    df = pds.read_csv(filename, sep=sep, dtype="string").fillna("")

    # add a group id column, but copy only IRIs (has ":" in it)
    try:
        df["group_id"] = df["id"].map(lambda x: x if ":" in x else "")
    except KeyError:
        raise ValueError("change sheet lacks 'id' column.")

    # fill in blank group ids
    for i in range(len(df)):
        if len(str(df.loc[i, "group_id"]).strip()) < 1:
            df.loc[i, "group_id"] = df.loc[i - 1, "group_id"]

    # fill in blank action columns
    try:
        for i in range(len(df)):
            if len(str(df.loc[i, "action"]).strip()) < 1:
                df.loc[i, "action"] = df.loc[i - 1, "action"]
    except KeyError:
        raise ValueError("change sheet lacks 'action' column.")

    # build dict to hold variables that have been defined
    # in the id column of the change sheet
    try:
        var_dict = {
            val: []
            for val, attr in df[["id", "attribute"]].values
            if len(val) > 0 and ":" not in val
        }
    except KeyError:
        # note the presence of the id column is checked above
        raise ValueError("change sheet lacks 'attribute' column.")

    # add group_var column to hold values from the id column
    # that are being used varialbe/blank nodes
    df["group_var"] = df["id"].map(lambda x: x if not (":" in x) else "")

    # add path column used to hold the path in the data to the data that will be changed
    # e.g. principal_investigator/name
    df["path"] = ""
    for ix, _id, attr, val in df[["id", "attribute", "value"]].itertuples():
        # case 1: a variable is in the id and value colums
        if _id in var_dict.keys() and val in var_dict.keys():
            # update the var_dict with the info
            # from the id var and attribute
            var_dict[val].extend(var_dict[_id])
            var_dict[val].append(attr)

        # case 2: a variable is only in the value column
        elif val in var_dict.keys():
            # upate var_dict with attribute
            var_dict[val].append(attr)

        # otherwise the value column has a change value
        # so, update the path column to hold the change path
        else:
            # check if there is a variable in the id column
            if len(_id) > 0 and _id in var_dict.keys():
                df.loc[ix, "path"] = (
                    path_separator.join(var_dict[_id]) + path_separator + attr
                )
            else:
                df.loc[ix, "path"] = attr

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


def update_var_group(data, var_group, schema=None, path_separator="/"):
    # split the id group by the group variables
    # var_group[0] -> the variable (if any) in the group_var column
    # var_group[1] -> the dataframe with these variables
    change_df = var_group[1]

    # the grouped dataframes may have indexes that don't
    # line with the row number, so reset the index
    change_df = change_df.reset_index(drop=True)

    for i in range(len(change_df)):
        attribute_path = change_df.loc[i, "path"]
        if len(attribute_path) > 0:
            attribute_path = change_df.loc[i, "path"].split(path_separator)
            if schema:
                check_attribute_path(schema, attribute_path)
            new_val = change_df.loc[i, "value"]
            if len(str(new_val)) > 0:
                data = assoc_in(data, attribute_path, new_val)

    return data


def update_data(
    df_change: pds.DataFrame,
    path_separator="/",
    print_data=False,
    print_update=False,
    copy_data=False,
) -> dict:
    # split the change sheet by the group id values
    id_group = df_change.groupby("group_id")
    for ig in id_group:
        # ig[0] -> id of the data
        # ig[1] -> dataframe with rows with the id

        schema = try_fetching_schema_for_id(ig[0])
        # data = get_study_data(ig[0])  # retrieve data

        # make a copy of the data for testing purposes
        if copy_data == True:
            data = copy.deepcopy(data)

        if print_data:
            print(data)

        # split the id group by the group variables
        var_group = ig[1].groupby("group_var")
        for vg in var_group:
            data = update_var_group(data, vg, schema, path_separator)

        if print_update:
            print(data)

    return data


def make_update_query(collection_name: str, id_val: str, update_values: list):
    update_query = {"update": f"{collection_name}", "updates": []}

    for uv in update_values:
        v = make_update_query_value(id_val, uv)
        update_query["updates"].append(v)

    return update_query


def make_update_query_value(id_val: str, update_value: dict):
    update_dict = {"q": {"id": f"{id_val}"}, "u": {"$set": update_value}}
    return update_dict
