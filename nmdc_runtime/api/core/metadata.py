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


def update_data_group(data, df_change, path_separator="/"):
    value_dicts = []

    for i in range(len(df_change)):
        # get path and pop off the last element
        attribute_path = str.split(df_change.loc[i, "path"], path_separator)
        last_attr = attribute_path.pop()

        # create a dict from the last attribute and value
        value_dict = {last_attr: df_change.loc[i, "value"]}
        value_dicts.append(value_dict)

    # merge the value dicts into a single merged value
    value = merge(value_dicts)

    return assoc_in(data, attribute_path, value)


def update_data(
    data: dict,
    df_change: pd.DataFrame,
    group_var: str,
    path_separator="/",
    print_data=False,
    print_update=False,
    copy_input_data=False,
) -> dict:
    # make a copy of the data for testing purposes
    if copy_input_data == True:
        data = copy.deepcopy(data)

    if print_data:
        print(data)

    schema = try_fetching_schema_for_id(data["id"])

    # the grouped dataframes may have indexes that don't
    # line with the row number, so reset the index
    df_change = df_change.reset_index(drop=True)
    print("group_var:", group_var)
    if group_var.strip() == "" or len(df_change) == 1:
        for i in range(len(df_change)):
            attribute_path = df_change.loc[i, "path"].split(path_separator)
            if len(attribute_path) > 0:
                check_attribute_path(schema, attribute_path)
                new_val = df_change.loc[i, "value"]
                data = assoc_in(data, attribute_path, new_val)
    else:
        data = update_data_group(data, df_change, path_separator)

    if print_update:
        print(data)

    return data
