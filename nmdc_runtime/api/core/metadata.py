import copy
import json

import pandas as pds
from pandas._typing import FilePathOrBuffer
from pymongo import MongoClient
from toolz import assoc_in
import jq

from nmdc_runtime.util import nmdc_jsonschema


def load_changesheet(
    filename: FilePathOrBuffer, mongodb, sep="\t", path_separator="."
) -> pds.DataFrame:

    # load dataframe replacing NaN with ''
    df = pds.read_csv(filename, sep=sep, dtype="string").fillna("")
    # df = pds.read_csv(filename, sep=sep, dtype="string")

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

    # create map between id and collection
    id_dict = map_id_to_collection(mongodb)

    # add collection for each id
    df["collection_name"] = ""
    prev_id = ""
    for ix, group_id in df[["group_id"]].itertuples():
        # check if there is a new id
        if group_id != prev_id:
            prev_id = group_id  # update prev id
            collection_name = get_collection_for_id(group_id, id_dict)
        df["collection_name"] = collection_name

    # add class name for each id
    df["class_name"] = ""
    prev_id = ""
    for ix, _id, collection_name in df[["group_id", "collection_name"]].itertuples():
        # check if there is a new id
        if _id != prev_id:
            prev_id = _id  # update prev id
            data = mongodb[collection_name].find_one({"id": _id})

            # find the type of class the data instantiates
            if "type" in list(data.keys()):
                # get part after the ":"
                class_name = data["type"].split(":")[-1]
            else:
                raise Exception("Cannot determine the type of class for ", _id)

        # set class name for id
        df["class_name"] = class_name

    # add info about the level of property nesting, prop range
    # and type of item for arrays
    df["prop_depth"] = ""
    df["prop_range"] = ""
    df["item_type"] = ""
    for ix, path, class_name in df[["path", "class_name"]].itertuples():
        if len(path) > 0:
            props = path.split(path_separator)
            df.loc[ix, "prop_depth"] = len(props)

            for p in props:
                prop_range = get_schema_range(class_name, p)

                # nested paths will have range None
                # so, clean these up by getting range of base property
                if prop_range is None:
                    base_prop = path.split(path_separator)[0]
                    prop_range = get_schema_range(class_name, base_prop)

                # if the range is an array a tuple is returned
                # the second element is the item type
                if type(prop_range) == tuple:
                    df.loc[ix, "prop_range"] = prop_range[0]
                    df.loc[ix, "item_type"] = prop_range[1]
                else:
                    df.loc[ix, "prop_range"] = prop_range

    return df


def get_schema_range(class_name, prop_name, schema=nmdc_jsonschema):
    # define jq query
    query = f" .. | .{class_name}? | .properties | .{prop_name} | select(. != null)"
    # print(query)

    # find property
    try:
        prop_schema = jq.compile(query).input(schema).first()
        # print("schema:", prop_schema)
    except StopIteration:
        prop_schema = None

    # find property range/type
    if prop_schema:
        if "type" in prop_schema.keys():
            rv = prop_schema["type"]
        elif "$ref" in prop_schema.keys():
            rv = prop_schema["$ref"].split("/")[-1]
        else:
            rv = ""
    else:
        rv = None

    # find item types for arrays
    if prop_schema is not None and "array" == rv:
        if "items" in prop_schema.keys():
            # create tuple with array item type info
            if "type" in prop_schema["items"]:
                rv = rv, prop_schema["items"]["type"]

            if "$ref" in prop_schema["items"]:
                rv = rv, prop_schema["items"]["$ref"].split("/")[-1]

    return rv


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


####################################################################
# functions for updating raw data
####################################################################


def update_var_group(data, var_group, collection_name=None, path_separator=".") -> dict:
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
            if collection_name:
                check_attribute_path(collection_name, attribute_path)
            new_val = change_df.loc[i, "value"]
            if len(str(new_val)) > 0:
                data = assoc_in(data, attribute_path, new_val)

    return data


def update_data(
    df_change: pds.DataFrame,
    path_separator=".",
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


####################################################################
# funcitons to update mongodb
####################################################################


def fetch_schema_for_class(class_name: str) -> dict:
    # find schema info for the class
    if class_name not in nmdc_jsonschema["definitions"]:
        raise Exception(f"{class_name} not found in the NMDC Schema")
    else:
        return nmdc_jsonschema["definitions"][class_name]


def make_update_query(collection_name: str, data: dict, update_values: list):
    # id value from the data
    if "id" not in data.keys():
        raise Exception("data does not have an ID")
    else:
        id_val = data["id"]

    # find the type of class the data instantiates
    if "type" in data.keys():
        # get part after the ":"
        class_name = data["type"].split(":")[-1]
    else:
        raise Exception("Cannot determine the type of class for ", id_val)

    # get schema for the class
    schema = fetch_schema_for_class(class_name)

    update_query = {"update": f"{collection_name}", "updates": []}
    for uv in update_values:
        v = make_update_query_value(id_val, uv)
        update_query["updates"].append(v)

    return update_query


def make_update_query_value(id_val: str, update_value: dict, val_type="string"):
    # TODO $set if non-array value, $addToSet otherwise.
    update_dict = {"q": {"id": f"{id_val}"}, "u": {"$set": update_value}}
    return update_dict


def map_id_to_collection(mongodb) -> dict:
    collection_names = [
        name for name in mongodb.list_collection_names() if name.endswith("_set")
    ]
    id_dict = {
        name: set(mongodb[name].distinct("id"))
        for name in collection_names
        if "id_1" in mongodb[name].index_information()
    }
    return id_dict


def get_collection_for_id(id_val, id_map):
    for collection_name in id_map:
        if id_val in id_map[collection_name]:
            return collection_name
    return None


def make_update_var_group(var_group, collection_name=None, path_separator=".") -> list:
    # split the id group by the group variables
    # var_group[0] -> the variable (if any) in the group_var column
    # var_group[1] -> the dataframe with these variables
    change_df = var_group[1]

    # the grouped dataframes may have indexes that don't
    # line with the row number, so reset the index
    change_df = change_df.reset_index(drop=True)

    updates = []  # list of dicts
    for i in range(len(change_df)):
        attribute_path = change_df.loc[i, "path"]
        if len(attribute_path) > 0:
            attribute_path = change_df.loc[i, "path"].split(path_separator)
            # if collection_name:
            #    check_attribute_path(collection_name, attribute_path)

            new_val = change_df.loc[i, "value"]
            if len(str(new_val)) > 0:
                # mongodb requries a '.' for nested objects
                update = {".".join(attribute_path): new_val}
                updates.append(update)

    return updates


def update_mongo_db(
    df_change: pds.DataFrame,
    mongodb,
    path_separator=".",
    print_data=False,
    print_update=False,
    print_query=False,
) -> dict:

    # create a dict between collections names and ids
    id_dict = map_id_to_collection(mongodb)

    # split the change sheet by the group id values
    id_group = df_change.groupby("group_id")

    updates = []  # list of dicts
    for ig in id_group:
        # ig[0] -> id of the data
        # ig[1] -> dataframe with rows with the id

        #         schema = try_fetching_schema_for_id(ig[0])
        collection_name = get_collection_for_id(ig[0], id_dict)

        # get data from mongodb with id value in ig[0]
        if collection_name is None:
            raise Exception("Cannot find ID", ig[0], "in any collection")
        else:
            data = mongodb[collection_name].find_one({"id": ig[0]})

        if print_data:
            print(data)

        # split the id group by the group variables
        var_group = ig[1].groupby("group_var")
        for vg in var_group:
            update = make_update_var_group(vg, collection_name, path_separator)
            if len(update) > 0:
                updates.extend(update)

        # update mongo
        update_query = make_update_query(collection_name, data, updates)
        status = mongodb.command(update_query)

        if print_query:
            print(json.dumps(update_query, indent=2))

        if print_update:
            data = mongodb[collection_name].find_one({"id": ig[0]})
            print(data)

    return status
