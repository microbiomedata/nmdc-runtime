import copy
import json

import pandas as pds
from pandas._typing import FilePathOrBuffer
from pymongo import MongoClient
from toolz import assoc_in
import jq
from toolz.dicttoolz import merge

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
        # collect vars in the id column
        var_dict = {
            id_val: None
            for id_val, attr in df[["id", "attribute"]].values
            if len(id_val) > 0 and ":" not in id_val
        }
    except KeyError:
        # note the presence of the id column is checked above
        raise ValueError("change sheet lacks 'attribute' column.")

    # add group_var column to hold values from the id column
    # that are being used varialbe/blank nodes
    # df["group_var"] = df["id"].map(lambda x: x if not (":" in x) else "")
    df["group_var"] = ""
    for ix, id_val, attr, value in df[["id", "attribute", "value"]].itertuples():
        if id_val in var_dict.keys() and value in var_dict.keys():
            var_dict[value] = f"{var_dict[id_val]}.{attr}"
            var_dict[f"{id_val}.{value}"] = f"{var_dict[id_val]}.{attr}"
            df.loc[ix, "group_var"] = f"{id_val}.{value}"
        elif value in var_dict.keys():
            var_dict[value] = attr
            df.loc[ix, "group_var"] = value
        elif id_val in var_dict.keys():
            df.loc[ix, "group_var"] = id_val

    # add path column used to hold the path in the data to the data that will be changed
    # e.g. principal_investigator.name
    df["path"] = ""
    # split into id groups, this allow each id group to have its own local variables
    # i.e., same var name can be used with different ids
    group_ids = df.groupby("group_id")
    for group_id in group_ids:
        df_id = group_id[1]  # dataframe of group_id

        # split into var groups
        var_groups = df_id.groupby("group_var")
        for var_group in var_groups:
            # var = var_group[0]  # value of group_var
            df_var = var_group[1]  # dataframe of group_var

            for ix, attr, value, group_var in df_var[
                ["attribute", "value", "group_var"]
            ].itertuples():
                # if group_var is empty, it is a simple property
                if "" == group_var:
                    df.loc[ix, "path"] = attr

                # otherwise, it is a nested property
                # if the value is not a var, then we are at bottom level
                elif value not in var_dict.keys():
                    df.loc[ix, "path"] = f"{var_dict[group_var]}.{attr}"

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

            if collection_name is None:
                raise Exception("Cannot find ID", group_id, "in any collection")

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
            props = path.split(".")
            df.loc[ix, "prop_depth"] = len(props)

            if 1 == len(props):
                prop_range = get_schema_range(class_name, props[0])
            else:
                for p in props:
                    prop_range = get_schema_range(class_name, p)
                    base_prop = path.split(".")[0]
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
            rv = f"""object:{prop_schema["$ref"].split("/")[-1]}"""
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
                rv = rv, f"""object:{prop_schema["items"]["$ref"].split("/")[-1]}"""

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


def make_updates(var_group: tuple) -> list:
    # group_var = var_group[0]  # the variable (if any) in the group_var column
    df = var_group[1]  # dataframe with group_var variables
    id_val = df["group_id"].values[0]  # get id for group

    objects = []  # collected object groups
    updates = []  # properties/values to updated
    for ix, value, path, prop_range, item_type in df[
        ["value", "path", "prop_range", "item_type"]
    ].itertuples():
        if len(path) > 0:
            update_dict = {}
            if "array" == prop_range:
                if "object" in item_type:
                    props = path.split(".")
                    # gather values into dict (part after first ".")
                    value_dict = assoc_in({}, props[1:], value)

                    # add values list; props[0] is the first element in path
                    objects.append({props[0]: value_dict})
                else:
                    update_dict = {
                        "q": {"id": f"{id_val}"},
                        "u": {"$addToSet": {path: value}},
                    }
            else:
                update_dict = {
                    "q": {"id": f"{id_val}"},
                    "u": {"$set": {path: value}},
                }

            if len(update_dict) > 0:
                updates.append(update_dict)

    # add collected objects to updates
    # these objects are added to an array
    if len(objects) > 0:
        key = list(objects[0].keys())[0]  # get key from first element
        values_dict = merge([list(d.values())[0] for d in objects])
        update_dict = {
            "q": {"id": f"{id_val}"},
            "u": {"$addToSet": {key: values_dict}},
        }
        updates.append(update_dict)

    return updates


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


def update_mongo_db(
    df_change: pds.DataFrame,
    mongodb,
    path_separator=".",
    print_data=False,
    print_update=False,
    print_query=False,
) -> dict:

    updates = []  # list of dicts to hold mongo update queries

    # split the change sheet by the group id values
    id_group = df_change.groupby("group_id")
    for ig in id_group:
        id_val = ig[0]
        df_id = ig[1]

        if print_data:
            data = mongodb[df_id["collection_name"][0]].find_one({"id": id_val})
            print(data)

        # split the id group by the group variables
        var_group = df_id.groupby("group_var")
        for vg in var_group:
            # vg[0] -> group_var for data
            # vg[1] -> dataframe with rows having the group_var
            update = make_updates(vg)

            if len(update) > 0:
                updates.extend(update)

        # print(json.dumps(updates, indent=2))

        # update mongo
        # update_query = make_update_query(df_id["collection_name"][0], data, updates)
        update_query = {"update": df_id["collection_name"][0], "updates": []}
        update_query["updates"].extend(updates)

        if print_query:
            print(json.dumps(update_query, indent=2))

        status = mongodb.command(update_query)

        if print_update:
            data = mongodb[df_id["collection_name"][0]].find_one({"id": id_val})
            print(data)

    return status
