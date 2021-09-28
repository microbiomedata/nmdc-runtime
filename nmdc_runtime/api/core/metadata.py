from collections import defaultdict

import jq
import pandas as pds
from jsonschema import Draft7Validator
from pandas._typing import FilePathOrBuffer
from pymongo.database import Database as MongoDatabase
from toolz import assoc_in
from toolz.dicttoolz import merge, dissoc

from nmdc_runtime.util import nmdc_jsonschema


def load_changesheet(
    filename: FilePathOrBuffer,
    mongodb,
    sep="\t",
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
        # TODO: Perhaps raise an error here
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
    # these objects are added to an array above
    if len(objects) > 0:
        update_key = list(objects[0].keys())[0]  # get key from first element

        # collect the values of each object into a list
        # note the filter for the values being a dict
        # e.g., the values in the objects are collected as:
        # [{'applied_role': 'Conceptualization'},
        #  {'applies_to_person': {'name': 'Kelly Wrighton'}},
        #  {'applies_to_person': {'email': 'Kelly.Wrighton@colostate.edu'}},
        #  {'applies_to_person': {'orcid': 'orcid:0000-0003-0434-4217'}}]
        values = [
            list(obj.values())[0]
            for obj in filter(lambda obj: type(obj) == dict, objects)
        ]

        # put the values into a dict with each unique key mapped to a list of values
        # e.g., the example above is tranformed to:
        # {'applied_role': 'Conceptualization',
        #  'applies_to_person': [{'name': 'Kelly Wrighton'},
        #  {'email': 'Kelly.Wrighton@colostate.edu'},
        #  {'orcid': 'orcid:0000-0003-0434-4217'}]}
        value_dict = {}
        for val in values:
            for k, v in val.items():
                if type(v) == dict:
                    if k in value_dict.keys():
                        value_dict[k].append(v)
                    else:
                        value_dict[k] = [v]
                else:
                    value_dict[k] = v

        # now merge values with lists of dicts
        # e.g., the value_dict above is transformed to:
        # {'applied_role': 'Conceptualization',
        #  'applies_to_person': {
        #     'name': 'Kelly Wrighton',
        #     'email': 'Kelly.Wrighton@colostate.edu',
        #     'orcid': 'orcid:0000-0003-0434-4217'}}
        for k, v in value_dict.items():
            if type(v) == list and type(v[0]) == dict:
                value_dict[k] = merge(v)

        update_dict = {
            "q": {"id": f"{id_val}"},
            "u": {"$addToSet": {update_key: value_dict}},
        }
        updates.append(update_dict)

    return updates


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


def mongo_update_command_for(df_change: pds.DataFrame) -> dict:
    update_cmd = {}  # list of dicts to hold mongo update queries

    # split the change sheet by the group id values
    id_group = df_change.groupby("group_id")
    for ig in id_group:
        id_val, df_id = ig
        # split the id group by the group variables
        var_group = df_id.groupby("group_var")
        ig_updates = []
        for vg in var_group:
            # vg[0] -> group_var for data
            # vg[1] -> dataframe with rows having the group_var
            ig_updates.extend(make_updates(vg))
        update_cmd[id_val] = {
            "update": df_id["collection_name"].values[0],
            "updates": ig_updates,
        }
    return update_cmd


def copy_docs_in_update_cmd(
    update_cmd, mdb_from: MongoDatabase, mdb_to: MongoDatabase, drop_mdb_to=True
):
    """Useful to apply and inspect updates on a test database."""
    doc_specs = defaultdict(list)
    for id_, update_cmd_doc in update_cmd.items():
        collection_name = update_cmd_doc["update"]
        doc_specs[collection_name].append(id_)

    if drop_mdb_to:
        mdb_to.client.drop_database(mdb_to.name)
    results = {}
    for collection_name, ids in doc_specs.items():
        docs = [
            dissoc(d, "_id")
            for d in mdb_from[collection_name].find({"id": {"$in": ids}})
        ]
        results[
            collection_name
        ] = f"{len(mdb_to[collection_name].insert_many(docs).inserted_ids)} docs inserted"
    return results


def update_mongo_db(mdb: MongoDatabase, update_cmd) -> dict:
    results = []
    validator = Draft7Validator(nmdc_jsonschema)

    for id_, update_cmd_doc in update_cmd.items():
        collection_name = update_cmd_doc["update"]
        doc_before = dissoc(mdb[collection_name].find_one({"id": id_}), "_id")
        update_result = mdb.command(update_cmd_doc)
        doc_after = dissoc(mdb[collection_name].find_one({"id": id_}), "_id")
        errors = list(validator.iter_errors({collection_name: [doc_after]}))
        results.append(
            {
                "id": id_,
                "doc_before": doc_before,
                "update_info": update_result,
                "doc_after": doc_after,
                "validation_errors": [e.message for e in errors],
            }
        )

    return results
