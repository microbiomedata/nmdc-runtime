from collections import defaultdict
import collections

import jq
import pandas as pds
from jsonschema import Draft7Validator
from pandas._typing import FilePathOrBuffer
from pandas.core.frame import DataFrame
from pymongo.database import Database as MongoDatabase
from toolz.dicttoolz import merge, dissoc, assoc_in, get_in
from functools import lru_cache
from typing import Optional, Dict, List, Tuple

from nmdc_runtime.util import nmdc_jsonschema


def load_changesheet(
    filename: FilePathOrBuffer, mongodb: MongoDatabase, sep="\t"
) -> pds.DataFrame:
    """
    Creates a datafame from the input file that includes extra columns used for
    determining the path for updating a Mongo document and the data type of the updated data.

    Returns
    -------
    Pandas DataFrame

    Parameters
    ----------
    filename : FilePathOrBuffer
        Name of the file containing the change sheet.
    mogodb : MongoDatabase
        The Mongo database that the change sheet will update.
    sep : str
        Column separator in file.

    Raises
    ------
    ValueError
        If input file lacks an id column.
    ValueError
        If input file lacks an attribute column.
    ValueError
        If input file lacks an action column.
    Exception
        If a document id is not found in the Mongo database.
    Exception
        If a class name cannot be determined.
    """

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
    # print("id_dict:", id_dict)
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
    df["base_range"] = ""
    df["base_item_type"] = ""
    df["prop_range"] = ""
    df["item_type"] = ""
    for ix, path, class_name in df[["path", "class_name"]].itertuples():
        if len(path) > 0:
            props = path.split(".")
            if 1 == len(props):
                prop_range = get_schema_range(class_name, props[0])
            else:
                # get the base range for the path (i.e., first part of path)
                # and set the class name to the range of the base
                base_range = get_schema_range(class_name, props[0])
                if tuple == type(base_range):
                    df.loc[ix, "base_range"] = base_range[0]
                    df.loc[ix, "base_item_type"] = base_range[1]
                    class_name = base_range[1]
                else:
                    df.loc[ix, "base_range"] = base_range
                    class_name = base_range

                # get the part of the class name after the ":"
                # e.g, "object:Study" -> Study
                if "object:" in class_name:
                    class_name = class_name.split(":")[1]

                # now find the ranges for the rest of the path
                for prop in props[1:]:
                    # class_name = get_schema_range(class_name, prop)
                    prop_range = get_schema_range(class_name, prop)

                    # tuple means the range was an array
                    # so get the class out of the tuple
                    if tuple == type(prop_range):
                        class_name = prop_range[1]

                    if "object:" in prop_range:
                        class_name = prop_range.split(":")[1]

            # if the range is an array a tuple is returned
            # the second element is the item type
            if type(prop_range) == tuple:
                df.loc[ix, "prop_range"] = prop_range[0]
                df.loc[ix, "item_type"] = prop_range[1]
            else:
                df.loc[ix, "prop_range"] = prop_range

    return df


@lru_cache
def get_schema_range(
    class_name: str, prop_name: str, schema: Dict = nmdc_jsonschema
) -> Optional[str]:
    """
    Search the schema to find the range property (prop_name) associated with a class (class_name).

    Parameters
    ----------
    class_name : str
        The name of class in the schema.
    prop_name : str
        The name of the property in the schema.
    schema : Dict
        A dictionary of the schema. Defaults to the nmdc_jsonschema.

    Returns
    -------
    Optional[str]
        Represents the data type of range of the property.
        None is returned if a range is not found.

    Raises
    ------
    Exception
        If the class name could not be found in the schema.
    Exception
        If the property name could not be found in the schema.
    """
    # find class schema
    query = f" .. | .{class_name}? | select(. != null)"
    try:
        class_schema = jq.compile(query).input(schema).first()
        # print("schema:", class_schema)
    except StopIteration:
        raise Exception(f"Could not find {class_name} in the schema")

    # find property
    query = f" .properties | .{prop_name}"
    try:
        prop_schema = jq.compile(query).input(class_schema).first()
        # print("schema:", prop_schema)
    except StopIteration:
        raise Exception(f"Could not find {prop_name} in the schema")

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


def make_updates(var_group: Tuple) -> List:
    """
    Creates a list of update commands to execute on the Mongo database.

    Parameters
    ----------
    var_group : Tuple
        Group of change sheet record based on the id column (generated by pandas.groupby()).
        var_group[0] -> the value (if any) in the group_var column
        var_group[1] -> the dataframe with group_var variables

    Returns
    -------
    List
        A list of Mongo update commands.
    """
    # group_var = var_group[0]  # the value (if any) in the group_var column
    df = var_group[1]  # dataframe with group_var variables
    id_ = df["group_id"].values[0]  # get id for group

    objects = []  # collected object groups
    updates = []  # collected properties/values to updated
    for (
        ix,
        action,
        value,
        path,
        base_range,
        base_item_type,
        prop_range,
        item_type,
    ) in df[
        [
            "action",
            "value",
            "path",
            "base_range",
            "base_item_type",
            "prop_range",
            "item_type",
        ]
    ].itertuples():
        if len(path) > 0:
            update_dict = {}  # holds the values for the update query
            action = action.strip()  # remove extra white space

            # if an array field is being updated, split based on pipe
            if (
                action in ["insert items", "remove items", "replace items"]
                or "array" == base_range
                or "array" == prop_range
            ):
                value = [v.strip() for v in value.split("|")]
            else:
                value = value.strip()  # remove extra white space

            if "insert items" == action:
                update_dict = {
                    "q": {"id": f"{id_}"},
                    "u": {"$addToSet": {path: {"$each": value}}},
                }
            elif "remove items" == action:
                update_dict = {
                    "q": {"id": f"{id_}"},
                    "u": {"$pull": {path: {"$in": value}}},
                }
            elif "replace items" == action:
                update_dict = {
                    "q": {"id": f"{id_}"},
                    "u": {"$set": {path: value}},
                }
            elif "array" == base_range or "array" == prop_range:
                if "object" in base_item_type or "object" in item_type:
                    props = path.split(".")
                    # gather values into dict (part after first ".")
                    value_dict = assoc_in({}, props[1:], value)

                    # add values list; props[0] is the first element in path
                    objects.append({props[0]: value_dict})
                else:
                    update_dict = {
                        "q": {"id": f"{id_}"},
                        "u": {"$addToSet": {path: {"$each": value}}},
                    }
            else:
                update_dict = {
                    "q": {"id": f"{id_}"},
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

        # we know we are updating an array b/c it
        # was added to the objects list
        update_dict = {
            "q": {"id": f"{id_}"},
            "u": {"$addToSet": {update_key: value_dict}},
        }

        # if update_key == "has_credit_associations":
        #     print(update_dict)

        updates.append(update_dict)

    return updates


def map_id_to_collection(mongodb: MongoDatabase) -> Dict:
    collection_names = [
        name for name in mongodb.list_collection_names() if name.endswith("_set")
    ]
    id_dict = {
        name: set(mongodb[name].distinct("id"))
        for name in collection_names
        if "id_1" in mongodb[name].index_information()
    }
    return id_dict


def get_collection_for_id(id_: str, id_map: Dict) -> Optional[str]:
    """
    Returns the name of the collect that contains the document idenfied by the id.

    Parameters
    ----------
    id_ : str
        The identifier of the document.
    id_map : Dict
        A dict mapping collection names to document ids.
        key: collection name
        value: set of document ids

    Returns
    -------
    Optional[str]
        Collection name containing the document.
        None if the id was not found.
    """
    for collection_name in id_map:
        if id_ in id_map[collection_name]:
            return collection_name
    return None


def mongo_update_command_for(df_change: pds.DataFrame) -> Dict[str, list]:
    """
    Creates a dictionary of update commands to be executed against the Mongo database.

    Parameters
    ----------
    df_change : pds.DataFrame
        A dataframe containing change sheet information

    Returns
    -------
    Dict
        A dict of the update commands to be executed.
        key: collection name
        value: list of update commands
    """
    update_cmd = {}  # list of dicts to hold mongo update queries

    # split data into groups by values in the group_id column (e.g., gold:Gs0103573)
    id_group = df_change.groupby("group_id")
    for ig in id_group:
        # ig[0] -> id_: group_id for data
        # ig[1] -> df_id: dataframe with rows having the group_id
        id_, df_id = ig

        # split data into groups by values in the group_var column (e.g, v1, v2)
        var_group = df_id.groupby("group_var")
        ig_updates = []  # update commands for the id group
        for vg in var_group:
            # vg[0] -> group_var for data
            # vg[1] -> dataframe with rows having the group_var
            ig_updates.extend(make_updates(vg))

        # add update commands for the group id to dict
        update_cmd[id_] = {
            "update": df_id["collection_name"].values[0],
            "updates": ig_updates,
        }
    return update_cmd


def copy_docs_in_update_cmd(
    update_cmd, mdb_from: MongoDatabase, mdb_to: MongoDatabase, drop_mdb_to: bool = True
) -> Dict[str, str]:
    """
    Copies data between Mongo databases.
    Useful to apply and inspect updates on a test database.

    Parameters
    ----------
    mdb_from: MongoDatbase
        Database from which data being copied (i.e., source).
    mdb_to: MongoDatabase
        Datbase which data is being copied into (i.e., destination).

    Returns
    -------
    results: Dict
        Dict with collection name as the key, and a message of number of docs inserted as value.
    """
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


def update_mongo_db(mdb: MongoDatabase, update_cmd: Dict):
    """
    Updates the Mongo database using commands in the update_cmd dict.

    Parameters
    ----------
    mdb : MongoDatabase
        Mongo database to be updated.
    update_cmd : Dict
        Contians update commands to be executed.

    Returns
    -------
    results: Dict
        Information about what was updated in the Mongo database.
    """
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
