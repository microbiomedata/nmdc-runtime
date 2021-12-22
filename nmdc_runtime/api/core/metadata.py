import os, sys
from collections import defaultdict, namedtuple

import inspect
import pandas as pds
from jsonschema import Draft7Validator
from pandas._typing import FilePathOrBuffer
from pandas.core.frame import DataFrame
from pymongo.database import Database as MongoDatabase
from toolz.dicttoolz import merge, dissoc, assoc_in, get_in
from functools import lru_cache
from types import ModuleType
from typing import Optional, Dict, List, Tuple, Any

from nmdc_schema import nmdc
from nmdc_runtime.util import nmdc_jsonschema
from linkml_runtime.utils.schemaview import SchemaView

# custom named tuple to hold path property information
SchemaPathProperties = namedtuple(
    "SchemaPathProperties", ["slots", "ranges", "multivalues"]
)


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
        df_id = group_id[1]  # dataframe for value group_id

        # split into var groups
        var_groups = df_id.groupby("group_var")
        for var_group in var_groups:
            # var = var_group[0]  # value of group_var
            df_var = var_group[1]  # dataframe for value group_var

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

    # add linkml class name for each id
    df["linkml_class"] = ""
    prev_id = ""
    class_name_dict = map_schema_class_names(nmdc)
    for ix, id_, collection_name in df[["group_id", "collection_name"]].itertuples():
        # check if there is a new id
        if id_ != prev_id:
            prev_id = id_  # update prev id
            data = mongodb[collection_name].find_one({"id": id_})

            # find the type of class the data instantiates
            if "type" in list(data.keys()):
                # get part after the ":"
                class_name = data["type"].split(":")[-1]
                class_name = class_name_dict[class_name]
            else:
                raise Exception("Cannot determine the type of class for ", id_)

        # set class name for id
        df["linkml_class"] = class_name

    # info about properties of slots in the property path
    df["linkml_slots"] = ""
    df["ranges"] = ""
    df["multivalues"] = ""
    view = SchemaView(
        os.path.join(os.path.dirname(sys.modules["nmdc_schema"].__file__), "nmdc.yaml")
    )
    for ix, attribute, path, class_name in df[
        ["attribute", "path", "linkml_class"]
    ].itertuples():
        # fetch the properites for the path
        if len(path) > 0:
            spp = fetch_schema_path_properties(view, path, class_name)
        else:
            spp = fetch_schema_path_properties(view, attribute, class_name)

        df.loc[ix, "linkml_slots"] = str.join("|", spp.slots)
        df.loc[ix, "ranges"] = str.join("|", spp.ranges)
        df.loc[ix, "multivalues"] = str.join("|", spp.multivalues)

    return df


def map_schema_class_names(nmdc_mod: ModuleType) -> Dict[str, str]:
    """Returns dict that maps the classes in the nmdc.py module (within the NMDC Schema PyPI library)
       to the class names used in the linkml schema.

    Parameters
    ----------
    nmdc_mod : ModuleType
        The nmdc.py module in the NMDC Schema library.

    Returns
    -------
    Dict[str, str]
        Maps the class as named in the module to the class name in the linkml schema.
        E.g., BiosampleProcessing -> biosample processing
    """
    class_dict = {}
    for name, member in inspect.getmembers(nmdc_mod):
        if inspect.isclass(member) and hasattr(member, "class_name"):
            class_dict[name] = member.class_name
    return class_dict


@lru_cache
def fetch_schema_path_properties(
    view: SchemaView, schema_path: str, class_name: str
) -> SchemaPathProperties:
    """Returns properies for a slot in the linkml schema.

    Parameters
    ----------
    view : SchemaView
        The SchemaView object holding the linkml schema
    schema_path : str
        The path in Mongo database to the value
    class_name : str
        The name of the class with the slot(s)

    Returns
    -------
    SchemaPathProperties
        A namedtuple of form "SchemaPathProperties", ["slots", "ranges", "multivalues"]
        that holds the property informaton about the slot.
          slots: a list of the linkml slots, this may differ from the path names
          ranges: a list of the range for slot in the slots list
          multivalues: a list of True/False strings specifying if the slot is multivaued

    Raises
    ------
    AttributeError
        If the slot is not found in the linkml schema, an AttributeError is raised.
    """
    # lists to hold properties for a value in the path
    slots = []
    ranges = []
    multivalues = []
    paths = schema_path.split(".")
    for path in paths:
        schema_class = view.get_class(class_name)  # get class from schema

        # first check if it is an induced slot
        # i.e., if slot properties have been overridden
        if path in schema_class.slot_usage.keys():
            schema_slot = view.induced_slot(path, class_name)
        elif path.replace("_", " ") in schema_class.slot_usage.keys():
            schema_slot = view.induced_slot(path.replace("_", " "), class_name)

        # if slot has not been overridden, check class attributes
        if path in schema_class.attributes.keys():
            schema_slot = view.induced_slot(path, class_name)
        elif path.replace("_", " ") in schema_class.attributes.keys():
            schema_slot = view.induced_slot(path.replace("_", " "), class_name)

        # if slot has not been overridden or is an attribute, get slot properties from view
        elif path in view.all_slots().keys():
            schema_slot = view.get_slot(path)
        elif path.replace("_", " ") in view.all_slots().keys():
            schema_slot = view.get_slot(path.replace("_", " "))

        # raise error if the slot is not found
        else:
            raise AttributeError(f"slot '{path}' not found for '{schema_class.name}'")

        # properties to lists as strings (strings are needed for dataframe)
        slots.append(str(schema_slot.name))

        if schema_slot.range is None:
            ranges.append("string")
        else:
            ranges.append(str(schema_slot.range))

        if schema_slot.multivalued is None:
            multivalues.append("False")
        else:
            multivalues.append(str(schema_slot.multivalued))

        # update the class name to range of slot
        class_name = schema_slot.range

    return SchemaPathProperties(slots, ranges, multivalues)


def make_vargroup_updates(df: pds.DataFrame) -> List:
    """Returns a list of update commands to execute on the Mongo database
       when updates are grouped with a grouping variable.

    Parameters
    ----------
    df : pds.DataFrame
        The dataframe that contains the values associated with the grouping variable.

    Returns
    -------
    List
        A list of Mongo update commands for that grouping variable.
    """
    id_ = df["group_id"].values[0]
    path_multivalued_dict = {}
    update_key = ""
    path_lists = []
    obj_dict = {}
    for (action, attribute, value, path, multivalues,) in df[
        [
            "action",
            "attribute",
            "value",
            "path",
            "multivalues",
        ]
    ].itertuples(index=False):
        if len(path) < 1:
            update_key = attribute
        else:
            # gather path lists
            path_list = path.split(".")
            path_lists.append(path_list)

            # determine if value is a list
            multivalues_list = multivalues.split("|")
            value = make_mongo_update_value(action, value, multivalues_list)

            # build dictionary that merges all keys and
            # values into a single object, e.g:
            # {'has_credit_associations': {
            #     'applied_role': 'Conceptualization',
            #     'applies_to_person': {
            #         'name': 'CREDIT NAME 1',
            #         'email': 'CREDIT_NAME_1@foo.edu',
            #         'orcid': 'orcid:0000-0000-0000-0001'}}}
            obj_dict = assoc_in(obj_dict, path_list, value)

            # for each potential path in the path list
            # deterimine if the value is multivalued
            for i in range(len(path_list)):
                key, value = ".".join(path_list[0 : i + 1]), multivalues_list[i]
                path_multivalued_dict[key] = value

    # sort path lists by length and reverse
    path_lists = list(reversed(sorted(path_lists, key=len)))
    longest = len(path_lists[0])

    # modify the values to have correct arity
    # start at the end of each path list and determine
    # if that path's value is multivalued
    for i in range(longest, 0, -1):
        for path_list in path_lists:
            # deermine if path is multivalued
            # note the use of the 0 to i portion of path list
            path_portion = path_list[0:i]
            is_multivalued = path_multivalued_dict[".".join(path_portion)]

            # modify object so that the key has correct multivalue
            temp = get_in(path_portion, obj_dict)
            if "True" == is_multivalued and (not isinstance(temp, list)):
                obj_dict = assoc_in(obj_dict, path_portion, [temp])

    update_dict = make_mongo_update_command_dict(
        action, id_, update_key, obj_dict[update_key]
    )

    return [update_dict]


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

    updates = []  # collected properties/values to updated
    for (action, value, path, multivalues,) in df[
        [
            "action",
            "value",
            "path",
            "multivalues",
        ]
    ].itertuples(index=False):
        # note: if a path is present, there is a value to be updated
        if len(path) > 0:
            update_dict = {}  # holds the values for the update query
            action = action.strip()  # remove extra white space

            # determine if value is a list
            value = make_mongo_update_value(action, value, multivalues.split("|"))

            # if a grouping variable (group_var) is present then a
            # complex object is used to update db
            # if len(group_var) > 0:
            #     obj = {}
            update_dict = make_mongo_update_command_dict(action, id_, path, value)
            updates.append(update_dict)  # add update commands to list

    return updates


def make_mongo_update_value(action: str, value: Any, multivalues_list: List) -> Any:
    """Based on the params, determines of the value for a Mongo update operation needs to be a list.

    Parameters
    ----------
    action : str
        The type of update that will be performed (e.g., insert items, replace)
    value : Any
        The value used for the update operation.
    multivalues_list : List
        List of 'True'/'False' values indicating if the value is to be multivalued (i.e., an array).

    Returns
    -------
    Any
        The value which may or may not be encapsulated in a list.
    """
    # if an array field is being updated, split based on pipe
    if multivalues_list[-1] == "True" or "|" in value:
        # value = value.strip()  # ****
        value = [v.strip() for v in value.split("|") if len(v.strip()) > 0]
    else:
        value = value.strip()  # remove extra white space

    return value


def make_mongo_update_command_dict(
    action: str, doc_id: str, update_key: str, update_value: Any
) -> Dict:
    """Returns a dict of the command need to execute a Mongo update opertation.

    Parameters
    ----------
    action : str
        The kind of update being performed (e.g., insert item, replace).
    doc_id : str
        The id of Mongo document being updated.
    update_key : str
        The property of document whose values are being updated.
    update_value : Any
        The new value used for updating.

    Returns
    -------
    Dict
        The Mongo command that when executed will update the document.
    """
    # build dict of update commands for Mongo
    if action in ["insert", "insert items", "insert item"]:
        update_dict = {
            "q": {"id": f"{doc_id}"},
            "u": {"$addToSet": {update_key: {"$each": update_value}}},
        }
    elif action in ["remove items", "remove item"]:
        update_dict = {
            "q": {"id": f"{doc_id}"},
            "u": {"$pull": {update_key: {"$in": update_value}}},
        }
    elif action in ["update", "set", "replace", "replace items"]:
        update_dict = {
            "q": {"id": f"{doc_id}"},
            "u": {"$set": {update_key: update_value}},
        }
    elif action in ["remove", "delete"]:  # remove the property from the object
        # note: the update_value in an $unset opertation doesn't matter
        # it is included so that we see it during debugging
        update_dict = {
            "q": {"id": f"{doc_id}"},
            "u": {"$unset": {update_key: update_value}},
        }
    else:
        raise ValueError(f"cannot execute action '{action}'")

    return update_dict


def map_id_to_collection(mongodb: MongoDatabase) -> Dict:
    """Returns dict using the collection name as a key and the ids of documents as values.

    Parameters
    ----------
    mongodb : MongoDatabase
        The Mongo database on which to build the dict.

    Returns
    -------
    Dict
        Dict mapping collection names to the set of document ids in the collection.
        key: collection name
        value: set(id of document)
    """
    collection_names = [
        name for name in mongodb.list_collection_names() if name.endswith("_set")
    ]
    id_dict = {
        name: set(mongodb[name].distinct("id"))
        for name in collection_names
        if "id_1" in mongodb[name].index_information()
    }
    return id_dict


def get_collection_for_id(
    id_: str, id_map: Dict, replace_underscore: bool = False
) -> Optional[str]:
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
    replace_underscore : bool
        If true, underscores in the collection name are replaced with spaces.

    Returns
    -------
    Optional[str]
        Collection name containing the document.
        None if the id was not found.
    """
    for collection_name in id_map:
        if id_ in id_map[collection_name]:
            if replace_underscore == True:
                return collection_name.replace("_", " ")
            else:
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
            if len(vg[0].strip()) > 0:
                ig_updates.extend(make_vargroup_updates(vg[1]))
            else:
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
    mdb_from : MongoDatbase
        Database from which data being copied (i.e., source).
    mdb_to: MongoDatabase
        Datbase which data is being copied into (i.e., destination).

    Returns
    -------
    results : Dict
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
