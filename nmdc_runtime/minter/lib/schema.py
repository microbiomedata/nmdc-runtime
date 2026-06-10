from functools import lru_cache

from nmdc_runtime.util import nmdc_schema_view
from nmdc_schema.id_helpers import get_compatible_typecodes


@lru_cache
def get_class_name_to_class_uri_map() -> dict[str, str]:
    """
    Returns a mapping from schema class name to class URI.

    >>> m = get_class_name_to_class_uri_map()
    >>> m['Study']
    'nmdc:Study'
    """

    class_name_to_class_uri_map = dict()

    sv = nmdc_schema_view()
    class_names = sv.all_classes()
    class_defs_by_name = {name: sv.get_class(name) for name in class_names}
    for class_name, class_def in class_defs_by_name.items():
        class_uri = sv.get_uri(class_name)
        class_name_to_class_uri_map[class_name] = class_uri

    return class_name_to_class_uri_map


@lru_cache
def get_class_uri_to_class_name_map() -> dict[str, str]:
    """
    Returns a mapping from class URI to schema class name.

    >>> m = get_class_uri_to_class_name_map()
    >>> m['nmdc:Study']
    'Study'
    """

    class_uri_to_class_name_map = dict()

    sv = nmdc_schema_view()
    class_names = sv.all_classes()
    class_defs_by_name = {name: sv.get_class(name) for name in class_names}
    for class_name, class_def in class_defs_by_name.items():
        class_uri = sv.get_uri(class_name)
        class_uri_to_class_name_map[class_uri] = class_name

    return class_uri_to_class_name_map



@lru_cache
def does_class_uri_belong_to_concrete_subclass_of_workflow_execution(
    class_uri: str,
) -> bool:
    """
    Returns `True` if the specified `class_uri` belongs to a concrete subclass of the `WorkflowExecution` class,
    and `False` otherwise.

    # Valid class ancestry and not abstract.
    >>> does_class_uri_belong_to_concrete_subclass_of_workflow_execution("nmdc:NomAnalysis")
    True

    # Invalid class ancestry and is abstract.
    >>> does_class_uri_belong_to_concrete_subclass_of_workflow_execution("nmdc:WorkflowExecution")
    False

    # Invalid class ancestry.
    >>> does_class_uri_belong_to_concrete_subclass_of_workflow_execution("nmdc:Study")
    False

    References:
    - https://linkml.io/linkml/developers/schemaview.html#linkml_runtime.utils.schemaview.SchemaView.get_uri
    """

    sv = nmdc_schema_view()
    class_names = sv.class_descendants("WorkflowExecution", reflexive=False)  # excludes `WorkflowExecution`
    class_defs_by_name = {name: sv.get_class(name) for name in class_names}

    valid_class_uris = set()
    for class_name, class_def in class_defs_by_name.items():
        if not class_def.abstract:
            class_uri_ = sv.get_uri(class_name)
            valid_class_uris.add(class_uri_)

    return class_uri in valid_class_uris


@lru_cache
def get_typecodes_compatible_with_schema_class(class_name: str) -> list[str]:
    """
    Returns a list of all typecodes the schema says are compatible with the specified schema class.

    >>> get_typecodes_compatible_with_schema_class('NonExistentClass')
    []
    >>> get_typecodes_compatible_with_schema_class('Study')
    ['sty']
    >>> get_typecodes_compatible_with_schema_class('NucleotideSequencing')
    ['dgns', 'omprc']
    """

    sv = nmdc_schema_view()

    # If the specified class doesn't exist, return an empty list.
    if not sv.get_class(class_name):
        return []

    # Accumulate a list of all the compatible typecodes.
    compatible_typecodes = []
    for slot_def in sv.class_induced_slots(class_name):
        if slot_def.name == "id":
            compatible_typecodes.extend(get_compatible_typecodes(slot_def.pattern))

    return compatible_typecodes


@lru_cache
def get_class_name_from_class_uri(class_uri: str) -> str | None:
    """
    Returns the name of the schema class, if any, having the specified class URI.

    >>> get_class_name_from_class_uri("nmdc:NonExistentClassName") is None
    True
    >>> get_class_name_from_class_uri("nmdc:Study")
    'Study'
    """

    class_uri_to_class_name_map = get_class_uri_to_class_name_map()
    class_name = class_uri_to_class_name_map.get(class_uri, None)
    return class_name
