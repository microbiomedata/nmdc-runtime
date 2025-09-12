def flatten(value: dict | list | str | int | float | bool | None, result: dict, base_json_path: str = "$") -> dict:
    r"""Translates the specified value into a Python dictionary, which can be written to a "single-row" CSV string.

    Args:
        value: The value you want to translate, originally read from a valid JSON string.
        result: The Python dictionary to which you want to add the translated value.
        base_json_path: The JSONPath expression that indicates where the value was read from.

    Returns:
        A Python dictionary that contains the translated value, ready to be written to a CSV string via
        an instance of `csv.DictWriter` (see: https://docs.python.org/3/library/csv.html#csv.DictWriter).

    Note: This function invokes itself recursively.
    
    In the resulting dictionary:
    - Each key is a JSONPath expression indicating where the primitive value originated within the JSON value.
    - Each value is the primitive value at that location within the JSON value.

    For example:
    - The JSONPath expression, `$.has_input[1].name`, refers to the `name` property of the object
      that is the second element (0-indexed) of the array in the `has_input` property of the root value.
    
    # Primitive values:
    >>> flatten(1, {})  # int
    {'$': 1}
    >>> flatten(0.1, {})  # float
    {'$': 0.1}
    >>> flatten("potato", {})  # str
    {'$': 'potato'}
    >>> flatten(True, {})  # bool
    {'$': True}
    >>> flatten(None, {})  # None
    {'$': None}
    
    # Invalid value:
    >>> flatten(lambda: 123, {})  # function
    Traceback (most recent call last):
    ...
    ValueError: The data type of the value is invalid.

    # Empty dict and list values:
    >>> flatten({}, {})  # empty dict
    {}
    >>> flatten([], {})  # empty list
    {}

    # Non-empty dict and list values:
    >>> flatten({"a": 1}, {})  # dict having 1 key-value pair
    {'$.a': 1}
    >>> flatten([1], {})  # list having 1 element
    {'$[0]': 1}

    # Compound value:
    >>> flatten([{"a": 1}, {"a": 2, "b": 3}], {})  # list of dicts
    {'$[0].a': 1, '$[1].a': 2, '$[1].b': 3}
    >>> flatten({"a": [1], "b": [2, 3]}, {})  # dict of lists
    {'$.a[0]': 1, '$.b[0]': 2, '$.b[1]': 3}

    # (Mostly) real-world API response value:
    #
    # Note: In this doctest, we `pprint` the resulting dictionary so it spans multiple lines.
    #       We find that easier to read compared to all key-value pairs being on a single line.
    #
    >>> from pprint import pprint
    >>> api_response_body = {
    ...     "resources": [
    ...         {
    ...         "id": "nmdc:wfmag-11-00jn7876.2",
    ...         "name": "My workflow execution",
    ...         "started_at_time": "2024-03-24T16:04:04.936972+00:00",
    ...         "ended_at_time": "2024-03-24T17:49:34.756540+00:00",
    ...         "was_informed_by": [
    ...             "nmdc:omprc-11-7yj0jg57"
    ...         ],
    ...         "execution_resource": "NERSC-Perlmutter",
    ...         "git_url": "https://github.com/microbiomedata/metaMAGs",
    ...         "has_input": [
    ...             "nmdc:dobj-11-yjp1xw52",
    ...             "nmdc:dobj-11-3av14y79",
    ...             "nmdc:dobj-11-wa5pnq42"
    ...         ],
    ...         "type": "nmdc:MagsAnalysis",
    ...         "has_output": [],
    ...         "version": "v1.1.0",
    ...         "processing_institution": "NMDC"
    ...         }
    ...     ],
    ...     "next_page_token": "nmdc:sys0xvg3j376"
    ... }
    >>> result = flatten(api_response_body, {})
    >>> pprint(result)
    {'$.next_page_token': 'nmdc:sys0xvg3j376',
     '$.resources[0].ended_at_time': '2024-03-24T17:49:34.756540+00:00',
     '$.resources[0].execution_resource': 'NERSC-Perlmutter',
     '$.resources[0].git_url': 'https://github.com/microbiomedata/metaMAGs',
     '$.resources[0].has_input[0]': 'nmdc:dobj-11-yjp1xw52',
     '$.resources[0].has_input[1]': 'nmdc:dobj-11-3av14y79',
     '$.resources[0].has_input[2]': 'nmdc:dobj-11-wa5pnq42',
     '$.resources[0].id': 'nmdc:wfmag-11-00jn7876.2',
     '$.resources[0].name': 'My workflow execution',
     '$.resources[0].processing_institution': 'NMDC',
     '$.resources[0].started_at_time': '2024-03-24T16:04:04.936972+00:00',
     '$.resources[0].type': 'nmdc:MagsAnalysis',
     '$.resources[0].version': 'v1.1.0',
     '$.resources[0].was_informed_by[0]': 'nmdc:omprc-11-7yj0jg57'}
    """

    # If the value is a primitive, store it as is.
    if isinstance(value, (str, int, float, bool)) or value is None:
        result.update({base_json_path: value})
    # Else, if the value is a dictionary, process each of its key-value pairs.
    elif isinstance(value, dict):
        for key, val in value.items():
            result.update(flatten(val, result, f"{base_json_path}.{key}"))
    # Else, if the value is a list, process each of its elements.
    elif isinstance(value, list):
        for index, val in enumerate(value):
            result.update(flatten(val, result, f"{base_json_path}[{index}]"))
    # Else, raise an exception indicating the value is invalid.
    else:
        raise ValueError("The data type of the value is invalid.")

    return result


def flatten_each(dicts: list[dict]) -> dict:
    r"""Translates a list of dictionaries into a single flattened dictionary, which can be written to a "multi-row" CSV string.

    Args:
        dicts: The list of dictionaries you want to translate, each originally read from a valid JSON string.

    Returns:
        A Python list that contains the flattened dictionaries, ready to be written to a CSV string via
        an instance of `csv.DictWriter` (see: https://docs.python.org/3/library/csv.html#csv.DictWriter).

    >>> result = flatten_each([
    ...     {"a": 1,                                               },
    ...     {"a": 2, "b": 3,                                       },
    ...     {                "c": {"foo": "bar"},                  },
    ...     {                                     "d": [4, None, 5]},
    ... ])
    >>> result == {
    ...     '$.a':     [1,    2,    None,  None],
    ...     '$.b':     [None, 3,    None,  None],
    ...     '$.c.foo': [None, None, "bar", None],
    ...     '$.d[0]':  [None, None, None,  4   ],
    ...     '$.d[1]':  [None, None, None,  None],
    ...     '$.d[2]':  [None, None, None,  5   ],
    ... }
    True
    """

    # For each dictionary in the list, flatten it.
    flat_dicts = [flatten(d, {}) for d in dicts]

    # Make a list of all the distinct keys among all the flat dictionaries.
    distinct_keys: set = {key for flat_dict in flat_dicts for key in flat_dict.keys()}

    # Create a new dictionary that has one key-value pair for each distinct key,
    # and initialize each value to an empty list.
    result: dict = {distinct_key: [] for distinct_key in distinct_keys}

    # For each flat dictionary, append its values to the corresponding lists,
    # appending `None` whenever the flat dictionary lacks a given key.
    for flat_dict in flat_dicts:
        for distinct_key in distinct_keys:
            value_from_flat_dict = flat_dict.get(distinct_key, None)
            result[distinct_key].append(value_from_flat_dict)

    return result
