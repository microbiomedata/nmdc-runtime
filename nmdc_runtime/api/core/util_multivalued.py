"""
Utility functions for handling fields that can be single-valued or multivalued.

This module provides functions to handle the transition of schema fields from
single-valued to multivalued, ensuring backward compatibility.
"""

from typing import Union, List, Any, Iterable


def normalize_to_list(value: Union[str, List[str], None]) -> List[str]:
    """
    Normalize a single value or list of values to a list.

    Args:
        value: A single string, list of strings, or None

    Returns:
        A list of strings. Empty list if value is None.

    Examples:
        >>> normalize_to_list("single_value")
        ['single_value']
        >>> normalize_to_list(["value1", "value2"])
        ['value1', 'value2']
        >>> normalize_to_list(None)
        []
    """
    if value is None:
        return []
    elif isinstance(value, str):
        return [value]
    elif isinstance(value, list):
        return value
    else:
        # Handle other iterable types by converting to list
        try:
            return list(value)
        except TypeError:
            # If it's not iterable, treat as single value
            return [str(value)]


def get_was_informed_by_values(doc: dict) -> List[str]:
    """
    Extract was_informed_by values from a document, handling both single and multivalued cases.

    Args:
        doc: Document that may contain was_informed_by field

    Returns:
        List of was_informed_by values, empty if field is not present
    """
    return normalize_to_list(doc.get("was_informed_by"))


def create_was_informed_by_query(values: Union[str, List[str], None]) -> dict:
    """
    Create a MongoDB query for was_informed_by field that works with both single and multivalued data.

    Args:
        values: Single value, list of values, or None to query for

    Returns:
        MongoDB query dict

    Examples:
        >>> create_was_informed_by_query("single_value")
        {'was_informed_by': 'single_value'}
        >>> create_was_informed_by_query(["value1", "value2"])
        {'was_informed_by': {'$in': ['value1', 'value2']}}
    """
    normalized_values = normalize_to_list(values)

    if not normalized_values:
        return {}
    elif len(normalized_values) == 1:
        # For single value, use direct equality - works for both single-valued and multivalued fields
        return {"was_informed_by": normalized_values[0]}
    else:
        # For multiple values, use $in operator
        return {"was_informed_by": {"$in": normalized_values}}


def create_was_informed_by_reverse_query(target_id: str) -> dict:
    """
    Create a MongoDB query to find documents where was_informed_by contains the target_id.
    This works for both single-valued and multivalued was_informed_by fields.

    Args:
        target_id: The ID to search for in was_informed_by fields

    Returns:
        MongoDB query dict that works for both single and multivalued fields
    """
    return {"was_informed_by": target_id}
