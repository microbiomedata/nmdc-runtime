"""This module defines a utility to parse a path segment with semicolon-delimited parameters.

# Example: Single segment - no lambda needed when FastAPI-path-parameter name matches `parse_path_segment`'s.

@app.get("/path/{path_segment}")
async def handle_segment(parsed: ParsedPathSegment = Depends(parse_path_segment)):
    return {
        "segment_name": parsed.segment_name,
        "parameters": parsed.segment_parameters
    }

# Example: Multiple segments with different names - lambda required.

@app.get("/items/{item_segment}/actions/{action_segment}")
async def handle_two_segments(
    item_parsed: ParsedPathSegment = Depends(lambda item_segment: parse_path_segment(item_segment)),
    action_parsed: ParsedPathSegment = Depends(lambda action_segment: parse_path_segment(action_segment))
):
    return {
        "item": {
            "name": item_parsed.segment_name,
            "params": item_parsed.segment_parameters
        },
        "action": {
            "name": action_parsed.segment_name,
            "params": action_parsed.segment_parameters
        }
    }

# Example URL: /items/book;category=fiction;new/actions/edit;quick;mode=advanced
# Results in:
# item_parsed.segment_name = "book"
# item_parsed.segment_parameters = {"category": "fiction", "new": None}
# action_parsed.segment_name = "edit"
# action_parsed.segment_parameters = {"quick": None, "mode": "advanced"}

# Implementation note

Ordering of segment parameters MUST be preserved so that they may be used e.g. to specify a transformation pipeline.
A hypothetical example:
GET `/some/image;crop=200,100,1200,900;scale=640,480` might
1. GET `/some/image`,
2. POST the response to `/transform/crop?x=200&y=100&width=1200&height=900`,
3. POST the response to `/transform/scale?width=640&height=480`, and finally
4. yield the last response to the client.

"""

from typing import Dict, List, Union, Annotated
from urllib.parse import unquote

from fastapi import HTTPException, Path


class ParsedPathSegment:
    """Container for parsed path segment data."""

    def __init__(
        self,
        segment_name: str,
        segment_parameters: Dict[str, Union[str, List[str], None]],
    ):
        self.segment_name = segment_name
        self.segment_parameters = segment_parameters

    def __repr__(self):
        return f"ParsedPathSegment(name='{self.segment_name}', params={self.segment_parameters})"


def parse_path_segment(
    path_segment: Annotated[str, Path(description="Foo")],
) -> ParsedPathSegment:
    """
    FastAPI dependency to parse a path segment with semicolon-delimited parameters.

    See [the last paragraph of RFC3986 Section 3.3](https://datatracker.ietf.org/doc/html/rfc3986#section-3.3) for
    insight into the below parsing rules.

    Parsing rules:
    - Semicolon (`;`) delimits parameters from segment name and from each other
    - Equals sign (`=`) separates parameter names from values
    - Comma (`,`) separates multiple values for a single parameter
    - Other RFC3986 sub-delimiters should be percent-encoded

    Args:
        path_segment: The raw path segment string from FastAPI

    Returns:
        ParsedPathSegment containing the segment name and parsed parameters

    Raises:
        HTTPException: 400 Bad Request if segment name is empty

    Examples:
        >>> result = parse_path_segment("name")
        >>> result.segment_name
        'name'
        >>> result.segment_parameters
        {}

        >>> result = parse_path_segment("name;param1;param2")
        >>> result.segment_name
        'name'
        >>> result.segment_parameters
        {'param1': None, 'param2': None}

        >>> result = parse_path_segment("name;param=")
        >>> result.segment_name
        'name'
        >>> result.segment_parameters
        {'param': ''}

        >>> result = parse_path_segment("name;param=value")
        >>> result.segment_name
        'name'
        >>> result.segment_parameters
        {'param': 'value'}

        >>> result = parse_path_segment("name;param=val1,val2,val3")
        >>> result.segment_name
        'name'
        >>> result.segment_parameters
        {'param': ['val1', 'val2', 'val3']}

        >>> result = parse_path_segment("name;p1=v1;p2=v2,v3;p3")
        >>> result.segment_name
        'name'
        >>> result.segment_parameters
        {'p1': 'v1', 'p2': ['v2', 'v3'], 'p3': None}
    """
    # URL decode the entire segment first
    decoded_segment = unquote(path_segment)

    # Split on semicolons - first part is segment name, rest are parameters
    parts = decoded_segment.split(";")
    segment_name = parts[0] if parts else ""

    # Raise HTTP 400 if segment name is empty
    if not segment_name:
        raise HTTPException(status_code=400, detail="Segment name cannot be empty")

    segment_parameters: Dict[str, Union[str, List[str], None]] = {}

    # Process each parameter
    for param_part in parts[1:]:
        if not param_part:  # Skip empty parts
            continue

        # Split on first equals sign to separate name from value
        if "=" in param_part:
            param_name, param_value = param_part.split("=", 1)

            # Split values on commas
            if "," in param_value:
                # Multiple values - return as list
                values = [v.strip() for v in param_value.split(",")]
                segment_parameters |= {param_name: values}
            else:
                # Single value - return as string
                segment_parameters |= {param_name: param_value}
        else:
            # Parameter without value (flag-style parameter)
            segment_parameters |= {param_part: None}

    return ParsedPathSegment(segment_name, segment_parameters)
