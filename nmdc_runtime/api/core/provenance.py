from datetime import datetime, timezone
from typing import Any, Dict, Optional, Set


# Names of collections that can contain documents having the `provenance_metadata` field.
NAMES_OF_COLLECTIONS_ALLOWING_DOCUMENTS_HAVING_PROVENANCE_METADATA_FIELD: Set[str] = {
    "biosample_set",
    "data_generation_set",
    "study_set",
}

# Types of NMDC schema classes that have the `provenance_metadata` slot.
TYPES_OF_CLASSES_HAVING_PROVENANCE_METADATA_SLOT: Set[str] = {
    "nmdc:Biosample",
    "nmdc:DataGeneration",
    "nmdc:MassSpectrometry",
    "nmdc:NucleotideSequencing",
    "nmdc:Study",
}

PROVENANCE_METADATA_TYPE = "nmdc:ProvenanceMetadata"


def make_timestamp(dt: Optional[datetime] = None) -> str:
    """
    Return an RFC 3339 timestamp string in UTC with second precision.

    Reference: https://datatracker.ietf.org/doc/html/rfc3339

    >>> make_timestamp(datetime(1970, 1, 1, 0, 0, 0, tzinfo=timezone.utc))
    '1970-01-01T00:00:00Z'
    >>> make_timestamp(datetime(2025, 12, 31, 23, 30, 59, tzinfo=timezone.utc))
    '2025-12-31T23:30:59Z'
    """

    dt_utc = (dt or datetime.now(timezone.utc)).astimezone(timezone.utc)
    return dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ")


def set_provenance_metadata_field(
    document: Dict[str, Any],
    field_name: str,
    value: Any,
) -> Dict[str, Any]:
    """
    Set the specified field of the `nmdc:ProvenanceMetadata` instance nested in the specified
    document's `provenance_metadata` field, to the specified value; creating the nested
    `nmdc:ProvenanceMetadata` instance if the document has no `provenance_metadata` field.

    >>> set_provenance_metadata_field(
    ...     {
    ...         "id": "nmdc:sty-00-000001"
    ...     },
    ...     "add_date",
    ...     "1999-12-25T12:45:59Z"
    ... )
    {'id': 'nmdc:sty-00-000001', 'provenance_metadata': {'type': 'nmdc:ProvenanceMetadata', 'add_date': '1999-12-25T12:45:59Z'}}
    >>> set_provenance_metadata_field(
    ...     {
    ...         "id": "nmdc:sty-00-000001",
    ...         "provenance_metadata": {"type": "nmdc:ProvenanceMetadata", "add_date": "2020-01-01T00:00:00Z"}
    ...     },
    ...     "add_date",
    ...     "1999-12-25T12:45:59Z"
    ... )
    {'id': 'nmdc:sty-00-000001', 'provenance_metadata': {'type': 'nmdc:ProvenanceMetadata', 'add_date': '1999-12-25T12:45:59Z'}}
    >>> set_provenance_metadata_field(
    ...     {
    ...         "id": "nmdc:sty-00-000001",
    ...         "provenance_metadata": {"type": "nmdc:ProvenanceMetadata", "mod_date": "2020-01-01T00:00:00Z"}
    ...     },
    ...     "add_date",
    ...     "1999-12-25T12:45:59Z"
    ... )
    {'id': 'nmdc:sty-00-000001', 'provenance_metadata': {'type': 'nmdc:ProvenanceMetadata', 'mod_date': '2020-01-01T00:00:00Z', 'add_date': '1999-12-25T12:45:59Z'}}
    >>> set_provenance_metadata_field(
    ...     {
    ...         "id": "nmdc:sty-00-000001",
    ...         "provenance_metadata": {"type": "nmdc:ProvenanceMetadata", "add_date": "2021-01-01T00:00:00Z", "mod_date": "2020-01-01T00:00:00Z"}
    ...     },
    ...     "add_date",
    ...     "1999-12-25T12:45:59Z"
    ... )
    {'id': 'nmdc:sty-00-000001', 'provenance_metadata': {'type': 'nmdc:ProvenanceMetadata', 'add_date': '1999-12-25T12:45:59Z', 'mod_date': '2020-01-01T00:00:00Z'}}
    """

    # If the specified document doesn't already contain a `provenance_metadata` field,
    # introduce that field having a minimal `nmdc:ProvenanceMetadata` instance.
    if "provenance_metadata" not in document:
        document["provenance_metadata"] = {"type": PROVENANCE_METADATA_TYPE}

    # Set the specified field of the nested `nmdc:ProvenanceMetadata` instance to the specified
    # value.
    document["provenance_metadata"][field_name] = value
    return document


def set_provenance_metadata_add_date(
    document: Dict[str, Any],
    add_date: str = make_timestamp(),
) -> Dict[str, Any]:
    """
    Set the `add_date` field of the `nmdc:ProvenanceMetadata` instance nested in the specified
    document's `provenance_metadata` field, to the specified value; creating the nested
    `nmdc:ProvenanceMetadata` instance if the document has no `provenance_metadata` field.

    >>> set_provenance_metadata_add_date(
    ...     {
    ...         "id": "nmdc:sty-00-000001"
    ...     },
    ...     "1999-12-25T12:45:59Z"
    ... )
    {'id': 'nmdc:sty-00-000001', 'provenance_metadata': {'type': 'nmdc:ProvenanceMetadata', 'add_date': '1999-12-25T12:45:59Z'}}
    >>> set_provenance_metadata_add_date(
    ...     {
    ...         "id": "nmdc:sty-00-000001",
    ...         "provenance_metadata": {"type": "nmdc:ProvenanceMetadata", "add_date": "2020-01-01T00:00:00Z"}
    ...     },
    ...     "1999-12-25T12:45:59Z"
    ... )
    {'id': 'nmdc:sty-00-000001', 'provenance_metadata': {'type': 'nmdc:ProvenanceMetadata', 'add_date': '1999-12-25T12:45:59Z'}}
    >>> set_provenance_metadata_add_date(
    ...     {
    ...         "id": "nmdc:sty-00-000001",
    ...         "provenance_metadata": {"type": "nmdc:ProvenanceMetadata", "mod_date": "2020-01-01T00:00:00Z"}
    ...     },
    ...     "1999-12-25T12:45:59Z"
    ... )
    {'id': 'nmdc:sty-00-000001', 'provenance_metadata': {'type': 'nmdc:ProvenanceMetadata', 'mod_date': '2020-01-01T00:00:00Z', 'add_date': '1999-12-25T12:45:59Z'}}
    >>> set_provenance_metadata_add_date(
    ...     {
    ...         "id": "nmdc:sty-00-000001",
    ...         "provenance_metadata": {"type": "nmdc:ProvenanceMetadata", "add_date": "2021-01-01T00:00:00Z", "mod_date": "2020-01-01T00:00:00Z"}
    ...     },
    ...     "1999-12-25T12:45:59Z"
    ... )
    {'id': 'nmdc:sty-00-000001', 'provenance_metadata': {'type': 'nmdc:ProvenanceMetadata', 'add_date': '1999-12-25T12:45:59Z', 'mod_date': '2020-01-01T00:00:00Z'}}
    """

    return set_provenance_metadata_field(document, "add_date", add_date)


def set_provenance_metadata_mod_date(
    document: Dict[str, Any],
    mod_date: str = make_timestamp(),
) -> Dict[str, Any]:
    """
    Set the `mod_date` field of the `nmdc:ProvenanceMetadata` instance nested in the specified
    document's `provenance_metadata` field, to the specified value; creating the nested
    `nmdc:ProvenanceMetadata` instance if the document has no `provenance_metadata` field.

    >>> set_provenance_metadata_mod_date(
    ...     {
    ...         "id": "nmdc:sty-00-000001"
    ...     },
    ...     "1999-12-25T12:45:59Z"
    ... )
    {'id': 'nmdc:sty-00-000001', 'provenance_metadata': {'type': 'nmdc:ProvenanceMetadata', 'mod_date': '1999-12-25T12:45:59Z'}}
    >>> set_provenance_metadata_mod_date(
    ...     {
    ...         "id": "nmdc:sty-00-000001",
    ...         "provenance_metadata": {"type": "nmdc:ProvenanceMetadata", "mod_date": "2020-01-01T00:00:00Z"}
    ...     },
    ...     "1999-12-25T12:45:59Z"
    ... )
    {'id': 'nmdc:sty-00-000001', 'provenance_metadata': {'type': 'nmdc:ProvenanceMetadata', 'mod_date': '1999-12-25T12:45:59Z'}}
    >>> set_provenance_metadata_mod_date(
    ...     {
    ...         "id": "nmdc:sty-00-000001",
    ...         "provenance_metadata": {"type": "nmdc:ProvenanceMetadata", "mod_date": "2020-01-01T00:00:00Z"}
    ...     },
    ...     "1999-12-25T12:45:59Z"
    ... )
    {'id': 'nmdc:sty-00-000001', 'provenance_metadata': {'type': 'nmdc:ProvenanceMetadata', 'mod_date': '1999-12-25T12:45:59Z'}}
    >>> set_provenance_metadata_mod_date(
    ...     {
    ...         "id": "nmdc:sty-00-000001",
    ...         "provenance_metadata": {"type": "nmdc:ProvenanceMetadata", "add_date": "2020-01-01T00:00:00Z", "mod_date": "2020-01-01T00:00:00Z"}
    ...     },
    ...     "1999-12-25T12:45:59Z"
    ... )
    {'id': 'nmdc:sty-00-000001', 'provenance_metadata': {'type': 'nmdc:ProvenanceMetadata', 'add_date': '2020-01-01T00:00:00Z', 'mod_date': '1999-12-25T12:45:59Z'}}
    """

    return set_provenance_metadata_field(document, "mod_date", mod_date)


def set_provenance_metadata_timestamps(
    document: Dict[str, Any],
    add_date: str = make_timestamp(),
    mod_date: str = make_timestamp(),
) -> Dict[str, Any]:
    """
    Set the `add_date` and `mod_date` fields of the `nmdc:ProvenanceMetadata` instance nested in the
    specified document's `provenance_metadata` field, to the specified value; creating the nested
    `nmdc:ProvenanceMetadata` instance if the document has no `provenance_metadata` field.

    >>> set_provenance_metadata_timestamps(
    ...     {
    ...         "id": "nmdc:sty-00-000001"
    ...     },
    ...     "1999-12-25T12:45:59Z",
    ...     "2025-10-31T23:30:00Z"
    ... )
    {'id': 'nmdc:sty-00-000001', 'provenance_metadata': {'type': 'nmdc:ProvenanceMetadata', 'add_date': '1999-12-25T12:45:59Z', 'mod_date': '2025-10-31T23:30:00Z'}}
    >>> set_provenance_metadata_timestamps(
    ...     {
    ...         "id": "nmdc:sty-00-000001",
    ...         "provenance_metadata": {"type": "nmdc:ProvenanceMetadata", "add_date": "2020-01-01T00:00:00Z"}
    ...     },
    ...     "1999-12-25T12:45:59Z",
    ...     "2025-10-31T23:30:00Z"
    ... )
    {'id': 'nmdc:sty-00-000001', 'provenance_metadata': {'type': 'nmdc:ProvenanceMetadata', 'add_date': '1999-12-25T12:45:59Z', 'mod_date': '2025-10-31T23:30:00Z'}}
    """

    intermediate_document = set_provenance_metadata_add_date(document, add_date)
    return set_provenance_metadata_mod_date(intermediate_document, mod_date)
