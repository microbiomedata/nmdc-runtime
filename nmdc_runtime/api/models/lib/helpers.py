from nmdc_runtime.api.models.query import (
    DeleteCommand,
    DeleteSpecs,
    UpdateCommand,
    UpdateSpecs,
)


def derive_delete_specs(delete_command: DeleteCommand) -> DeleteSpecs:
    r"""
    Derives a list of delete specifications from the given `DeleteCommand`.

    Note: This algorithm was copied from the `_run_mdb_cmd`
          function in `nmdc_runtime/api/endpoints/queries.py`.

    To run doctests: $ python -m doctest nmdc_runtime/api/models/lib/helpers.py

    class DeleteStatement(BaseModel):
        q: Document
        limit: OneOrZero
        hint: Optional[Dict[str, OneOrMinusOne]] = None

    >>> delete_command = DeleteCommand(**{
    ...     "delete": "collection_name",
    ...     "deletes": [
    ...         {
    ...             "q": {"color": "blue"},
    ...             "limit": 0,
    ...             "hint": {"potato": 1}
    ...         },
    ...         {
    ...             "q": {"color": "green"},
    ...             "limit": 1,
    ...         }
    ...     ],
    ... })
    >>> delete_specs = derive_delete_specs(delete_command)
    >>> delete_specs[0]
    {'filter': {'color': 'blue'}, 'limit': 0}
    >>> delete_specs[1]
    {'filter': {'color': 'green'}, 'limit': 1}
    """

    return [
        {"filter": delete_statement.q, "limit": delete_statement.limit}
        for delete_statement in delete_command.deletes
    ]


def derive_update_specs(update_command: UpdateCommand) -> UpdateSpecs:
    r"""
    Derives a list of update specifications from the given `UpdateCommand`.

    Note: This algorithm was copied from the `_run_mdb_cmd`
          function in `nmdc_runtime/api/endpoints/queries.py`.

    >>> update_command = UpdateCommand(**{
    ...     "update": "collection_name",
    ...     "updates": [
    ...         {
    ...             "q": {"color": "blue"},
    ...             "u": {"$set": {"color": "red"}},
    ...             "upsert": False,
    ...             "multi": True,
    ...             "hint": {"potato": 1}
    ...         },
    ...         {
    ...             "q": {"color": "green"},
    ...             "u": {"$set": {"color": "yellow"}},
    ...         }
    ...     ],
    ... })
    >>> update_specs = derive_update_specs(update_command)
    >>> update_specs[0]
    {'filter': {'color': 'blue'}, 'limit': 0}
    >>> update_specs[1]
    {'filter': {'color': 'green'}, 'limit': 1}
    """

    return [
        {"filter": update_statement.q, "limit": 0 if update_statement.multi else 1}
        for update_statement in update_command.updates
    ]
