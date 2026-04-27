from copy import deepcopy
from typing import Any

from pymongo.database import Database


class RoleDefinition:
    """
    Definition of a MongoDB role's direct privileges and inherited roles.

    The following doctest demonstrates that the "getter" methods return _copies_ of the attributes,
    not references to the real attributes. That behavior prevents callers from manipulating the
    attributes, which could complicate the process of restoring users' original access.

    >>> role_definition = RoleDefinition(
    ...     privileges=[{"resource": {"db": "nmdc", "collection": "jobs"}}],
    ...     roles=[{"db": "nmdc", "role": "read"}],
    ... )
    >>> role_definition.privileges[0]["resource"]["collection"] = "mutated"
    >>> role_definition.roles[0]["role"] = "mutated"
    >>> role_definition.privileges
    [{'resource': {'db': 'nmdc', 'collection': 'jobs'}}]
    >>> role_definition.roles
    [{'db': 'nmdc', 'role': 'read'}]
    """

    def __init__(
        self,
        privileges: list[dict[str, Any]] | None = None,
        roles: list[dict[str, str]] | None = None,
    ) -> None:
        self._privileges = deepcopy(privileges) if privileges is not None else []
        self._roles = deepcopy(roles) if roles is not None else []

    @property
    def privileges(self) -> list[dict[str, Any]]:
        """Return a deep copy of the MongoDB role's direct privileges."""

        return deepcopy(self._privileges)

    @property
    def roles(self) -> list[dict[str, str]]:
        """Return a deep copy of the MongoDB role's inherited roles."""

        return deepcopy(self._roles)


# These are the definitions of the standard NMDC MongoDB roles (excluding the "nmdc_migrator" role).
#
# Note: The authoritative role creation script is at:
#       https://github.com/microbiomedata/infra-admin/blob/main/mongodb/roles/createRoles.mongo.js
#
STANDARD_ROLE_DEFINITIONS: dict[str, RoleDefinition] = {
    "nmdc_runtime": RoleDefinition(
        privileges=[],
        roles=[
            {"db": "admin", "role": "readWriteAnyDatabase"},
            {"db": "admin", "role": "dbAdminAnyDatabase"},
        ],
    ),
    "nmdc_scheduler": RoleDefinition(
        privileges=[
            {
                "resource": {"db": "nmdc", "collection": "jobs"},
                "actions": ["find", "insert", "update", "remove"],
            },
        ],
        roles=[
            {"db": "nmdc", "role": "read"},
        ],
    ),
    "nmdc_aggregator": RoleDefinition(
        privileges=[
            {
                "resource": {
                    "db": "nmdc",
                    "collection": "functional_annotation_agg",
                },
                "actions": ["find", "insert", "update", "remove"],
            },
        ],
        roles=[
            {"db": "nmdc", "role": "read"},
        ],
    ),
    "nmdc_reader": RoleDefinition(
        privileges=[
            {
                "resource": {"db": "", "collection": ""},
                "actions": ["changeOwnPassword"],
            },
        ],
        roles=[
            {"db": "nmdc", "role": "read"},
            {"db": "nmdc_updated", "role": "read"},
            {"db": "nmdc_deleted", "role": "read"},
            {"db": "nmdc_changesheet_submission_results", "role": "read"},
        ],
    ),
    "nmdc_editor": RoleDefinition(
        privileges=[
            {
                "resource": {"db": "", "collection": ""},
                "actions": ["changeOwnPassword"],
            },
        ],
        roles=[
            {"db": "nmdc", "role": "readWrite"},
            {"db": "nmdc_updated", "role": "readWrite"},
            {"db": "nmdc_deleted", "role": "readWrite"},
            {"db": "nmdc_changesheet_submission_results", "role": "readWrite"},
        ],
    ),
    "all_dumper": RoleDefinition(
        privileges=[
            {
                "resource": {"db": "config", "collection": "system.preimages"},
                "actions": ["find"],
            },
        ],
        roles=[
            {"db": "admin", "role": "backup"},
        ],
    ),
}

STANDARD_ROLE_NAMES = list(STANDARD_ROLE_DEFINITIONS.keys())


def has_role(admin_database: Database, role_name: str) -> bool:
    """
    Return whether the specified role already exists in the admin database.

    Reference: https://www.mongodb.com/docs/manual/reference/command/rolesInfo/

    Note: The `db.getRole` mongosh method does not seem to be available via pymongo,
          so, we use the `rolesInfo` command here.

    >>> from unittest.mock import MagicMock
    >>> admin_database = MagicMock()
    >>> admin_database.command.return_value = {"roles": [{"role": "some_role", "db": "admin"}]}
    >>> has_role(admin_database, "some_role")
    True
    >>> admin_database.command.return_value = {"roles": []}
    >>> has_role(admin_database, "some_role")
    False
    """

    output = admin_database.command({"rolesInfo": role_name})
    role_exists = len(output["roles"]) > 0
    return role_exists


def define_mongo_roles(
    admin_database: Database,
    role_definitions: dict[str, RoleDefinition],
) -> dict[str, dict[str, bool]]:
    """
    Ensures the specified database contains the specified roles, even when that involves creating
    new roles or updating existing roles. This can be used to temporarily redefine roles in order
    to effectively revoke user access during sensitive database administrative operations.
    """

    results: dict[str, dict[str, bool]] = {}
    for role_name, role_definition in role_definitions.items():
        if has_role(admin_database=admin_database, role_name=role_name):
            admin_database.command(
                "updateRole",
                role_name,
                privileges=role_definition.privileges,
                roles=role_definition.roles,
            )
            results[role_name] = {"created": False, "updated": True}
        else:
            admin_database.command(
                "createRole",
                role_name,
                privileges=role_definition.privileges,
                roles=role_definition.roles,
            )
            results[role_name] = {"created": True, "updated": False}
    return results


def revoke_standard_role_privileges(
    admin_database: Database,
) -> dict[str, dict[str, bool]]:
    """Revoke all privileges and role inheritance from the standard NMDC roles (except the "nmdc_migrator" role)."""

    # Revoked roles.
    revoked_role_definitions: dict[str, RoleDefinition] = {
        role_name: RoleDefinition(privileges=[], roles=[])  # empty lists
        for role_name in STANDARD_ROLE_NAMES
    }

    return define_mongo_roles(
        admin_database=admin_database,
        role_definitions=revoked_role_definitions,
    )


def restore_standard_role_privileges(
    admin_database: Database,
) -> dict[str, dict[str, bool]]:
    """Restore the standard definitions of the standard NMDC roles (except the "nmdc_migrator" role)."""

    return define_mongo_roles(
        admin_database=admin_database,
        role_definitions=STANDARD_ROLE_DEFINITIONS,
    )
