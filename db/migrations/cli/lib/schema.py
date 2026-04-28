from inspect import isclass
from importlib import import_module
from typing import Type

import typer
from linkml.validator import SchemaDefinition, ValidationReport, Validator
from linkml.validator.plugins import JsonschemaValidationPlugin
from linkml_runtime import SchemaView
from rich import print
from refscan.lib.helpers import derive_schema_class_name_from_document


def get_migrator_class(migrator_module_name: str) -> Type:
    """
    Imports the `Migrator` class from the specified module within the `nmdc_schema` package and returns a reference to it.
    """

    migrator_module = import_module(f".{migrator_module_name}", package="nmdc_schema.migrators")
    Migrator = getattr(migrator_module, "Migrator")  # gets the class
    if not isclass(Migrator):
        raise RuntimeError(f"Failed to import 'Migrator' class from module {migrator_module.__name__}")
    return Migrator


def get_mongo_adapter_class() -> Type:
    """
    Imports the `MongoAdapter` class from the `nmdc_schema` package and returns a reference to it.
    """

    mongo_adapter_module = import_module(".mongo_adapter", package="nmdc_schema.migrators.adapters")
    MongoAdapter = getattr(mongo_adapter_module, "MongoAdapter")  # gets the class
    if not isclass(MongoAdapter):
        raise RuntimeError(f"Failed to import 'MongoAdapter' class from module {mongo_adapter_module.__name__}")
    return MongoAdapter


def create_schema_definition() -> SchemaDefinition:
    """
    Returns a `SchemaDefinition` instance reflecting the NMDC schema.
    """

    # Import a function that returns a `SchemaDefinition` reflecting the NMDC schema.
    data_module = import_module(".nmdc_data", package="nmdc_schema")
    get_nmdc_schema_definition = getattr(data_module, "get_nmdc_schema_definition")  # gets the function
    if not callable(get_nmdc_schema_definition):
        raise RuntimeError(f"Failed to import 'get_nmdc_schema_definition' function from module {data_module.__name__}")
    # Get a `SchemaDefinition` from the imported function.
    schema_definition = get_nmdc_schema_definition()
    if not isinstance(schema_definition, SchemaDefinition):
        raise RuntimeError(f"Failed to get NMDC schema definition from module {data_module.__name__}")
    return schema_definition


def create_schema_view() -> SchemaView:
    """
    Returns a LinkML SchemaView instance that can be used to programmatically traverse the NMDC schema.
    """

    schema_definition = create_schema_definition()
    schema_view = SchemaView(schema_definition)
    return schema_view


def create_validator() -> Validator:
    """
    Creates and returns a `Validator` instance that can be used to validate data against the NMDC schema.

    References:
    - https://linkml.io/linkml/code/validator.html#linkml.validator.Validator
    - https://linkml.io/linkml/data/validating-data.html#validation-in-python-code
    """

    # Import the NMDC schema validation plugin, which we'll plug into to the validator.
    validation_plugin_module = import_module(".nmdc_schema_validation_plugin", package="nmdc_schema")
    NmdcSchemaValidationPlugin = getattr(validation_plugin_module, "NmdcSchemaValidationPlugin")  # gets the class
    if not isclass(NmdcSchemaValidationPlugin):
        raise typer.BadParameter(
            f"Failed to import 'NmdcSchemaValidationPlugin' class from module {validation_plugin_module.__name__}"
        )

    # Intantiate the validator.
    schema = create_schema_definition()
    validator = Validator(
        schema=schema,
        validation_plugins=[
            JsonschemaValidationPlugin(closed=True),
            NmdcSchemaValidationPlugin(),
        ],
    )

    print(f"Creating validator for NMDC schema version {schema.version}")

    return validator


def validate_document(document: dict, validator: Validator) -> None:
    """
    Validate a document against the NMDC schema using the provided `Validator` instance.
    Raises a `TypeError` if the document is invalid.
    """

    # Strip the `_id` field from the document, since it's not described by the schema.
    #
    # Note: Dictionaries originating as Mongo documents include a Mongo-generated key named `_id`. However,
    #       the NMDC Schema does not describe that key and, indeed, data validators consider dictionaries
    #       containing that key to be invalid with respect to the NMDC Schema. So, here, we validate a
    #       copy (i.e. a shallow copy) of the document that lacks that specific key.
    #
    document_without_oid = {key: value for key, value in document.items() if key != "_id"}

    # Determine the name of the schema class the document represents an instance of.
    schema_class_name = derive_schema_class_name_from_document(
        schema_view=create_schema_view(), document=document_without_oid
    )
    if schema_class_name is None:
        raise TypeError(f"Failed to determine schema class for document.\n{document_without_oid=}")

    # Validate the document, raising an error if it's invalid.
    validation_report: ValidationReport = validator.validate(document_without_oid, target_class=schema_class_name)
    if len(validation_report.results) > 0:
        result_messages = [result.message for result in validation_report.results]
        raise TypeError(f"Document is invalid.\n{result_messages=}\n{document_without_oid=}")
