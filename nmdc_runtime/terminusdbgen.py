import json
import os
from typing import Union, TextIO

import click
from linkml.utils.generator import Generator, shared_arguments
from linkml_runtime.linkml_model.meta import (
    SchemaDefinition,
    ClassDefinition,
    SlotDefinition,
)
from linkml_runtime.utils.formatutils import camelcase, be, underscore

# http://books.xmlschemata.org/relaxng/relax-CHP-19.html
XSD_Ok = {
    "xsd:anyURI",
    "xsd:base64Binary",
    "xsd:boolean",
    "xsd:byte",
    "xsd:date",
    "xsd:dateTime",
    "xsd:decimal",
    "xsd:double",
    "xsd:duration",
    "xsd:ENTITIES",
    "xsd:ENTITY",
    "xsd:float",
    "xsd:gDay",
    "xsd:gMonth",
    "xsd:gMonthDay",
    "xsd:gYear",
    "xsd:gYearMonth",
    "xsd:hexBinary",
    "xsd:ID",
    "xsd:IDREF",
    "xsd:IDREFS",
    "xsd:int",
    "xsd:integer",
    "xsd:language",
    "xsd:long",
    "xsd:Name",
    "xsd:NCName",
    "xsd:negativeInteger",
    "xsd:NMTOKEN",
    "xsd:NMTOKENS",
    "xsd:nonNegativeInteger",
    "xsd:nonPositiveInteger",
    "xsd:normalizedString",
    "xsd:NOTATION",
    "xsd:positiveInteger",
    "xsd:short",
    "xsd:string",
    "xsd:time",
    "xsd:token",
    "xsd:unsignedByte",
    "xsd:unsignedInt",
    "xsd:unsignedLong",
    "xsd:unsignedShort",
}


class TerminusdbGenerator(Generator):
    """Generates JSON file to pass to WOQLClient.insert_document(..., graph_type="schema")`."""

    generatorname = os.path.basename(__file__)
    generatorversion = "0.1.0"
    valid_formats = ["json"]
    visit_all_class_slots = True

    def __init__(self, schema: Union[str, TextIO, SchemaDefinition], **kwargs) -> None:
        super().__init__(schema, **kwargs)
        self.classes = None
        self.raw_additions = None
        self.cls_json = {}

    def visit_schema(self, inline: bool = False, **kwargs) -> None:
        self.classes = []

    def end_schema(self, **_) -> None:
        print(json.dumps(self.classes, indent=2))

    def visit_class(self, cls: ClassDefinition) -> bool:
        self.cls_json = {
            "@type": "Class",
            "@id": camelcase(cls.name),
            "@documentation": {
                "@comment": be(cls.description),
                "@properties": {},
            },
        }
        if cls.is_a:
            self.cls_json["@inherits"] = camelcase(cls.is_a)
        if cls.abstract:
            self.cls_json["@abstract"] = []
        return True

    def end_class(self, cls: ClassDefinition) -> None:
        self.classes.append(self.cls_json)

    def visit_class_slot(
        self, cls: ClassDefinition, aliased_slot_name: str, slot: SlotDefinition
    ) -> None:
        if slot not in self.own_slots(cls):
            return

        if slot.range in self.schema.classes:
            rng = camelcase(slot.range)
        elif slot.range in self.schema.types:
            # XXX Why does `linkml.utils.metamodelcore.Identifier` subclass `str`??
            rng = str(self.schema.types[slot.range].uri)
        else:
            rng = "xsd:string"

        # name = (
        #     f"{cls.name} {aliased_slot_name}"
        #     if slot.is_usage_slot
        #     else aliased_slot_name
        # )
        name = aliased_slot_name
        # TODO fork nmdc schema and make any slots NOT required in parent class
        #  also NOT required in child classes. Can have opt-in entity validation logic in code.

        # # translate to terminusdb xsd builtins:
        # if rng == "xsd:int":
        #     rng = "xsd:integer"
        # elif rng == "xsd:float":
        #     rng = "xsd:double"
        # elif rng == "xsd:language":
        #     rng = "xsd:string"

        if rng not in XSD_Ok and slot.range not in self.schema.classes:
            raise Exception(
                f"slot range for {name} must be schema class or supported xsd type. "
                f"Range {rng} is of type {type(rng)}."
            )

        self.cls_json[underscore(name)] = rng
        self.cls_json["@documentation"]["@properties"][
            underscore(name)
        ] = slot.description
        if not slot.required:
            self.cls_json[underscore(name)] = {"@type": "Optional", "@class": rng}
        if slot.multivalued:  # XXX what about an required multivalued field?
            self.cls_json[underscore(name)] = {"@type": "Set", "@class": rng}


@shared_arguments(TerminusdbGenerator)
@click.command()
def cli(yamlfile, **args):
    """Generate graphql representation of a biolink model"""
    print(TerminusdbGenerator(yamlfile, **args).serialize(**args))


if __name__ == "__main__":
    cli()
