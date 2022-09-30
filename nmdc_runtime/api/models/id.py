import re
from enum import Enum
from functools import lru_cache
from typing import Union, Any, Optional, Literal

from pydantic import BaseModel, constr, PositiveInt, root_validator

# NO i, l, o or u.
# ref: https://www.crockford.com/base32.html
from toolz import concat, pluck

base32_letters = "abcdefghjkmnpqrstvwxyz"
base32_chars = "0123456789" + base32_letters

# Archival Resource Key (ARK) identifier scheme
# ref: https://www.ietf.org/archive/id/draft-kunze-ark-35.html
#
# NAAN - Name Assigning Authority Number
# ref: https://n2t.net/e/pub/naan_registry.txt
NAAN = {
    "nmdc": "76954",
}

# The base compact name assigned by the NAA consists of
# (a) a "shoulder", and (b) a final string known as the "blade".
# (The shoulder plus blade terminology mirrors locksmith jargon describing
# the information-bearing parts of a key.)
#
# Shoulders may reserved for internal departments or units.
# In the case of one central minting service, there technically need only be one shoulder.
# ref: https://www.ietf.org/archive/id/draft-kunze-ark-35.html#name-optional-shoulders
#
# For NMDC, semantically meaningful typecodes are desired for IDs.
# Solution described at <https://gist.github.com/dwinston/083a1cb508bbff21d055e7613f3ac02f>.
# In essence, bridging is needed between (a) the now-legacy shoulders and identifier structure
# `nmdc:<shoulder><generated_id>`and (b) the desired structure
# `nmdc:<type_code><shoulder><generated_id>`.
# The difference in shoulder structure is that legacy shoulders are of the pattern r"[a-z]+[0-9]",
# whereas current shoulders are to be of the pattern r"[0-9][a-z]*[0-9]" so that, in concert with
# the requirement that typcodes be of the pattern r"[a-z]{1,6}", a processor can identify (optional)
# typecode and subsequent shoulder syntactically.
#
# legacy shoulders
LEGACY_FAKE_SHOULDERS = [f"fk{n}" for n in range(10)]
LEGACY_ALLOCATED_SHOULDERS = ["mga0", "mta0", "mba0", "mpa0", "oma0"]
LEGACY_SHOULDER_VALUES = LEGACY_FAKE_SHOULDERS + LEGACY_ALLOCATED_SHOULDERS

TYPECODES = [
    {
        "codes": ["sa"],
        "type": "nmdc:Biosample",
        "note": "a sample",
    },
    {
        "codes": ["st"],
        "type": "nmdc:Study",
        "note": "a study",
    },
    {
        "codes": ["saw"],
        "type": "prov:NamedThing",
        "note": "the workflow/plan for sampling / sample prep",
    },
    {
        "codes": ["saa"],
        "type": "prov:BiosampleProcessing",
        "note": (
            "sample activity (sa prov:wasGeneratedBy saa, saa prov:used saw) "
            "- the execution of the plan"
        ),
    },
    {
        "codes": ["p"],
        "type": "nmdc:Agent",
        "note": "party (e.g. instrument, person, software agent)",
    },
    {
        "codes": ["do"],
        "type": "nmdc:DataObject",
        "note": "data object",
    },
    {
        "codes": ["opw"],
        "type": "nmdc:NamedThing",
        "note": "omics processing - the workflow/plan for data object (do) production",
    },
    {
        "codes": ["opa"],
        "type": "nmdc:OmicsProcessing",
        "note": (
            "omics processing activity (opa prov:used sample, do prov:wasGeneratedBy opa) "
            "- the execution of the plan"
        ),
    },
    {
        "codes": ["oaw"],
        "type": "nmdc:NamedThing",
        "note": "omics analysis - the (computational) workflow/plan",
    },
    {
        "codes": ["oaa"],
        "type": "nmdc:WorkflowExecutionActivity",
        "note": (
            "omics analysis activity (oaa prov:used do, oaa prov:used oa, "
            "do prov:wasGeneratedBy oaa) - "
            "the execution of the plan"
        ),
    },
    {
        "codes": [
            "sys",
            "sysqy",
            "sysop",
            "syssc",
            "syspt",
            "sysdo",
            "sysjob",
            "sysrun",
        ],
        "type": "nmdc:NamedThing",
        "note": "for internal use by the nmdc-runtime site",
    },
]


@lru_cache
def typecode_type(typecode):
    for t in TYPECODES:
        if typecode in t["codes"]:
            return t["type"], t["note"]
    else:
        raise ValueError(f"typecode '{typecode}' not found")


FAKE_SHOULDERS = [f"{n}fk{n}" for n in range(10)]
ALLOCATED_SHOULDERS = ["11"]  # '11' is allocated to central NMDC ID minter
SHOULDER_VALUES = FAKE_SHOULDERS + ALLOCATED_SHOULDERS

_naa = rf"(?P<naa>({'|'.join(list(NAAN.keys()))}))"
_blade = rf"(?P<blade>[{base32_chars}]{{4,}})"

_legacy_shoulder = rf"(?P<shoulder>({'|'.join(LEGACY_SHOULDER_VALUES)}))"
_legacy_assigned_base_name = rf"{_legacy_shoulder}{_blade}"
_legacy_base_object_name = rf"{_naa}:{_legacy_assigned_base_name}"

_typecode = rf"(?P<typecode>({'|'.join(concat(pluck('codes', TYPECODES)))})"
_shoulder = rf"(?P<shoulder>({'|'.join(SHOULDER_VALUES)}))"
_assigned_base_name = rf"{_typecode}{_shoulder}{_blade}"
_base_object_name = rf"{_naa}:{_assigned_base_name}"

pattern = {
    "naa": re.compile(_naa),
    "blade": re.compile(_blade),
    "legacy": {
        "shoulder": re.compile(_legacy_shoulder),
        "assigned_base_name": re.compile(_legacy_assigned_base_name),
        "base_object_name": re.compile(_legacy_base_object_name),
    },
    "typecode": re.compile(_typecode),
    "shoulder": re.compile(_shoulder),
    "assigned_base_name": re.compile(_assigned_base_name),
    "base_object_name": re.compile(_base_object_name),
}

Naa = Enum("Naa", names=zip(NAAN, NAAN), type=str)
Blade = constr(regex=_blade, min_length=4)

LegacyShoulder = Enum(
    "LegacyShoulder",
    names=zip(LEGACY_SHOULDER_VALUES, LEGACY_SHOULDER_VALUES),
    type=str,
)
LegacyAssignedBaseName = constr(regex=_legacy_assigned_base_name)
LegacyBaseObjectName = constr(regex=_legacy_base_object_name)

Typecode = Enum(
    "Typecode",
    names=zip(concat(pluck("codes", TYPECODES)), concat(pluck("codes", TYPECODES))),
    type=str,
)
# Shoulder = constr(regex=rf"^{_shoulder}$", min_length=2)
Shoulder = Enum("Shoulder", names=zip(SHOULDER_VALUES, SHOULDER_VALUES), type=str)
AssignedBaseName = constr(regex=_assigned_base_name)
BaseObjectName = constr(regex=_base_object_name)

NameAssigningAuthority = Literal[tuple(NAAN.keys())]


class MintRequest(BaseModel):
    populator: str = ""
    naa: NameAssigningAuthority = "nmdc"
    typecode: Typecode = "oaa"
    shoulder: Shoulder = "1fk1"  # "fake" shoulder
    number: PositiveInt = 1


class LegacyStructuredId(BaseModel):
    naa: Naa
    shoulder: LegacyShoulder
    blade: Blade


class StructuredId(BaseModel):
    naa: Naa
    typecode: Typecode
    shoulder: Shoulder
    blade: Blade


class IdBindings(BaseModel):
    where: Union[LegacyBaseObjectName, BaseObjectName]


class IdBindingOp(str, Enum):
    set = "set"
    addToSet = "addToSet"
    rm = "rm"
    purge = "purge"


class IdBindingRequest(BaseModel):
    i: Union[LegacyBaseObjectName, BaseObjectName]
    o: IdBindingOp = IdBindingOp.set
    a: Optional[str]
    v: Any

    @root_validator()
    def set_or_add_needs_value(cls, values):
        op = values.get("o")
        if op in (IdBindingOp.set, IdBindingOp.addToSet):
            if "v" not in values:
                raise ValueError("{'set','add'} operations needs value 'v'.")
        return values

    @root_validator()
    def set_or_add_or_rm_needs_attribute(cls, values):
        op = values.get("o")
        if op in (IdBindingOp.set, IdBindingOp.addToSet, IdBindingOp.rm):
            if not values.get("a"):
                raise ValueError("{'set','add','rm'} operations need attribute 'a'.")
        return values
