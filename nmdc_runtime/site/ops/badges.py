"""
Dagster ops related to managing the "badges" field of documents in the MongoDB collection
named "biosample_set".
"""

from functools import lru_cache
from logging import Logger
from numbers import Number
from typing import Any

from dagster import OpExecutionContext, op
from linkml_runtime import SchemaView

from nmdc_runtime.util import nmdc_schema_view


class BadgeMan:
    """
    Manages badges that can be awarded to biosamples. The name is short for "badge manager".
    Its official theme song is "da na na na na na na na na na na na na na na na, the badge manager."
    """

    def __init__(self, schema_view: SchemaView, logger: Logger):
        """
        Initializes a `BadgeMan` instance with a `SchemaView` bound to the schema defining the
        badges, and with a `Logger`.

        Docs: https://microbiomedata.github.io/nmdc-schema/badges/
        """

        self.schema_view = schema_view
        self.logger = logger

    @lru_cache
    def get_names_of_slots_in_subset(self, subset_name: str) -> list[str]:
        """
        Returns a list of the names of the slots that are in the specified subset.

        Note: Because the `SubsetDefinition` instance doesn't provide a list of the slots that
              are in it (bummer), we make that list by examining all slots in the schema, since
              a given `SlotDefinition` does say whether it is in a given subset.
        """

        slot_names = []
        for slot_name, slot_definition in self.schema_view.all_slots().items():
            in_subset = slot_definition.in_subset
            if isinstance(in_subset, list) and subset_name in in_subset:
                slot_names.append(slot_name)
            elif isinstance(in_subset, str) and subset_name == in_subset:
                slot_names.append(slot_name)
            else:
                pass

        return slot_names

    @lru_cache
    def get_value_of_subset_annotation(
        self, subset_name: str, annotation_name: str
    ) -> Any:
        """Returns the value of the specified annotation on the specified subset."""

        subset = self.schema_view.get_subset(subset_name=subset_name)
        if subset is None:
            raise ValueError(f"Failed to access definition of subset: {subset_name}")

        try:
            # Note: I failed to find documentation about accessing values of a subset's annotations
            #       in the LinkML docs (docs about subsets seems sparse to me). I came up with this
            #       approach via trial and error.
            annotation_value = subset.annotations[annotation_name].value
        except:
            self.logger.error(
                f"Failed to access value of annotation: {annotation_name}"
            )
            raise

        return annotation_value

    def count_nonempty_fields(
        self, biosample: dict, field_names: list[str], limit: int | None = None
    ) -> int:
        """
        Returns the number of the specified fields that are non-empty on the specified biosample,
        and stops counting once `limit` is reached (or counts them all if `limit` is `None`).
        """

        num_nonempty_fields = 0
        for field_name in field_names:
            if self.has_nonempty_value(biosample, field_name):
                num_nonempty_fields += 1

            # If the caller wants to stop at a certain number and we've reached that number, stop.
            if isinstance(limit, int) and num_nonempty_fields >= limit:
                break

        return num_nonempty_fields

    def qualifies_for_biogeochemistry_badge(self, biosample: dict) -> bool:
        """
        Returns a boolean indicating whether the biosample qualifies for the "biogeochemistry" badge.

        A biosample qualifies if it has at least some specific number of fields (among a designated
        set of fields) that are non-empty.

        That specific number is specified in the schema, as the value of the "badge_minimum_slots"
        annotation of the "biogeochemistry" subset. Similarly, that designated set of fields is
        specified in the schema, as the names of the slots of the "biogeochemistry" subset. At this
        point, you may be itching to get your hands on the documentation for that subset. Have no
        fear, BadgeMan is here! Docs: https://microbiomedata.github.io/nmdc-schema/Biogeochemistry/
        """

        subset_name = "biogeochemistry"

        # Get the minimum number of designated fields that must be non-empty in order to quality for
        # the badge.
        min_num_nonempty_fields = self.get_value_of_subset_annotation(
            subset_name=subset_name,
            annotation_name="badge_minimum_slots",
        )
        if not isinstance(min_num_nonempty_fields, int):
            raise ValueError("Value read from schema has invalid data type.")

        # Get the names of the fields in the designated set.
        field_names = self.get_names_of_slots_in_subset(subset_name=subset_name)

        # Check whether at least `min_num_nonempty_fields` of those designated fields on this
        # biosample are present and non-empty.
        num_nonempty_fields = self.count_nonempty_fields(
            biosample=biosample,
            field_names=field_names,
            limit=min_num_nonempty_fields,
        )
        does_qualify = num_nonempty_fields >= min_num_nonempty_fields
        return does_qualify

    def qualifies_for_host_information_badge(self, biosample: dict) -> bool:
        """
        Returns a boolean indicating whether the biosample qualifies for the "host_information" badge.

        A biosample qualifies if it has at least some specific number of fields (among a designated
        set of fields) that are non-empty. Like the "biogeochemistry" badge, this badge is awarded
        based on how many fields among a designated set of fields are non-empty.

        Docs: https://microbiomedata.github.io/nmdc-schema/HostInformation/
        """

        subset_name = "host_information"

        # Get the minimum number of designated fields that must be non-empty in order to quality for
        # the badge.
        min_num_nonempty_fields = self.get_value_of_subset_annotation(
            subset_name=subset_name,
            annotation_name="badge_minimum_slots",
        )
        if not isinstance(min_num_nonempty_fields, int):
            raise ValueError("Value read from schema has invalid data type.")

        # Get the names of the fields in the designated set.
        field_names = self.get_names_of_slots_in_subset(subset_name=subset_name)

        # Check whether at least `min_num_nonempty_fields` of those designated fields on this
        # biosample are present and non-empty.
        num_nonempty_fields = self.count_nonempty_fields(
            biosample=biosample,
            field_names=field_names,
            limit=min_num_nonempty_fields,
        )
        does_qualify = num_nonempty_fields >= min_num_nonempty_fields
        return does_qualify

    def qualifies_for_expert_curation_badge(self, biosample: dict) -> bool:
        """
        Returns a boolean indicating whether the biosample qualifies for the "expert_curation" badge.

        Unlike the "biogeochemistry" and "host_information" badges, this one is defined using plain
        language instead of programmatically-accessible schema elements. The criteria is:
        A biosample that qualifies if its `provenance_metadata.source_system_of_record` field
        consists of the string "NMDC_Submission_Portal". That's it.

        Docs: https://microbiomedata.github.io/nmdc-schema/provenance_metadata/
        Docs: https://microbiomedata.github.io/nmdc-schema/source_system_of_record/
        """

        does_qualify = False
        if "provenance_metadata" in biosample:
            provenance_metadata = biosample["provenance_metadata"]
            if isinstance(provenance_metadata, dict):
                if "source_system_of_record" in provenance_metadata:
                    source_system_of_record = provenance_metadata[
                        "source_system_of_record"
                    ]
                    if source_system_of_record == "NMDC_Submission_Portal":
                        does_qualify = True

        return does_qualify

    def has_nonempty_value(self, biosample: dict, field_name: str) -> bool:
        """
        Check whether a given biosample has a non-empty value in the specified field. The criteria
        for "non-emptiness" is determined based upon the range of the field.

        The doctests below focus on the checks that happen directly within this method. For the
        range-dependent emptiness checks, see the doctests of the "emptiness checker" helper methods.

        >>> fn = BadgeMan(nmdc_schema_view(), Logger(__name__)).has_nonempty_value
        >>> fn({}, "samp_name")  # field is absent
        False
        >>> fn({"samp_name": None}, "samp_name")  # value is null
        False
        >>> fn({"samp_name": ""}, "samp_name")  # string is empty
        False
        >>> fn({"samp_name": " "}, "samp_name")  # string is whitespace only
        False
        >>> fn({"ph": 0}, "ph")  # zero is a populated numeric value
        True
        >>> fn({"ph": 0.1}, "ph")
        True
        >>> fn({"biotic_relationship": "free living"}, "biotic_relationship")  # range of `BioticRelationshipEnum`
        True
        >>> fn({"plant_sex": "Monoecious"}, "plant_sex")  # range of `PlantSexEnum`
        True
        >>> fn({"host_family_relation": [None, "", " "]}, "host_family_relation")  # multivalued, no non-empty elements
        False
        >>> fn({"host_family_relation": ["", "sibling"]}, "host_family_relation")  # multivalued, mixture of empty and non-empty elements
        True
        >>> fn({"host_diet": [{"type": "nmdc:TextValue"}]}, "host_diet")  # multivalued, range of `TextValue`, empty
        False
        >>> fn({"host_diet": [{"type": "nmdc:TextValue", "has_raw_value": "plants"}]}, "host_diet")  # multivalued, range of `TextValue`, non-empty
        True
        """

        # If the biosample lacks the field altogether, we already know it doesn't have a non-empty
        # value in that field.
        if field_name not in biosample:
            return False

        # If the biosample has the field, but its value is `null`, then (regardless of the slot's
        # range) it doesn't have a non-empty value.
        field_value = biosample[field_name]
        if field_value is None:
            return False

        # Get the field's slot definition (as induced on the `Biosample` class) from the schema,
        # so we can get the slot's range, which will allow us to apply emptiness criteria that makes
        # sense for that slot.
        slot_definition = self.schema_view.induced_slot(field_name, "Biosample")
        slot_range = slot_definition.range
        if slot_range is None:
            raise ValueError(f"Biosample slot {field_name} lacks a range.")

        # Here, we normalize the value or values into a list. We do this to account for multivalued
        # slots, since their values are lists.
        #
        # Note: The ranges of multivalued slots apply to each element of the list, not to the list, itself.
        #
        if slot_definition.multivalued:
            if not isinstance(field_value, list):
                raise ValueError(
                    "Multivalued field %s contains a non-list value: %s",
                    field_name,
                    field_value,
                )
            values = field_value
        else:
            values = [field_value]

        # Dispatch each value to the emptiness checker method associated with its range. Once we
        # find a nonempty one, break out of the loop.
        #
        # Note: These ranges account for the currently-defined badges. We may add support for
        #       additional ranges later, as we introduce additional badges.
        #
        is_nonempty = False
        for value in values:
            if slot_range == "TextValue":
                is_nonempty = self.is_text_value_nonempty(value)
            elif slot_range == "QuantityValue":
                is_nonempty = self.is_quantity_value_nonempty(value)
            elif slot_range == "ControlledTermValue":
                is_nonempty = self.is_controlled_term_value_nonempty(value)
            elif slot_range == "ControlledIdentifiedTermValue":
                is_nonempty = self.is_controlled_identified_term_value_nonempty(value)
            elif slot_range == "string":
                is_nonempty = isinstance(value, str) and value.strip() != ""
            elif slot_range in self.schema_view.all_enums():
                is_nonempty = isinstance(value, str) and value.strip() != ""
            elif slot_range == "float":
                is_nonempty = isinstance(value, Number)
            else:
                raise ValueError(
                    f"Biosample slot {field_name} has unsupported range: {slot_range}"
                )

            # If we've already found a non-empty value in this field, stop evaluating additional
            # values in the field.
            if is_nonempty:
                break

        return is_nonempty

    @staticmethod
    def is_text_value_nonempty(value: Any) -> bool:
        """
        Check whether the specified value constitutes a non-empty `TextValue` value.
        Docs: https://microbiomedata.github.io/nmdc-schema/TextValue/

        >>> fn = BadgeMan.is_text_value_nonempty
        >>> fn(None)
        False
        >>> fn("a")
        False
        >>> fn({})
        False
        >>> fn({"type": "nmdc:TextValue", "language": "en"})
        False
        >>> fn({"type": "nmdc:TextValue", "has_raw_value": ""})
        False
        >>> fn({"type": "nmdc:TextValue", "has_raw_value": "   "})
        False
        >>> fn({"type": "nmdc:TextValue", "has_raw_value": "a"})
        True
        >>> fn({"type": "nmdc:TextValue", "has_raw_value": " a "})
        True
        """
        if isinstance(value, dict) and value.get("type") == "nmdc:TextValue":
            if "has_raw_value" in value:
                has_raw_value = value["has_raw_value"]
                if isinstance(has_raw_value, str) and has_raw_value.strip() != "":
                    return True
        return False

    @staticmethod
    def is_quantity_value_nonempty(value: Any) -> bool:
        """
        Check whether the specified value constitutes a non-empty `QuantityValue` value.
        Docs: https://microbiomedata.github.io/nmdc-schema/QuantityValue/

        >>> fn = BadgeMan.is_quantity_value_nonempty
        >>> fn(None)
        False
        >>> fn(12)
        False
        >>> fn({})
        False
        >>> fn({"type": "nmdc:QuantityValue", "has_unit": "g", "has_raw_value": "12 g"})  # by convention, has_raw_value is insufficient for a QV
        False
        >>> fn({"type": "nmdc:QuantityValue", "has_numeric_value": None, "has_minimum_numeric_value": None, "has_maximum_numeric_value": None})
        False
        >>> fn({"type": "nmdc:QuantityValue", "has_numeric_value": 0})
        True
        >>> fn({"type": "nmdc:QuantityValue", "has_minimum_numeric_value": 0})
        True
        >>> fn({"type": "nmdc:QuantityValue", "has_maximum_numeric_value": 0})
        True
        >>> fn({"type": "nmdc:QuantityValue", "has_numeric_value": 12})
        True
        """
        if isinstance(value, dict) and value.get("type") == "nmdc:QuantityValue":
            if any(
                [
                    value.get("has_maximum_numeric_value", None) is not None,
                    value.get("has_minimum_numeric_value", None) is not None,
                    value.get("has_numeric_value", None) is not None,
                ]
            ):
                return True
        return False

    @staticmethod
    def is_controlled_term_value_nonempty(value: Any) -> bool:
        """
        Check whether the specified value constitutes a non-empty `ControlledTermValue` value.

        Returns `True` if the value is a dictionary having "type" == "nmdc:ControlledTermValue" and
        either (a) the dictionary has a "has_raw_value" value that is a string that, when stripped,
        is a non-empty string; or (b) the dictionary has a "term" value that is a dictionary having
        an "id" value that is a string that, when stripped, is a non-empty string.

        Docs: https://microbiomedata.github.io/nmdc-schema/ControlledTermValue/
        Docs: https://microbiomedata.github.io/nmdc-schema/OntologyClass/ (the range of "term" slot)

        >>> fn = BadgeMan.is_controlled_term_value_nonempty
        >>> fn(None)
        False
        >>> fn({})
        False
        >>> fn({"type": "nmdc:ControlledTermValue"})
        False
        >>> fn({"type": "nmdc:ControlledTermValue", "has_raw_value": ""})
        False
        >>> fn({"type": "nmdc:ControlledTermValue", "has_raw_value": "   "})
        False
        >>> fn({"type": "nmdc:ControlledTermValue", "has_raw_value": "a"})  # has_raw_value, stripped, is a non-empty string
        True
        >>> fn({"type": "nmdc:ControlledTermValue", "term": None})
        False
        >>> fn({"type": "nmdc:ControlledTermValue", "term": {}})
        False
        >>> fn({"type": "nmdc:ControlledTermValue", "term": {"id": None}})
        False
        >>> fn({"type": "nmdc:ControlledTermValue", "term": {"id": "  "}})
        False
        >>> fn({"type": "nmdc:ControlledTermValue", "term": {"id": "a"}})
        True
        """
        if isinstance(value, dict) and value.get("type") == "nmdc:ControlledTermValue":
            if "has_raw_value" in value:
                has_raw_value = value["has_raw_value"]
                if isinstance(has_raw_value, str) and has_raw_value.strip() != "":
                    return True

            # If the "has_raw_value" field was inadequate, check the "term" field.
            term = value.get("term")
            if isinstance(term, dict):
                term_id = term.get("id")
                if isinstance(term_id, str) and term_id.strip() != "":
                    return True

        return False

    @staticmethod
    def is_controlled_identified_term_value_nonempty(value: Any) -> bool:
        """
        Check whether the specified value constitutes a non-empty `ControlledIdentifiedTermValue` value.

        Returns `True` if the value is a dictionary having "type" == "nmdc:ControlledIdentifiedTermValue"
        and the dictionary has a "term" value that is a dictionary having an "id" value that is a
        string that, when stripped, is a non-empty string.

        Docs: https://microbiomedata.github.io/nmdc-schema/ControlledIdentifiedTermValue/
        Docs: https://microbiomedata.github.io/nmdc-schema/OntologyClass/ (the range of "term" slot)

        >>> fn = BadgeMan.is_controlled_identified_term_value_nonempty
        >>> fn(None)
        False
        >>> fn({})
        False
        >>> fn({"type": "nmdc:ControlledIdentifiedTermValue"})
        False
        >>> fn({"type": "nmdc:ControlledIdentifiedTermValue", "has_raw_value": "a"})  # has_raw_value is not relevant for this check
        False
        >>> fn({"type": "nmdc:ControlledIdentifiedTermValue", "term": None})
        False
        >>> fn({"type": "nmdc:ControlledIdentifiedTermValue", "term": {}})
        False
        >>> fn({"type": "nmdc:ControlledIdentifiedTermValue", "term": {"id": None}})
        False
        >>> fn({"type": "nmdc:ControlledIdentifiedTermValue", "term": {"id": "  "}})
        False
        >>> fn({"type": "nmdc:ControlledIdentifiedTermValue", "term": {"id": "a"}})
        True
        """
        if (
            isinstance(value, dict)
            and value.get("type") == "nmdc:ControlledIdentifiedTermValue"
        ):
            term = value.get("term")
            if isinstance(term, dict):
                term_id = term.get("id")
                if isinstance(term_id, str) and term_id.strip() != "":
                    return True
        return False

    @staticmethod
    def make_pipeline_that_applies_badges(badges: list[str]) -> list[dict]:
        """
        Returns a pymongo update pipeline that puts the specified badges into the
        "badges" field of document(s), initializing the field if necessary,
        and de-duplicating the list items.
        """

        # This stage sets the "badges" field to its current value if the field exists and its value
        # is not null; otherwise, it initializes the field to an empty list.
        # Docs: https://www.mongodb.com/docs/manual/reference/operator/aggregation/ifnull/
        ensure_list_stage = {"$set": {"badges": {"$ifNull": ["$badges", []]}}}

        # This stage sets the "badges" field to the union of the existing badges and the new badges.
        # Being a set operation, it removes duplicates, but does not preserve order.
        #
        # Note: The "$literal" tells MongoDB not to treat any badge names as expressions, in case
        #       they happen to look like MongoDB expressions. I think this is MongoDB's protection
        #       against their version of "SQL injection."
        #
        # Docs: https://www.mongodb.com/docs/manual/reference/operator/aggregation/setunion/
        # Docs: https://www.mongodb.com/docs/manual/reference/operator/aggregation/literal/
        #
        insert_and_dedupe_stage = {
            "$set": {"badges": {"$setUnion": ["$badges", {"$literal": badges}]}}
        }

        return [ensure_list_stage, insert_and_dedupe_stage]


@op(required_resource_keys={"mongo"})
def award_badges_to_biosamples_op(context: OpExecutionContext) -> None:
    """
    Award badges to biosamples, without revoking any badges from any biosamples.
    """

    logger = context.log

    # Instantiate a badge manager.
    badge_man = BadgeMan(
        schema_view=nmdc_schema_view(),
        logger=logger,
    )

    # Award badges to the biosamples that qualify for them.
    # TODO: Consider projecting only the fields that are relevant to badges.
    logger.info("Evaluating biosamples and awarding badges.")
    num_biosamples_evaluated = 0
    num_biosamples_awarded_badges = 0
    biosample_set = context.resources.mongo.db.get_collection("biosample_set")
    with biosample_set.find({}, batch_size=1000) as cursor:
        for biosample in cursor:
            num_biosamples_evaluated += 1

            # Read the initial badges from the biosample. This will allow us to avoid performing
            # unnecessary database writes (i.e. for badges that the biosample already has).
            # TODO: Another optimization is to not bother checking whether the biosample qualifies
            #       for any of these badges; since NMDC has a policy of never rescinding a badge.
            initial_badges = []
            if "badges" in biosample and isinstance(biosample["badges"], list):
                initial_badges = biosample["badges"]

            # Determine whether this biosample has earned any badges.
            earned_badges = []
            if badge_man.qualifies_for_biogeochemistry_badge(biosample):
                earned_badges.append("biogeochemistry")
            if badge_man.qualifies_for_host_information_badge(biosample):
                earned_badges.append("host_information")
            if badge_man.qualifies_for_expert_curation_badge(biosample):
                earned_badges.append("expert_curation")

            if len(earned_badges) > 0:
                # If any of the earned badges aren't among the biosample's initial badges,
                # perform an atomic update that will award those additional badges to the biosample.
                newly_earned_badges = [eb for eb in earned_badges if eb not in initial_badges]
                if len(newly_earned_badges) > 0:
                    logger.debug(
                        "Biosample %s qualifies for %d additional badges: %s",
                        biosample["id"],
                        len(newly_earned_badges),
                        ", ".join(sorted(newly_earned_badges))
                    )
                    biosample_set.update_one(
                        {"_id": biosample["_id"]},
                        badge_man.make_pipeline_that_applies_badges(earned_badges),
                    )
                    num_biosamples_awarded_badges += 1

    logger.info(
        "Evaluated %d biosamples, %d of which were awarded additional badges.",
        num_biosamples_evaluated,
        num_biosamples_awarded_badges,
    )
