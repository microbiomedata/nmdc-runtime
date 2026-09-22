"""
Dagster ops related to managing the "badges" field of documents in the MongoDB collection
named "biosample_set".
"""

from functools import lru_cache
from logging import Logger
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
        """
        Returns the integer value of the specified annotation on the specified subset.
        """

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

    @staticmethod
    def has_nonempty_value(biosample: dict, field_name: str) -> bool:
        """
        Returns `True` if the specified field exists and has a non-empty value.

        >>> fn = BadgeMan.has_nonempty_value
        >>> fn({}, "name")  # field is absent
        False
        >>> fn({"name": ""}, "name")
        False
        >>> fn({"name": None}, "name")
        False
        >>> fn({"names": []}, "names")
        False
        >>> fn({"names": {}}, "names")
        False

        >>> fn({"name": " "}, "name")  # here, we consider whitespace to be non-empty
        True
        >>> fn({"name": "a"}, "name")
        True
        >>> fn({"name": 0}, "name")
        True
        >>> fn({"names": ["a"]}, "names")
        True
        >>> fn({"names": {"a": False}}, "names")
        True
        """

        if field_name in biosample:
            if biosample[field_name] not in (None, "", [], {}):
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
    biosample_set = context.resources.mongo.db.get_collection("biosample_set")
    with biosample_set.find({}, batch_size=1000) as cursor:
        for biosample in cursor:

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
                # perform an atomic update that will award them to the biosample.
                if any(eb not in initial_badges for eb in earned_badges):
                    biosample_set.update_one(
                        {"_id": biosample["_id"]},
                        badge_man.make_pipeline_that_applies_badges(earned_badges),
                    )
