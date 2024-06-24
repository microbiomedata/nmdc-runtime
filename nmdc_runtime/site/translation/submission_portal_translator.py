import logging
import re
from datetime import datetime
from functools import lru_cache
from importlib import resources
from typing import Any, List, Optional, Union

from linkml_runtime import SchemaView
from linkml_runtime.linkml_model import SlotDefinition
from nmdc_schema import nmdc
from toolz import get_in, groupby, concat, valmap, dissoc

from nmdc_runtime.site.translation.translator import JSON_OBJECT, Translator


BIOSAMPLE_UNIQUE_KEY_SLOT = "samp_name"


@lru_cache
def _get_schema_view():
    """Return a SchemaView instance representing the NMDC schema"""
    return SchemaView(
        str(resources.path("nmdc_schema", "nmdc_materialized_patterns.yaml"))
    )


def group_dicts_by_key(key: str, seq: Optional[list[dict]]) -> Optional[dict]:
    """Transform a sequence of dicts into a single dict based on values of `key` in each dict.

    Unlike toolz.groupby: 1) this method only applies to sequences of dicts, 2) it only keeps one
    value for each key, 3) it removes the key from the original dict.

    Example:
        >>> seq = [{'a': 'one', 'b': 1}, {'a': 'two', 'b': 2}, {'a': 'two', 'b': 3}]
        >>> group_dicts_by_key('a', seq)
        {'one': {'b': 1}, 'two': {'b': 3}}
    """
    if seq is None:
        return None

    grouped = {}
    for idx, item in enumerate(seq):
        key_value = item.pop(key)
        if not key_value:
            logging.warning(f"No key value found in index {idx}")
            continue
        if key_value in grouped:
            logging.warning(
                f"Duplicate key value {key_value}. Previous value will be overwritten."
            )
        grouped[key_value] = item
    return grouped


class SubmissionPortalTranslator(Translator):
    """A Translator subclass for handling submission portal entries

    This translator is constructed with a metadata_submission object from the
    submission portal. Since the submission schema is built by importing slots
    from the nmdc:Biosample class this translator largely works by introspecting
    the nmdc:Biosample class (via a SchemaView instance)
    """

    def __init__(
        self,
        metadata_submission: JSON_OBJECT = {},
        omics_processing_mapping: Optional[list] = None,
        data_object_mapping: Optional[list] = None,
        *args,
        # Additional study-level metadata not captured by the submission portal currently
        # See: https://github.com/microbiomedata/submission-schema/issues/162
        study_doi_category: Optional[str] = None,
        study_doi_provider: Optional[str] = None,
        study_category: Optional[str] = None,
        study_pi_image_url: Optional[str] = None,
        study_funding_sources: Optional[list[str]] = None,
        # Additional biosample-level metadata with optional column mapping information not captured
        # by the submission portal currently.
        # See: https://github.com/microbiomedata/submission-schema/issues/162
        biosample_extras: Optional[list[dict]] = None,
        biosample_extras_slot_mapping: Optional[list[dict]] = None,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)

        self.metadata_submission = metadata_submission
        self.omics_processing_mapping = omics_processing_mapping
        self.data_object_mapping = data_object_mapping

        self.study_doi_category = (
            nmdc.DoiCategoryEnum(study_doi_category)
            if study_doi_category
            else nmdc.DoiCategoryEnum.dataset_doi
        )
        self.study_doi_provider = (
            nmdc.DoiProviderEnum(study_doi_provider) if study_doi_provider else None
        )
        self.study_category = (
            nmdc.StudyCategoryEnum(study_category) if study_category else None
        )
        self.study_pi_image_url = study_pi_image_url
        self.study_funding_sources = study_funding_sources

        self.biosample_extras = group_dicts_by_key(
            BIOSAMPLE_UNIQUE_KEY_SLOT, biosample_extras
        )
        self.biosample_extras_slot_mapping = group_dicts_by_key(
            "subject_id", biosample_extras_slot_mapping
        )

        self.schema_view: SchemaView = _get_schema_view()

    def _get_pi(
        self, metadata_submission: JSON_OBJECT
    ) -> Union[nmdc.PersonValue, None]:
        """Construct an nmdc:PersonValue object using values from the study form data

        :param metadata_submission: submission portal entry
        :return: nmdc:PersonValue
        """
        study_form = metadata_submission.get("studyForm")
        if not study_form:
            return None

        return nmdc.PersonValue(
            name=study_form.get("piName"),
            email=study_form.get("piEmail"),
            orcid=study_form.get("piOrcid"),
            profile_image_url=self.study_pi_image_url,
        )

    def _get_doi(self, metadata_submission: JSON_OBJECT) -> Union[List[nmdc.Doi], None]:
        """Get DOI information from the context form data

        :param metadata_submission: submission portal entry
        :return: list of strings or None
        """
        dataset_doi = get_in(["contextForm", "datasetDoi"], metadata_submission)
        if not dataset_doi:
            return None

        if not dataset_doi.startswith("doi:"):
            dataset_doi = f"doi:{dataset_doi}"

        return [
            nmdc.Doi(
                doi_value=dataset_doi,
                doi_provider=self.study_doi_provider,
                doi_category=self.study_doi_category,
            )
        ]

    def _get_has_credit_associations(
        self, metadata_submission: JSON_OBJECT
    ) -> Union[List[nmdc.CreditAssociation], None]:
        """Construct a list of nmdc:CreditAssociation from the study form data

        :param metadata_submission: submission portal entry
        :return: nmdc.CreditAssociation list
        """
        contributors = get_in(["studyForm", "contributors"], metadata_submission)
        if not contributors:
            return None

        return [
            nmdc.CreditAssociation(
                applies_to_person=nmdc.PersonValue(
                    name=contributor.get("name"),
                    orcid=contributor.get("orcid"),
                ),
                applied_roles=contributor.get("roles"),
            )
            for contributor in contributors
        ]

    def _get_gold_study_identifiers(
        self, metadata_submission: JSON_OBJECT
    ) -> Union[List[str], None]:
        """Construct a GOLD CURIE from the multiomics from data

        :param metadata_submission: submission portal entry
        :return: GOLD CURIE
        """
        gold_study_id = get_in(["multiOmicsForm", "GOLDStudyId"], metadata_submission)
        if not gold_study_id:
            return None

        return [self._get_curie("GOLD", gold_study_id)]

    def _get_quantity_value(
        self, raw_value: Optional[str], unit: Optional[str] = None
    ) -> Union[nmdc.QuantityValue, None]:
        """Construct a nmdc:QuantityValue from a raw value string

        The regex pattern minimally matches on a single numeric value (possibly
        floating point). The pattern can also identify a range represented by
        two numeric values separated by a hyphen. It can also identify non-numeric
        characters at the end of the string which are interpreted as a unit. A unit
        may also be explicitly provided as an argument to this function. If parsing
        identifies a unit and a unit argument is provided, the unit argument is used.
        If the pattern is not matched at all None is returned.

        TODO: currently the parsed unit string is used as-is. In the future we may want
        to be stricter about what we accept or coerce into a controlled value set

        :param raw_value: string to parse
        :param unit: optional unit, defaults to None
        :return: nmdc:QuantityValue
        """
        if raw_value is None:
            return None

        match = re.fullmatch(
            "([+-]?(?=\.\d|\d)(?:\d+)?(?:\.?\d*)(?:[eE][+-]?\d+)?)(?: *- *([+-]?(?=\.\d|\d)(?:\d+)?(?:\.?\d*)(?:[eE][+-]?\d+)?))?(?: *(\S+))?",
            raw_value,
        )
        if not match:
            return None

        qv = nmdc.QuantityValue(has_raw_value=raw_value)
        if match.group(2):
            # having group 2 means the value is a range like "0 - 1". Either
            # group 1 or group 2 might be the minimum especially when handling
            # negative ranges like "0 - -1"
            num_1 = float(match.group(1))
            num_2 = float(match.group(2))
            qv.has_minimum_numeric_value = min(num_1, num_2)
            qv.has_maximum_numeric_value = max(num_1, num_2)
        else:
            # otherwise we just have a single numeric value
            qv.has_numeric_value = float(match.group(1))

        if unit:
            # a unit was manually specified
            if match.group(3) and unit != match.group(3):
                # a unit was also found in the raw string; issue a warning
                # if they don't agree, but keep the manually specified one
                logging.warning(f'Unit mismatch: "{unit}" and "{match.group(3)}"')
            qv.has_unit = unit
        elif match.group(3):
            # a unit was found in the raw string
            qv.has_unit = match.group(3)

        return qv

    def _get_ontology_class(
        self, raw_value: Optional[str]
    ) -> Union[nmdc.OntologyClass, None]:
        """Construct a nmdc:OntologyClass from a raw value string

        The regexp pattern matches values of the format "name [identifier]". If this pattern is
        not matched None is returned.

        :param raw_value: string to parse
        :return: nmdc.OntologyClass
        """
        match = re.match("_*([^\[]+)(?:\[([^\]]+)\])", raw_value)
        if not match or not match.group(2):
            logging.warning(
                f'Could not infer OntologyClass id from value "{raw_value}"'
            )
            return None

        return nmdc.OntologyClass(
            name=match.group(1).strip(),
            id=match.group(2).strip(),
        )

    def _get_controlled_identified_term_value(
        self, raw_value: Optional[str]
    ) -> Union[nmdc.ControlledIdentifiedTermValue, None]:
        """Construct a nmdc.ControlledIdentifiedTermValue from a raw value string

        The regexp pattern matches values of the format "name [identifier]". If this pattern is
        not matched None is returned.

        :param raw_value: string to parse
        :return: nmdc.ControlledIdentifiedTermValue
        """
        if not raw_value:
            return None

        ontology_class = self._get_ontology_class(raw_value)
        if ontology_class is None:
            return None

        return nmdc.ControlledIdentifiedTermValue(
            has_raw_value=raw_value, term=ontology_class
        )

    def _get_controlled_term_value(
        self, raw_value: Optional[str]
    ) -> Union[nmdc.ControlledTermValue, None]:
        """Construct a nmdc.ControlledTermValue from a raw value string

        The regexp pattern matches values of the format "name [identifier]". The identifier portion
        is optional. If it is not found then the returned object's `term` field will be none.

        :param raw_value: string to parse
        :return: nmdc.ControlledTermValue
        """
        if not raw_value:
            return None

        value = nmdc.ControlledTermValue(has_raw_value=raw_value)
        ontology_class = self._get_ontology_class(raw_value)
        if ontology_class is not None:
            value.term = ontology_class

        return value

    def _get_geolocation_value(
        self, raw_value: Optional[str]
    ) -> Union[nmdc.GeolocationValue, None]:
        """Construct a nmdc.GeolocationValue from a raw string value

        The regexp pattern matches a latitude and a longitude value separated by a space or comma. The
        latitude and longitude values should be in decimal degrees. If the pattern is not matched, None
        is returned.

        :param raw_value: string to parse
        :return: nmdc.GeolocationValue
        """
        if raw_value is None:
            return None

        match = re.fullmatch(
            "([-+]?(?:[1-8]?\d(?:\.\d+)?|90(?:\.0+)?))[\s,]+([-+]?(?:180(?:\.0+)?|(?:(?:1[0-7]\d)|(?:[1-9]?\d))(?:\.\d+)?))",
            raw_value,
        )
        if match is None:
            return None

        return nmdc.GeolocationValue(
            has_raw_value=raw_value, latitude=match.group(1), longitude=match.group(2)
        )

    def _get_float(self, raw_value: Optional[str]) -> Union[float, None]:
        """Construct a float from a raw string value

        If a float cannot be parsed from the string, None is returned.

        :param raw_value: string to parse
        :return: float
        """
        try:
            return float(raw_value)
        except (ValueError, TypeError):
            return None

    def _get_from(self, metadata_submission: JSON_OBJECT, field: Union[str, List[str]]):
        """Extract and sanitize a value from a nested dict

        For field = [i0, i1, ..., iN] extract metadata_submission[i0][i1]...[iN]. This
        value is then sanitized by trimming strings, replacing empty strings with None,
        filtering Nones and empty strings from arrays.

        :param metadata_submission: submission portal entry
        :param field: list of nested fields to extract
        :return: sanitized value
        """
        if not isinstance(field, list):
            field = [field]
        value = get_in(field, metadata_submission)

        def sanitize(val):
            sanitized = val
            if isinstance(sanitized, str):
                sanitized = sanitized.strip()
            if sanitized == "":
                sanitized = None
            return sanitized

        if isinstance(value, list):
            value = [sanitize(v) for v in value]
            value = [v for v in value if v is not None]
            if not value:
                value = None
        else:
            value = sanitize(value)

        return value

    def _translate_study(
        self, metadata_submission: JSON_OBJECT, nmdc_study_id: str
    ) -> nmdc.Study:
        """Translate a metadata submission into an `nmdc:Study` object.

        This method translates a metadata submission object into an equivalent
        `nmdc:Study` object. The minted NMDC study ID must be passed to this method.

        :param gold_study: metadata submission object
        :param nmdc_study_id: Minted nmdc:Study identifier for the translated object
        :return: nmdc:Study object
        """
        return nmdc.Study(
            alternative_identifiers=self._get_from(
                metadata_submission, ["multiOmicsForm", "JGIStudyId"]
            ),
            alternative_names=self._get_from(
                metadata_submission, ["multiOmicsForm", "alternativeNames"]
            ),
            associated_dois=self._get_doi(metadata_submission),
            description=self._get_from(
                metadata_submission, ["studyForm", "description"]
            ),
            funding_sources=self.study_funding_sources,
            # emsl_proposal_identifier=self._get_from(
            #     metadata_submission, ["multiOmicsForm", "studyNumber"]
            # ),
            gold_study_identifiers=self._get_gold_study_identifiers(
                metadata_submission
            ),
            has_credit_associations=self._get_has_credit_associations(
                metadata_submission
            ),
            id=nmdc_study_id,
            insdc_bioproject_identifiers=self._get_from(
                metadata_submission, ["multiOmicsForm", "NCBIBioProjectId"]
            ),
            name=self._get_from(metadata_submission, ["studyForm", "studyName"]),
            notes=self._get_from(metadata_submission, ["studyForm", "notes"]),
            principal_investigator=self._get_pi(metadata_submission),
            study_category=self.study_category,
            title=self._get_from(metadata_submission, ["studyForm", "studyName"]),
            websites=self._get_from(
                metadata_submission, ["studyForm", "linkOutWebpage"]
            ),
        )

    def _transform_value_for_slot(
        self, value: Any, slot: SlotDefinition, unit: Optional[str] = None
    ):
        transformed_value = None
        if slot.range == "TextValue":
            transformed_value = nmdc.TextValue(has_raw_value=value)
        elif slot.range == "QuantityValue":
            transformed_value = self._get_quantity_value(value, unit=unit)
        elif slot.range == "ControlledIdentifiedTermValue":
            transformed_value = self._get_controlled_identified_term_value(value)
        elif slot.range == "ControlledTermValue":
            transformed_value = self._get_controlled_term_value(value)
        elif slot.range == "TimestampValue":
            transformed_value = nmdc.TimestampValue(has_raw_value=value)
        elif slot.range == "GeolocationValue":
            transformed_value = self._get_geolocation_value(value)
        elif slot.range == "float":
            transformed_value = self._get_float(value)
        elif slot.range == "string":
            transformed_value = str(value).strip()
        else:
            transformed_value = value

        return transformed_value

    def _transform_dict_for_class(
        self, raw_values: dict, class_name: str, slot_mappings: Optional[dict] = None
    ) -> dict:
        """Transform a dict of values according to class slots.

        raw_values is a dict where the keys are slot names and the values are plain strings.
        Each of the items in this dict will be transformed by the _transform_value_for_slot
        method. If the slot is multivalued each individual value will be transformed. If the
        slot is multivalued and the value is a string it will be split at pipe characters
        before transforming.

        If slot_mappings is provided it should be a dict where each key is a potential non-NMDC
        column name and corresponding value is a dict with keys for `object_id` (representing
        the NMDC slot being mapped to; the name `object_id` is to overlap with the SSSOM standard
        https://github.com/mapping-commons/sssom) and for `subject_unit` (useful for when the column
        maps to a QuantityValue slot and the source metadata itself does not include the unit)
        """
        slot_names = self.schema_view.class_slots(class_name)
        transformed_values = {}
        for column, value in raw_values.items():
            slot_name = column
            unit = None
            if slot_mappings and column in slot_mappings:
                mapped_column_name = slot_mappings[column].get("object_id")
                if mapped_column_name and mapped_column_name.startswith("nmdc:"):
                    slot_name = mapped_column_name.replace("nmdc:", "")

                unit = slot_mappings[column].get("subject_unit")

            if slot_name not in slot_names:
                logging.warning(f"No slot '{slot_name}' on class '{class_name}'")
                continue

            slot_definition = self.schema_view.induced_slot(slot_name, class_name)
            if slot_definition.multivalued:
                value_list = value
                if isinstance(value, str):
                    value_list = [v.strip() for v in value.split("|")]
                transformed_value = [
                    self._transform_value_for_slot(item, slot_definition, unit)
                    for item in value_list
                ]
            else:
                transformed_value = self._transform_value_for_slot(
                    value, slot_definition, unit
                )

            transformed_values[slot_name] = transformed_value
        return transformed_values

    def _translate_biosample(
        self,
        sample_data: List[JSON_OBJECT],
        nmdc_biosample_id: str,
        nmdc_study_id: str,
        default_env_package: str,
    ) -> nmdc.Biosample:
        """Translate sample data from portal submission into an `nmdc:Biosample` object.

        sample_data is a list of objects where each object represents one row from a tab in
        the submission portal. Each of the objects represents information about the same
        underlying biosample. For each of the rows, each of the columns is iterated over.
        For each column, the corresponding slot from the nmdc:Biosample class is identified.
        The raw value from the submission portal is the transformed according to the range
        of the nmdc:Biosample slot.

        :param sample_data: collection of rows representing data about a single biosample
                            from each applicable submission portal tab
        :param nmdc_biosample_id: Minted nmdc:Biosample identifier for the translated object
        :param nmdc_study_id: Minted nmdc:Study identifier for the related Study
        :param default_env_package: Default value for `env_package` slot
        :return: nmdc:Biosample
        """
        biosample_key = sample_data[0].get(BIOSAMPLE_UNIQUE_KEY_SLOT, "").strip()
        slots = {
            "id": nmdc_biosample_id,
            "part_of": nmdc_study_id,
            "name": sample_data[0].get("samp_name", "").strip(),
            "env_package": nmdc.TextValue(has_raw_value=default_env_package),
        }
        for tab in sample_data:
            transformed_tab = self._transform_dict_for_class(tab, "Biosample")
            slots.update(transformed_tab)

        if self.biosample_extras:
            raw_extras = self.biosample_extras.get(biosample_key)
            if raw_extras:
                transformed_extras = self._transform_dict_for_class(
                    raw_extras, "Biosample", self.biosample_extras_slot_mapping
                )
                slots.update(transformed_extras)

        return nmdc.Biosample(**slots)

    def get_database(self) -> nmdc.Database:
        """Translate the submission portal entry to an nmdc:Database

        This method translates the submission portal entry into one nmdc:Study and a set
        of nmdc:Biosample objects. These are wrapped into an nmdc:Database. NMDC identifiers
        are minted for each new object.

        :return: nmdc:Database object
        """
        database = nmdc.Database()

        nmdc_study_id = self._id_minter("nmdc:Study")[0]

        metadata_submission_data = self.metadata_submission.get(
            "metadata_submission", {}
        )
        database.study_set = [
            self._translate_study(metadata_submission_data, nmdc_study_id)
        ]

        sample_data = metadata_submission_data.get("sampleData", {})
        package_name = metadata_submission_data["packageName"]
        sample_data_by_id = groupby(
            BIOSAMPLE_UNIQUE_KEY_SLOT, concat(sample_data.values())
        )
        nmdc_biosample_ids = self._id_minter("nmdc:Biosample", len(sample_data_by_id))
        sample_data_to_nmdc_biosample_ids = dict(
            zip(sample_data_by_id.keys(), nmdc_biosample_ids)
        )

        database.biosample_set = [
            self._translate_biosample(
                sample_data,
                nmdc_biosample_id=sample_data_to_nmdc_biosample_ids[sample_data_id],
                nmdc_study_id=nmdc_study_id,
                default_env_package=package_name,
            )
            for sample_data_id, sample_data in sample_data_by_id.items()
            if sample_data
        ]

        if self.omics_processing_mapping:
            # If there is data from an OmicsProcessing mapping file, process it now. This part
            # assumes that there is a column in that file with the header __biosample_samp_name
            # that can be used to join with the sample data from the submission portal. The
            # biosample identified by that `samp_name` will be referenced in the `has_input`
            # slot of the OmicsProcessing object. If a DataObject mapping file was also provided,
            # those objects will also be generated and referenced in the `has_output` slot of the
            # OmicsProcessing object. By keying off of the `samp_name` slot of the submission's
            # sample data there is an implicit 1:1 relationship between Biosample objects and
            # OmicsProcessing objects generated here.
            join_key = f"__biosample_{BIOSAMPLE_UNIQUE_KEY_SLOT}"
            database.omics_processing_set = []
            database.data_object_set = []
            data_objects_by_sample_data_id = {}
            today = datetime.now().strftime("%Y-%m-%d")

            if self.data_object_mapping:
                # If DataObject mapping data was provided, group it by the sample ID key and then
                # strip that key out of the resulting grouped data.
                grouped = groupby(join_key, self.data_object_mapping)
                data_objects_by_sample_data_id = valmap(
                    lambda data_objects: [
                        dissoc(data_object, join_key) for data_object in data_objects
                    ],
                    grouped,
                )

            for omics_processing_row in self.omics_processing_mapping:
                # For each row in the OmicsProcessing mapping file, first grab the minted Biosample
                # id that corresponds to the sample ID from the submission
                sample_data_id = omics_processing_row.pop(join_key)
                if (
                    not sample_data_id
                    or sample_data_id not in sample_data_to_nmdc_biosample_ids
                ):
                    logging.warning(
                        f"Unrecognized biosample {BIOSAMPLE_UNIQUE_KEY_SLOT}: {sample_data_id}"
                    )
                    continue
                nmdc_biosample_id = sample_data_to_nmdc_biosample_ids[sample_data_id]

                # Transform the raw row data according to the OmicsProcessing class's slots, and
                # generate an instance. A few key slots do not come from the mapping file, but
                # instead are defined here.
                omics_processing_slots = {
                    "id": self._id_minter("nmdc:OmicsProcessing", 1)[0],
                    "has_input": [nmdc_biosample_id],
                    "has_output": [],
                    "part_of": nmdc_study_id,
                    "add_date": today,
                    "mod_date": today,
                    "type": "nmdc:OmicsProcessing",
                }
                omics_processing_slots.update(
                    self._transform_dict_for_class(
                        omics_processing_row, "OmicsProcessing"
                    )
                )
                omics_processing = nmdc.OmicsProcessing(**omics_processing_slots)

                for data_object_row in data_objects_by_sample_data_id.get(
                    sample_data_id, []
                ):
                    # For each row in the DataObject mapping file that corresponds to the sample ID,
                    # transform the raw row data according to the DataObject class's slots, generate
                    # an instance, and connect that instance's minted ID to the OmicsProcessing
                    # instance
                    data_object_id = self._id_minter("nmdc:DataObject", 1)[0]
                    data_object_slots = {
                        "id": data_object_id,
                        "type": "nmdc:DataObject",
                    }
                    data_object_slots.update(
                        self._transform_dict_for_class(data_object_row, "DataObject")
                    )
                    data_object = nmdc.DataObject(**data_object_slots)

                    omics_processing.has_output.append(data_object_id)

                    database.data_object_set.append(data_object)

                database.omics_processing_set.append(omics_processing)

        return database
