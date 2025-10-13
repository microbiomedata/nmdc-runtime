import logging
import re
from collections import namedtuple
from datetime import datetime
from decimal import Decimal
from enum import Enum
from functools import lru_cache
from importlib import resources
from typing import Any, List, Optional, Union, Tuple
from urllib.parse import urlparse

from linkml_runtime import SchemaView
from linkml_runtime.linkml_model import SlotDefinition
from nmdc_schema import nmdc
from toolz import concat, dissoc, get_in, groupby, valmap

from nmdc_runtime.site.translation.translator import JSON_OBJECT, Translator


DataUrlSet = namedtuple("DataUrlSet", ["url", "md5_checksum"])

READ_1 = DataUrlSet("read_1_url", "read_1_md5_checksum")
READ_2 = DataUrlSet("read_2_url", "read_2_md5_checksum")
INTERLEAVED = DataUrlSet("interleaved_url", "interleaved_md5_checksum")

DATA_URL_SETS: list[DataUrlSet] = [READ_1, READ_2, INTERLEAVED]

BIOSAMPLE_UNIQUE_KEY_SLOT = "samp_name"

TAB_NAME_KEY = "__tab_name"
METAGENOME = nmdc.NucleotideSequencingEnum(nmdc.NucleotideSequencingEnum.metagenome)
METATRANSCRIPTOME = nmdc.NucleotideSequencingEnum(
    nmdc.NucleotideSequencingEnum.metatranscriptome
)
TAB_NAME_TO_ANALYTE_CATEGORY: dict[str, nmdc.NucleotideSequencingEnum] = {
    "metagenome_sequencing_non_interleaved_data": METAGENOME,
    "metagenome_sequencing_interleaved_data": METAGENOME,
    "metatranscriptome_sequencing_non_interleaved_data": METATRANSCRIPTOME,
    "metatranscriptome_sequencing_interleaved_data": METATRANSCRIPTOME,
}

DATA_URL_SET_AND_ANALYTE_TO_DATA_OBJECT_TYPE: dict[tuple[DataUrlSet, str], str] = {
    (READ_1, str(METAGENOME)): "Metagenome Raw Read 1",
    (READ_2, str(METAGENOME)): "Metagenome Raw Read 2",
    (INTERLEAVED, str(METAGENOME)): "Metagenome Raw Reads",
    (READ_1, str(METATRANSCRIPTOME)): "Metatranscriptome Raw Read 1",
    (READ_2, str(METATRANSCRIPTOME)): "Metatranscriptome Raw Read 2",
    (INTERLEAVED, str(METATRANSCRIPTOME)): "Metatranscriptome Raw Reads",
}

UNIT_OVERRIDES: dict[str, dict[str, str]] = {
    "Biosample": {
        "depth": "m",
    }
}


class EnvironmentPackage(Enum):
    r"""
    Enumeration of all possible environmental packages.

    >>> EnvironmentPackage.AIR.value
    'air'
    >>> EnvironmentPackage.SEDIMENT.value
    'sediment'
    """

    AIR = "air"
    BIOFILM = "microbial mat_biofilm"
    BUILT_ENV = "built environment"
    HCR_CORES = "hydrocarbon resources-cores"
    HRC_FLUID_SWABS = "hydrocarbon resources-fluids_swabs"
    HOST_ASSOCIATED = "host-associated"
    MISC_ENVS = "miscellaneous natural or artificial environment"
    PLANT_ASSOCIATED = "plant-associated"
    SEDIMENT = "sediment"
    SOIL = "soil"
    WATER = "water"


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


def split_strip(string: str | None, sep: str) -> list[str] | None:
    """Split a string by a separator and strip whitespace from each part.

    :param string: string to split
    :param sep: separator to split by
    :return: list of stripped strings
    """
    if string is None:
        return None
    return [s.strip() for s in string.split(sep)]


class SubmissionPortalTranslator(Translator):
    """A Translator subclass for handling submission portal entries

    This translator is constructed with a metadata_submission object from the
    submission portal. Since the submission schema is built by importing slots
    from the nmdc:Biosample class this translator largely works by introspecting
    the nmdc:Biosample class (via a SchemaView instance)
    """

    def __init__(
        self,
        metadata_submission: Optional[JSON_OBJECT] = None,
        *args,
        nucleotide_sequencing_mapping: Optional[list] = None,
        data_object_mapping: Optional[list] = None,
        illumina_instrument_mapping: Optional[dict[str, str]] = None,
        # Additional study-level metadata not captured by the submission portal currently
        # See: https://github.com/microbiomedata/submission-schema/issues/162
        study_category: Optional[str] = None,
        study_pi_image_url: Optional[str] = None,
        study_id: Optional[str] = None,
        # Additional biosample-level metadata with optional column mapping information not captured
        # by the submission portal currently.
        # See: https://github.com/microbiomedata/submission-schema/issues/162
        biosample_extras: Optional[list[dict]] = None,
        biosample_extras_slot_mapping: Optional[list[dict]] = None,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)

        self.metadata_submission: JSON_OBJECT = metadata_submission or {}
        self.nucleotide_sequencing_mapping = nucleotide_sequencing_mapping
        self.data_object_mapping = data_object_mapping
        self.illumina_instrument_mapping: dict[str, str] = (
            illumina_instrument_mapping or {}
        )

        self.study_category = (
            nmdc.StudyCategoryEnum(study_category) if study_category else None
        )
        self.study_pi_image_url = study_pi_image_url
        self.study_id = study_id

        self.biosample_extras = group_dicts_by_key(
            BIOSAMPLE_UNIQUE_KEY_SLOT, biosample_extras
        )
        self.biosample_extras_slot_mapping = group_dicts_by_key(
            "subject_id", biosample_extras_slot_mapping
        )

        self.schema_view: SchemaView = _get_schema_view()
        self._material_processing_subclass_names = []
        for class_name in self.schema_view.class_descendants(
            "MaterialProcessing", reflexive=False
        ):
            class_def = self.schema_view.get_class(class_name)
            if not class_def.abstract:
                self._material_processing_subclass_names.append(class_name)

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
            type=nmdc.PersonValue.class_class_curie,
        )

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
                    type="nmdc:PersonValue",
                ),
                applied_roles=contributor.get("roles"),
                type="nmdc:CreditAssociation",
            )
            for contributor in contributors
        ]

    def _get_gold_study_identifiers(
        self, metadata_submission: JSON_OBJECT
    ) -> Union[List[str], None]:
        """Construct a GOLD CURIE from the study form data

        :param metadata_submission: submission portal entry
        :return: GOLD CURIE
        """
        gold_study_id = get_in(["studyForm", "GOLDStudyId"], metadata_submission)
        if not gold_study_id:
            return None

        return [self._ensure_curie(gold_study_id, default_prefix="gold")]

    def _get_ncbi_bioproject_identifiers(
        self, metadata_submission: JSON_OBJECT
    ) -> Union[List[str], None]:
        """Construct a NCBI Bioproject CURIE from the study form data"""

        ncbi_bioproject_id = get_in(
            ["studyForm", "NCBIBioProjectId"], metadata_submission
        )
        if not ncbi_bioproject_id:
            return None

        return [self._ensure_curie(ncbi_bioproject_id, default_prefix="bioproject")]

    def _get_jgi_study_identifiers(
        self, metadata_submission: JSON_OBJECT
    ) -> Union[List[str], None]:
        """Construct a JGI proposal CURIE from the multiomics form data

        :param metadata_submission: submission portal entry
        :return: JGI proposal CURIE
        """
        jgi_study_id = get_in(["multiOmicsForm", "JGIStudyId"], metadata_submission)
        if not jgi_study_id:
            return None

        return [self._ensure_curie(jgi_study_id, default_prefix="jgi.proposal")]

    def _get_emsl_project_identifiers(
        self, metadata_submission: JSON_OBJECT
    ) -> Union[List[str], None]:
        """Construct an EMSL project CURIE from the multiomics form data

        :param metadata_submission: submission portal entry
        :return: EMSL project CURIE
        """
        emsl_project_id = get_in(["multiOmicsForm", "studyNumber"], metadata_submission)
        if not emsl_project_id:
            return None

        return [self._ensure_curie(emsl_project_id, default_prefix="emsl.project")]

    def _get_quantity_value(
        self, raw_value: Optional[str | int | float], slot_definition: SlotDefinition, unit: Optional[str] = None
    ) -> Union[nmdc.QuantityValue, None]:
        """Construct a nmdc:QuantityValue from a raw value string"""

        # If the storage_units annotation is present on the slot and it only contains one unit (i.e.
        # not a pipe-separated list of units) then use that unit.
        if "storage_units" in slot_definition.annotations:
            storage_units = slot_definition.annotations["storage_units"].value
            if storage_units and "|" not in storage_units:
                unit = storage_units

        # If the raw_value is numeric, directly construct a QuantityValue with the inferred unit.
        if isinstance(raw_value, (int, float)):
            if unit is None:
                raise ValueError(f"While processing value for slot {slot_definition.name}, a numeric value was provided but no unit could be inferred.")
            # Constructing a Decimal directly from a float will maintain the full precision of the
            # float (i.e. numbers like 0.5 cannot be represented exactly). Converting the float to
            # a string first and then constructing the Decimal from that string will give a more
            # expected value.
            value_as_str = str(raw_value)
            return nmdc.QuantityValue(
                has_raw_value=value_as_str,
                has_numeric_value=Decimal(value_as_str),
                has_unit=unit,
                type="nmdc:QuantityValue",
            )

        return self._parse_quantity_value(raw_value, unit)

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
            type="nmdc:OntologyClass",
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
            has_raw_value=raw_value,
            term=ontology_class,
            type="nmdc:ControlledIdentifiedTermValue",
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

        value = nmdc.ControlledTermValue(
            has_raw_value=raw_value,
            type="nmdc:ControlledTermValue",
        )
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
            has_raw_value=raw_value,
            latitude=match.group(1),
            longitude=match.group(2),
            type="nmdc:GeolocationValue",
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

    def _get_study_dois(self, metadata_submission) -> Union[List[nmdc.Doi], None]:
        """Collect and format DOIs from submission portal schema in nmdc format DOIs

        If there were no DOIs, None is returned.

        :param metadata_submission: submission portal entry
        :return: list of nmdc.DOI objects
        """
        data_dois = self._get_from(metadata_submission, ["studyForm", "dataDois"])
        award_dois = self._get_from(
            metadata_submission, ["multiOmicsForm", "awardDois"]
        )
        if data_dois and len(data_dois) > 0:
            updated_data_dois = [
                nmdc.Doi(
                    doi_category="dataset_doi",
                    doi_provider=doi["provider"],
                    doi_value=self._ensure_curie(doi["value"], default_prefix="doi"),
                    type="nmdc:Doi",
                )
                for doi in data_dois
            ]
        else:
            updated_data_dois = []

        if award_dois and len(award_dois) > 0:
            updated_award_dois = [
                nmdc.Doi(
                    doi_category="award_doi",
                    doi_provider=doi["provider"],
                    doi_value=self._ensure_curie(doi["value"], default_prefix="doi"),
                    type="nmdc:Doi",
                )
                for doi in award_dois
            ]
        else:
            updated_award_dois = []

        return_val = updated_data_dois + updated_award_dois
        if len(return_val) == 0:
            return_val = None

        return return_val

    def _get_data_objects_from_fields(
        self,
        sample_data: JSON_OBJECT,
        *,
        url_field_name: str,
        md5_checksum_field_name: str,
        nucleotide_sequencing_id: str,
        data_object_type: nmdc.FileTypeEnum,
    ) -> Tuple[List[nmdc.DataObject], nmdc.Manifest | None]:
        """Get a DataObject instances based on the URLs and MD5 checksums in the given fields.

        If the field provides multiple URLs, multiple DataObject instances will be created and a
        Manifest will be created and provided in the second return value.

        :param sample_data: sample data
        :param url_field_name: field name for the URL
        :param md5_checksum_field_name: field name for the MD5 checksum
        :param nucleotide_sequencing_id: ID for the nmdc:NucleotideSequencing object that generated the data object(s)
        :param data_object_type: FileTypeEnum representing the type of the data object
        :return: nmdc.DataObject or None
        """
        data_objects: List[nmdc.DataObject] = []
        urls = split_strip(sample_data.get(url_field_name), ";")
        if not urls:
            return data_objects, None

        md5_checksums = split_strip(sample_data.get(md5_checksum_field_name), ";")
        if md5_checksums and len(urls) != len(md5_checksums):
            raise ValueError(
                f"{url_field_name} and {md5_checksum_field_name} must have the same number of values"
            )

        data_object_ids = self._id_minter("nmdc:DataObject", len(urls))
        manifest: nmdc.Manifest | None = None
        if len(urls) > 1:
            manifest_id = self._id_minter("nmdc:Manifest", 1)[0]
            manifest = nmdc.Manifest(
                id=manifest_id,
                manifest_category=nmdc.ManifestCategoryEnum(
                    nmdc.ManifestCategoryEnum.poolable_replicates
                ),
                type="nmdc:Manifest",
            )

        for i, url in enumerate(urls):
            data_object_id = data_object_ids[i]
            parsed_url = urlparse(url)
            possible_filename = parsed_url.path.rsplit("/", 1)[-1]
            data_object_slots = {
                "id": data_object_id,
                "name": possible_filename,
                "description": f"{data_object_type} for {nucleotide_sequencing_id}",
                "type": "nmdc:DataObject",
                "url": url,
                "md5_checksum": md5_checksums[i] if md5_checksums else None,
                "in_manifest": [manifest.id] if manifest else None,
                "data_category": nmdc.DataCategoryEnum(
                    nmdc.DataCategoryEnum.instrument_data
                ),
                "data_object_type": data_object_type,
                "was_generated_by": nucleotide_sequencing_id,
            }
            data_object_slots.update(
                self._transform_dict_for_class(sample_data, "DataObject")
            )
            data_objects.append(nmdc.DataObject(**data_object_slots))

        return data_objects, manifest

    def _parse_sample_link(self, sample_link: str) -> tuple[str, list[str]] | None:
        """Parse a sample link in the form of `ProcessingName:SampleName,..."""
        pattern = r"(" + "|".join(self._material_processing_subclass_names) + r"):(.+)"
        match = re.match(pattern, sample_link)
        if not match:
            return None
        return match.group(1), split_strip(match.group(2), ",")

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
            alternative_names=self._get_from(
                metadata_submission, ["studyForm", "alternativeNames"]
            ),
            description=self._get_from(
                metadata_submission, ["studyForm", "description"]
            ),
            funding_sources=self._get_from(
                metadata_submission, ["studyForm", "fundingSources"]
            ),
            emsl_project_identifiers=self._get_emsl_project_identifiers(
                metadata_submission
            ),
            gold_study_identifiers=self._get_gold_study_identifiers(
                metadata_submission
            ),
            has_credit_associations=self._get_has_credit_associations(
                metadata_submission
            ),
            id=nmdc_study_id,
            insdc_bioproject_identifiers=self._get_ncbi_bioproject_identifiers(
                metadata_submission
            ),
            jgi_portal_study_identifiers=self._get_jgi_study_identifiers(
                metadata_submission
            ),
            name=self._get_from(metadata_submission, ["studyForm", "studyName"]),
            notes=self._get_from(metadata_submission, ["studyForm", "notes"]),
            principal_investigator=self._get_pi(metadata_submission),
            study_category=self.study_category,
            title=self._get_from(metadata_submission, ["studyForm", "studyName"]),
            type="nmdc:Study",
            websites=self._get_from(
                metadata_submission, ["studyForm", "linkOutWebpage"]
            ),
            associated_dois=self._get_study_dois(metadata_submission),
        )

    def _transform_value_for_slot(
        self, value: Any, slot: SlotDefinition, unit: Optional[str] = None
    ):
        transformed_value = None
        if slot.range == "TextValue":
            transformed_value = nmdc.TextValue(
                has_raw_value=value,
                type="nmdc:TextValue",
            )
        elif slot.range == "QuantityValue":
            transformed_value = self._get_quantity_value(
                value,
                slot,
                unit=unit,
            )
        elif slot.range == "ControlledIdentifiedTermValue":
            transformed_value = self._get_controlled_identified_term_value(value)
        elif slot.range == "ControlledTermValue":
            transformed_value = self._get_controlled_term_value(value)
        elif slot.range == "TimestampValue":
            transformed_value = nmdc.TimestampValue(
                has_raw_value=value,
                type="nmdc:TimestampValue",
            )
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

            # This step handles cases where the submission portal/schema instructs a user to
            # provide a value in a specific unit. The unit cannot be parsed out of the raw value
            # in these cases, so we have to manually set it via UNIT_OVERRIDES. This part can
            # go away once units are encoded in the schema itself.
            # See: https://github.com/microbiomedata/nmdc-schema/issues/2517
            if class_name in UNIT_OVERRIDES:
                # If the class has unit overrides, check if the slot is in the overrides
                unit_overrides = UNIT_OVERRIDES[class_name]
                if slot_name in unit_overrides:
                    unit = unit_overrides[slot_name]

            slot_definition = self.schema_view.induced_slot(slot_name, class_name)
            if slot_definition.multivalued:
                value_list = value
                if isinstance(value, str):
                    value_list = split_strip(value, "|")
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
        :return: nmdc:Biosample
        """
        env_idx = next(
            (
                i
                for i, tab in enumerate(sample_data)
                if tab.get("env_package") is not None
            ),
            0,
        )
        biosample_key = sample_data[env_idx].get(BIOSAMPLE_UNIQUE_KEY_SLOT, "").strip()
        slots = {
            "id": nmdc_biosample_id,
            "associated_studies": [nmdc_study_id],
            "type": "nmdc:Biosample",
            "name": sample_data[env_idx].get("samp_name", "").strip(),
            "env_package": sample_data[env_idx].get("env_package"),
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
        metadata_submission_data = self.metadata_submission.get(
            "metadata_submission", {}
        )

        # Generate one Study instance based on the metadata submission, if a study_id wasn't provided
        if self.study_id:
            nmdc_study_id = self.study_id
        else:
            nmdc_study_id = self._id_minter("nmdc:Study")[0]
            database.study_set = [
                self._translate_study(metadata_submission_data, nmdc_study_id)
            ]

        # Automatically populate the `env_package` field in the sample data based on which
        # environmental data tab the sample data came from.
        sample_data = metadata_submission_data.get("sampleData", {})
        for key in sample_data.keys():
            env = key.removesuffix("_data").upper()
            try:
                package_name = EnvironmentPackage[env].value
                for sample in sample_data[key]:
                    sample["env_package"] = package_name
            except KeyError:
                # This is expected when processing rows from tabs like the JGI/EMSL tabs or external
                # sequencing data tabs.
                pass

        # Before regrouping the data by sample name, record which tab each object came from
        for tab_name in sample_data.keys():
            for tab in sample_data[tab_name]:
                tab[TAB_NAME_KEY] = tab_name

        # Reorganize the sample data by sample name and generate a unique NMDC ID for each
        sample_data_by_id = groupby(
            BIOSAMPLE_UNIQUE_KEY_SLOT,
            concat(sample_data.values()),
        )
        nmdc_biosample_ids = self._id_minter("nmdc:Biosample", len(sample_data_by_id))
        sample_data_to_nmdc_biosample_ids = dict(
            zip(sample_data_by_id.keys(), nmdc_biosample_ids)
        )

        # Translate the sample data into nmdc:Biosample objects
        database.biosample_set = []
        for sample_data_id, sample_data in sample_data_by_id.items():
            # This shouldn't happen, but just in case skip empty sample data
            if not sample_data:
                continue

            # Find the first tab that has a sample_link value and attempt to parse it
            sample_link = ""
            for tab in sample_data:
                if tab.get("sample_link"):
                    sample_link = tab.get("sample_link")
                    break
            parsed_sample_link = self._parse_sample_link(sample_link)

            # If the sample_link could be parsed according to the [ProcessName]:[InputSample,...]
            # format, then create a ProcessedSample and MaterialProcessing instance instead of a
            # Biosample instance. The input samples must be present in the submission for this to
            # work. An exception is raised if any of the referenced input samples are missing.
            if parsed_sample_link is not None:
                processing_type, processing_inputs = parsed_sample_link
                if not all(
                    input_id in sample_data_to_nmdc_biosample_ids
                    for input_id in processing_inputs
                ):
                    raise ValueError(
                        f"Could not find all input samples in sample_link '{sample_link}'"
                    )
                processed_sample_id = self._id_minter("nmdc:ProcessedSample")[0]
                database.processed_sample_set.append(
                    nmdc.ProcessedSample(
                        id=processed_sample_id,
                        type="nmdc:ProcessedSample",
                        name=sample_data[0].get(BIOSAMPLE_UNIQUE_KEY_SLOT, "").strip(),
                    )
                )

                processing_class = getattr(nmdc, processing_type)
                material_processing = processing_class(
                    id=self._id_minter(f"nmdc:{processing_type}")[0],
                    type=f"nmdc:{processing_type}",
                    has_input=[
                        sample_data_to_nmdc_biosample_ids[input_id]
                        for input_id in processing_inputs
                    ],
                    has_output=[processed_sample_id],
                )
                database.material_processing_set.append(material_processing)

            # If there was no sample_link or it doesn't follow the expected format, create a
            # Biosample instance as normal.
            else:
                biosample = self._translate_biosample(
                    sample_data,
                    nmdc_biosample_id=sample_data_to_nmdc_biosample_ids[sample_data_id],
                    nmdc_study_id=nmdc_study_id,
                )
                database.biosample_set.append(biosample)

        # This section handles the translation of information in the external sequencing tabs into
        # various NMDC objects.
        database.data_generation_set = []
        database.data_object_set = []
        database.instrument_set = []
        database.manifest_set = []
        today = datetime.now().strftime("%Y-%m-%d")
        for sample_data_id, sample_data in sample_data_by_id.items():
            for tab in sample_data:
                tab_name = tab.get(TAB_NAME_KEY)
                analyte_category = TAB_NAME_TO_ANALYTE_CATEGORY.get(tab_name)
                if not analyte_category:
                    # If the tab name cannot be mapped to an analyte category, that means we're
                    # not in an external sequencing data tabs (e.g. this is an environmental data
                    # tab or a JGI/EMSL tab). Skip this tab.
                    continue

                # Start by generating one NucleotideSequencing instance with a has_input
                # relationship to the current Biosample instance.
                nucleotide_sequencing_id = self._id_minter(
                    "nmdc:NucleotideSequencing", 1
                )[0]
                nucleotide_sequencing_slots = {
                    "id": nucleotide_sequencing_id,
                    "has_input": sample_data_to_nmdc_biosample_ids[sample_data_id],
                    "has_output": [],
                    "associated_studies": [nmdc_study_id],
                    "add_date": today,
                    "mod_date": today,
                    "analyte_category": analyte_category,
                    "type": "nmdc:NucleotideSequencing",
                }
                # If the protocol_link column was filled in, expand it into an nmdc:Protocol object
                if "protocol_link" in tab:
                    protocol_link = tab.pop("protocol_link")
                    nucleotide_sequencing_slots["protocol_link"] = nmdc.Protocol(
                        url=protocol_link,
                        type="nmdc:Protocol",
                    )
                # If model column was filled in, expand it into an nmdc:Instrument object. This is
                # done by first checking the provided instrument mapping to see if the model is
                # already present. If it is not, a new instrument object is created and added to the
                # instrument_set. Currently, we only accept sequencing data in the submission portal
                # that was generated by Illumina instruments, so the vendor is hardcoded here.
                if "model" in tab:
                    model = tab.pop("model")
                    if model not in self.illumina_instrument_mapping:
                        # If the model is not already in the mapping, create a new record for it
                        nmdc_instrument_id = self._id_minter("nmdc:Instrument", 1)[0]
                        database.instrument_set.append(
                            nmdc.Instrument(
                                id=nmdc_instrument_id,
                                vendor=nmdc.InstrumentVendorEnum(
                                    nmdc.InstrumentVendorEnum.illumina
                                ),
                                model=nmdc.InstrumentModelEnum(model),
                                type="nmdc:Instrument",
                            )
                        )
                        self.illumina_instrument_mapping[model] = nmdc_instrument_id
                    nucleotide_sequencing_slots["instrument_used"] = (
                        self.illumina_instrument_mapping[model]
                    )
                # Process the remaining columns according to the NucleotideSequencing class
                # definition
                nucleotide_sequencing_slots.update(
                    self._transform_dict_for_class(tab, "NucleotideSequencing")
                )
                nucleotide_sequencing = nmdc.NucleotideSequencing(
                    **nucleotide_sequencing_slots
                )
                database.data_generation_set.append(nucleotide_sequencing)

                # Iterate over the columns that contain URLs and MD5 checksums and translate them
                # into DataObject instances. Each of these DataObject instances will be connected
                # to the NucleotideSequencing instance via the has_output/was_generated_by
                # relationships.
                for data_url in DATA_URL_SETS:
                    data_object_type = DATA_URL_SET_AND_ANALYTE_TO_DATA_OBJECT_TYPE[
                        (data_url, str(analyte_category))
                    ]
                    data_objects, manifest = self._get_data_objects_from_fields(
                        tab,
                        url_field_name=data_url.url,
                        md5_checksum_field_name=data_url.md5_checksum,
                        nucleotide_sequencing_id=nucleotide_sequencing_id,
                        data_object_type=nmdc.FileTypeEnum(data_object_type),
                    )
                    if manifest:
                        database.manifest_set.append(manifest)
                    for data_object in data_objects:
                        nucleotide_sequencing.has_output.append(data_object.id)
                        database.data_object_set.append(data_object)

        # This is the older way of handling attaching NucleotideSequencing and DataObject instances
        # to the Biosample instances. This should now mainly be handled by the external sequencing
        # data tabs in the submission portal. This code is being left in place for now in case it is
        # needed in the future.
        if self.nucleotide_sequencing_mapping:
            # If there is data from an NucleotideSequencing mapping file, process it now. This part
            # assumes that there is a column in that file with the header __biosample_samp_name
            # that can be used to join with the sample data from the submission portal. The
            # biosample identified by that `samp_name` will be referenced in the `has_input`
            # slot of the NucleotideSequencing object. If a DataObject mapping file was also
            # provided, those objects will also be generated and referenced in the `has_output` slot
            # of the NucleotideSequencing object. By keying off of the `samp_name` slot of the
            # submission's sample data there is an implicit 1:1 relationship between Biosample
            # objects and NucleotideSequencing objects generated here.
            join_key = f"__biosample_{BIOSAMPLE_UNIQUE_KEY_SLOT}"
            database.data_generation_set = []
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

            for nucleotide_sequencing_row in self.nucleotide_sequencing_mapping:
                # For each row in the NucleotideSequencing mapping file, first grab the minted
                # Biosample id that corresponds to the sample ID from the submission
                sample_data_id = nucleotide_sequencing_row.pop(join_key)
                if (
                    not sample_data_id
                    or sample_data_id not in sample_data_to_nmdc_biosample_ids
                ):
                    logging.warning(
                        f"Unrecognized biosample {BIOSAMPLE_UNIQUE_KEY_SLOT}: {sample_data_id}"
                    )
                    continue
                nmdc_biosample_id = sample_data_to_nmdc_biosample_ids[sample_data_id]

                # Transform the raw row data according to the NucleotideSequencing class's slots,
                # and generate an instance. A few key slots do not come from the mapping file, but
                # instead are defined here.
                nucleotide_sequencing_slots = {
                    "id": self._id_minter("nmdc:NucleotideSequencing", 1)[0],
                    "has_input": [nmdc_biosample_id],
                    "has_output": [],
                    "associated_studies": [nmdc_study_id],
                    "add_date": today,
                    "mod_date": today,
                    "type": "nmdc:NucleotideSequencing",
                }
                nucleotide_sequencing_slots.update(
                    self._transform_dict_for_class(
                        nucleotide_sequencing_row, "NucleotideSequencing"
                    )
                )
                nucleotide_sequencing = nmdc.NucleotideSequencing(
                    **nucleotide_sequencing_slots
                )

                for data_object_row in data_objects_by_sample_data_id.get(
                    sample_data_id, []
                ):
                    # For each row in the DataObject mapping file that corresponds to the sample ID,
                    # transform the raw row data according to the DataObject class's slots, generate
                    # an instance, and connect that instance's minted ID to the NucleotideSequencing
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

                    nucleotide_sequencing.has_output.append(data_object_id)

                    database.data_object_set.append(data_object)

                database.data_generation_set.append(nucleotide_sequencing)

        return database
