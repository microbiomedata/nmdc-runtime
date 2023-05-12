import logging
import re
from typing import Any, List, Optional, Union
from nmdc_runtime.site.translation.translator import JSON_OBJECT, Translator
from nmdc_schema import nmdc
from toolz import get_in, groupby, concat
from importlib import resources
from linkml_runtime import SchemaView
from linkml_runtime.linkml_model import SlotDefinition
from functools import lru_cache


@lru_cache
def _get_schema_view():
    """Return a SchemaView instance representing the NMDC schema"""
    return SchemaView(str(resources.path('nmdc_schema', 'nmdc_materialized_patterns.yaml')))


class SubmissionPortalTranslator(Translator):
    """A Translator subclass for handling submission portal entries

    This translator is constructed with a metadata_submission object from the
    submission portal. Since the submission schema is built by importing slots
    from the nmdc:Biosample class this translator largely works by introspecting
    the nmdc:Biosample class (via a SchemaView instance)
    """
    def __init__(self, metadata_submission: JSON_OBJECT = {}, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.metadata_submission = metadata_submission
        self.schema_view: SchemaView = _get_schema_view()

    def _get_pi(self, metadata_submission: JSON_OBJECT) -> Union[nmdc.PersonValue, None]:
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
        )

    def _get_doi(self, metadata_submission: JSON_OBJECT) -> Union[nmdc.AttributeValue, None]:
        """Construct an nmdc:AttributeValue object using information from the context form data

        :param metadata_submission: submission portal entry
        :return: nmdc:AttributeValue
        """
        doi = get_in(['contextForm', 'datasetDoi'], metadata_submission)
        if not doi:
            return None
        
        return nmdc.AttributeValue(has_raw_value=doi)

    def _get_has_credit_associations(self, metadata_submission: JSON_OBJECT) -> Union[List[nmdc.CreditAssociation], None]:
        """Construct a list of nmdc:CreditAssociation from the study form data

        :param metadata_submission: submission portal entry
        :return: nmdc.CreditAssociation list
        """
        contributors = get_in(['studyForm', 'contributors'], metadata_submission)
        if not contributors:
            return None
        
        return [
            nmdc.CreditAssociation(
                applies_to_person=nmdc.PersonValue(
                    name=contributor.get("name"),
                    orcid=contributor.get("orcid"),
                ),
                applied_roles=contributor.get("roles")
            )
            for contributor in contributors
        ]
    
    def _get_gold_study_identifiers(self, metadata_submission: JSON_OBJECT) -> Union[List[str], None]:
        """Construct a GOLD CURIE from the multiomics from data

        :param metadata_submission: submission portal entry
        :return: GOLD CURIE
        """
        gold_study_id = get_in(['multiOmicsForm', 'GOLDStudyId'], metadata_submission)
        if not gold_study_id:
            return None
        
        return [self._get_curie('GOLD', gold_study_id)]
    
    def _get_quantity_value(self, raw_value: Optional[str], unit: Optional[str] = None) -> Union[nmdc.QuantityValue, None]:
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
        
        match = re.fullmatch('([+-]?(?=\.\d|\d)(?:\d+)?(?:\.?\d*)(?:[eE][+-]?\d+)?)(?: *- *([+-]?(?=\.\d|\d)(?:\d+)?(?:\.?\d*)(?:[eE][+-]?\d+)?))?(?: *(\S+))?', raw_value)
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
    

    def _get_ontology_class(self, raw_value: Optional[str]) -> Union[nmdc.OntologyClass, None]:
        """Construct a nmdc:OntologyClass from a raw value string

        The regexp pattern matches values of the format "name [identifier]". If this pattern is
        not matched None is returned.

        :param raw_value: string to parse
        :return: nmdc.OntologyClass
        """
        match = re.fullmatch('_*([^\[]+)(?:\[([^\]]+)\])', raw_value)
        if not match or not match.group(2):
            logging.warning(f'Could not infer OntologyClass id from value "{raw_value}"')
            return None
        
        return nmdc.OntologyClass(
            name=match.group(1).strip(),
            id=match.group(2).strip(),
        )
    

    def _get_controlled_identified_term_value(self, raw_value: Optional[str]) -> Union[nmdc.ControlledIdentifiedTermValue, None]:
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
            term=ontology_class
        )
        

    def _get_controlled_term_value(self, raw_value: Optional[str]) -> Union[nmdc.ControlledTermValue, None]:
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
    
    
    def _get_geolocation_value(self, raw_value: Optional[str]) -> Union[nmdc.GeolocationValue, None]:
        """Construct a nmdc.GeolocationValue from a raw string value

        The regexp pattern matches a latitude and a longitude value separated by a space or comma. The
        latitude and longitude values should be in decimal degrees. If the pattern is not matched, None
        is returned.

        :param raw_value: string to parse
        :return: nmdc.GeolocationValue
        """
        if raw_value is None:
            return None
        
        match = re.fullmatch('([-+]?(?:[1-8]?\d(?:\.\d+)?|90(?:\.0+)?))[\s,]+([-+]?(?:180(?:\.0+)?|(?:(?:1[0-7]\d)|(?:[1-9]?\d))(?:\.\d+)?))', raw_value)
        if match is None:
            return None
        
        return nmdc.GeolocationValue(
            has_raw_value=raw_value,
            latitude=match.group(1),
            longitude=match.group(2)
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
            alternative_identifiers=self._get_from(metadata_submission, ['multiOmicsForm', 'JGIStudyId']),
            alternative_names=self._get_from(metadata_submission, ['multiOmicsForm', 'alternativeNames']),
            description=self._get_from(metadata_submission, ['studyForm', 'description']),
            doi=self._get_doi(metadata_submission),
            emsl_proposal_identifier=self._get_from(metadata_submission, ['multiOmicsForm', 'studyNumber']),
            gold_study_identifiers=self._get_gold_study_identifiers(metadata_submission),
            has_credit_associations=self._get_has_credit_associations(metadata_submission),
            id=nmdc_study_id,
            insdc_bioproject_identifiers=self._get_from(metadata_submission, ['multiOmicsForm', 'NCBIBioProjectId']),
            name=self._get_from(metadata_submission, ['studyForm', 'studyName']),
            notes=self._get_from(metadata_submission, ['studyForm', 'notes']),
            principal_investigator=self._get_pi(metadata_submission),
            title=self._get_from(metadata_submission, ['studyForm', 'studyName']),
            websites=self._get_from(metadata_submission, ['studyForm', 'linkOutWebpage']),
        )
    
    def _transform_value_for_slot(self, value: Any, slot: SlotDefinition):
        transformed_value = None
        if slot.range == 'TextValue':
            transformed_value = nmdc.TextValue(has_raw_value=value)
        elif slot.range == 'QuantityValue':
            transformed_value = self._get_quantity_value(value)
        elif slot.range == 'ControlledIdentifiedTermValue':
            transformed_value = self._get_controlled_identified_term_value(value)
        elif slot.range == 'ControlledTermValue':
            transformed_value = self._get_controlled_term_value(value)
        elif slot.range == 'TimestampValue':
            transformed_value = nmdc.TimestampValue(has_raw_value=value)
        elif slot.range == 'GeolocationValue':
            transformed_value = self._get_geolocation_value(value)
        elif slot.range == 'float':
            transformed_value = self._get_float(value)
        elif slot.range == 'string':
            transformed_value = str(value).strip()
        else:
            transformed_value = value
        
        return transformed_value
    
    def _translate_biosample(self, sample_data: List[JSON_OBJECT], nmdc_biosample_id: str, nmdc_study_id: str) -> nmdc.Biosample:
        """Translate sample data from portal submission into an `nmdc:Biosample` object.

        sample_data is a list of objects where each object represents one row from a tab in
        the submission portal. Each of the objects represent information about the same 
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
        slots = {
            'id': nmdc_biosample_id,
            'part_of': nmdc_study_id
        }
        biosample_slot_names = self.schema_view.class_slots('Biosample')
        for tab in sample_data:
            for column, value in tab.items():
                if column not in biosample_slot_names:
                    logging.warning(f'No slot {column} on nmdc:Biosample')
                    continue
                slot = self.schema_view.induced_slot(column, 'Biosample')

                transformed_value = None
                if slot.multivalued:
                    transformed_value = [self._transform_value_for_slot(item, slot) for item in value]
                else:
                    transformed_value = self._transform_value_for_slot(value, slot)

                slots[column] = transformed_value

        return nmdc.Biosample(**slots)
    

    def get_database(self) -> nmdc.Database:
        """Translate the submission portal entry to an nmdc:Database

        This method translates the submission portal entry into one nmdc:Study and a set
        of nmdc:Biosample objects. THese are wrapped into an nmdc:Database. NMDC identifiers 
        are minted for each new object.

        :return: nmdc:Database object
        """
        database = nmdc.Database()

        nmdc_study_id = self._id_minter("nmdc:Study")[0]

        metadata_submission_data = self.metadata_submission.get('metadata_submission', {})
        database.study_set = [self._translate_study(metadata_submission_data, nmdc_study_id)]

        sample_data = metadata_submission_data.get('sampleData', {})
        sample_data_by_id = groupby('source_mat_id', concat(sample_data.values()))
        nmdc_biosample_ids = self._id_minter("nmdc:Biosample", len(sample_data_by_id))
        sample_data_to_nmdc_biosample_ids = dict(zip(sample_data_by_id.keys(), nmdc_biosample_ids))

        database.biosample_set = [self._translate_biosample(
            sample_data, 
            sample_data_to_nmdc_biosample_ids[sample_data_id],
            nmdc_study_id
        ) for sample_data_id, sample_data in sample_data_by_id.items()]

        return database
