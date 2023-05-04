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
    return SchemaView(str(resources.path('nmdc_schema', 'nmdc_materialized_patterns.yaml')))


class SubmissionPortalTranslator(Translator):
    def __init__(self, metadata_submission: JSON_OBJECT = {}, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.metadata_submission = metadata_submission
        self.schema_view: SchemaView = _get_schema_view()

    def _get_pi(self, metadata_submission: JSON_OBJECT) -> Union[nmdc.PersonValue, None]:
        study_form = metadata_submission.get("studyForm")
        if not study_form:
            return None

        return nmdc.PersonValue(
            name=study_form.get("piName"),
            email=study_form.get("piEmail"),
            orcid=study_form.get("piOrcid"),
        )

    def _get_doi(self, metadata_submission: JSON_OBJECT) -> Union[nmdc.AttributeValue, None]:
        doi = get_in(['contextForm', 'datasetDoi'], metadata_submission)
        if not doi:
            return None
        
        return nmdc.AttributeValue(has_raw_value=doi)

    def _get_has_credit_associations(self, metadata_submission: JSON_OBJECT) -> Union[List[nmdc.CreditAssociation], None]:
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
        gold_study_id = get_in(['multiOmicsForm', 'GOLDStudyId'], metadata_submission)
        if not gold_study_id:
            return None
        
        return [self._get_curie('GOLD', gold_study_id)]
    
    def _get_quantity_value(self, raw_value: Optional[str], unit: Optional[str] = None) -> Union[nmdc.QuantityValue, None]:
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
        match = re.fullmatch('_*([^\[]+)(?:\[([^\]]+)\])', raw_value)
        if not match or not match.group(2):
            logging.warning(f'Could not infer OntologyClass id from value "{raw_value}"')
            return None
        
        return nmdc.OntologyClass(
            name=match.group(1).strip(),
            id=match.group(2).strip(),
        )
    

    def _get_controlled_identified_term_value(self, raw_value: Optional[str]) -> Union[nmdc.ControlledIdentifiedTermValue, None]:
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
        if not raw_value:
            return None
        
        value = nmdc.ControlledTermValue(has_raw_value=raw_value)
        ontology_class = self._get_ontology_class(raw_value)
        if ontology_class is not None:
            value.term = ontology_class

        return value
    
    
    def _get_geolocation_value(self, raw_value: Optional[str]) -> Union[nmdc.GeolocationValue, None]:
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
        try:
            return float(raw_value)
        except (ValueError, TypeError):
            return None


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
            alternative_identifiers=get_in(['multiOmicsForm', 'JGIStudyId'], metadata_submission),
            alternative_names=get_in(['multiOmicsForm', 'alternativeNames'], metadata_submission),
            description=get_in(['studyForm', 'description'], metadata_submission),
            doi=self._get_doi(metadata_submission),
            emsl_proposal_identifier=get_in(['multiOmicsForm', 'studyNumber'], metadata_submission),
            gold_study_identifiers=self._get_gold_study_identifiers(metadata_submission),
            has_credit_associations=self._get_has_credit_associations(metadata_submission),
            id=nmdc_study_id,
            insdc_bioproject_identifiers=get_in(['multiOmicsForm', 'NCBIBioProjectId'], metadata_submission) or None,
            name=get_in(['studyForm', 'studyName'], metadata_submission),
            notes=get_in(['studyForm', 'notes'], metadata_submission),
            principal_investigator=self._get_pi(metadata_submission),
            title=get_in(['studyForm', 'studyName'], metadata_submission),
            websites=get_in(['studyForm', 'linkOutWebpage'], metadata_submission),
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
        else:
            transformed_value = value
        
        return transformed_value
    
    def _translate_biosample(self, sample_data: List[JSON_OBJECT], nmdc_biosample_id: str, nmdc_study_id: str) -> nmdc.Biosample:
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
