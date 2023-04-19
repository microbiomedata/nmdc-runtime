from typing import List, Union
from nmdc_runtime.site.translation.translator import JSON_OBJECT, Translator
from nmdc_schema import nmdc
from toolz import get_in

class SubmissionPortalTranslator(Translator):
    def __init__(self, metadata_submission: JSON_OBJECT = {}, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.metadata_submission = metadata_submission

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
            gold_study_identifiers=get_in(['multiOmicsForm', 'GOLDStudyId'], metadata_submission),
            has_credit_associations=self._get_has_credit_associations(metadata_submission),
            id=nmdc_study_id,
            insdc_bioproject_identifiers=get_in(['multiOmicsForm', 'NCBIBioProjectId'], metadata_submission),
            name=get_in(['studyForm', 'studyName'], metadata_submission),
            notes=get_in(['studyForm', 'notes'], metadata_submission),
            principal_investigator=self._get_pi(metadata_submission),
            websites=get_in(['studyForm', 'linkOutWebpage'], metadata_submission),
        )

    def get_database(self) -> nmdc.Database:
        database = nmdc.Database()

        nmdc_study_id = self._id_minter("nmdc:Study")[0]
        database.study_set = [self._translate_study(self.metadata_submission, nmdc_study_id)]

        return database
