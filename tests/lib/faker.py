from typing import List

class Faker:
    r"""
    A class whose methods can be used to generate fake data for testing.

    Note: Currently, the methods of this class generate _dictionaries_
          representing instances of schema classes, as opposed to generating
          the instances, themselves. Also, so far, we've only implemented
          methods for a few specific schema classes (implemented on demand).
          
          In the future, instead of only having methods that return
          dictionaries, we may have methods that return the schema class
          instances, themselves. As a reminder, `nmdc_schema.nmdc` exports
          Pydantic classes for schema classes; and Pydantic classes have a
          `model_dump()` method that can be used to serialize an instance
          into a dictionary.
    """
    
    def __init__(self):
        pass

    @staticmethod
    def generate_studies(quantity: int, **overrides) -> List[dict]:
        """
        Generates the specified number of schema-compliant `study_set` documents.

        :param quantity: Number of documents to create
        :param overrides: Fields, if any, to add or override in each document
        :return: The generated documents

        >>> f = Faker()
        >>> studies = f.generate_studies(2)
        >>> len(studies)
        2
        >>> studies[0]
        {'id': 'nmdc:sty-00-000001', 'study_category': 'research_study', 'type': 'nmdc:Study'}
        >>> studies[1]
        {'id': 'nmdc:sty-00-000002', 'study_category': 'research_study', 'type': 'nmdc:Study'}

        # Test: Overriding a default value and adding an optional field.
        >>> f.generate_studies(1, study_category='consortium', title='My Study')[0]
        {'id': 'nmdc:sty-00-000001', 'study_category': 'consortium', 'type': 'nmdc:Study', 'title': 'My Study'}
        """

        return [
            {
                "id": f"nmdc:sty-00-{n:06}",
                "study_category": "research_study",
                "type": "nmdc:Study",
                **overrides,
            }
            for n in range(1, quantity + 1)
        ]

    @staticmethod
    def generate_biosamples(quantity: int, associated_studies: List[str], **overrides) -> List[dict]:
        """
        Generates the specified number of schema-compliant `biosample_set` documents.

        :param quantity: Number of documents to create
        :param associated_studies: IDs of studies associated with each biosample
        :param overrides: Fields, if any, to add or override in each document
        :return: The generated documents

        >>> f = Faker()
        >>> biosamples = f.generate_biosamples(1, associated_studies=['nmdc:sty-00-000001'])
        >>> len(biosamples)
        1
        >>> biosamples[0]['id']
        'nmdc:bsm-00-000001'
        >>> biosamples[0]['associated_studies']
        ['nmdc:sty-00-000001']
        """

        return [
            {
                "id": f"nmdc:bsm-00-{n:06}",
                "env_broad_scale": {
                    "has_raw_value": "ENVO_00000446",
                    "term": {
                        "id": "ENVO:00000446",
                        "name": "terrestrial biome",
                        "type": "nmdc:OntologyClass",
                    },
                    "type": "nmdc:ControlledIdentifiedTermValue",
                },
                "env_local_scale": {
                    "has_raw_value": "ENVO_00005801",
                    "term": {
                        "id": "ENVO:00005801",
                        "name": "rhizosphere",
                        "type": "nmdc:OntologyClass",
                    },
                    "type": "nmdc:ControlledIdentifiedTermValue",
                },
                "env_medium": {
                    "has_raw_value": "ENVO_00001998",
                    "term": {
                        "id": "ENVO:00001998",
                        "name": "soil",
                        "type": "nmdc:OntologyClass",
                    },
                    "type": "nmdc:ControlledIdentifiedTermValue",
                },
                "type": "nmdc:Biosample",
                "associated_studies": associated_studies,
                **overrides,
            }
            for n in range(1, quantity + 1)
        ]
