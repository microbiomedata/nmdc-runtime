from typing import List

class Faker:
    r"""
    A class whose methods can be used to generate fake data for testing.
    
    The class keeps track of how many dictionaries representing instances of
    each schema class it has generated. It uses that information to generate
    unique `id` values for subsequently-generated documents.

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
        self.counts = {}

    def make_unique_id(self, prefix: str) -> str:
        """
        Generates an `id` string for a document. The `id` will be unique among all the `id`s generated for that same prefix.

        Note: One can "defeat" the uniqueness by using prefixes that are substrings of one another. For example,
              if one prefix is `nmdc:sty-00-` and another is `nmdc:sty-00-1`, the 1,111,111th `id` generated for the
              former prefix (i.e. `nmdc:sty-00-1111111`) will be the same as the 111,111th `id` generated for the
              latter prefix (i.e. `nmdc:sty-00-1111111`). We have accepted this limitation in exchange for keeping
              the algorithm simple.
        
        :param prefix: The prefix to use for the `id` value
        :return: The generated `id` value

        >>> f1 = Faker()
        >>> f1.make_unique_id('nmdc:sty-00-')
        'nmdc:sty-00-000001'
        >>> f1.make_unique_id('nmdc:sty-00-')
        'nmdc:sty-00-000002'
        >>> f1.make_unique_id('nmdc:bsm-00-')  # same instance, different prefix
        'nmdc:bsm-00-000001'
        >>> f2 = Faker()
        >>> f2.make_unique_id('nmdc:sty-00-')  # different instance
        'nmdc:sty-00-000001'
        """

        # Increment the counter for this prefix, initializing the counter if necessary.
        if prefix not in self.counts:
            self.counts[prefix] = 0
        self.counts[prefix] += 1

        # Return an `id` based upon that count.
        return f"{prefix}{self.counts[prefix]:06}"


    def generate_studies(self, quantity: int, **overrides) -> List[dict]:
        """
        Generates the specified number of schema-compliant `study_set` documents.
        The documents comply with schema v11.5.1.

        Reference: https://microbiomedata.github.io/nmdc-schema/Study/

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
        {'id': 'nmdc:sty-00-000003', 'study_category': 'consortium', 'type': 'nmdc:Study', 'title': 'My Study'}
        """

        return [
            {
                "id": self.make_unique_id(f"nmdc:sty-00-"),
                "study_category": "research_study",
                "type": "nmdc:Study",
                **overrides,
            }
            for n in range(1, quantity + 1)
        ]

    def generate_biosamples(self, quantity: int, associated_studies: List[str], **overrides) -> List[dict]:
        """
        Generates the specified number of schema-compliant `biosample_set` documents.
        The documents comply with schema v11.5.1.

        Reference: https://microbiomedata.github.io/nmdc-schema/Biosample/

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
                "id": self.make_unique_id(f"nmdc:bsm-00-"),
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

    def generate_metagenome_annotations(self, quantity: int, was_informed_by: str, has_input: List[str], **overrides) -> List[dict]:
        """
        Generates the specified number of documents representing `MetagenomeAnnotation` instances,
        which can be stored in the `workflow_execution_set` collection.
        The documents comply with schema v11.5.1.

        Reference: https://microbiomedata.github.io/nmdc-schema/MetagenomeAnnotation/

        :param quantity: Number of documents to create
        :param was_informed_by: The `id` of a `DataGeneration` instance
        :param has_input: The `id`s of one or more `NamedThing` instances
        :param overrides: Fields, if any, to add or override in each document
        :return: The generated documents

        >>> f = Faker()
        >>> metagenome_annotations = f.generate_metagenome_annotations(1, was_informed_by='nmdc:dgns-00-000001', has_input=['nmdc:bsm-00-000001'])
        >>> len(metagenome_annotations)
        1
        >>> metagenome_annotations[0]['id']
        'nmdc:wfmgan-00-000001'
        >>> metagenome_annotations[0]['was_informed_by']
        'nmdc:dgns-00-000001'
        >>> metagenome_annotations[0]['has_input'][0]
        'nmdc:bsm-00-000001'
        >>> metagenome_annotations[0]['type']
        'nmdc:MetagenomeAnnotation'
        """

        return [
            {
                "id": self.make_unique_id(f"nmdc:wfmgan-00-"),
                "type": "nmdc:MetagenomeAnnotation",
                "execution_resource": "JGI",
                "git_url": "https://www.example.com",
                "started_at_time": "2000-01-01 12:00:00",
                "was_informed_by": was_informed_by,
                "has_input": has_input,
                **overrides,
            }
            for n in range(1, quantity + 1)
        ]

    def generate_nucleotide_sequencings(self, quantity: int, associated_studies: List[str], has_input: List[str], **overrides) -> List[dict]:
        """
        Generates the specified number of documents representing `NucleotideSequencing` instances,
        which can be stored in the `data_generation_set` collection.
        The documents comply with schema v11.5.1.

        Reference: https://microbiomedata.github.io/nmdc-schema/NucleotideSequencing/

        :param quantity: Number of documents to create
        :param associated_studies: The `id`s of one or more `Study` instances
        :param has_input: The `id`s of one or more `Sample` instances
        :param overrides: Fields, if any, to add or override in each document
        :return: The generated documents

        >>> f = Faker()
        >>> nucleotide_sequencings = f.generate_nucleotide_sequencings(1, associated_studies=['nmdc:sty-00-000001'], has_input=['nmdc:bsm-00-000001'])
        >>> len(nucleotide_sequencings)
        1
        >>> nucleotide_sequencings[0]['id']
        'nmdc:dgns-00-000001'
        >>> nucleotide_sequencings[0]['associated_studies'][0]
        'nmdc:sty-00-000001'
        >>> nucleotide_sequencings[0]['has_input'][0]
        'nmdc:bsm-00-000001'
        >>> nucleotide_sequencings[0]['type']
        'nmdc:NucleotideSequencing'
        """

        return [
            {
                "id": self.make_unique_id(f"nmdc:dgns-00-"),
                "type": "nmdc:NucleotideSequencing",
                "analyte_category": "arbitrary_string",
                "associated_studies": associated_studies,
                "has_input": has_input,
                **overrides,
            }
            for n in range(1, quantity + 1)
        ]

    def generate_data_objects(self, quantity: int, **overrides) -> List[dict]:
        """
        Generates the specified number of documents representing `DataObject` instances,
        which can be stored in the `data_object_set` collection.
        The documents comply with schema v11.5.1.

        Reference: https://microbiomedata.github.io/nmdc-schema/DataObject/

        :param quantity: Number of documents to create
        :param associated_studies: The `id`s of one or more `Study` instances
        :param overrides: Fields, if any, to add or override in each document
        :return: The generated documents

        >>> f = Faker()
        >>> data_objects = f.generate_data_objects(1, name='my_data_object')
        >>> len(data_objects)
        1
        >>> data_objects[0]['id']
        'nmdc:dobj-00-000001'
        >>> data_objects[0]['type']
        'nmdc:DataObject'
        >>> data_objects[0]['name']
        'my_data_object'
        """

        return [
            {
                "id": self.make_unique_id(f"nmdc:dobj-00-"),
                "type": "nmdc:DataObject",
                "name": "arbitrary_string",
                "description": "arbitrary_string",
                **overrides,
            }
            for n in range(1, quantity + 1)
        ]
