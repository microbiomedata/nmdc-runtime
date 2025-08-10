from typing import List

from linkml_runtime.dumpers import json_dumper
from nmdc_schema.nmdc import (
    Biosample,
    ControlledIdentifiedTermValue,
    DataCategoryEnum,
    DataObject,
    ExecutionResourceEnum,
    FileTypeEnum,
    MetagenomeAnnotation,
    MetagenomeAssembly,
    NucleotideSequencing,
    NucleotideSequencingEnum,
    OntologyClass,
    ProcessingInstitutionEnum,
    ReadQcAnalysis,
    Study,
    StudyCategoryEnum,
)

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
          instances, themselves.

    TODO: Try to leverage the type hints defined on the Pydantic models, for
          the signatures of the methods of this class (rather than hard-coding
          type hints in those methods' signatures).
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
        The documents comply with schema v11.8.0.

        Reference: https://microbiomedata.github.io/nmdc-schema/Study/

        :param quantity: Number of documents to create
        :param overrides: Fields, if any, to add or override in each document
        :return: The generated documents

        >>> f = Faker()
        >>> studies = f.generate_studies(2)
        >>> len(studies)
        2
        >>> studies[0]
        {'id': 'nmdc:sty-00-000001', 'type': 'nmdc:Study', 'study_category': 'research_study'}
        >>> studies[1]
        {'id': 'nmdc:sty-00-000002', 'type': 'nmdc:Study', 'study_category': 'research_study'}

        # Test: Overriding a default value and adding an optional field.
        >>> f.generate_studies(1, study_category='consortium', title='My Study')[0]
        {'id': 'nmdc:sty-00-000003', 'type': 'nmdc:Study', 'study_category': 'consortium', 'title': 'My Study'}

        # Test: Generating a study with a referencing field (referential integrity is not checked).
        >>> f.generate_studies(1, part_of=['nmdc:sty-00-no_such_study'])[0]
        {'id': 'nmdc:sty-00-000004', 'type': 'nmdc:Study', 'study_category': 'research_study', 'part_of': ['nmdc:sty-00-no_such_study']}
        
        # Test: Generating an invalid document.
        >>> f.generate_studies(1, study_category='no_such_category')
        Traceback (most recent call last):
            ...
        ValueError: Unknown StudyCategoryEnum enumeration code: no_such_category
        """

        documents = []
        for i in range(quantity):
            # Apply any overrides passed in.
            params = {
                "id": self.make_unique_id("nmdc:sty-00-"),
                "study_category": StudyCategoryEnum.research_study.text,
                "type": Study.class_class_curie,
                **overrides,
            }

            # Validate the parameters by attempting to instantiate a `Study`.
            #
            # Note: Here are some caveats I've noticed so far as I've experimented with this validation:
            #       1. Pydantic doesn't complain when I populate an `Optional[str]` field with
            #          a number, a list, or a dict.
            #
            instance = Study(**params)

            # Dump the instance to a `dict` (technically, to a `JsonObj`).
            document = json_dumper.to_dict(instance)
            documents.append(document)

        return documents


    def generate_biosamples(self, quantity: int, associated_studies: List[str], **overrides) -> List[dict]:
        """
        Generates the specified number of schema-compliant `biosample_set` documents.
        The documents comply with schema v11.8.0.

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

        # Test: Omitting a required parameter.
        >>> f.generate_biosamples(1)
        Traceback (most recent call last):
            ...
        TypeError: Faker.generate_biosamples() missing 1 required positional argument: 'associated_studies'

        # Test: Generating an invalid document.
        >>> f.generate_biosamples(1, associated_studies=['nmdc:sty-00-000001'], depth=123)
        Traceback (most recent call last):
            ...
        TypeError: nmdc_schema.nmdc.QuantityValue() argument after ** must be a mapping, not int
        """

        documents = []
        for i in range(quantity):
            # Apply any overrides passed in.
            params = {
                "id": self.make_unique_id("nmdc:bsm-00-"),
                "type": Biosample.class_class_curie,
                "associated_studies": associated_studies,
                "env_broad_scale": {
                    "has_raw_value": "ENVO_00000446",
                    "term": {
                        "id": "ENVO:00000446",
                        "name": "terrestrial biome",
                        "type": OntologyClass.class_class_curie,
                    },
                    "type": ControlledIdentifiedTermValue.class_class_curie,
                },
                "env_local_scale": {
                    "has_raw_value": "ENVO_00005801",
                    "term": {
                        "id": "ENVO:00005801",
                        "name": "rhizosphere",
                        "type": OntologyClass.class_class_curie,
                    },
                    "type": ControlledIdentifiedTermValue.class_class_curie,
                },
                "env_medium": {
                    "has_raw_value": "ENVO_00001998",
                    "term": {
                        "id": "ENVO:00001998",
                        "name": "soil",
                        "type": OntologyClass.class_class_curie,
                    },
                    "type": ControlledIdentifiedTermValue.class_class_curie,
                },
                **overrides,
            }

            # Validate the parameters by attempting to instantiate a `Biosample`.
            instance = Biosample(**params)
            
            # Dump the instance to a `dict` (technically, to a `JsonObj`).
            document = json_dumper.to_dict(instance)
            documents.append(document)

        return documents


    def generate_metagenome_annotations(self, quantity: int, was_informed_by: List[str], has_input: List[str], **overrides) -> List[dict]:
        """
        Generates the specified number of documents representing `MetagenomeAnnotation` instances,
        which can be stored in the `workflow_execution_set` collection.
        The documents comply with schema v11.10.0rc3.

        Reference: https://microbiomedata.github.io/nmdc-schema/MetagenomeAnnotation/

        :param quantity: Number of documents to create
        :param was_informed_by: The `id`s of `DataGeneration` instances
        :param has_input: The `id`s of one or more `NamedThing` instances
        :param overrides: Fields, if any, to add or override in each document
        :return: The generated documents

        >>> f = Faker()
        >>> metagenome_annotations = f.generate_metagenome_annotations(1, was_informed_by=['nmdc:dgns-00-000001'], has_input=['nmdc:bsm-00-000001'])
        >>> len(metagenome_annotations)
        1
        >>> metagenome_annotations[0]['id']
        'nmdc:wfmgan-00-000001.1'
        >>> metagenome_annotations[0]['was_informed_by'][0]
        'nmdc:dgns-00-000001'
        >>> metagenome_annotations[0]['has_input'][0]
        'nmdc:bsm-00-000001'
        >>> metagenome_annotations[0]['type']
        'nmdc:MetagenomeAnnotation'

        # Test: Omitting required parameters.
        >>> f.generate_metagenome_annotations(1)
        Traceback (most recent call last):
            ...
        TypeError: Faker.generate_metagenome_annotations() missing 2 required positional arguments: 'was_informed_by' and 'has_input'
        """

        documents = []
        for i in range(quantity):
            # Apply any overrides passed in.
            params = {
                "id": self.make_unique_id(f"nmdc:wfmgan-00-") + ".1",
                "type": MetagenomeAnnotation.class_class_curie,
                "execution_resource": getattr(ExecutionResourceEnum, "NERSC-Perlmutter").text,
                "git_url": "https://www.example.com",
                "processing_institution": ProcessingInstitutionEnum.NMDC.text,
                "started_at_time": "2000-01-01 12:00:00",
                "was_informed_by": was_informed_by,
                "has_input": has_input,
                **overrides,
            }

            # Validate the parameters by attempting to instantiate a `MetagenomeAnnotation`.
            instance = MetagenomeAnnotation(**params)
            
            # Dump the instance to a `dict` (technically, to a `JsonObj`).
            document = json_dumper.to_dict(instance)
            documents.append(document)

        return documents


    def generate_nucleotide_sequencings(self, quantity: int, associated_studies: List[str], has_input: List[str], **overrides) -> List[dict]:
        """
        Generates the specified number of documents representing `NucleotideSequencing` instances,
        which can be stored in the `data_generation_set` collection.
        The documents comply with schema v11.8.0.

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

        # Test: Omitting required parameters.
        >>> f.generate_nucleotide_sequencings(1)
        Traceback (most recent call last):
            ...
        TypeError: Faker.generate_nucleotide_sequencings() missing 2 required positional arguments: 'associated_studies' and 'has_input'

        # Test: Generating an invalid document.
        >>> f.generate_nucleotide_sequencings(1, associated_studies=['nmdc:sty-00-000001'], has_input=['nmdc:bsm-00-000001'], analyte_category='no_such_category')
        Traceback (most recent call last):
            ...
        ValueError: Unknown NucleotideSequencingEnum enumeration code: no_such_category
        """

        documents = []
        for i in range(quantity):
            # Apply any overrides passed in.
            params = {
                "id": self.make_unique_id(f"nmdc:dgns-00-"),
                "type": NucleotideSequencing.class_class_curie,
                "analyte_category": NucleotideSequencingEnum.metagenome.text,
                "associated_studies": associated_studies,
                "has_input": has_input,
                **overrides,
            }

            # Validate the parameters by attempting to instantiate a `NucleotideSequencing`.
            instance = NucleotideSequencing(**params)
            
            # Dump the instance to a `dict` (technically, to a `JsonObj`).
            document = json_dumper.to_dict(instance)
            documents.append(document)

        return documents

    def generate_data_objects(self, quantity: int, **overrides) -> List[dict]:
        """
        Generates the specified number of documents representing `DataObject` instances,
        which can be stored in the `data_object_set` collection.
        The documents comply with schema v11.8.0.

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
        >>> 'data_category' in data_objects[0]
        True
        >>> 'data_object_type' in data_objects[0]
        True

        # Test: Generating an invalid document.
        >>> f.generate_data_objects(1, data_category='no_such_category')
        Traceback (most recent call last):
            ...
        ValueError: Unknown DataCategoryEnum enumeration code: no_such_category
        """

        documents = []
        for i in range(quantity):
            # Apply any overrides passed in.
            params = {
                "id": self.make_unique_id(f"nmdc:dobj-00-"),
                "type": DataObject.class_class_curie,
                "name": "arbitrary_string",
                "description": "arbitrary_string",
                "data_category": DataCategoryEnum.processed_data.text,
                "data_object_type": FileTypeEnum["Protein Report"].text,
                **overrides,
            }

            # Validate the parameters by attempting to instantiate a `DataObject`.
            instance = DataObject(**params)
            
            # Dump the instance to a `dict` (technically, to a `JsonObj`).
            document = json_dumper.to_dict(instance)
            documents.append(document)

        return documents

    def generate_workflow_executions(self, quantity: int, workflow_type: str, was_informed_by: List[str], has_input: List[str], **overrides) -> List[dict]:
        """
        Generates the specified number of documents representing generic workflow execution instances,
        which can be stored in the `workflow_execution_set` collection.
        The documents comply with schema v11.10.0rc3.

        :param quantity: Number of documents to create
        :param workflow_type: The type of workflow (e.g., 'nmdc:MetagenomeAnnotation', 'nmdc:MetagenomeAssembly')
        :param was_informed_by: The `id`s of `DataGeneration` instances
        :param has_input: The `id`s of one or more `NamedThing` instances
        :param overrides: Fields, if any, to add or override in each document
        :return: The generated documents

        >>> f = Faker()
        >>> workflow_executions = f.generate_workflow_executions(1, 'nmdc:MetagenomeAnnotation', was_informed_by=['nmdc:dgns-00-000001'], has_input=['nmdc:dobj-00-000001'], has_output=['nmdc:dobj-00-000002'])
        >>> len(workflow_executions)
        1
        >>> workflow_executions[0]['type']
        'nmdc:MetagenomeAnnotation'
        >>> workflow_executions[0]['was_informed_by'][0]
        'nmdc:dgns-00-000001'
        >>> workflow_executions[0]['has_input'][0]
        'nmdc:dobj-00-000001'

        # Demonstrate that `has_output` will be set if provided.
        >>> workflow_executions = f.generate_workflow_executions(1, 'nmdc:MetagenomeAnnotation', was_informed_by=['nmdc:dgns-00-000001'], has_input=['nmdc:dobj-00-000001'], has_output=['nmdc:dobj-00-000002'])
        >>> 'has_output' in workflow_executions[0]
        True
        >>> workflow_executions[0]['has_output'][0]
        'nmdc:dobj-00-000002'

        # Demonstrate that unsupported workflow types raise an error.
        >>> f.generate_workflow_executions(1, 'nmdc:NoSuchWorkflowType', was_informed_by=['nmdc:dgns-00-000001'], has_input=['nmdc:dobj-00-000001'])
        Traceback (most recent call last):
            ...
        ValueError: Unsupported workflow type: 'nmdc:NoSuchWorkflowType'
        """

        documents = []
        for i in range(quantity):
            # Generate appropriate ID prefix based on workflow type
            # TODO: Add support for other workflow types as needed
            if workflow_type == MetagenomeAnnotation.class_class_curie:
                id_prefix = "nmdc:wfmgan-00-"
            elif workflow_type == MetagenomeAssembly.class_class_curie:
                id_prefix = "nmdc:wfmgas-00-"
            elif workflow_type == ReadQcAnalysis.class_class_curie:
                id_prefix = "nmdc:wfrqc-00-"
            else:
                # Generic workflow execution ID
                raise ValueError(f"Unsupported workflow type: '{workflow_type}'")
            
            # Apply any overrides passed in.
            params = {
                "id": self.make_unique_id(id_prefix) + ".1",
                "type": workflow_type,
                "execution_resource": getattr(ExecutionResourceEnum, "NERSC-Perlmutter").text,
                "git_url": "https://www.example.com",
                "processing_institution": ProcessingInstitutionEnum.NMDC.text,
                "started_at_time": "2000-01-01 12:00:00",
                "was_informed_by": was_informed_by,
                "has_input": has_input,
                **overrides,
            }

            # Validate the parameters by attempting to instantiate the relevant subclass of `WorkflowExecution`.
            wfe_type_to_schema_class_map = {
                MetagenomeAnnotation.class_class_curie: MetagenomeAnnotation,
                MetagenomeAssembly.class_class_curie: MetagenomeAssembly,
                ReadQcAnalysis.class_class_curie: ReadQcAnalysis,
            }
            schema_class = wfe_type_to_schema_class_map[workflow_type]
            instance = schema_class(**params)
            
            # Dump the instance to a `dict` (technically, to a `JsonObj`).
            document = json_dumper.to_dict(instance)
            documents.append(document)

        return documents
