import collections
import re
from typing import List, Tuple, Union
from nmdc_schema import nmdc
import pandas as pd

from nmdc_runtime.site.translation.translator import JSON_OBJECT, Translator

# Dictionary of sequencing strategies from GOLD that we are filtering on
# based on the kind of samples that are required for NMDC
SEQUENCING_STRATEGIES = {"Metagenome", "Metatranscriptome"}


def _is_valid_project(project: dict) -> bool:
    """A project is considered valid if:
    1. `sequencingStrategy` is in {"Metagenome", "Metatranscriptome"}
    2. if `sequencingCenters` == 'DOE Joint Genome Institute (JGI)' then
        `projectStatus` must be in ("Permanent Draft", "Complete and Published")
    3. otherwise, no `projectStatus` filter is applied

    :param project: GOLD project object (structurally similar to response
                    from `/projects` endpoint)
    :return: True if the project is valid, False otherwise
    """
    if project.get("sequencingStrategy") not in SEQUENCING_STRATEGIES:
        return False

    if project.get("sequencingCenters") == "DOE Joint Genome Institute (JGI)":
        return project.get("projectStatus") in (
            "Permanent Draft",
            "Complete and Published",
        )

    return True


class GoldStudyTranslator(Translator):
    def __init__(
        self,
        study: JSON_OBJECT = {},
        study_type: str = "research_study",
        biosamples: List[JSON_OBJECT] = [],
        projects: List[JSON_OBJECT] = [],
        analysis_projects: List[JSON_OBJECT] = [],
        gold_nmdc_instrument_map_df: pd.DataFrame = pd.DataFrame(),
        include_field_site_info: bool = False,
        enable_biosample_filtering: bool = True,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)

        self.study = study
        self.study_type = nmdc.StudyCategoryEnum(study_type)
        self.include_field_site_info = include_field_site_info
        self.enable_biosample_filtering = enable_biosample_filtering
        # Filter biosamples to only those with `sequencingStrategy` of
        # "Metagenome" or "Metatranscriptome" if filtering is enabled
        if enable_biosample_filtering:
            self.biosamples = [
                biosample
                for biosample in biosamples
                if any(
                    _is_valid_project(project)
                    for project in biosample.get("projects", [])
                )
            ]
        else:
            self.biosamples = biosamples
        # Fetch the valid projectGoldIds that are associated with filtered
        # biosamples on their `projects` field
        valid_project_ids = {
            project.get("projectGoldId")
            for project in projects
            if _is_valid_project(project)
        }
        # Filter projects to only those with `projectGoldId` in valid_project_ids
        self.projects = [
            project
            for project in projects
            if project.get("projectGoldId") in valid_project_ids
        ]
        # Filter analysis_projects to only those with all `projects` in valid_project_ids
        self.analysis_projects = [
            analysis_project
            for analysis_project in analysis_projects
            if all(
                project_id in valid_project_ids
                for project_id in analysis_project.get("projects", [])
            )
        ]
        self.gold_nmdc_instrument_map_df = gold_nmdc_instrument_map_df

        self._projects_by_id = self._index_by_id(self.projects, "projectGoldId")
        self._analysis_projects_by_id = self._index_by_id(
            self.analysis_projects, "apGoldId"
        )

        self._project_ids_by_biosample_id = collections.defaultdict(list)
        for project in self.projects:
            self._project_ids_by_biosample_id[project["biosampleGoldId"]].append(
                project["projectGoldId"]
            )

        self._analysis_project_ids_by_biosample_id = collections.defaultdict(list)
        for analysis_project in self.analysis_projects:
            for project_id in analysis_project["projects"]:
                project = self._projects_by_id[project_id]
                self._analysis_project_ids_by_biosample_id[
                    project["biosampleGoldId"]
                ].append(analysis_project["apGoldId"])

    def _get_pi(self, gold_entity: JSON_OBJECT) -> Union[nmdc.PersonValue, None]:
        """Construct a PersonValue from the first PI in the `contacts` field

        This function iterates over the items in the `contacts` array of the given GOLD
        entity object. Using the first item where `PI` is in its `roles` array, it
        constructs an `nmdc:PersonValue` using the item's `name` and `email` fields.
        If no item with `PI` in the `roles` is found, `None` is returned.

        :param gold_entity: GOLD entity object
        :return: PersonValue corresponding to the first PI in the `contacts` field
        """
        if "contacts" not in gold_entity:
            return None

        pi_dict = next(
            (
                contact
                for contact in gold_entity["contacts"]
                if "PI" in contact["roles"]
            ),
            None,
        )

        if pi_dict is None or "name" not in pi_dict or "email" not in pi_dict:
            return None

        return nmdc.PersonValue(
            has_raw_value=pi_dict.get("name"),
            name=pi_dict.get("name"),
            email=pi_dict.get("email"),
            type="nmdc:PersonValue",
        )

    def _get_mod_date(self, gold_entity: JSON_OBJECT) -> Union[str, None]:
        """Get a mod_date for a GOLD entity

        Return the `modDate` field if it exists and is not `None`. Otherwise return
        the `addDate` field if it exists. Otherwise return `None`.

        :param gold_entity: GOLD entity with `modDate` or `addDate` fields
        :return: Date string or `None`
        """
        mod_date = gold_entity.get("modDate")
        if mod_date is not None:
            return mod_date

        return gold_entity.get("addDate")

    def _get_insdc_biosample_identifiers(self, gold_biosample_id: str) -> List[str]:
        """Get a list of INDSC biosample identifiers related to the given GOLD biosample identifier

        This method finds the GOLD projects which are related to the given GOLD biosample.
        For each project found it returns the project's `ncbiBioSampleAccession` field,
        prefixed with `biosample:`.

        :param gold_biosample_id: GOLD biosample identifier
        :return: List of INDSC biosample identifiers
        """
        biosample_projects = (
            self._projects_by_id[id]
            for id in self._project_ids_by_biosample_id[gold_biosample_id]
        )
        return [
            self._ensure_curie(
                project["ncbiBioSampleAccession"], default_prefix="biosample"
            )
            for project in biosample_projects
            if project.get("ncbiBioSampleAccession")
        ]

    def _get_samp_taxon_id(
        self, gold_biosample: JSON_OBJECT
    ) -> Union[nmdc.ControlledIdentifiedTermValue, None]:
        """Get a ControlledIdentifiedTermValue representing the NCBI taxon
        for a GOLD biosample

        This method gets the `ncbiTaxName` and `ncbiTaxId` from a GOLD biosample object.
        If both are not `None`, it constructs a ControlledIdentifiedTermValue of the format
        `{ncbiTaxName} [NCBITaxon:{ncbiTaxId}]`. Otherwise, it returns `None`

        :param gold_biosample: GOLD biosample object
        :return: ControlledIdentifiedTermValue object
        """
        ncbi_tax_name = gold_biosample.get("ncbiTaxName")
        ncbi_tax_id = gold_biosample.get("ncbiTaxId")
        if ncbi_tax_name is None or ncbi_tax_id is None:
            return None

        raw_value = f"{ncbi_tax_name} [NCBITaxon:{ncbi_tax_id}]"

        return nmdc.ControlledIdentifiedTermValue(
            has_raw_value=raw_value,
            term=nmdc.OntologyClass(
                id=f"NCBITaxon:{ncbi_tax_id}",
                name=ncbi_tax_name,
                type="nmdc:OntologyClass",
            ),
            type="nmdc:ControlledIdentifiedTermValue",
        )

    def _get_host_taxid(
        self, gold_biosample: JSON_OBJECT
    ) -> Union[nmdc.ControlledIdentifiedTermValue, None]:
        """Get a ControlledIdentifiedTermValue representing the NCBI host taxon id
        for a GOLD biosample

        This method gets the `hostNcbiTaxid` from a GOLD biosample object.
        It constructs a ControlledIdentifiedTermValue of the format
        `[NCBITaxon:{hostNcbiTaxid}]`. Otherwise, it returns `None`

        :param gold_biosample: GOLD biosample object
        :return: ControlledIdentifiedTermValue object
        """
        host_taxid = gold_biosample.get("hostNcbiTaxid")
        if host_taxid is None:
            return None
        return nmdc.ControlledIdentifiedTermValue(
            has_raw_value=f"NCBITaxon:{host_taxid}",
            term=nmdc.OntologyClass(
                id=f"NCBITaxon:{host_taxid}",
                type="nmdc:OntologyClass",
            ),
            type="nmdc:ControlledIdentifiedTermValue",
        )

    def _get_samp_name(self, gold_biosample: JSON_OBJECT) -> Union[str, None]:
        """Get a sample name for a GOLD biosample object

        The method assumes that the `biosampleName` field on a GOLD biosample object
        is structured as: "{study name} - {sample name}". Therefore it returns the portion
        of that string after the last occurrence of ` - `. This appears to be a common
        pattern for GOLD biosamples, but you may need to subclass `GoldStudyTranslator`
        and override this method for GOLD studies that do not use this convention.

        :param gold_biosample: GOLD biosample object
        :return: Sample name
        """
        biosample_name = gold_biosample.get("biosampleName")
        if biosample_name is None:
            return None

        tokens = biosample_name.rsplit(" - ", 1)
        if len(tokens) == 1:
            return biosample_name

        return tokens[1].strip()

    def _get_img_identifiers(self, gold_biosample_id: str) -> List[str]:
        """Get a list of IMG database identifiers related to the given GOLD biosample ID

        This method gets all of the GOLD analysis_projects related to the given
        GOLD biosample ID. It returns the unique set of all values from the `imgTaxonOid`
        field of each analysis_project.

        :param gold_biosample_id: GOLD biosample identifier
        :return: List of IMG database identifiers
        """
        biosample_analysis_projects = [
            self._analysis_projects_by_id[id]
            for id in self._analysis_project_ids_by_biosample_id[gold_biosample_id]
        ]
        return sorted(
            {
                f"img.taxon:{ap['imgTaxonOid']}"
                for ap in biosample_analysis_projects
                if ap["imgTaxonOid"]
            }
        )

    def _get_collection_date(
        self, gold_biosample: JSON_OBJECT
    ) -> Union[nmdc.TimestampValue, None]:
        """Get a TimestampValue representing the collection date of a GOLD biosample

        This method gets the `dateCollected` from a GOLD biosample object and returns
        it as a `nmdc:TimestampValue` if it is not None. Otherwise it returns None.

        :param gold_biosample: GOLD biosample object
        :return: TimestampValue for the collection date
        """
        date_collected = gold_biosample.get("dateCollected")
        if date_collected is None:
            return None
        return nmdc.TimestampValue(
            has_raw_value=date_collected, type="nmdc:TimestampValue"
        )

    def _get_quantity_value(
        self,
        gold_entity: JSON_OBJECT,
        gold_field: Union[str, Tuple[str, str]],
        unit: Union[str, None] = None,
    ) -> Union[nmdc.QuantityValue, None]:
        """Get any field of a GOLD entity object as a QuantityValue

        This method extracts any single field of a GOLD entity object (study, biosample, etc)
        and if it is not `None` returns it as an `nmdc:QuantityValue`. A has_numeric_value will
        be inferred from the gold_field value in gold_entity if it is a simple string value. If
        it is a tuple of two fields, a has_minimum_numeric_value and has_maximum_numeric_value
        will be inferred from the gold_field values in gold_entity.

        :param gold_entity: GOLD entity object
        :param gold_field: Name of the field to extract, or a tuple of two fields to extract a range
        :param unit: An optional unit as a string, defaults to None
        :return: QuantityValue object
        """
        if isinstance(gold_field, tuple):
            minimum_numeric_value = gold_entity.get(gold_field[0])
            maximum_numeric_value = gold_entity.get(gold_field[1])

            if minimum_numeric_value is None and maximum_numeric_value is None:
                return None
            elif minimum_numeric_value is not None and maximum_numeric_value is None:
                return nmdc.QuantityValue(
                    has_raw_value=minimum_numeric_value,
                    has_numeric_value=nmdc.Double(minimum_numeric_value),
                    has_unit=unit,
                    type="nmdc:QuantityValue",
                )
            else:
                return nmdc.QuantityValue(
                    has_minimum_numeric_value=nmdc.Double(minimum_numeric_value),
                    has_maximum_numeric_value=nmdc.Double(maximum_numeric_value),
                    has_unit=unit,
                    type="nmdc:QuantityValue",
                )

        field_value = gold_entity.get(gold_field)
        if field_value is None:
            return None

        return nmdc.QuantityValue(
            has_raw_value=field_value,
            has_numeric_value=nmdc.Double(field_value),
            has_unit=unit,
            type="nmdc:QuantityValue",
        )

    def _get_text_value(
        self, gold_entity: JSON_OBJECT, gold_field: str
    ) -> Union[nmdc.TextValue, None]:
        """Get any field of a GOLD entity object as a TextValue

        This method extracts any single field of a GOLD entity object (study, biosample, etc)
        and if it is not `None` returns it as an `nmdc:TextValue`. If the value of the field
        is `None`, `None` will be returned.

        :param gold_entity: GOLD entity object
        :param gold_field: Name of the field to extract
        :return: TextValue object
        """
        field_value = gold_entity.get(gold_field)
        if field_value is None:
            return None
        return nmdc.TextValue(has_raw_value=field_value, type="nmdc:TextValue")

    def _get_controlled_term_value(
        self, gold_entity: JSON_OBJECT, gold_field: str
    ) -> Union[nmdc.ControlledTermValue, None]:
        """Get any field of a GOLD entity object as a ControlledTermValue

        This method extracts any single field of a GOLD entity object (study, biosample, etc)
        and if it is not `None` returns it as an `nmdc:ControlledTermValue`. If the value of
        the field is `None`, `None` will be returned.

        :param gold_entity: GOLD entity object
        :param gold_field: Name of the field to extract
        :return: ControlledTermValue object
        """
        field_value = gold_entity.get(gold_field)
        if field_value is None:
            return None
        return nmdc.ControlledTermValue(
            has_raw_value=field_value, type="nmdc:ControlledTermValue"
        )

    def _get_env_term_value(
        self, gold_biosample: JSON_OBJECT, gold_field: str
    ) -> nmdc.ControlledIdentifiedTermValue:
        """Get an ENVO term as a ControlledIdentifiedTermValue from a GOLD biosample object field

        In GOLD entities ENVO terms are represented as a nested object with `id` and `label`
        fields. This method extracts this type of nested object by the given field name, and
        returns it as an `nmdc:ControlledIdentifiedTermValue` object. The `id` in the original
        GOLD object should be reformatted by replacing `_` with `:` (e.g. `ENVO_00005801` to
        `ENVO:00005801`). If the value of the given field is `None` or if it does not contain
        a nested object with an `id` field, `None` is returned.

        :param gold_biosample: GOLD biosample object
        :param gold_field: Name of the field to extract
        :return: ControlledIdentifiedTermValue object
        """
        env_field = gold_biosample.get(gold_field)
        if env_field is None or "id" not in env_field:
            return None
        return nmdc.ControlledIdentifiedTermValue(
            term=nmdc.OntologyClass(
                id=env_field["id"].replace("_", ":"),
                name=env_field.get("label"),
                type="nmdc:OntologyClass",
            ),
            has_raw_value=env_field["id"],
            type="nmdc:ControlledIdentifiedTermValue",
        )

    def _get_lat_lon(
        self, gold_biosample: JSON_OBJECT
    ) -> Union[nmdc.GeolocationValue, None]:
        """Get a GeolocationValue object representing a GOLD biosample lat/lon

        The method retrieves a GOLD biosample object's `latitude` and `longitude`
        fields. If both are not `None`, it constructs an `nmdc:GeolocationValue` object
        from them and returns it. Otherwise, it returns `None`

        :param gold_biosample: GOLD biosample object
        :return: GeolocationValue object
        """
        latitude = gold_biosample.get("latitude")
        longitude = gold_biosample.get("longitude")
        if latitude is None or longitude is None:
            return None
        return nmdc.GeolocationValue(
            has_raw_value=f"{latitude} {longitude}",
            latitude=nmdc.DecimalDegree(latitude),
            longitude=nmdc.DecimalDegree(longitude),
            type="nmdc:GeolocationValue",
        )

    def _get_instrument(self, gold_project: JSON_OBJECT) -> Union[str, None]:
        """Get instrument id referenced in instrument_set collection in Mongo.
        Note: The instrument id is not retrieved by making a call to the database,
        but rather parsed out from a TSV file in the nmdc-schema repo stored at
        self.gold_instrument_set_mapping_file_path.

        This method gets the seqMethod field from a GOLD project object. If
        that value is not None and is in the self.gold_instrument_set_mapping_file_path
        file's GOLD SeqMethod column, the corresponding instrument id from
        NMDC instrument_set id column is returned. If the value of the field
        is None, None is returned.

        :param gold_project: GOLD project object
        :return: id corresponding to an Instrument from instrument_set collection
        """
        seq_method = gold_project.get("seqMethod")
        if not seq_method:
            return None

        seq_method = seq_method[0].strip()
        df = self.gold_nmdc_instrument_map_df

        matching_row = df[df["GOLD SeqMethod"] == seq_method]

        if not matching_row.empty:
            instrument_id = matching_row["NMDC instrument_set id"].values[0]
            return instrument_id

        raise ValueError(
            f"seqMethod '{seq_method}' could not be found in the GOLD-NMDC instrument mapping TSV file."
        )

    def _get_processing_institution(
        self, gold_project: JSON_OBJECT
    ) -> Union[nmdc.ProcessingInstitutionEnum, None]:
        """Get a processing institution for a GOLD project object

        This method gets the `sequencingCenters` field from a GOLD project object. For each item
        in this list, it attempts to match against known regex patterns that map to
        `nmdc:ProcessingInstitutionEnum` values. For the first item in the list that matches
        one of the patterns, the corresponding `nmdc:ProcessingInstitutionEnum` value is returned.
        If the list is empty, the field is `None` or none of the items match any of the patterns,
        `None` is returned.

        :param gold_project: GOLD project object
        :return: ProcessingInstitutionEnum value
        """
        matchers = {
            r"University of California[,]? San Diego": nmdc.ProcessingInstitutionEnum.UCSD,
            r"Environmental Molecular Sciences Laboratory": nmdc.ProcessingInstitutionEnum.EMSL,
            r"Joint Genome Institute": nmdc.ProcessingInstitutionEnum.JGI,
        }

        sequencing_centers = gold_project.get("sequencingCenters")
        if sequencing_centers is None or len(sequencing_centers) == 0:
            return None

        for sequencing_center in sequencing_centers:
            for regex, code in matchers.items():
                if re.search(regex, sequencing_center, flags=re.IGNORECASE):
                    return nmdc.ProcessingInstitutionEnum(code)

        return None

    def _get_field_site_name(self, gold_biosample: JSON_OBJECT) -> Union[str, None]:
        """Get a field site name from a GOLD biosample object

        This method gets the sample name from a GOLD biosample object according
        to the logic of `._get_samp_name`. It then returns the text of the sample
        name up to the *last* space in the string. Similar to `._get_samp_name`
        this may not be a convention for all GOLD studies and pipelines for specific
        studies may need to subclass `GoldStudyTranslator` and override this method.

        :param gold_biosample: GOLD biosample object
        :return: Field stie name
        """
        samp_name = self._get_samp_name(gold_biosample)
        if samp_name is None:
            return None
        last_space_index = samp_name.rfind(" ")
        if last_space_index < 0:
            return samp_name
        return samp_name[:last_space_index]

    def _translate_study(
        self, gold_study: JSON_OBJECT, nmdc_study_id: str
    ) -> nmdc.Study:
        """Translate a GOLD study object into an `nmdc:Study` object.

        This method translates a GOLD study object into an equivalent `nmdc:Study`
        object. Any minted NMDC IDs must be passed to this method. Internally, each
        slot of the `nmdc:Study` is either directly pulled from the GOLD object or
        one of the `_get_*` methods is used.

        :param gold_study: GOLD study object
        :param nmdc_study_id: Minted nmdc:Study identifier for the translated object
        :return: nmdc:Study object
        """
        return nmdc.Study(
            description=gold_study.get("description"),
            gold_study_identifiers=self._ensure_curie(
                gold_study["studyGoldId"], default_prefix="gold"
            ),
            id=nmdc_study_id,
            name=gold_study.get("studyName"),
            principal_investigator=self._get_pi(gold_study),
            title=gold_study.get("studyName"),
            type="nmdc:Study",
            study_category=self.study_type,
        )

    def _translate_biosample(
        self,
        gold_biosample: JSON_OBJECT,
        nmdc_biosample_id: str,
        nmdc_study_id: str,
        nmdc_field_site_id: str,
    ) -> nmdc.Biosample:
        """Translate a GOLD biosample object into an `nmdc:Biosample` object.

        This method translates a GOLD biosample object into an equivalent `nmdc:Biosample`
        object. Any minted NMDC IDs must be passed to this method. Internally, each
        slot of the `nmdc:Biosample` is either directly pulled from the GOLD object or
        one of the `_get_*` methods is used.

        :param gold_study: GOLD biosample object
        :param nmdc_biosample_id: Minted nmdc:Biosample identifier for the translated object
        :param nmdc_study_id: Minted nmdc:Study identifier for the related Study
        :param nmdc_field_site_id: Minted nmdc:FieldResearchSite identifier for the related site
        :return: nmdc:Biosample object
        """
        gold_biosample_id = gold_biosample["biosampleGoldId"]
        return nmdc.Biosample(
            add_date=gold_biosample.get("addDate"),
            alt=self._get_quantity_value(
                gold_biosample, "altitudeInMeters", unit="meters"
            ),
            collected_from=nmdc_field_site_id,
            collection_date=self._get_collection_date(gold_biosample),
            depth=self._get_quantity_value(
                gold_biosample, ("depthInMeters", "depthInMeters2"), unit="meters"
            ),
            description=gold_biosample.get("description"),
            diss_oxygen=self._get_quantity_value(gold_biosample, "oxygenConcentration"),
            ecosystem_category=gold_biosample.get("ecosystemCategory"),
            ecosystem_subtype=gold_biosample.get("ecosystemSubtype"),
            ecosystem_type=gold_biosample.get("ecosystemType"),
            ecosystem=gold_biosample.get("ecosystem"),
            elev=gold_biosample.get("elevationInMeters"),
            env_broad_scale=self._get_env_term_value(gold_biosample, "envoBroadScale"),
            env_local_scale=self._get_env_term_value(gold_biosample, "envoLocalScale"),
            env_medium=self._get_env_term_value(gold_biosample, "envoMedium"),
            geo_loc_name=self._get_text_value(gold_biosample, "geoLocation"),
            gold_biosample_identifiers=self._ensure_curie(
                gold_biosample_id, default_prefix="gold"
            ),
            habitat=gold_biosample.get("habitat"),
            host_name=gold_biosample.get("hostName"),
            host_taxid=self._get_host_taxid(gold_biosample),
            id=nmdc_biosample_id,
            img_identifiers=self._get_img_identifiers(gold_biosample_id),
            insdc_biosample_identifiers=self._get_insdc_biosample_identifiers(
                gold_biosample_id
            ),
            lat_lon=self._get_lat_lon(gold_biosample),
            location=gold_biosample.get("isoCountry"),
            mod_date=self._get_mod_date(gold_biosample),
            name=gold_biosample.get("biosampleName"),
            ncbi_taxonomy_name=gold_biosample.get("ncbiTaxName"),
            nitrite=self._get_quantity_value(gold_biosample, "nitrateConcentration"),
            ph=gold_biosample.get("ph"),
            pressure=self._get_quantity_value(gold_biosample, "pressure"),
            samp_name=self._get_samp_name(gold_biosample),
            samp_taxon_id=self._get_samp_taxon_id(gold_biosample),
            sample_collection_site=gold_biosample.get(
                "sampleCollectionSite", gold_biosample.get("sampleBodySite")
            ),
            specific_ecosystem=gold_biosample.get("specificEcosystem"),
            subsurface_depth=self._get_quantity_value(
                gold_biosample, "subsurfaceDepthInMeters", unit="meters"
            ),
            temp=self._get_quantity_value(
                gold_biosample, "sampleCollectionTemperature"
            ),
            type="nmdc:Biosample",
            associated_studies=[nmdc_study_id],
        )

    def _translate_nucleotide_sequencing(
        self,
        gold_project: JSON_OBJECT,
        nmdc_nucleotide_sequencing_id: str,
        nmdc_biosample_id: str,
        nmdc_study_id: str,
    ):
        """Translate a GOLD project object into an `nmdc:NucleotideSequencing` object.

        This method translates a GOLD project object into an equivalent `nmdc:NucleotideSequencing`
        object. Any minted NMDC IDs must be passed to this method. Internally, each
        slot of the `nmdc:NucleotideSequencing` is either directly pulled from the GOLD object or
        one of the `_get_*` methods is used.

        :param gold_project: GOLD project object
        :param nmdc_omics_processing_id: Minted nmdc:NucleotideSequencing identifier for the translated object
        :param nmdc_biosample_id: Minted nmdc:Biosample identifier for the related Biosample
        :param nmdc_study_id: Minted nmdc:Study identifier for the related Study
        :return: nmdc:NucleotideSequencing object
        """
        gold_project_id = gold_project["projectGoldId"]
        ncbi_bioproject_identifier = gold_project.get("ncbiBioProjectAccession")
        insdc_bioproject_identifiers = []
        if ncbi_bioproject_identifier:
            insdc_bioproject_identifiers.append(
                self._ensure_curie(
                    ncbi_bioproject_identifier,
                    default_prefix="bioproject",
                )
            )

        return nmdc.NucleotideSequencing(
            id=nmdc_nucleotide_sequencing_id,
            name=gold_project.get("projectName"),
            gold_sequencing_project_identifiers=self._ensure_curie(
                gold_project_id, default_prefix="gold"
            ),
            ncbi_project_name=gold_project.get("projectName"),
            type="nmdc:NucleotideSequencing",
            has_input=nmdc_biosample_id,
            add_date=gold_project.get("addDate"),
            mod_date=self._get_mod_date(gold_project),
            insdc_bioproject_identifiers=insdc_bioproject_identifiers,
            principal_investigator=self._get_pi(gold_project),
            processing_institution=self._get_processing_institution(gold_project),
            instrument_used=self._get_instrument(gold_project),
            analyte_category=(
                gold_project.get("sequencingStrategy").lower()
                if gold_project.get("sequencingStrategy")
                else None
            ),
            associated_studies=[nmdc_study_id],
        )

    def get_database(self) -> nmdc.Database:
        """Translate the GOLD study and associated objects to an nmdc:Database

        This method translates the GOLD study and associated biosample, project, and
        analysis_project objects provided to the constructor into their equivalent
        NMDC objects. NMDC identifiers are minted for each new object. The objects are
        then bundled into the `study_set`, `biosample_set`, `field_research_site_set`,
        and `omics_processing_set` slots of a `nmdc:Database` object.

        :return: nmdc:Database object
        """
        database = nmdc.Database()

        nmdc_study_id = self._id_minter("nmdc:Study")[0]

        gold_biosample_ids = [
            biosample["biosampleGoldId"] for biosample in self.biosamples
        ]
        nmdc_biosample_ids = self._id_minter("nmdc:Biosample", len(self.biosamples))
        gold_to_nmdc_biosample_ids = dict(zip(gold_biosample_ids, nmdc_biosample_ids))

        if self.include_field_site_info:
            gold_field_site_names = sorted(
                {self._get_field_site_name(biosample) for biosample in self.biosamples}
            )
            nmdc_field_site_ids = self._id_minter(
                "nmdc:FieldResearchSite", len(gold_field_site_names)
            )
            gold_name_to_nmdc_field_site_ids = dict(
                zip(gold_field_site_names, nmdc_field_site_ids)
            )
            gold_biosample_to_nmdc_field_site_ids = {
                biosample["biosampleGoldId"]: gold_name_to_nmdc_field_site_ids[
                    self._get_field_site_name(biosample)
                ]
                for biosample in self.biosamples
            }
        else:
            gold_biosample_to_nmdc_field_site_ids = {}

        gold_project_ids = [project["projectGoldId"] for project in self.projects]
        nmdc_nucleotide_sequencing_ids = self._id_minter(
            "nmdc:NucleotideSequencing", len(gold_project_ids)
        )
        gold_project_to_nmdc_nucleotide_sequencing_ids = dict(
            zip(gold_project_ids, nmdc_nucleotide_sequencing_ids)
        )

        database.study_set = [self._translate_study(self.study, nmdc_study_id)]
        database.biosample_set = [
            self._translate_biosample(
                biosample,
                nmdc_biosample_id=gold_to_nmdc_biosample_ids[
                    biosample["biosampleGoldId"]
                ],
                nmdc_study_id=nmdc_study_id,
                nmdc_field_site_id=gold_biosample_to_nmdc_field_site_ids.get(
                    biosample["biosampleGoldId"], None
                ),
            )
            for biosample in self.biosamples
        ]
        if self.include_field_site_info:
            database.field_research_site_set = [
                nmdc.FieldResearchSite(id=id, name=name, type="nmdc:FieldResearchSite")
                for name, id in gold_name_to_nmdc_field_site_ids.items()
            ]
        database.data_generation_set = [
            self._translate_nucleotide_sequencing(
                project,
                nmdc_nucleotide_sequencing_id=gold_project_to_nmdc_nucleotide_sequencing_ids[
                    project["projectGoldId"]
                ],
                nmdc_biosample_id=gold_to_nmdc_biosample_ids[
                    project["biosampleGoldId"]
                ],
                nmdc_study_id=nmdc_study_id,
            )
            for project in self.projects
        ]

        return database
