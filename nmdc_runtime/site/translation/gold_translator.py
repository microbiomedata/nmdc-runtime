import collections
import re
from typing import Any, Callable, Dict, List, Optional, Union
from nmdc_schema import nmdc

JSON_OBJECT = Dict[str, Any]


class Translator():

    def __init__(self, id_minter: Optional[Callable[[str, Optional[int]], List[str]]] = None) -> None:
        self._id_minter = id_minter

    def _index_by_id(self, collection, id):
        return {item[id]: item for item in collection}

    def _get_curie(self, prefix: str, local: str) -> str:
        return f"{prefix}:{local}"


class GoldStudyTranslator(Translator):

    def __init__(self, study, biosamples, projects, analysis_projects, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

        self.study = study
        self.biosamples = biosamples
        self.projects = projects
        self.analysis_projects = analysis_projects

        self._projects_by_id = self._index_by_id(self.projects, "projectGoldId")
        self._analysis_projects_by_id = self._index_by_id(self.analysis_projects, "apGoldId")

        self._project_ids_by_biosample_id = collections.defaultdict(list)
        for project in self.projects:
            self._project_ids_by_biosample_id[project["biosampleGoldId"]].append(project["projectGoldId"])

        self._analysis_project_ids_by_biosample_id = collections.defaultdict(list)
        for analysis_project in self.analysis_projects:
            for project_id in analysis_project["projects"]:
                project = self._projects_by_id[project_id]
                self._analysis_project_ids_by_biosample_id[project["biosampleGoldId"]].append(analysis_project["apGoldId"])

    def _get_pi(self, gold_entity: JSON_OBJECT) -> Union[nmdc.PersonValue, None]:
        pi_dict = next(
            (contact for contact in gold_entity["contacts"] if "PI" in contact["roles"]),
            None
        )

        if pi_dict is None or "name" not in pi_dict or "email" not in pi_dict:
            return None

        return nmdc.PersonValue(
            has_raw_value=pi_dict.get("name"),
            name=pi_dict.get("name"),
            email=pi_dict.get("email"),
        )

    def _get_mod_date(self, gold_entity: JSON_OBJECT) -> Union[str, None]:
        return gold_entity.get("modDate", gold_entity.get("addDate"))

    def _get_insdc_biosample_identifiers(self, gold_biosample_id: str) -> List[str]:
        biosample_projects = (self._projects_by_id[id] for id in self._project_ids_by_biosample_id[gold_biosample_id])
        return [
            self._get_curie("biosample", project["ncbiBioSampleAccession"])
            for project in biosample_projects
            if project["ncbiBioSampleAccession"]
        ]

    def _get_samp_taxon_id(self, gold_biosample: JSON_OBJECT) -> Union[nmdc.TextValue, None]:
        ncbi_tax_name = gold_biosample.get("ncbiTaxName")
        if ncbi_tax_name is None:
            return None

        ncbi_tax_id = gold_biosample.get("ncbiTaxId")
        return nmdc.TextValue(f"{ncbi_tax_name} [NCBITaxon:{ncbi_tax_id}]")

    def _get_samp_name(self, gold_biosample: JSON_OBJECT) -> Union[str, None]:
        biosample_name = gold_biosample.get("biosampleName")
        if biosample_name is None:
            return None
        return biosample_name.split(' - ')[-1].strip()

    def _get_img_identifiers(self, gold_biosample_id: str) -> List[str]:
        biosample_analysis_projects = [
            self._analysis_projects_by_id[id]
            for id
            in self._analysis_project_ids_by_biosample_id[gold_biosample_id]
        ]
        return list({ap["imgTaxonOid"] for ap in biosample_analysis_projects if ap["imgTaxonOid"]})

    def _get_collection_date(self, gold_biosample: JSON_OBJECT) -> Union[nmdc.TimestampValue, None]:
        date_collected = gold_biosample.get("dateCollected")
        if date_collected is None:
            return None
        return nmdc.TimestampValue(has_raw_value=date_collected)

    def _get_quantity_value(
                self,
                gold_entity: JSON_OBJECT,
                gold_field: str, unit: Union[str, None] = None) -> Union[nmdc.QuantityValue, None]:
        field_value = gold_entity.get(gold_field)
        if field_value is None:
            return None
        return nmdc.QuantityValue(
            has_raw_value=field_value,
            has_unit=unit,
        )

    def _get_text_value(self, gold_entity: JSON_OBJECT, gold_field: str) -> Union[nmdc.TextValue, None]:
        field_value = gold_entity.get(gold_field)
        if field_value is None:
            return None
        return nmdc.TextValue(has_raw_value=field_value)

    def _get_controlled_term_value(self, gold_entity: JSON_OBJECT, gold_field: str) -> Union[nmdc.ControlledTermValue, None]:
        field_value = gold_entity.get(gold_field)
        if field_value is None:
            return None
        return nmdc.ControlledTermValue(has_raw_value=field_value)

    def _get_env_term_value(self, gold_biosample: JSON_OBJECT, gold_field: str) -> nmdc.ControlledIdentifiedTermValue:
        env_field = gold_biosample.get(gold_field)
        if env_field is None:
            raise ValueError(f"{gold_field} is not defined on biosample {gold_biosample['biosampleGoldId']}")
        return nmdc.ControlledIdentifiedTermValue(
            term=nmdc.OntologyClass(
                id=env_field["id"].replace("_", ":"),
                name=env_field["label"],
            ),
            has_raw_value=env_field["id"],
        )

    def _get_lat_lon(self, gold_biosample: JSON_OBJECT) -> Union[nmdc.GeolocationValue, None]:
        latitude = gold_biosample.get("latitude")
        longitude = gold_biosample.get("longitude")
        if latitude is None or longitude is None:
            return None
        return nmdc.GeolocationValue(f"{latitude} {longitude}")

    def _get_instrument_name(self, gold_project: JSON_OBJECT) -> Union[str, None]:
        seq_method = gold_project.get("seqMethod")
        if not seq_method:
            return None
        return seq_method[0]

    def _get_processing_institution(self, gold_project: JSON_OBJECT) -> Union[nmdc.ProcessingInstitutionEnum, None]:
        matchers = {
            r"University of California[,]? San Diego": nmdc.ProcessingInstitutionEnum.UCSD,
            r"Environmental Molecular Sciences Laboratory": nmdc.ProcessingInstitutionEnum.EMSL,
            r"Joint Genome Institute": nmdc.ProcessingInstitutionEnum.JGI
        }

        sequencing_centers = gold_project.get("sequencingCenters")
        if sequencing_centers is None or len(sequencing_centers) == 0:
            return None

        # processing_institution is not a multivalued slot in the NMDC schema, so take the first one?
        for regex, code in matchers.items():
            if re.search(regex, sequencing_centers[0], flags=re.IGNORECASE):
                return nmdc.ProcessingInstitutionEnum(code)

        return None

    def _get_field_site_name(self, gold_biosample: JSON_OBJECT) -> Union[str, None]:
        samp_name = self._get_samp_name(gold_biosample)
        if samp_name is None:
            return None
        last_space_index = samp_name.rfind(' ')
        if last_space_index < 0:
            return samp_name
        return samp_name[:last_space_index]

    def _translate_study(self, gold_study: JSON_OBJECT, nmdc_study_id: str) -> nmdc.Study:
        return nmdc.Study(
            description=gold_study.get("description"),
            gold_study_identifiers=self._get_curie("GOLD", gold_study["studyGoldId"]),
            id=nmdc_study_id,
            name=gold_study.get("studyName"),
            principal_investigator=self._get_pi(gold_study),
            title=gold_study.get("studyName"),
            type="nmdc:Study",
        )

    def _translate_biosample(
                self,
                gold_biosample: JSON_OBJECT,
                nmdc_biosample_id: str,
                nmdc_study_id: str,
                nmdc_field_site_id: str) -> nmdc.Biosample:
        gold_biosample_id = gold_biosample["biosampleGoldId"]
        return nmdc.Biosample(
            add_date=gold_biosample.get("addDate"),
            alt=self._get_quantity_value(gold_biosample, "altitudeInMeters", unit="meters"),
            collected_from=nmdc_field_site_id,
            collection_date=self._get_collection_date(gold_biosample),
            depth=self._get_quantity_value(gold_biosample, "depthInMeters", unit="meters"),
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
            gold_biosample_identifiers=self._get_curie("GOLD", gold_biosample_id),
            habitat=gold_biosample.get("habitat"),
            host_name=gold_biosample.get("hostName"),
            host_taxid=self._get_text_value(gold_biosample, "hostNcbiTaxid"),
            id=nmdc_biosample_id,
            img_identifiers=self._get_img_identifiers(gold_biosample_id),
            insdc_biosample_identifiers=self._get_insdc_biosample_identifiers(gold_biosample_id),
            lat_lon=self._get_lat_lon(gold_biosample),
            location=gold_biosample.get("isoCountry"),
            mod_date=self._get_mod_date(gold_biosample),
            name=gold_biosample.get("biosampleName"),
            ncbi_taxonomy_name=gold_biosample.get("ncbiTaxName"),
            nitrite=self._get_quantity_value(gold_biosample, "nitrateConcentration"),
            part_of=nmdc_study_id,
            ph=gold_biosample.get("ph"),
            pressure=self._get_quantity_value(gold_biosample, "pressure"),
            samp_name=self._get_samp_name(gold_biosample),
            samp_taxon_id=self._get_samp_taxon_id(gold_biosample),
            sample_collection_site=gold_biosample.get("sampleCollectionSite", gold_biosample.get("sampleBodySite")),
            specific_ecosystem=gold_biosample.get("specificEcosystem"),
            subsurface_depth=self._get_quantity_value(gold_biosample, "subsurfaceDepthInMeters", unit="meters"),
            temp=self._get_quantity_value(gold_biosample, "sampleCollectionTemperature"),
            type="nmdc:Biosample",
        )

    def _translate_omics_processing(
                self,
                gold_project: JSON_OBJECT,
                nmdc_omics_processing_id: str,
                nmdc_biosample_id: str,
                nmdc_study_id: str) -> nmdc.OmicsProcessing:
        gold_project_id = gold_project["projectGoldId"]
        return nmdc.OmicsProcessing(
            id=nmdc_omics_processing_id,
            name=gold_project.get("projectName"),
            gold_sequencing_project_identifiers=self._get_curie("GOLD", gold_project_id),
            ncbi_project_name=gold_project.get("projectName"),
            type="nmdc:OmicsProcessing",
            has_input=nmdc_biosample_id,
            part_of=nmdc_study_id,
            add_date=gold_project.get("addDate"),
            mod_date=self._get_mod_date(gold_project),
            principal_investigator=self._get_pi(gold_project),
            omics_type=self._get_controlled_term_value(gold_project, "sequencingStrategy"),
            instrument_name=self._get_instrument_name(gold_project),
            processing_institution=self._get_processing_institution(gold_project)
        )

    def get_database(self) -> nmdc.Database:
        database = nmdc.Database()

        nmdc_study_id = self._id_minter("nmdc:Study")[0]

        gold_biosample_ids = [biosample["biosampleGoldId"] for biosample in self.biosamples]
        nmdc_biosample_ids = self._id_minter("nmdc:Biosample", len(self.biosamples))
        gold_to_nmdc_biosample_ids = dict(zip(gold_biosample_ids, nmdc_biosample_ids))

        gold_field_site_names = {self._get_field_site_name(biosample) for biosample in self.biosamples}
        nmdc_field_site_ids = self._id_minter("nmdc:FieldResearchSite", len(gold_field_site_names))
        gold_name_to_nmdc_field_site_ids = dict(zip(gold_field_site_names, nmdc_field_site_ids))
        gold_biosample_to_nmdc_field_site_ids = {
            biosample["biosampleGoldId"]: gold_name_to_nmdc_field_site_ids[self._get_field_site_name(biosample)]
            for biosample in self.biosamples
        }

        gold_project_ids = [project["projectGoldId"] for project in self.projects]
        nmdc_omics_processing_ids = self._id_minter("nmdc:OmicsProcessing", len(gold_project_ids))
        gold_project_to_nmdc_omics_processing_ids = dict(zip(gold_project_ids, nmdc_omics_processing_ids))

        database.study_set = [self._translate_study(self.study, nmdc_study_id)]
        database.biosample_set = [
            self._translate_biosample(
                    biosample,
                    nmdc_biosample_id=gold_to_nmdc_biosample_ids[biosample["biosampleGoldId"]],
                    nmdc_study_id=nmdc_study_id,
                    nmdc_field_site_id=gold_biosample_to_nmdc_field_site_ids[biosample["biosampleGoldId"]])
            for biosample in self.biosamples
        ]
        database.field_research_site_set = [
            nmdc.FieldResearchSite(
                id=id,
                name=name
            )
            for name, id in gold_name_to_nmdc_field_site_ids.items()
        ]
        database.omics_processing_set = [
            self._translate_omics_processing(
                    project,
                    nmdc_omics_processing_id=gold_project_to_nmdc_omics_processing_ids[project["projectGoldId"]],
                    nmdc_biosample_id=gold_to_nmdc_biosample_ids[project["biosampleGoldId"]],
                    nmdc_study_id=nmdc_study_id)
            for project in self.projects
        ]

        return database
