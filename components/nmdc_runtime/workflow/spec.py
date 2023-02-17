from typing import Literal

from pydantic import BaseModel, Field


class Inputs(BaseModel):
    proj: str = ""
    informed_by: str = ""


class Workflow(BaseModel):
    name: str
    enabled: bool
    git_repo: str
    version: str
    activity: str
    predecessor: str
    input_prefix: str
    id_type: str
    inputs: Inputs


class Sequencing(Workflow):
    name: str = "Metagenome Sequencing"
    enabled: bool = True
    git_repo: str = ""
    version: str = "1.0.0"
    activity: Literal[
        "metagenome_sequencing_activity_set"
    ] = "metagenome_sequencing_activity_set"
    predecessor: str = ""
    id_type: str = ""
    input_prefix: str = ""
    inputs: Inputs = Inputs()


class ReadQcAnalysisInputs(Inputs):
    input_files: str = "Metagenome Raw Reads"
    informed_by: str = ""
    resource: str = "NERSC-Cori"
    proj: str = ""


class ReadQcAnalysis(Workflow):
    name: str = "Read QC Analysis"
    enabled: bool = True
    git_repo: str = " https://github.com/microbiomedata/ReadsQC"
    version: str = "1.0.6"
    wdl: str = "rqcfilter.wdl"
    activity: Literal["read_qc_analysis_activity_set"] = "read_qc_analysis_activity_set"
    predecessor: str = "Sequencing"
    input_prefix: str = "nmdc_rqcfilter"
    id_type: str = "mgrc"
    inputs: ReadQcAnalysisInputs = ReadQcAnalysisInputs()


class MetagenomeAnnotationInputs(Inputs):
    input_file: str = "Assembly Contigs"
    imgap_project_id: str = "actid"
    resource: str = "NERSC-Cori"
    proj: str = ""
    informed_by: str = ""
    git_url: str = "https://github.com/microbiomedata/mg_annotation"


class MetagenomeAnnotation(Workflow):
    name: str = "Metagenome Annotation"
    enabled: bool = True
    git_repo: str = "https://github.com/microbiomedata/mg_annotation"
    version: str = "1.0.0"
    wdl: str = "annotation_full.wdl"
    activity: Literal[
        "metagenome_annotation_activity_set"
    ] = "metagenome_annotation_activity_set"
    predecessor: str = "MetagenomeAssembly"
    input_prefix: str = "annotation"
    id_type: str = "mgann"
    inputs: MetagenomeAnnotationInputs = MetagenomeAnnotationInputs()


class MetagenomeAssemblyInputs(Inputs):
    input_file: str = "Filtered Sequencing Reads"
    rename_contig_prefix: str = "actid"
    resource: str = "NERSC-Cori"
    proj: str = ""
    informed_by: str = ""
    git_url: str = "https://github.com/microbiomedata/meta_assembly"


class MetagenomeAssembly(Workflow):
    name: str = "Metagenome Assembly"
    enabled: bool = True
    git_repo: str = "https://github.com/microbiomedata/metaAssembly"
    version: str = "1.0.3"
    wdl: str = "jgi_assembly.wdl"
    activity: Literal["metagenome_assembly_set"] = "metagenome_assembly_set"
    predecessor: str = "Read QC Analysis"
    input_prefix: str = "jgi_metaASM"
    id_type: str = "mgasm"
    inputs: MetagenomeAssemblyInputs = MetagenomeAssemblyInputs()


class MAGsInputs(Inputs):
    input_file: str = "Assembly Contigs"
    contig_file: str = "Assembly Contigs"
    gff_file: str = "Functional Annotation GFF"
    cath_funfam_file: str = "CATH FunFams (Functional Families) Annotation GFF"
    supfam_file: str = "SUPERFam Annotation GFF"
    cog_file: str = "Clusters of Orthologous Groups (COG) Annotation GFF"
    pfam_file: str = "Pfam Annotation GFF"
    product_names_file: str = "Product names"
    tigrfam_file: str = "TIGRFam Annotation GFF"
    ec_file: str = "Annotation Enzyme Commission"
    ko_file: str = "Annotation KEGG Orthology"
    sam_file: str = "Assembly Coverage BAM"
    smart_file: str = "SMART Annotation GFF"
    proteins_file: str = "Annotation Amino Acid FASTA"
    gene_phylogeny_file: str = "Gene Phylogeny"
    resource: str = "NERSC-Cori"
    proj: str = ""
    informed_by: str = ""
    git_url: str = "https://github.com/microbiomedata/metaMAGs"
    url_root: str = "https://data.microbiomedata.org/data/"


class MAGs(Workflow):
    name: str = "MAGs"
    enabled: bool = True
    git_repo: str = "https://github.com/microbiomedata/metaMAGs"
    version: str = "1.0.4"
    wdl: str = "mbin_nmdc.wdl"
    activity: Literal["mags_activity_set"] = "mags_activity_set"
    predecessor: str = "Metagenome Annotation"
    input_prefix: str = "nmdc_mags"
    id_type: str = "mgmag"
    inputs: MAGsInputs = MAGsInputs()


class ReadBasedAnalysisInputs(Inputs):
    input_file: str = "Filtered Sequencing Reads"
    prefix: str = "actid"
    resource: str = "NERSC-Cori"
    proj: str = ""
    informed_by: str = ""
    git_url: str = "https://github.com/microbiomedata/ReadbasedAnalysis"
    url_root: str = "https://data.microbiomedata.org/data/"


class ReadBasedAnalysis(Workflow):
    name: str = "Readbased Analysis"
    enabled: bool = True
    git_repo: str = "https://github.com/microbiomedata/ReadbasedAnalysis"
    version: str = "1.0.2"
    wdl: str = "ReadbasedAnalysis.wdl"
    activity: Literal[
        "read_based_taxonomy_analysis_activity_set"
    ] = "read_based_taxonomy_analysis_activity_set"
    predecessor: str = "Read QC Analysis"
    input_prefix: str = "nmdc_rba"
    id_type: str = "mgrba"
    inputs: ReadBasedAnalysisInputs = ReadBasedAnalysisInputs()


class WorkflowModel(BaseModel):
    workflow: ReadQcAnalysis | MetagenomeAssembly | MAGs | ReadBasedAnalysis | Sequencing | MetagenomeAnnotation = Field(
        ..., discriminator="activity"
    )


def get_all_workflows() -> list[Workflow]:
    return [
        ReadQcAnalysis(inputs=ReadQcAnalysisInputs()),
        MetagenomeAssembly(inputs=MetagenomeAssemblyInputs()),
        MetagenomeAnnotation(inputs=MetagenomeAnnotationInputs()),
        MAGs(inputs=MAGsInputs()),
        ReadBasedAnalysis(inputs=ReadBasedAnalysisInputs()),
        Sequencing(inputs=Inputs()),
    ]
