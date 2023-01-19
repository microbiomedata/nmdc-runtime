from typing import Literal, Optional, Union

from pydantic import BaseModel, Field


class Sequencing(BaseModel):
    Name: str = "Sequencing"
    Enabled: bool = False
    Git_repo: str = ""
    Version: str = "v1.0.0"
    Activity: Literal[
        "metagenome_sequencing_activity_set"
    ] = "metagenome_sequencing_activity_set"
    Predecessor: str = ""
    Input_prefix: str = ""
    Inputs: list = []


class ReadQcAnalysisInputs(BaseModel):
    input_files: str = "Metagenome Raw Reads"
    informed_by: str = ""
    resource: str = "NERSC-Cori"
    proj: str = ""


class ReadQcAnalysis(BaseModel):
    Name: str = "Read QC Analysis"
    Enabled: bool = True
    Git_repo: str = " https://github.com/microbiomedata/ReadsQC"
    Version: str = "v1.0.6"
    WDL: str = "rqcfilter.wdl"
    Activity: Literal["read_qc_analysis_activity_set"] = "read_qc_analysis_activity_set"
    Predecessor: str = "Sequencing"
    Input_prefix: str = "nmdc_rqcfilter"
    ID_type: str = "mgrc"
    Inputs: ReadQcAnalysisInputs = ReadQcAnalysisInputs()


class MetagenomeAnnotationInputs(BaseModel):
    input_file: str = "Assembly Contigs"
    imgap_project_id: str = "actid"
    resource: str = "NERSC-Cori"
    proj: str = ""
    informed_by: str = ""
    git_url: str = "https://github.com/microbiomedata/mg_annotation"


class MetagenomeAnnotation(BaseModel):
    Name: str = "Metagenome Annotation"
    Enabled: bool = True
    Git_repo: str = "https://github.com/microbiomedata/mg_annotation"
    Version: str = "v1.0.0-beta"
    WDL: str = "annotation_full.wdl"
    Activity: Literal[
        "metagenome_annotation_activity_set"
    ] = "metagenome_annotation_activity_set"
    Predecessor: str = "MetagenomeAssembly"
    Input_prefix: str = "annotation"
    ID_type: str = "mgann"
    Inputs: MetagenomeAnnotationInputs = MetagenomeAnnotationInputs()


class MetagenomeAssemblyInputs(BaseModel):
    input_file: str = "Filtered Sequencing Reads"
    rename_contig_prefix: str = "actid"
    resource: str = "NERSC-Cori"
    proj: str = ""
    informed_by: str = ""
    git_url: str = "https://github.com/microbiomedata/meta_assembly"


class MetagenomeAssembly(BaseModel):
    Name: str = "Metagenome Assembly"
    Enabled: bool = True
    Git_repo: str = "https://github.com/microbiomedata/metaAssembly"
    Version: str = "v1.0.3-beta"
    WDL: str = "jgi_assembly.wdl"
    Activity: Literal[
        "metagenome_assembly_activity_set"
    ] = "metagenome_assembly_activity_set"
    Predecessor: str = "Read QC Analysis"
    Input_prefix: str = "jgi_metaASM"
    ID_type: str = "mgasm"
    Inputs: MetagenomeAssemblyInputs = MetagenomeAssemblyInputs()


class MAGsInputs(BaseModel):
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


class MAGs(BaseModel):
    Name: str = "MAGs"
    Enabled: bool = True
    Git_repo: str = "https://github.com/microbiomedata/metaMAGs"
    Version: str = "v1.0.4-beta"
    WDL: str = "mbin_nmdc.wdl"
    Activity: Literal["mags_activity_set"] = "mags_activity_set"
    Predecessor: str = "Metagenome Annotation"
    Input_prefix: str = "nmdc_mags"
    ID_type: str = "mgmag"
    Inputs: MAGsInputs = MAGsInputs()


class ReadBasedAnalysisInputs(BaseModel):
    input_file: str = "Filtered Sequencing Reads"
    prefix: str = "actid"
    resource: str = "NERSC-Cori"
    proj: str = ""
    informed_by: str = ""
    git_url: str = "https://github.com/microbiomedata/ReadbasedAnalysis"
    url_root: str = "https://data.microbiomedata.org/data/"


class ReadBasedAnalysis(BaseModel):
    Name: str = "Readbased Analysis"
    Enabled: bool = True
    Git_repo: str = "https://github.com/microbiomedata/ReadbasedAnalysis"
    Version: str = "v1.0.2-beta"
    WDL: str = "ReadbasedAnalysis.wdl"
    Activity: Literal[
        "read_based_analysis_activity_set"
    ] = "read_based_analysis_activity_set"
    Predecessor: str = "Read QC Analysis"
    Input_prefix: str = "nmdc_rba"
    ID_type: str = "mgrba"
    Inputs: ReadBasedAnalysisInputs = ReadBasedAnalysisInputs()


class WorkflowModel(BaseModel):
    workflow: Union[
        ReadQcAnalysis,
        MetagenomeAssembly,
        MAGs,
        ReadBasedAnalysis,
        Sequencing,
        MetagenomeAnnotation,
    ] = Field(..., discriminator="Activity")


def get_all_workflows():
    return [
        ReadQcAnalysis(Inputs=ReadQcAnalysisInputs()),
        MetagenomeAssembly(Inputs=MetagenomeAssemblyInputs()),
        MetagenomeAnnotation(Inputs=MetagenomeAnnotationInputs()),
        MAGs(Inputs=MAGsInputs()),
        ReadBasedAnalysis(Inputs=ReadBasedAnalysisInputs()),
        Sequencing(),
    ]
