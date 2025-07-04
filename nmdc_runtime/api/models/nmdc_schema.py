import inspect
from enum import Enum
from typing import List

from pydantic import BaseModel, Field, create_model


class FileTypeEnum(str, Enum):
    ft_icr_ms_analysis_results = "FT ICR-MS Analysis Results"
    gc_ms_metabolomics_results = "GC-MS Metabolomics Results"
    metaproteomics_workflow_statistics = "Metaproteomics Workflow Statistics"
    protein_report = "Protein Report"
    peptide_report = "Peptide Report"
    unfiltered_metaproteomics_results = "Unfiltered Metaproteomics Results"
    read_count_and_rpkm = "Read Count and RPKM"
    qc_non_rrna_r2 = "QC non-rRNA R2"
    qc_non_rrna_r1 = "QC non-rRNA R1"
    metagenome_bins = "Metagenome Bins"
    checkm_statistics = "CheckM Statistics"
    gottcha2_krona_plot = "GOTTCHA2 Krona Plot"
    kraken2_krona_plot = "Kraken2 Krona Plot"
    centrifuge_krona_plot = "Centrifuge Krona Plot"
    kraken2_classification_report = "Kraken2 Classification Report"
    kraken2_taxonomic_classification = "Kraken2 Taxonomic Classification"
    centrifuge_classification_report = "Centrifuge Classification Report"
    centrifuge_taxonomic_classification = "Centrifuge Taxonomic Classification"
    structural_annotation_gff = "Structural Annotation GFF"
    functional_annotation_gff = "Functional Annotation GFF"
    annotation_amino_acid_fasta = "Annotation Amino Acid FASTA"
    annotation_enzyme_commission = "Annotation Enzyme Commission"
    annotation_kegg_orthology = "Annotation KEGG Orthology"
    assembly_coverage_bam = "Assembly Coverage BAM"
    assembly_agp = "Assembly AGP"
    assembly_scaffolds = "Assembly Scaffolds"
    assembly_contigs = "Assembly Contigs"
    assembly_coverage_stats = "Assembly Coverage Stats"
    filtered_sequencing_reads = "Filtered Sequencing Reads"
    qc_statistics = "QC Statistics"
    tigrfam_annotation_gff = "TIGRFam Annotation GFF"
    clusters_of_orthologous_groups__cog__annotation_gff = (
        "Clusters of Orthologous Groups (COG) Annotation GFF"
    )
    cath_funfams__functional_families__annotation_gff = (
        "CATH FunFams (Functional Families) Annotation GFF"
    )
    superfam_annotation_gff = "SUPERFam Annotation GFF"
    smart_annotation_gff = "SMART Annotation GFF"
    pfam_annotation_gff = "Pfam Annotation GFF"
    direct_infusion_ft_icr_ms_raw_data = "Direct Infusion FT ICR-MS Raw Data"


class DataObject(BaseModel):
    id: str = Field(None)
    name: str = Field(None, description="A human readable label for an entity")
    description: str = Field(
        None, description="a human-readable description of a thing"
    )
    alternative_identifiers: List[str] = Field(
        None, description="A list of alternative identifiers for the entity."
    )
    compression_type: str = Field(
        None, description="If provided, specifies the compression type"
    )
    data_object_type: FileTypeEnum = Field(None)
    file_size_bytes: int = Field(None, description="Size of the file in bytes")
    md5_checksum: str = Field(None, description="MD5 checksum of file (pre-compressed)")
    type: str = Field(
        "nmdc:DataObject",
        description="An optional string that specifies the type object.  This is used to allow for searches for different kinds of objects.",
    )
    url: str = Field(None)
    was_generated_by: str = Field(None)


list_request_ops_per_field_type = {
    (object,): ["$eq", "$neq", "$in", "$nin"],
    (str,): ["$regex"],
    (int, float): ["$gt", "$gte", "$lt", "$lte"],
}

list_request_ops_with_many_args = {"$in", "$nin"}


def create_list_request_model_for(cls):
    field_filters = []
    sig = inspect.signature(cls)
    for p in sig.parameters.values():
        field_name, field_type = p.name, p.annotation
        if hasattr(field_type, "__origin__"):  # a GenericAlias object, e.g. List[str].
            field_type = field_type.__args__[0]
        field_default = (
            None if p.default in (inspect.Parameter.empty, []) else p.default
        )
        field_ops = []
        for types_ok, type_ops in list_request_ops_per_field_type.items():
            if field_type in types_ok or isinstance(field_type, types_ok):
                field_ops.extend(type_ops)
        for op in field_ops:
            field_filters.append(
                {
                    "name": field_name,
                    "arg_type": field_type,
                    "default": field_default,
                    "op": op,
                }
            )
    create_model_kwargs = {"max_page_size": (int, 20), "page_token": (str, None)}
    for ff in field_filters:
        model_field_name = f'{ff["name"]}_{ff["op"][1:]}'
        model_field_type = ff["arg_type"]
        create_model_kwargs[model_field_name] = (model_field_type, ff["default"])
    return create_model(f"{cls.__name__}ListRequest", **create_model_kwargs)


def list_request_filter_to_mongo_filter(req: dict):
    filter_ = {}
    for k, v in req.items():
        if not v:
            continue
        field, op = k.rsplit("_", maxsplit=1)
        op = f"${op}"
        if field not in filter_:
            filter_[field] = {}
        if op not in filter_[field]:
            if op in list_request_ops_with_many_args:
                filter_[field][op] = v.split(",")
            else:
                filter_[field][op] = v
    return filter_


DataObjectListRequest = create_list_request_model_for(DataObject)
