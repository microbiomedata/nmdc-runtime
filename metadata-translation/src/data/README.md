# NMDC Data Sources

Data for the NDMDC is collected from a number of sources. This directory contains copies of these data sources used in the metadata translation process. Below is a listing from where the copies were obtained. Follow the source link to see from where the data was downloaded (note: you may need to login to the relevant Google Drive site).  

## Genomes OnLine Database (GOLD)
Data from GOLD is contained in the SQL data export file [nmdc-version5.zip](nmdc-version5.zip). Within the zip file, the following tab-delimited files are used:
- Biosample metadata: `export.sql/BIOSAMPLE_DATA_TABLE.dsv`
- Study metadata: `export.sql/STUDY_DATA_TABLE.dsv`
- Omics processing metadata: `export.sql/PROJECT_DATA_TABLE.dsv` 
- Additional metadata needed to link a biosmaple to a sequencing project (i.e., omics processing) and a study to a PI: `export.sql/PROJECT_BIOSAMPLE_DATA_TABLE.dsv`, `export.sql/CONTACT_DATA_TABLE.dsv`
- FASTQ file metadata that result from sequencing project: [ficus_project_fastq.tsv](ficus_project_fastq.tsv) ([source](https://docs.google.com/spreadsheets/d/17DscHMVs5sdoQPsDSilZdsPOnQFKRjxzTepn_w0xbU4/edit#gid=408564629))

## Environmental Molecular Sciences Lab (EMSL)
Data from EMSL is contained in the spreadsheets listed below. 
- EMSL omics processing metadata and the processing output: [EMSL_FICUS_project_process_data_export.xlsx](EMSL_FICUS_project_process_data_export.xlsx) ([source](https://docs.google.com/spreadsheets/d/1VMJ_Tvld3cZYh9NJHdItVWpV0DmOUYH4/edit#gid=969785947))
- Mappings between the omics processing and GOLD biosamples: [EMSL_Hess_Stegen_Blanchard_DatasetToMetagenomeMapping.tsv](EMSL_FICUS_project_process_data_export.xlsx) ([source](https://docs.google.com/spreadsheets/d/1Vx1X7APaqSVHoRJRhZEMf1WnkCQ85tkQ2PZzUR_uXj4/edit#gid=0))

## Joint Genome Institute (JGI)
- Mappings between FICUS study and EMSL proposal: [FICUS - JGI-EMSL Proposal - Gold Study - ID mapping and PI.xlsx](FICUS\ -\ JGI-EMSL\ Proposal\ -\ Gold\ Study\ -\ ID\ mapping\ and\ PI.xlsx) ([source](https://docs.google.com/spreadsheets/d/1BX35JZsRkA5cZ-3Y6x217T3Aif30Ptxe_SjIC7JqPx4/edit#gid=0))  
- Links a GOLD study to a DOI: [JGI-EMSL-FICUS-proposals.fnl.tsv](JGI-EMSL-FICUS-proposals.fnl.tsv) ([source](https://docs.google.com/spreadsheets/d/1sowTCYooDrOMq0ErD4s3xtgH3PLoxwa7/edit#gid=1363834365))

## Metagenome Annotation
- NMDC metagenome annotaton workflow activites metadata: [metagenome_annotation_activities.json](aim-2-workflows/metagenome_annotation_activities.json) ([source](https://portal.nersc.gov/project/m3408/meta/mg_annotation_objects.json))
- Data object (i.e., files) metadata that results from the metagenome annotaton workflow: [metagenome_annotation_data_objects.json](aim-2-workflows/metagenome_annotation_data_objects.json) ([source](https://portal.nersc.gov/project/m3408/meta/mg_annotation_data_objects.json))

## Metagenome Assembly  
- NMDC metagenome assembly workflow activites metadata: [metagenome_assembly_activities.json](aim-2-workflows/metagenome_assembly_activities.json) ([source](https://portal.nersc.gov/project/m3408/meta/metagenomeAssembly_activity.json))
- Data object (i.e., files) metadata that results from the metagenome assembly workflow: [metagenome_assembly_data_objects.json](aim-2-workflows/metagenome_assembly_data_objects.json) ([source](https://portal.nersc.gov/project/m3408/meta/metagenomeAssembly_data_objects.json))

## Read QC
- NMDC read QC workflow activites metadata: [readQC_activities.json](aim-2-workflows/readQC_activities.json) ([source](https://portal.nersc.gov/project/m3408/meta/readQC_activity.json]))
- Data object (i.e., files) metadata that results from the read QC workflow: [readQC_data_objects.json](aim-2-workflows/readQC_data_objects.json) ([source](https://portal.nersc.gov/project/m3408/meta/readQC_activity_data_objects.json))

## Metaproteomics
- Metaproteomics workflow activity metadata performed on Hess' biosamples: [Hess_metaproteomic_analysis_activities.json](aim-2-workflows/Hess_metaproteomic_analysis_activities.json) ([source](https://portal.nersc.gov/project/m3408/meta/MetaProteomicAnalysis/hessMetaProteomicAnalysis_activity.json))
- Data object (i.e., files) metadata that results from Hess' metaproteomics workflow activity: [Hess_metaproteomic_analysis_activities.json](aim-2-workflows/Hess_metaproteomic_analysis_activities.json) ([source](https://portal.nersc.gov/project/m3408/meta/MetaProteomicAnalysis/hessemsl_analysis_data_objects.json))
- Metaproteomics workflow activity metadata performed on Stegen's biosamples: [Stegen_metaproteomic_analysis_activities.json](aim-2-workflows/Stegen_metaproteomic_analysis_activities.json) ([source](https://portal.nersc.gov/project/m3408/meta/MetaProteomicAnalysis/stegenMetaProteomicAnalysis_activity.json))
- Data object (i.e., files) metadata that results from Stegen's metaproteomics workflow activity: [Stegen_emsl_analysis_data_objects.json](aim-2-workflows/Stegen_emsl_analysis_data_objects.json) ([source](https://portal.nersc.gov/project/m3408/meta/MetaProteomicAnalysis/stegenemsl_analysis_data_objects.json))
