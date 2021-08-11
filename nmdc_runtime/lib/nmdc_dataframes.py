## author: Bill Duncan
## summary: Contains methods for creating dataframes needed for NMDC ETL pipeline.

## system level modules
import pandas as pds
import jsonasobj
import json
import zipfile
import yaml
from pandas.core.dtypes.missing import notnull
from yaml import CLoader as Loader, CDumper as Dumper
from dotted_dict import DottedDict
from collections import namedtuple


def make_dataframe(
    file_name,
    subset_cols=[],
    exclude_cols=[],
    nrows=None,
    lowercase_col_names=True,
    replace_spaces=True,
    replace_hash=True,
    strip_spaces=True,
    comment_str=None,
    file_type="tsv",
    delimiter="\t",
    sheet_name=0,
    file_archive_name="",
):
    """
    Builds a pandas dataframe from the designated file.

    Args:
        file_name: The name of the file containing the data for the dataframe. If the file is not in the same directory, then specify the path as part of the file name.
        subset_cols: Specifies a specific of subset of columns to be included in the dataframe.
        exclude_cols: Specifies a specific of subset of columns to be excluded from the dataframe.
        nrows: Specifies the number of rows to be returned in the dataframe (useful for testing).
        lowercase_col_names: If true, the column names are converted to lower case.
        replace_spaces: If true, spaces in column names are replaced with underscores.
        replace_hash: If true, hashes ('#') in column names are replaced with empty strings.
        strip_spaces: If true, extra surrounding spaces are stripped from the column names.
        comment_str: Specifies the string that is used for comments with the data.
        file_type: Speicfies the type of file. Current acceptable file types are tsv, csv, and excel. Note that when using excel, you may need to specify a sheet name.
        delimiter: Specifies the delimiter character used between fields.
        sheet_name: If the files is an Excel spreadsheet, this parameter specifies a particular sheet.
        archive_name: If the file_name is contained in an zip or file, this is the name of archive file.
    Returns:
        Pandas dataframe
    """
    ## normalize paramaters for use with pandas
    if len(subset_cols) < 1:
        subset_cols = None
    if len(exclude_cols) < 1:
        exclude_cols = None

    ## check if file is contained in an archive
    file_archive = None
    if len(file_archive_name) > 1:
        file_archive = zipfile.ZipFile(file_archive_name, "r")

    ## load data from file
    if "tsv" == file_type.lower() or "csv" == file_type.lower():
        if None != file_archive:
            df = pds.read_csv(
                file_archive.open(file_name),
                sep=delimiter,
                nrows=nrows,
                comment=comment_str,
            )
        else:
            df = pds.read_csv(
                file_name, sep=delimiter, nrows=nrows, comment=comment_str
            )
    elif "excel" == file_type.lower():
        if None != file_archive:
            df = pds.read_excel(
                file_archive.open(file_name),
                sheet_name=sheet_name,
                nrows=nrows,
                comment=comment_str,
                engine="openpyxl",
            )
        else:
            df = pds.read_excel(
                file_name,
                sheet_name=sheet_name,
                nrows=nrows,
                comment=comment_str,
                engine="openpyxl",
            )
    elif "multi-sheet-excel" == file_type.lower():
        if None != file_archive:
            df = pds.concat(
                pds.read_excel(
                    file_archive.open(file_name),
                    sheet_name=None,
                    index_col=None,
                    nrows=nrows,
                    comment=comment_str,
                    engine="openpyxl",
                )
            )
        else:
            df = pds.concat(
                pds.read_excel(
                    file_name,
                    sheet_name=None,
                    index_col=None,
                    nrows=nrows,
                    comment=comment_str,
                    engine="openpyxl",
                )
            )

    ## clean column names
    df = clean_dataframe_column_names(
        df, lowercase_col_names, replace_spaces, replace_hash, strip_spaces
    )

    ## create subset of columns
    ## note: since column names are case sensitive, this needs to happen after cleaning column names
    if subset_cols:
        df = df[subset_cols]

    ## return dataframe
    return df


def clean_dataframe_column_names(
    df,
    lowercase_col_names=True,
    replace_spaces=True,
    replace_hash=True,
    strip_spaces=True,
):
    """
    Changes the column names of a dataframe into a standard format. The default settings change the column names to:
      - lower case
      - replace spaces with underscores
      - replace hash ('#') with empty string
    Args:
        df: The dataframe whose columns will be cleaned.
        lowercase_col_names: If true, the column names are converted to lower case.
        replace_spaces: If true, spaces in column names are replaced with underscores.
        replace_hash: If true, hashes ('#') in column names are replaced with empty strings.
        strip_spaces: If true, extra surrounding spaces are stripped from the column names.
    Returns:
      Pandas dataframe
    """

    ## clean column names
    if lowercase_col_names:
        df.columns = [c.strip().lower() for c in df.columns]

    if replace_spaces:
        df.columns = [c.replace(" ", "_") for c in df.columns]

    if replace_hash:
        df.columns = [c.replace("#", "") for c in df.columns]

    if strip_spaces:
        df.columns = [c.strip() for c in df.columns]

    return df


def merge_dataframes(dataframes: list, data_source_names=[]):
    merged_df = pds.DataFrame(
        columns=["nmdc_data_source", "nmdc_record_id", "attribute", "value"]
    )

    for idx, df in enumerate(dataframes):
        if "pandas.core.frame.DataFrame" == type(df):
            data_source_name = data_source_names[idx]
            data = df
        else:
            data_source_name = df.name
            data = df.data

        ## convert data into an EAV structure
        eavdf = data.melt(id_vars=["nmdc_record_id"], var_name="attribute")
        eavdf["nmdc_data_source"] = data_source_name
        # print(data_source_name, len(eavdf))

        merged_df = merged_df.append(eavdf, ignore_index=True)

    return merged_df


def make_dataframe_dictionary(
    file_name,
    subset_cols=[],
    exclude_cols=[],
    nrows=None,
    lowercase_col_names=True,
    replace_spaces=True,
    file_type="tsv",
    delimiter="\t",
    sheet_name=0,
    file_archive_name="",
):
    """
    Builds a dictionary based on the structure of the pandas dataframe generated from the designated file.
    The dictionary is oriented for records.
    E.g.:
      [
        {
          'col1': 1,
          'col2': 0.5
        },
        {
          'col1': 2,
          'col2': 0.75
        }
      ]

    Essentially, this function is a shortcut for calling make_dataframe() and then transforming the result into a dictionary.
    E.g.:
      df = make_dataframe(file_name)
      dictdf = dictdf = df.to_dict(orient="records")


    Args:
        file_name: The name of the file containing the data for the dataframe. If the file is not in the same directory, then specify the path as part of the file name.
        subset_cols: Specifies a specific of subset of columns to be included in the dataframe.
        exclude_cols: Specifies a specific of subset of columns to be excluded from the dataframe.
        nrows: Specifies the number of rows to be returned in the dataframe (useful for testing).
        lowercase_col_names: If true, the column names are converted to lower case.
        replace_spaces: If true, spaces in column names are replaced with spaces.
        file_type: Speicfies the type of file. Current acceptable file types are tsv, csv, and excel. Note that when using excel, you may need to specify a sheet name.
        delimiter: Specifies the delimiter character used between fields.
        sheet_name: If the files is an Excel spreadsheet, this parameter specifies a particular sheet.
        archive_name: If the file_name is contained in an zip or file, this is the name of archive file.
    Returns:
        Dictionary built from a Pandas dataframe.
    """
    df = make_dataframe(
        file_name,
        subset_cols=[],
        exclude_cols=[],
        nrows=None,
        lowercase_col_names=True,
        replace_spaces=True,
        file_type="tsv",
        delimiter=delimiter,
        sheet_name=sheet_name,
        file_archive_name=file_archive_name,
    )
    return df.to_dict(orient="records")


def make_collection_date(year_val, month_val, day_val, hour_val="", minute_val=""):
    def pad_value(val, pad_len=2):
        s = str(val)
        return s.zfill(pad_len)

    return_val = ""
    year_val = year_val.strip()
    month_val = month_val.strip()
    day_val = day_val.strip()
    hour_val = hour_val.strip()
    minute_val = minute_val.strip()
    return_val = ""

    ## if a year isn't provided simply return the empty string
    if len(year_val) < 1:
        return ""
    else:
        return_val = pad_value(year_val, 4)

    if len(month_val) > 0:
        return_val = return_val + "-" + pad_value(month_val)

    ## we only days that have months assocated with them
    if (len(month_val) > 0) and (len(day_val) > 0):
        return_val = return_val + "-" + pad_value(day_val)

    ## we only want times with months and days associated with them
    if (len(month_val) > 0) and (len(day_val) > 0):
        if (len(hour_val) > 0) and (len(minute_val) > 0):
            return_val = return_val + "T" + pad_value(hour_val) + ":" + minute_val
        elif len(hour_val) > 0:
            return_val = (
                return_val + "T" + pad_value(hour_val) + "00"
            )  # case for when no minute val is given

    return return_val


def make_lat_lon(latitude, longitude):
    # latitude = "" if pds.isnull(latitude) else str(latitude).strip().replace('\n', '')
    # longitude = "" if pds.isnull(longitude) else str(longitude).strip().replace('\n', '')
    latitude = None if pds.isnull(latitude) else float(latitude)
    longitude = None if pds.isnull(longitude) else float(longitude)

    if (not (latitude is None)) and (not (longitude is None)):
        return f"{latitude} {longitude}".strip()
    else:
        return None


def make_study_dataframe(study_table, contact_table, proposals_table, result_cols=[]):
    ## subset dataframes
    contact_table_splice = contact_table[
        ["contact_id", "principal_investigator_name"]
    ].copy()
    proposals_table_splice = proposals_table[["gold_study", "doi"]].copy()

    ## make sure the contact ids are strings with the ".0" removed from the end (i.e., the strings aren't floats)
    study_table["contact_id"] = (
        study_table["contact_id"].astype(str).replace("\.0", "", regex=True)
    )
    contact_table_splice["contact_id"] = (
        contact_table_splice["contact_id"].astype(str).replace("\.0", "", regex=True)
    )
    # print(study_table[['contact_id', 'principal_investigator_name']].head())

    ## left join data from contact
    temp1_df = pds.merge(study_table, contact_table_splice, how="left", on="contact_id")

    ## left join data from proposals
    temp2_df = pds.merge(
        temp1_df,
        proposals_table_splice,
        how="left",
        left_on="gold_id",
        right_on="gold_study",
    )

    ## add prefix
    temp2_df.gold_id = "gold:" + temp2_df.gold_id
    temp2_df.gold_study = "gold:" + temp2_df.gold_study

    if len(result_cols) > 0:
        return temp2_df[result_cols]
    else:
        return temp2_df


def make_project_dataframe(
    project_table,
    study_table,
    contact_table,
    data_object_table,
    project_biosample_table=None,
    biosample_table=None,
    result_cols=[],
):
    ## subset data
    study_table_splice = study_table[["study_id", "gold_id"]].copy()
    contact_table_splice = contact_table[
        ["contact_id", "principal_investigator_name"]
    ].copy()

    ## rename study.gold_id to study_gold_id
    study_table_splice.rename(columns={"gold_id": "study_gold_id"}, inplace=True)

    ## inner join on study (project must be part of study)
    temp1_df = pds.merge(
        project_table,
        study_table_splice,
        how="inner",
        left_on="master_study_id",
        right_on="study_id",
    )

    ## left join contact data
    temp2_df = pds.merge(
        temp1_df,
        contact_table_splice,
        how="left",
        left_on="pi_id",
        right_on="contact_id",
    )

    ## add prefix
    temp2_df.gold_id = "gold:" + temp2_df.gold_id
    temp2_df.study_gold_id = "gold:" + temp2_df.study_gold_id

    ## if present join data objects as output of project
    if not (data_object_table is None):
        ## make copy and add prefix
        data_object_table = data_object_table.copy()
        data_object_table.gold_project_id = data_object_table.gold_project_id.map(
            lambda x: x if "gold:" == x[0:5] else "gold:" + x
        )

        ## create a group concat for all file ids in the data objects
        groups = data_object_table.groupby("gold_project_id")["file_id"]
        output_files = (
            pds.DataFrame(groups.apply(lambda x: ",".join(filter(None, x))))
            .drop_duplicates()
            .reset_index()
        )
        output_files.rename(columns={"file_id": "output_file_ids"}, inplace=True)
        output_files["output_file_ids"] = output_files["output_file_ids"].astype(str)

        ## require output files for projects (i.e., inner join)
        temp2_df = pds.merge(
            temp2_df,
            output_files,
            how="inner",
            left_on="gold_id",
            right_on="gold_project_id",
        )

    ## if present join biosamples as inputs to project
    if (not (project_biosample_table is None)) and (not (biosample_table is None)):
        ## make local copies & rename column
        project_biosample_table = project_biosample_table.copy()
        biosample_table = biosample_table[["biosample_id", "gold_id"]].copy()
        biosample_table.rename(columns={"gold_id": "biosample_gold_id"}, inplace=True)

        ## add prefix
        biosample_table["biosample_gold_id"] = biosample_table["biosample_gold_id"].map(
            lambda x: x if "gold:" == x[0:5] else "gold:" + x
        )

        ## join project biosamples to biosamples
        input_samples = pds.merge(
            project_biosample_table, biosample_table, how="inner", on="biosample_id"
        )
        # require input samples (i.e., inner join)
        temp2_df = pds.merge(temp2_df, input_samples, how="inner", on="project_id")

    if len(result_cols) > 0:
        return temp2_df[result_cols]
    else:
        return temp2_df


def make_biosample_dataframe(
    biosample_table,
    soil_package_table,
    water_package_table,
    project_biosample_table,
    project_table,
    study_table,
    result_cols=[],
):
    def make_collection_date_from_row(row):
        def _format_date_part_value(val):
            if pds.isnull(val):
                return ""

            if type("") == type(val):
                if "." in val:
                    return val[0 : val.find(".")].strip()
                else:
                    return val.strip()
            else:
                return str(int(val)).strip()

        year_val = _format_date_part_value(row["sample_collection_year"])
        month_val = _format_date_part_value(row["sample_collection_month"])
        day_val = _format_date_part_value(row["sample_collection_day"])
        hour_val = _format_date_part_value(row["sample_collection_hour"])
        minute_val = _format_date_part_value(row["sample_collection_minute"])

        return make_collection_date(year_val, month_val, day_val, hour_val, minute_val)

    ## subset data
    project_biosample_table_splice = project_biosample_table[
        ["biosample_id", "project_id"]
    ].copy()
    project_table_splice = project_table[
        ["project_id", "gold_id", "master_study_id"]
    ].copy()
    study_table_splice = study_table[["study_id", "gold_id"]].copy()

    ## add prefix
    project_table_splice.gold_id = "gold:" + project_table_splice.gold_id
    study_table_splice.gold_id = "gold:" + study_table_splice.gold_id

    ## rename columns
    project_table_splice.rename(columns={"gold_id": "project_gold_id"}, inplace=True)
    study_table_splice.rename(columns={"gold_id": "study_gold_id"}, inplace=True)

    ## inner join projects and studies
    project_table_splice = pds.merge(
        project_table_splice,
        study_table_splice,
        how="inner",
        left_on="master_study_id",
        right_on="study_id",
    )

    ## drop biosample rows that don't have required fields
    biosample_table = biosample_table[biosample_table["env_broad_scale"].notnull()]
    biosample_table = biosample_table[biosample_table["env_local_scale"].notnull()]
    biosample_table = biosample_table[biosample_table["env_medium"].notnull()]

    ## left join package tables to biosample table
    temp0_df = pds.merge(
        biosample_table, soil_package_table, how="left", on="soil_package_id"
    )
    temp0_df = pds.merge(
        temp0_df, water_package_table, how="left", on="water_package_id"
    )

    ## inner join on project_biosample and project; i.e., biosamples must be linked to project
    temp1_df = pds.merge(
        temp0_df, project_biosample_table_splice, how="inner", on="biosample_id"
    )
    temp2_df = pds.merge(temp1_df, project_table_splice, how="inner", on="project_id")

    ## add collection date and lat_lon columns
    temp2_df["collection_date"] = temp2_df.apply(
        lambda row: make_collection_date_from_row(row), axis=1
    )
    temp2_df["lat_lon"] = temp2_df.apply(
        lambda row: make_lat_lon(row.latitude, row.longitude), axis=1
    )

    ## convert latitude and longitute columns to floats
    temp2_df["latitude"] = temp2_df["latitude"].map(
        lambda x: None if pds.isnull(x) else float(x)
    )
    temp2_df["longitude"] = temp2_df["longitude"].map(
        lambda x: None if pds.isnull(x) else float(x)
    )

    ## add gold prefix
    temp2_df["gold_id"] = "gold:" + temp2_df["gold_id"]

    ## biosample might belong to more than one project; so do the equivalent of a group_cat
    ## see: https://queirozf.com/entries/pandas-dataframe-groupby-examples
    ## see: https://stackoverflow.com/questions/18138693/replicating-group-concat-for-pandas-dataframe
    groups = (
        temp2_df.groupby("biosample_id")["project_gold_id"]
        .apply(lambda pid: ",".join(filter(None, pid)))
        .reset_index()
    )
    groups.rename(columns={"project_gold_id": "project_gold_ids"}, inplace=True)

    # join concat groups to dataframe
    temp3_df = pds.merge(temp2_df, groups, how="left", on="biosample_id")

    ## A biosample may belong to multiple projects
    # E.g. see biosample_id 247352 with gold_id "Gb0247352", belongs to projects 467278, 467306
    ## So, remove uneeded columns & drop dups
    temp3_df.drop(columns=["project_gold_id"], inplace=True)
    temp3_df.drop(columns=["project_id"], inplace=True)
    temp3_df.drop_duplicates(inplace=True)

    ## for 'env_broad_scale', 'env_local_scale', 'env_medium' fields change 'ENVO_' to 'ENVO:'
    # temp3_df['env_broad_scale'] = temp3_df
    for idx in temp3_df.index:
        if pds.notnull(temp3_df.loc[idx, "env_broad_scale"]):
            temp3_df.loc[idx, "env_broad_scale"] = str(
                temp3_df.loc[idx, "env_broad_scale"]
            ).replace("_", ":", 1)
        if pds.notnull(temp3_df.loc[idx, "env_local_scale"]):
            temp3_df.loc[idx, "env_local_scale"] = str(
                temp3_df.loc[idx, "env_local_scale"]
            ).replace("_", ":", 1)
        if pds.notnull(temp3_df.loc[idx, "env_medium"]):
            temp3_df.loc[idx, "env_medium"] = str(
                temp3_df.loc[idx, "env_medium"]
            ).replace("_", ":", 1)

    if len(result_cols) > 0:
        return temp3_df[result_cols]
    else:
        return temp3_df


def make_jgi_emsl_dataframe(jgi_emsl_table, study_table, result_cols=[]):
    ## subset data
    study_table_splice = study_table[["study_id", "gold_id"]].copy()

    ## inner join jgi-emsl data on study (must be part of study)
    temp1_df = pds.merge(
        jgi_emsl_table,
        study_table_splice,
        how="inner",
        left_on="gold_study_id",
        right_on="gold_id",
    )

    ## add prefix
    temp1_df.gold_id = "gold:" + temp1_df.gold_id
    temp1_df.gold_study_id = "gold:" + temp1_df.gold_study_id

    if len(result_cols) > 0:
        return temp1_df[result_cols]
    else:
        return temp1_df


def make_emsl_dataframe(
    emsl_table, jgi_emsl_table, study_table, emsl_biosample_table, result_cols=[]
):
    ## subset data
    study_table_splice = study_table[["study_id", "gold_id"]].copy()
    jgi_emsl_table_splice = jgi_emsl_table[["gold_study_id", "emsl_proposal_id"]].copy()
    biosample_slice = emsl_biosample_table[["dataset_id", "biosample_gold_id"]].copy()
    biosample_slice["biosample_gold_id"] = (
        "gold:" + biosample_slice["biosample_gold_id"]
    )  # add prefix

    ## inner join jgi-emsl data on study (must be part of study)
    temp1_df = pds.merge(
        jgi_emsl_table_splice,
        study_table_splice,
        how="inner",
        left_on="gold_study_id",
        right_on="gold_id",
    )

    ## inner join emsl data on jgi-emsl proposal ids
    temp2_df = pds.merge(emsl_table, temp1_df, how="inner", on="emsl_proposal_id")

    ## add data obect id column
    temp2_df["data_object_id"] = "output_"
    temp2_df["data_object_id"] = temp2_df["data_object_id"] + temp2_df[
        "dataset_id"
    ].map(
        str
    )  # build data object id

    ## add data object name column
    temp2_df["data_object_name"] = "output: "
    temp2_df["data_object_name"] = temp2_df["data_object_name"] + temp2_df[
        "dataset_name"
    ].map(
        str
    )  # build data object id

    ## group concat & join the biosample ids that are inputs to the omics process
    ## With filter function as None, the function defaults to Identity function,
    ## and each element in random_list is checked if it's true or not.
    ## see https://www.programiz.com/python-programming/methods/built-in/filter
    groups = biosample_slice.groupby("dataset_id")["biosample_gold_id"]
    input_biosamples = (
        pds.DataFrame(groups.apply(lambda x: ",".join(filter(None, x))))
        .drop_duplicates()
        .reset_index()
    )

    input_biosamples.reset_index(inplace=True)  # make dataset_id a column
    input_biosamples.rename(
        columns={"biosample_gold_id": "biosample_gold_ids"}, inplace=True
    )  # change column name

    input_biosamples["biosample_gold_ids"] = input_biosamples[
        "biosample_gold_ids"
    ].astype(
        str
    )  # make sure biosample_ids are strings

    temp2_df = pds.merge(temp2_df, input_biosamples, how="left", on="dataset_id")

    ## add "emsl:TBD" id for missing biosamples
    temp2_df["biosample_gold_ids"] = temp2_df["biosample_gold_ids"].map(
        lambda x: "emsl:TBD" if pds.isnull(x) else x
    )

    ## add prefix
    temp2_df.gold_id = "gold:" + temp2_df.gold_id
    temp2_df.gold_study_id = "gold:" + temp2_df.gold_study_id
    temp2_df.dataset_id = "emsl:" + temp2_df.dataset_id
    temp2_df.data_object_id = "emsl:" + temp2_df.data_object_id

    ## replace NaNs with None
    temp2_df = temp2_df.where(pds.notnull(temp2_df), None)

    ## drop duplicates
    temp2_df.drop_duplicates(inplace=True)

    if len(result_cols) > 0:
        return temp2_df[result_cols]
    else:
        return temp2_df


def make_data_objects_dataframe(
    faa_table, fna_table, fastq_table, project_table, result_cols=[]
):
    ## subset data
    project_table_splice = project_table[["gold_id"]].copy()

    ## copy tables
    faa_df = faa_table.copy()
    fna_df = fna_table.copy()
    fastq_df = fastq_table.copy()

    ## add prefixes for faa, fna, and fastq files
    faa_df.file_id = "nmdc:" + faa_df.file_id
    fna_df.file_id = "nmdc:" + fna_df.file_id
    fastq_df.file_id = "jgi:" + fastq_df.file_id

    ## merge tables
    data_objects = pds.concat([faa_df, fna_df, fastq_df], axis=0)

    ## inner joing data objects (e.g., faa, fna, fasq) to projects
    temp1_df = pds.merge(
        data_objects,
        project_table_splice,
        how="inner",
        left_on="gold_project_id",
        right_on="gold_id",
    )

    ## add prefix for gold
    temp1_df.gold_project_id = "gold:" + temp1_df.gold_project_id
    temp1_df.gold_id = "gold:" + temp1_df.gold_id

    if len(result_cols) > 0:
        return temp1_df[result_cols]
    else:
        return temp1_df[data_objects.columns]


def make_jgi_fastq_dataframe(fastq_table, project_table, result_cols=[]):
    ## subset data
    project_table_splice = project_table[["gold_id"]].copy()

    ## copy tables
    fastq_df = fastq_table.copy()

    ## add prefixes for fastq file id
    fastq_df.file_id = "jgi:" + fastq_df.file_id

    ## inner join to projects
    temp1_df = pds.merge(
        fastq_df,
        project_table_splice,
        how="inner",
        left_on="gold_project_id",
        right_on="gold_id",
    )

    ## add prefix for gold
    temp1_df.gold_project_id = "gold:" + temp1_df.gold_project_id
    temp1_df.gold_id = "gold:" + temp1_df.gold_id

    if len(result_cols) > 0:
        return temp1_df[result_cols]
    else:
        return temp1_df[fastq_df.columns]


def make_dataframe_from_spec_file(data_spec_file, nrows=None):
    def make_df_from_file(data_source, nrows):
        file_type = data_source["file_type"]
        fname = data_source["file_name"]

        if "file_archive_name" in data_source.keys():
            farchive = data_source["file_archive_name"]
            df = make_dataframe(
                fname, file_archive_name=farchive, file_type=file_type, nrows=nrows
            )
        else:
            df = make_dataframe(fname, file_type=file_type, nrows=nrows)

        return df

    def make_df(source, source_type="file_name"):
        name = source[0]
        data = source[1]
        data_source = source[1]["data_source"]

        if source_type not in data_source.keys():
            return None

        ## get data from file
        if "file_name" in data_source.keys():
            df = make_df_from_file(data_source, nrows=nrows)

        ## add extra columns
        if "append_columns" in data.keys():
            for col in data["append_columns"]:
                df[col["name"]] = col["value"]

        ## filter rows by specific values
        if "filters" in data.keys():
            for fltr in data["filters"]:
                if "include" in fltr:
                    df = df[
                        df[fltr["include"]["field"]].isin(fltr["include"]["values"])
                    ]
                elif "exclude" in fltr:
                    df = df[
                        ~df[fltr["exclude"]["field"]].isin(fltr["exclude"]["values"])
                    ]
                else:
                    df = df[df[fltr["field"]].isin(fltr["values"])]

        ## select a subset of the columns
        if "subset_cols" in data.keys():
            df = df[data["subset_cols"]]

        ## rename columns
        if "rename_slots" in data.keys():
            for slot in data["rename_slots"]:
                df.rename(columns={slot["old_name"]: slot["new_name"]}, inplace=True)

        ## add 'nmdc_record_id' as a primary key
        if "id_key" in data.keys():
            df["nmdc_record_id"] = df[data["id_key"]]
            df["nmdc_record_id"] = df["nmdc_record_id"].astype(
                str
            )  # ensure all keys are strings
        else:
            df.index.name = "nmdc_record_id"  # rename the current index
            df.reset_index(inplace=True)  # turn index into a column
            df["nmdc_record_id"] = df["nmdc_record_id"].astype(
                str
            )  # ensure all keys are strings

        return df

    with open(data_spec_file, "r") as input_file:
        # spec = DottedDict(yaml.load(input_file, Loader=Loader))
        spec = yaml.load(input_file, Loader=Loader)

    Data_source = namedtuple("Data_source", "data name")

    dataframes = []
    for source in spec["data_sources"].items():
        df = make_df(source)
        ds = Data_source(df, source[0])
        dataframes.append(ds)
        print(source[0], len(df))

    merged_df = merge_dataframes(dataframes)
    return merged_df
