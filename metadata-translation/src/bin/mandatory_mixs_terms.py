#!/usr/bin/env python3
# -*- coding: utf-8 -*-


## search for lib package and add to path
import os, sys, re

subdirs = [
    os.path.abspath(x[0])
    for x in os.walk(".")
    if "lib" in x[0] and "__pycache__" not in x[0]
]
[sys.path.append(p) for p in subdirs]

import click
import data_operations as dop
from pandasql import sqldf

## helper for pandasql
def pysqldf(q):
    return sqldf(q, globals())


@click.command()
@click.option("--input", "-i", help="name of mixs to process")
@click.option("--output", "-o", default="", help="name of output file")
@click.option("--delimiter", "-sep", default="\t", help="specifies delimiter for file")
@click.option(
    "--sheet-number",
    "-sheet",
    default=2,
    help="specifies the sheet number to process for Excel files",
)
@click.option(
    "--output-format",
    "-of",
    default="tsv",
    help="specifies delimiter for the output file; use xls or xlsx output Excel file",
)

## main work starts here
def main(input, output, delimiter, sheet_number, output_format):
    ## inner function to generate query string
    def get_query_str(data):
        if 1 == sheet_number:
            q = """
                select structured_comment_name, migs_eu, migs_ba, migs_pl, migs_vi, migs_org, me, mimarks_s, mimarks_c,	misag, mimag, miuvig
                from data
                where 
                  migs_eu = 'M'
                  and migs_ba = 'M'
                  and migs_pl = 'M'
                  and migs_vi = 'M'
                  and migs_org = 'M'
                  and me = 'M'
                  and mimarks_s = 'M'
                  and mimarks_c = 'M'
                  and misag = 'M'
	          and mimag = 'M'
                  and miuvig = 'M'
                """
        elif 2 == sheet_number:
            q = """
                select environmental_package, structured_comment_name, requirement
                from data
                where requirement = 'M'
                order by environmental_package, structured_comment_name
                """
        else:
            q = ""

        return q

    file_ext = os.path.splitext(input)[1]
    if ".xls" in file_ext.strip():
        data = dop.make_dataframe(
            input.strip(), file_type="excel", sheet_name=sheet_number
        )
    else:
        data = dop.make_dataframe(input, delimiter=delimiter)

    query = get_query_str(data)
    df = sqldf(query)

    if output:
        if "xls" in output_format.strip():
            df.to_excel(output)
        else:
            df.to_csv(output, sep=delimiter, index=False)


if __name__ == "__main__":
    sys.argv[0] = re.sub(r"(-script\.pyw|\.exe)?$", "", sys.argv[0])
    #    import os; print("path: ", os.path.abspath("./lib"))
    sys.exit(main())
