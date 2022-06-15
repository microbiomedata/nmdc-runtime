"""
@author: Michal Babinski
"""

import re

from BCBio import GFF
import pandas as pd


def prepare_curie(k: str, term: str) -> str:
    """
    Given a key and a term, prepare a CURIE for the term.

    Parameters
    ----------
    k: str
        The key
    term: str
        A database entity

    Returns
    -------
    str
        A CURIE representation of the given term

    """

    if re.match(r"^[^ <()>:]*:[^/ :]+$", term):
        prefix, reference = term.split(":", 1)
        if prefix == "KO":
            curie = f"KEGG.ORTHOLOGY:{reference}"
        else:
            curie = term
    else:
        if k.lower() == "ko":
            curie = f"KEGG.ORTHOLOGY:{term}"
        elif k.lower() == "pfam":
            curie = f"PFAM:{term}"
        elif k.lower() == "smart":
            curie = f"SMART:{term}"
        elif k.lower() == "cog":
            curie = f"EGGNOG:{term}"
        elif k.lower() == "cath_funfam":
            curie = f"CATH:{term}"
        elif k.lower() == "superfamily":
            curie = f"SUPFAM:{term}"
        elif k.lower() == "product":
            curie = term
        else:
            curie = f":{term}"
    return curie


def generate_counts(gff3file, md5_sum, informed_by):
    """
    Given gff, md5 of gff, and activity id
     Parameters
     ----------
     functional gff: gff3
        gff file containing functional annotation
     md5: str
         Unique md5 identifier
     informed by: str
         Activity ID informed by
    Returns
     -------
     json file
         Contains fields: metagenome_annotation_id | gene_function_id | count
    """

    # list of acceptable keys to extract curie term
    acceptable_terms = [
        "cog",
        "product",
        "smart",
        "cath_funfam",
        "ko",
        "ec_number",
        "pfam",
        "superfamily",
    ]

    gff3 = open(gff3file, "r")
    count_terms = {}
    for rec in GFF.parse(gff3):
        for feature in rec.features:
            qualifiers = feature.qualifiers
            for (key, value) in qualifiers.items():
                if key in acceptable_terms:
                    for term in value:
                        curie_term = prepare_curie(key, term)
                        if curie_term not in count_terms:
                            count_terms[curie_term] = 0
                        count_terms[curie_term] += 1
    gff3.close()

    with open(md5_sum, "r") as file:
        annotation_md5 = file.read().rstrip()

    df = pd.DataFrame(
        list(count_terms.items()), columns=["gene_function_id", "count"]
    )
    df = df.assign(metagenome_annotation_id=f"nmdc:{annotation_md5}")
    first_column = df.pop("metagenome_annotation_id")
    df.insert(0, "metagenome_annotation_id", first_column)

    out = df.to_json(orient="records", indent=2)

    return out
