BIOME = "ENVO:00000428"
ENVIRONMENTAL_MATERIAL = "ENVO:00010483"
ASTRONOMICAL_BODY_PART = "ENVO:01000813"

ENV_TRIAD_FIELDS = ["env_broad_scale", "env_local_scale", "env_medium"]

FIELD_HIERARCHY_RULES = {
    "env_broad_scale": {"required": BIOME, "disallowed": []},
    "env_medium": {"required": ENVIRONMENTAL_MATERIAL, "disallowed": [BIOME]},
    "env_local_scale": {"required": ASTRONOMICAL_BODY_PART, "disallowed": [BIOME]},
}

# Non-ENVO prefixes allowed for env_local_scale (skip hierarchy check).
# PO (Plant Ontology) and UBERON (anatomy) terms are valid for env_local_scale
# but lack ENVO ancestry, so the ASTRONOMICAL_BODY_PART hierarchy check
# does not apply to them.
LOCAL_SCALE_EXTRA_PREFIXES = {"PO", "UBERON"}


def _extract_curie(biosample, field_name):
    """Extract the CURIE from a biosample's env triad field, or None."""
    field_val = biosample.get(field_name)
    if not isinstance(field_val, dict):
        return None
    term = field_val.get("term")
    if not isinstance(term, dict):
        return None
    return term.get("id")


def _prefetch_ontology_data(curies, db):
    """Batch-fetch ontology existence, obsolete status, and ancestry from MongoDB.

    Returns (existing_curies, obsolete_curies, ancestry_map) where:
      - existing_curies: set of CURIEs found in ontology_class_set
      - obsolete_curies: set of CURIEs that are marked is_obsolete=True
      - ancestry_map: dict mapping each CURIE to the set of ancestor CURIEs
        it relates to via the entailed_isa_partof_closure predicate
    """
    curie_list = list(curies)

    existing_curies = set()
    obsolete_curies = set()
    for doc in db["ontology_class_set"].find(
        {"id": {"$in": curie_list}}, {"id": 1, "is_obsolete": 1}
    ):
        existing_curies.add(doc["id"])
        if doc.get("is_obsolete"):
            obsolete_curies.add(doc["id"])

    ancestor_ids = [
        BIOME,
        ENVIRONMENTAL_MATERIAL,
        ASTRONOMICAL_BODY_PART,
    ]
    ancestry_map = {c: set() for c in curie_list}
    for doc in db["ontology_class_set"].find(
        {"id": {"$in": curie_list}},
        {"id": 1, "relations": 1},
    ):
        for rel in doc.get("relations", []):
            if (
                rel.get("predicate") == "entailed_isa_partof_closure"
                and rel.get("object") in ancestor_ids
            ):
                ancestry_map[doc["id"]].add(rel["object"])

    return existing_curies, obsolete_curies, ancestry_map


def _validate_single_biosample(
    biosample, existing_curies, obsolete_curies, ancestry_map
):
    """Validate env triad fields for a single biosample dict.

    Returns a list of error strings. Empty list means valid.
    """
    errors = []
    seen_curies = {}

    for field_name in ENV_TRIAD_FIELDS:
        curie = _extract_curie(biosample, field_name)

        if curie is None:
            if field_name not in biosample:
                errors.append(f"Missing required field '{field_name}'.")
            else:
                errors.append(f"Field '{field_name}' is missing term.id.")
            continue

        if curie not in existing_curies:
            errors.append(f"Field '{field_name}': unknown term '{curie}'.")
            continue

        if curie in obsolete_curies:
            errors.append(f"Field '{field_name}': term '{curie}' is obsolete.")
            continue

        rules = FIELD_HIERARCHY_RULES[field_name]
        ancestors = ancestry_map.get(curie, set())

        prefix = curie.split(":")[0] if ":" in curie else ""
        skip_hierarchy = (
            field_name == "env_local_scale" and prefix in LOCAL_SCALE_EXTRA_PREFIXES
        )

        if not skip_hierarchy and rules["required"] not in ancestors:
            errors.append(
                f"Field '{field_name}': term '{curie}' is not a descendant "
                f"of required ancestor '{rules['required']}'."
            )

        for disallowed in rules["disallowed"]:
            if disallowed in ancestors:
                errors.append(
                    f"Field '{field_name}': term '{curie}' must not be "
                    f"a descendant of '{disallowed}'."
                )

        if curie in seen_curies:
            errors.append(
                f"Field '{field_name}': term '{curie}' is already used "
                f"in '{seen_curies[curie]}'."
            )
        seen_curies[curie] = field_name

    return errors


def validate_env_triad(biosamples, db):
    """Validate env_broad_scale, env_local_scale, and env_medium for biosamples.

    Args:
        biosamples: list of biosample dicts
        db: pymongo Database handle with ontology_class_set collection

    Returns:
        dict mapping biosample id to list of error strings.
        Only biosamples with errors are included. Empty dict = all valid.
    """
    all_curies = set()
    for bs in biosamples:
        for field_name in ENV_TRIAD_FIELDS:
            curie = _extract_curie(bs, field_name)
            if curie is not None:
                all_curies.add(curie)

    if not all_curies:
        existing_curies, obsolete_curies, ancestry_map = set(), set(), {}
    else:
        existing_curies, obsolete_curies, ancestry_map = _prefetch_ontology_data(
            all_curies, db
        )

    result = {}
    for bs in biosamples:
        bs_id = bs.get("id", "unknown")
        errs = _validate_single_biosample(
            bs, existing_curies, obsolete_curies, ancestry_map
        )
        if errs:
            result[bs_id] = errs

    return result
