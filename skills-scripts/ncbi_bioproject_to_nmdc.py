#!/usr/bin/env python
"""
Translate an NCBI BioProject (with linked BioSamples and SRA runs)
into an NMDC-schema-compliant Database JSON file.

Usage:
    python ncbi_bioproject_to_nmdc.py PRJNA1452545
    python ncbi_bioproject_to_nmdc.py PRJNA1452545 --fetch-only
    python ncbi_bioproject_to_nmdc.py PRJNA1452545 --out results/my_study.json
"""

import argparse
import hashlib
import json
import re
import sys
import time
from datetime import datetime, timezone
from typing import List, Optional, Tuple

import requests
from lxml import etree
from nmdc_schema import nmdc
from linkml_runtime.dumpers import json_dumper

EUTILS_BASE = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
RATE_LIMIT_DELAY = 0.35
BATCH_SIZE = 200


# ---------------------------------------------------------------------------
# E-utils helpers
# ---------------------------------------------------------------------------

def _eutils_get(endpoint: str, params: dict) -> bytes:
    time.sleep(RATE_LIMIT_DELAY)
    url = f"{EUTILS_BASE}/{endpoint}"
    resp = requests.get(url, params=params, timeout=60)
    resp.raise_for_status()
    return resp.content


def esearch(db: str, term: str, retmax: int = 9999) -> List[str]:
    raw = _eutils_get("esearch.fcgi", {"db": db, "term": term, "retmax": retmax})
    root = etree.fromstring(raw)
    return [el.text for el in root.findall(".//Id")]


def efetch_xml(db: str, ids: List[str]) -> List[etree._Element]:
    """Fetch records in batches, return list of parsed XML roots."""
    roots = []
    for i in range(0, len(ids), BATCH_SIZE):
        batch = ids[i : i + BATCH_SIZE]
        raw = _eutils_get("efetch.fcgi", {
            "db": db,
            "id": ",".join(batch),
            "rettype": "xml",
            "retmode": "xml",
        })
        roots.append(etree.fromstring(raw))
    return roots


# ---------------------------------------------------------------------------
# Placeholder ID minting
# ---------------------------------------------------------------------------

def make_id(typecode: str, source_id: str) -> str:
    short_hash = hashlib.sha256(source_id.encode()).hexdigest()[:8]
    return f"nmdc:{typecode}-99-{short_hash}"


# ---------------------------------------------------------------------------
# XML parsing: BioProject
# ---------------------------------------------------------------------------

def fetch_bioproject(accession: str) -> dict:
    ids = esearch("bioproject", f"{accession}[Project Accession]")
    if not ids:
        ids = esearch("bioproject", accession)
    if not ids:
        sys.exit(f"ERROR: BioProject {accession} not found in NCBI.")

    roots = efetch_xml("bioproject", ids[:1])
    root = roots[0]
    proj = root.find(".//Project")
    if proj is None:
        proj = root.find(".//DocumentSummary")
    if proj is None:
        sys.exit(f"ERROR: Could not parse BioProject XML for {accession}.")

    title_el = proj.find(".//ProjectDescr/Title")
    desc_el = proj.find(".//ProjectDescr/Description")
    name_el = proj.find(".//ProjectDescr/Name")
    organism_el = proj.find(".//ProjectType/ProjectTypeSubmission/Target/Organism")
    target_el = proj.find(".//ProjectType/ProjectTypeSubmission/Target")

    publications = []
    for pub in proj.findall(".//ProjectDescr/Publication"):
        dbtype = pub.get("dbtype", "")
        pub_id = pub.get("id", "")
        if dbtype.lower() == "pubmed" and pub_id:
            publications.append(f"PMID:{pub_id}")
        doi_el = pub.find(".//Reference")
        if doi_el is not None and doi_el.text:
            publications.append(doi_el.text)

    org_name = ""
    if target_el is not None:
        org_name = target_el.get("organism", "")
    if organism_el is not None:
        org_label = organism_el.find("OrganismName")
        if org_label is not None and org_label.text:
            org_name = org_label.text

    return {
        "accession": accession,
        "uid": ids[0],
        "title": title_el.text if title_el is not None else "",
        "description": desc_el.text if desc_el is not None else "",
        "name": name_el.text if name_el is not None else "",
        "organism": org_name,
        "publications": publications,
    }


# ---------------------------------------------------------------------------
# XML parsing: SRA experiments + runs
# ---------------------------------------------------------------------------

def fetch_sra_experiments(bioproject_accession: str) -> List[dict]:
    ids = esearch("sra", f"{bioproject_accession}[BioProject]")
    if not ids:
        print(f"WARNING: No SRA records found for {bioproject_accession}.")
        return []

    print(f"  Found {len(ids)} SRA records.")
    roots = efetch_xml("sra", ids)

    experiments = []
    for root in roots:
        for pkg in root.findall(".//EXPERIMENT_PACKAGE"):
            exp = pkg.find("EXPERIMENT")
            run_set = pkg.find("RUN_SET")
            sample = pkg.find("SAMPLE")

            exp_accession = exp.get("accession", "") if exp is not None else ""

            instrument_el = exp.find(".//INSTRUMENT_MODEL") if exp is not None else None
            platform_el = exp.find(".//PLATFORM/*/INSTRUMENT_MODEL") if exp is not None else None
            platform_type_el = None
            if exp is not None:
                platform_parent = exp.find(".//PLATFORM")
                if platform_parent is not None and len(platform_parent) > 0:
                    platform_type_el = platform_parent[0]

            instrument_model = ""
            if instrument_el is not None and instrument_el.text:
                instrument_model = instrument_el.text
            elif platform_el is not None and platform_el.text:
                instrument_model = platform_el.text

            platform_type = ""
            if platform_type_el is not None:
                platform_type = platform_type_el.tag

            lib_desc = exp.find(".//LIBRARY_DESCRIPTOR") if exp is not None else None
            library_strategy = ""
            library_source = ""
            library_selection = ""
            library_layout = ""
            library_name = ""
            if lib_desc is not None:
                ls = lib_desc.find("LIBRARY_STRATEGY")
                library_strategy = ls.text if ls is not None else ""
                lsrc = lib_desc.find("LIBRARY_SOURCE")
                library_source = lsrc.text if lsrc is not None else ""
                lsel = lib_desc.find("LIBRARY_SELECTION")
                library_selection = lsel.text if lsel is not None else ""
                ll = lib_desc.find("LIBRARY_LAYOUT")
                if ll is not None and len(ll) > 0:
                    library_layout = ll[0].tag
                ln = lib_desc.find("LIBRARY_NAME")
                library_name = ln.text if ln is not None and ln.text else ""

            biosample_accession = ""
            if sample is not None:
                for xref in sample.findall(".//EXTERNAL_ID"):
                    if xref.get("namespace", "").upper() == "BIOSAMPLE":
                        biosample_accession = xref.text or ""
                        break
                if not biosample_accession:
                    for sid in sample.findall(".//IDENTIFIERS/EXTERNAL_ID"):
                        if sid.get("namespace", "").upper() == "BIOSAMPLE":
                            biosample_accession = sid.text or ""
                            break

            runs = []
            if run_set is not None:
                for run_el in run_set.findall("RUN"):
                    run_acc = run_el.get("accession", "")
                    total_bases = run_el.get("total_bases", "")
                    total_spots = run_el.get("total_spots", "")
                    runs.append({
                        "accession": run_acc,
                        "total_bases": total_bases,
                        "total_spots": total_spots,
                    })

            sample_title = ""
            if sample is not None:
                t = sample.find("TITLE")
                sample_title = t.text if t is not None else ""

            experiments.append({
                "experiment_accession": exp_accession,
                "biosample_accession": biosample_accession,
                "sample_title": sample_title,
                "instrument_model": instrument_model,
                "platform_type": platform_type,
                "library_strategy": library_strategy,
                "library_source": library_source,
                "library_selection": library_selection,
                "library_layout": library_layout,
                "library_name": library_name,
                "runs": runs,
            })

    return experiments


# ---------------------------------------------------------------------------
# XML parsing: BioSamples
# ---------------------------------------------------------------------------

def fetch_linked_biosample_uids(bioproject_uid: str) -> Optional[List[str]]:
    """Return BioSample UIDs linked to a BioProject via elink.

    Returns None if the elink query fails (NCBI outage, transient error, etc.)
    so callers can distinguish 'no biosamples' from 'lookup failed'.
    """
    try:
        raw = _eutils_get("elink.fcgi", {
            "dbfrom": "bioproject",
            "db": "biosample",
            "id": bioproject_uid,
            "linkname": "bioproject_biosample_all",
        })
        root = etree.fromstring(raw)
        err_el = root.find(".//ERROR")
        if err_el is not None and err_el.text:
            print(f"  WARNING: elink bioproject→biosample returned error: {err_el.text.strip()}")
            return None
        return [el.text for el in root.findall(".//LinkSetDb/Link/Id") if el.text]
    except Exception as e:
        print(f"  WARNING: elink bioproject→biosample call failed: {e}")
        return None


def _fetch_biosample_records(ids: List[str]) -> List[dict]:
    """Fetch and parse BioSample records for the given UIDs or accessions."""
    if not ids:
        return []

    print(f"  Fetching {len(ids)} BioSample records...")
    roots = efetch_xml("biosample", ids)

    samples = []
    for root in roots:
        for bs in root.findall(".//BioSample"):
            accession = bs.get("accession", "")
            attrs = {}
            for attr in bs.findall(".//Attribute"):
                key = attr.get("harmonized_name") or attr.get("attribute_name", "")
                if key:
                    attrs[key] = attr.text or ""

            title_el = bs.find(".//Description/Title")
            organism_el = bs.find(".//Description/Organism")
            org_name = ""
            taxonomy_id = ""
            if organism_el is not None:
                org_name = organism_el.get("taxonomy_name", "")
                taxonomy_id = organism_el.get("taxonomy_id", "")

            package_el = bs.find(".//Package")
            package = package_el.text if package_el is not None else ""

            model_els = bs.findall(".//Model")
            models = [m.text for m in model_els if m.text]

            samples.append({
                "accession": accession,
                "title": title_el.text if title_el is not None else "",
                "organism": org_name,
                "taxonomy_id": taxonomy_id,
                "package": package,
                "models": models,
                "attributes": attrs,
            })

    return samples


def fetch_biosamples(accessions: List[str]) -> List[dict]:
    """Fetch BioSample records by accession (e.g. SAMN*).

    Looks up UIDs via esearch, then efetches. Falls back to passing the
    accession strings directly to efetch if esearch returns nothing.
    """
    if not accessions:
        return []

    ids = esearch("biosample", " OR ".join(f"{a}[Accession]" for a in accessions))
    if not ids:
        ids = accessions
    return _fetch_biosample_records(ids)


# ---------------------------------------------------------------------------
# Lat/lon parsing
# ---------------------------------------------------------------------------

def parse_lat_lon(raw: str) -> Optional[Tuple[float, float]]:
    """Parse lat/lon strings like '34.27 N 108.08 E' or '34.27, -108.08'."""
    m = re.match(
        r"([+-]?\d+\.?\d*)\s*([NSns])[\s,]+([+-]?\d+\.?\d*)\s*([EWew])",
        raw.strip(),
    )
    if m:
        lat = float(m.group(1))
        if m.group(2).upper() == "S":
            lat = -lat
        lon = float(m.group(3))
        if m.group(4).upper() == "W":
            lon = -lon
        return (lat, lon)

    m2 = re.match(r"([+-]?\d+\.?\d*)[\s,]+([+-]?\d+\.?\d*)", raw.strip())
    if m2:
        return (float(m2.group(1)), float(m2.group(2)))

    return None


# ---------------------------------------------------------------------------
# NMDC object builders
# ---------------------------------------------------------------------------

def build_study(project_data: dict) -> nmdc.Study:
    accession = project_data["accession"]
    study_id = make_id("sty", accession)

    now = datetime.now(tz=timezone.utc)
    provenance = nmdc.ProvenanceMetadata(
        add_date=now,
        mod_date=now,
        source_system_of_record=nmdc.SourceSystemEnum.NCBI.text,
        type="nmdc:ProvenanceMetadata",
    )

    return nmdc.Study(
        id=study_id,
        name=project_data["title"],
        title=project_data["title"],
        description=project_data["description"],
        study_category=nmdc.StudyCategoryEnum.research_study.text,
        insdc_bioproject_identifiers=[f"insdc.sra:{accession}"],
        type="nmdc:Study",
        provenance_metadata=provenance,
    )


def build_biosample(
    sample_data: dict, study_id: str
) -> nmdc.Biosample:
    accession = sample_data["accession"]
    biosample_id = make_id("bsm", accession)
    attrs = sample_data["attributes"]

    env_broad = None
    env_local = None
    env_medium = None

    raw_broad = attrs.get("env_broad_scale", "")
    raw_local = attrs.get("env_local_scale", "")
    raw_medium = attrs.get("env_medium", "")

    def _parse_envo_term(raw: str) -> Optional[nmdc.ControlledIdentifiedTermValue]:
        if not raw:
            return None
        envo_match = re.search(r"(ENVO[:\s_]+\d+)", raw, re.IGNORECASE)
        if envo_match:
            curie = envo_match.group(1).replace(" ", ":").replace("_", ":")
            curie = re.sub(r"ENVO(\d)", r"ENVO:\1", curie)
            label = re.sub(r"\s*\[.*?\]\s*", "", raw).strip() or raw
            return nmdc.ControlledIdentifiedTermValue(
                term=nmdc.OntologyClass(id=curie, name=label, type="nmdc:OntologyClass"),
                type="nmdc:ControlledIdentifiedTermValue",
            )
        return nmdc.ControlledIdentifiedTermValue(
            term=nmdc.OntologyClass(
                id="ENVO:00000000",
                name=raw,
                type="nmdc:OntologyClass",
            ),
            has_raw_value=raw,
            type="nmdc:ControlledIdentifiedTermValue",
        )

    env_broad = _parse_envo_term(raw_broad)
    env_local = _parse_envo_term(raw_local)
    env_medium = _parse_envo_term(raw_medium)

    lat_lon = None
    raw_latlon = attrs.get("lat_lon", "")
    parsed = parse_lat_lon(raw_latlon) if raw_latlon else None
    if parsed:
        lat_lon = nmdc.GeolocationValue(
            latitude=nmdc.DecimalDegree(parsed[0]),
            longitude=nmdc.DecimalDegree(parsed[1]),
            type="nmdc:GeolocationValue",
        )

    collection_date = None
    raw_date = attrs.get("collection_date", "")
    if raw_date:
        collection_date = nmdc.TimestampValue(
            has_raw_value=raw_date, type="nmdc:TimestampValue"
        )

    depth = None
    raw_depth = attrs.get("depth", "")
    if raw_depth:
        depth_match = re.match(r"([+-]?\d+\.?\d*)", raw_depth)
        if depth_match:
            unit_match = re.search(r"[a-zA-Z]+", raw_depth)
            unit = unit_match.group() if unit_match else "m"
            depth = nmdc.QuantityValue(
                has_numeric_value=float(depth_match.group(1)),
                has_unit=unit,
                has_raw_value=raw_depth,
                type="nmdc:QuantityValue",
            )

    elev = None
    raw_elev = attrs.get("elev", "")
    if raw_elev:
        elev_match = re.match(r"([+-]?\d+\.?\d*)", raw_elev)
        if elev_match:
            elev = float(elev_match.group(1))

    geo_loc_name = None
    raw_geo = attrs.get("geo_loc_name", "")
    if raw_geo:
        geo_loc_name = nmdc.TextValue(has_raw_value=raw_geo, type="nmdc:TextValue")

    organism_name = sample_data.get("organism", "")
    taxonomy_id = sample_data.get("taxonomy_id", "")
    samp_taxon_id = None
    if taxonomy_id:
        samp_taxon_id = nmdc.ControlledIdentifiedTermValue(
            term=nmdc.OntologyClass(
                id=f"NCBITaxon:{taxonomy_id}",
                name=organism_name,
                type="nmdc:OntologyClass",
            ),
            type="nmdc:ControlledIdentifiedTermValue",
        )

    biosample = nmdc.Biosample(
        id=biosample_id,
        name=sample_data.get("title", "") or accession,
        env_broad_scale=env_broad,
        env_local_scale=env_local,
        env_medium=env_medium,
        lat_lon=lat_lon,
        collection_date=collection_date,
        depth=depth,
        elev=elev,
        geo_loc_name=geo_loc_name,
        samp_taxon_id=samp_taxon_id,
        associated_studies=[study_id],
        insdc_biosample_identifiers=[f"biosample:{accession}"],
        type="nmdc:Biosample",
    )

    return biosample


def _infer_analyte_category(library_source: str, library_strategy: str) -> str:
    source_lower = library_source.lower() if library_source else ""
    strategy_lower = library_strategy.lower() if library_strategy else ""
    if "metatranscriptomic" in source_lower or strategy_lower == "rna-seq":
        return "metatranscriptome"
    if "metagenomic" in source_lower or strategy_lower in ("wgs", "wcs"):
        return "metagenome"
    if strategy_lower == "amplicon":
        return "metabarcode"
    return "metagenome"


def build_pipeline_records(
    experiment: dict,
    study_id: str,
    biosample_id: str,
) -> dict:
    """Build Extraction, LibraryPreparation, NucleotideSequencing, DataObject,
    and ProcessedSample records for one SRA experiment."""

    exp_acc = experiment["experiment_accession"]

    extraction_id = make_id("extrp", exp_acc)
    extraction_ps_id = make_id("procsm", f"extr-{exp_acc}")
    libprep_id = make_id("libprp", exp_acc)
    libprep_ps_id = make_id("procsm", f"lib-{exp_acc}")
    ns_id = make_id("dgns", exp_acc)

    extraction = nmdc.Extraction(
        id=extraction_id,
        has_input=[biosample_id],
        has_output=[extraction_ps_id],
        type="nmdc:Extraction",
    )

    extraction_ps = nmdc.ProcessedSample(
        id=extraction_ps_id,
        type="nmdc:ProcessedSample",
    )

    source = (experiment.get("library_source") or "").upper()
    if "TRANSCRIPTOMIC" in source or "RNA" in source:
        library_type = "RNA"
    else:
        library_type = "DNA"

    libprep = nmdc.LibraryPreparation(
        id=libprep_id,
        has_input=[extraction_ps_id],
        has_output=[libprep_ps_id],
        library_type=library_type,
        type="nmdc:LibraryPreparation",
    )

    libprep_ps = nmdc.ProcessedSample(
        id=libprep_ps_id,
        type="nmdc:ProcessedSample",
    )

    source_lower = (experiment.get("library_source") or "").upper()
    if "TRANSCRIPTOMIC" in source_lower:
        data_object_type = "Metatranscriptome Raw Reads"
    else:
        data_object_type = "Metagenome Raw Reads"

    data_objects = []
    do_ids = []
    for run in experiment.get("runs", []):
        run_acc = run["accession"]
        do_id = make_id("dobj", run_acc)
        do_ids.append(do_id)
        data_objects.append(nmdc.DataObject(
            id=do_id,
            name=run_acc,
            description=f"SRA run {run_acc} for experiment {exp_acc}",
            url=f"https://www.ncbi.nlm.nih.gov/sra/{run_acc}",
            data_category=nmdc.DataCategoryEnum.instrument_data.text,
            data_object_type=data_object_type,
            type="nmdc:DataObject",
        ))

    analyte_category = _infer_analyte_category(
        experiment.get("library_source", ""),
        experiment.get("library_strategy", ""),
    )

    instrument_model = experiment.get("instrument_model", "")
    instrument_ids = []
    if instrument_model:
        instrument_ids = [make_id("inst", instrument_model)]

    nuc_seq = nmdc.NucleotideSequencing(
        id=ns_id,
        name=experiment.get("sample_title", "") or exp_acc,
        has_input=[libprep_ps_id],
        has_output=do_ids,
        associated_studies=[study_id],
        instrument_used=instrument_ids,
        analyte_category=analyte_category,
        insdc_experiment_identifiers=[f"insdc.sra:{exp_acc}"],
        type="nmdc:NucleotideSequencing",
    )

    return {
        "extraction": extraction,
        "extraction_processed_sample": extraction_ps,
        "library_preparation": libprep,
        "libprep_processed_sample": libprep_ps,
        "nucleotide_sequencing": nuc_seq,
        "data_objects": data_objects,
    }


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------

def fetch_all(accession: str) -> dict:
    print(f"Fetching BioProject {accession}...")
    project = fetch_bioproject(accession)
    print(f"  Title: {project['title']}")

    print(f"Fetching SRA experiments...")
    experiments = fetch_sra_experiments(accession)

    sra_biosample_accessions = {
        e["biosample_accession"] for e in experiments if e["biosample_accession"]
    }
    print(f"  Found {len(sra_biosample_accessions)} unique BioSamples across SRA records.")

    print(f"Querying elink for all BioSamples linked to BioProject...")
    linked_uids = fetch_linked_biosample_uids(project["uid"])

    if linked_uids is not None:
        print(f"  elink returned {len(linked_uids)} BioSample UIDs.")
        print(f"Fetching BioSamples by UID (covers both sequenced and unsequenced samples)...")
        biosamples = _fetch_biosample_records(linked_uids)
        linked_accessions = {s["accession"] for s in biosamples if s.get("accession")}

        missing_from_sra = linked_accessions - sra_biosample_accessions
        missing_from_elink = sra_biosample_accessions - linked_accessions

        if missing_from_sra:
            print(
                f"  NOTE: {len(missing_from_sra)} BioSample(s) under this BioProject "
                f"have no SRA submission yet (carried through without a DataGeneration)."
            )
        if missing_from_elink:
            print(
                f"  WARNING: {len(missing_from_elink)} BioSample(s) referenced by SRA "
                f"were not returned by elink — fetching them separately: "
                f"{sorted(missing_from_elink)}"
            )
            extras = fetch_biosamples(sorted(missing_from_elink))
            biosamples.extend(extras)
    else:
        print(
            "  elink unavailable — falling back to SRA-derived BioSample set only. "
            "Re-run later for a complete list."
        )
        print(f"Fetching BioSamples...")
        biosamples = fetch_biosamples(sorted(sra_biosample_accessions))

    print(f"  Retrieved {len(biosamples)} BioSample records.")

    return {
        "bioproject": project,
        "biosamples": biosamples,
        "sra_experiments": experiments,
    }


def build_nmdc_database(data: dict) -> nmdc.Database:
    project = data["bioproject"]
    biosamples = data["biosamples"]
    experiments = data["sra_experiments"]

    study = build_study(project)
    study_id = study.id

    biosample_acc_to_id = {}
    nmdc_biosamples = []
    for s in biosamples:
        bs = build_biosample(s, study_id)
        nmdc_biosamples.append(bs)
        biosample_acc_to_id[s["accession"]] = bs.id

    all_extractions = []
    all_libpreps = []
    all_nuc_seqs = []
    all_data_objects = []
    all_processed_samples = []

    warnings = []
    for exp in experiments:
        bs_acc = exp["biosample_accession"]
        if bs_acc not in biosample_acc_to_id:
            warnings.append(
                f"SRA experiment {exp['experiment_accession']} references "
                f"BioSample {bs_acc} which was not fetched. Skipping."
            )
            continue

        records = build_pipeline_records(exp, study_id, biosample_acc_to_id[bs_acc])
        all_extractions.append(records["extraction"])
        all_processed_samples.append(records["extraction_processed_sample"])
        all_libpreps.append(records["library_preparation"])
        all_processed_samples.append(records["libprep_processed_sample"])
        all_nuc_seqs.append(records["nucleotide_sequencing"])
        all_data_objects.extend(records["data_objects"])

    for w in warnings:
        print(f"WARNING: {w}")

    database = nmdc.Database()
    database.study_set = [study]
    database.biosample_set = nmdc_biosamples
    database.material_processing_set = all_extractions + all_libpreps
    database.data_generation_set = all_nuc_seqs
    database.processed_sample_set = all_processed_samples
    database.data_object_set = all_data_objects

    return database


def main():
    parser = argparse.ArgumentParser(
        description="Translate an NCBI BioProject to NMDC schema JSON."
    )
    parser.add_argument("accession", help="NCBI BioProject accession (e.g. PRJNA1452545)")
    parser.add_argument("--out", help="Output file path (default: results/ncbi_<accession>_nmdc.json)")
    parser.add_argument(
        "--fetch-only",
        action="store_true",
        help="Only fetch and dump intermediate NCBI data as JSON (no NMDC conversion).",
    )
    args = parser.parse_args()

    accession = args.accession.strip()
    out_path = args.out or f"results/ncbi_{accession}_nmdc.json"

    data = fetch_all(accession)

    if args.fetch_only:
        intermediate_path = out_path.replace(".json", "_intermediate.json")
        with open(intermediate_path, "w") as f:
            json.dump(data, f, indent=2, default=str)
        print(f"\nIntermediate data written to {intermediate_path}")
        print("Review this file, then re-run without --fetch-only to produce NMDC JSON.")
        return

    print("\nBuilding NMDC Database...")
    database = build_nmdc_database(data)

    print(f"  Study: {database.study_set[0].id}")
    print(f"  Biosamples: {len(database.biosample_set)}")
    print(f"  Extractions: {len([x for x in database.material_processing_set if isinstance(x, nmdc.Extraction)])}")
    print(f"  LibraryPreparations: {len([x for x in database.material_processing_set if isinstance(x, nmdc.LibraryPreparation)])}")
    print(f"  NucleotideSequencings: {len(database.data_generation_set)}")
    print(f"  DataObjects: {len(database.data_object_set)}")
    print(f"  ProcessedSamples: {len(database.processed_sample_set)}")

    json_str = json_dumper.dumps(database)

    with open(out_path, "w") as f:
        f.write(json_str)
    print(f"\nNMDC Database JSON written to {out_path}")

    print("\n⚠ PLACEHOLDER IDS: All IDs use shoulder '99' and are NOT real NMDC IDs.")
    print("  Real IDs must be minted via the NMDC Runtime API before ingest.")

    env_warnings = []
    for bs in database.biosample_set:
        if bs.env_broad_scale and hasattr(bs.env_broad_scale, "has_raw_value") and bs.env_broad_scale.has_raw_value:
            env_warnings.append(f"  {bs.id}: env_broad_scale needs ENVO CURIE (raw: {bs.env_broad_scale.has_raw_value})")
        if bs.env_local_scale and hasattr(bs.env_local_scale, "has_raw_value") and bs.env_local_scale.has_raw_value:
            env_warnings.append(f"  {bs.id}: env_local_scale needs ENVO CURIE (raw: {bs.env_local_scale.has_raw_value})")
        if bs.env_medium and hasattr(bs.env_medium, "has_raw_value") and bs.env_medium.has_raw_value:
            env_warnings.append(f"  {bs.id}: env_medium needs ENVO CURIE (raw: {bs.env_medium.has_raw_value})")

    if env_warnings:
        print("\n⚠ ENVO MAPPING NEEDED: The following biosamples have free-text env terms")
        print("  that should be mapped to proper ENVO CURIEs:")
        for w in env_warnings:
            print(w)


if __name__ == "__main__":
    main()
