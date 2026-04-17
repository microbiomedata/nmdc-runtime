---
name: ncbi-to-nmdc
description: Translate an NCBI BioProject (with BioSamples + SRA runs) into an NMDC-schema-compliant Database JSON file
---

# NCBI BioProject → NMDC JSON Translation

Given a BioProject accession (e.g. `PRJNA1452545`), fetch linked BioSample and SRA data from NCBI and produce an NMDC-schema-compliant `nmdc.Database` JSON file.

## Arguments

The user provides an NCBI BioProject accession as the argument (e.g. `/ncbi-to-nmdc PRJNA1452545`).

## Workflow

### Step 1: Fetch and review intermediate data

Run the helper script in fetch-only mode to see the raw NCBI data:

```bash
cd /Users/sujaypatil/src/nmdc-runtime
python skills-scripts/ncbi_bioproject_to_nmdc.py <ACCESSION> --fetch-only
```

Read the intermediate JSON file and review:
- **BioProject**: Does the title/description make sense? What `StudyCategoryEnum` fits? (Options: `research_study`, `consortium`)
- **BioSamples**: Do the `env_broad_scale`, `env_local_scale`, `env_medium` attributes have proper ENVO CURIEs or just free text?
- **SRA experiments**: What instrument models are used? What library strategies?

### Step 2: Generate NMDC JSON

Run the script without `--fetch-only`:

```bash
python skills-scripts/ncbi_bioproject_to_nmdc.py <ACCESSION>
```

### Step 3: Resolve ontology terms with `runoak`

Use `runoak` (the oaklib CLI) for **all** ontology lookups — ENVO for the env triad, NCBITaxon for host/samp taxon, etc. Do not hand-pick CURIEs from memory; look them up.

Common adapters:
- `sqlite:obo:envo` — ENVO (downloads the SQLite dump on first use, then cached)
- `sqlite:obo:ncbitaxon` — NCBITaxon
- `ols:envo` / `ols:ncbitaxon` — live OLS API (slower, no install footprint)

Useful runoak subcommands:

```bash
# Fuzzy search for a label
runoak -i sqlite:obo:envo search "forest floor"

# Fetch label + definition + synonyms for a known CURIE
runoak -i sqlite:obo:envo info ENVO:00002042

# Check ancestry (e.g. is this term under biome?)
runoak -i sqlite:obo:envo ancestors -p i ENVO:01000174

# List all descendants of an anchor class (used to constrain env triad — see below)
runoak -i sqlite:obo:envo descendants -p i ENVO:00000428

# NCBITaxon lookup by scientific name
runoak -i sqlite:obo:ncbitaxon search "Phallus rugulosus"
runoak -i sqlite:obo:ncbitaxon info NCBITaxon:5260
```

### Step 4: Map the env triad using MIxS value sets

The NMDC schema inherits MIxS env-triad semantics: each of the three slots must point into a specific ENVO subtree. Constrain `runoak search` output to these subtrees — **do not** pick arbitrary ENVO terms.

| Slot | Anchor class | MIxS intent |
|---|---|---|
| `env_broad_scale` | `ENVO:00000428` (biome) | The coarse biome containing the sample |
| `env_local_scale` | `ENVO:01000813` (astronomical body part) — practically, environmental features | Causal environmental entity at the sample's vicinity |
| `env_medium` | `ENVO:00010483` (environmental material) | The material the sample is composed of |

For **soil** biosamples (MIxS `soil` or `MIMS.me.soil.*` package), the submission schema further restricts each slot to a package-specific value set (a small curated list of ENVO terms). If `nmdc-submission-schema` is installed in the environment, pull the allowed values from there; otherwise fall back to the anchor-class descendants above and flag any choice outside the MIxS soil value set for manual review.

Workflow for each free-text value in the intermediate JSON:

1. Search ENVO: `runoak -i sqlite:obo:envo search "<raw value>"`
2. Filter hits to descendants of the correct anchor class
3. Pick the closest match; record its CURIE and label
4. If no good match, leave a placeholder and note it in the report

Edit the output JSON to replace each placeholder (`ENVO:00000000`) with the resolved CURIE and the ENVO-official label (not the raw submitter string).

### Step 5: Fix instrument and host references

- **Instrument**: the script stores SRA `instrument_model` strings verbatim and assigns a placeholder `nmdc:inst-99-*` ID. Leave the ID in place — real Instrument records are resolved at ingest. But verify the string is sensible (e.g. `Illumina NovaSeq X`, `Illumina NovaSeq 6000`, `Illumina HiSeq 2500`, `Illumina MiSeq`).
- **Host / samp_taxon**: if the BioProject implies a host organism (e.g. rhizosphere or host-associated samples), resolve its NCBI taxid via runoak: `runoak -i sqlite:obo:ncbitaxon search "<host name>"`. Only set `host_name` / `host_taxid` when the submitter's intent is unambiguous — otherwise leave unset and flag for PI follow-up.

### Step 6: Validate

Validate the output JSON against the NMDC schema:

```python
python -c "
from nmdc_schema import nmdc
from linkml_runtime.loaders import json_loader
db = json_loader.load('results/ncbi_<ACCESSION>_nmdc.json', target_class=nmdc.Database)
print(f'Loaded: {len(db.study_set)} studies, {len(db.biosample_set)} biosamples, {len(db.data_generation_set)} data generations')
print('Validation passed!')
"
```

### Step 7: Report summary

Report to the user:
- Study name and accession
- Number of Biosamples, Extractions, LibraryPreparations, NucleotideSequencings, DataObjects
- Any ENVO mappings that were applied or still need manual review
- The output file path
- Reminder that IDs are placeholders (shoulder `99`) and need real minting

## Output

The final JSON file is written to `results/ncbi_<ACCESSION>_nmdc.json`.

## Reference files

These files in the repo contain patterns for NMDC object construction:
- `nmdc_runtime/site/translation/translator.py` — base Translator class
- `nmdc_runtime/site/translation/gold_translator.py` — Study/Biosample/NucleotideSequencing patterns
- `nmdc_runtime/site/translation/neon_soil_translator.py` — Extraction/LibraryPreparation patterns
- `nmdc_runtime/site/translation/neon_utils.py` — helper functions for NMDC value types
