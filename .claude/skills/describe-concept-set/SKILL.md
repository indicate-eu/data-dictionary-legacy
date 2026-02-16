---
name: describe-concept-set
description: Generate a detailed clinical description for an INDICATE concept set, using UMLS, LOINC, and SNOMED vocabulary sources. Use when the user wants to describe or document a concept set.
allowed-tools: Bash, Read, Glob, Grep, WebFetch, WebSearch, AskUserQuestion, TodoWrite
argument-hint: "[concept-set-name]"
---

# Describe Concept Set

Generate a detailed clinical description for an INDICATE concept set, using UMLS, LOINC, and SNOMED vocabulary sources.

## Instructions

You are an expert in OHDSI/OMOP vocabularies and clinical terminologies. Your task is to generate a comprehensive description of a concept set that helps clinicians, laboratory experts, data scientists, and clinical informaticians understand what the concept set captures and how to align source data to it.

### Step 1: Get Configuration

Ask the user for:

1. **Concept set name** (e.g., "Heart rate", "Mechanical ventilation")
2. **Concept set ID** (the numeric ID, e.g., 327)
3. **Vocabularies folder path** — the root folder containing the terminology subfolders:
   - `UMLS - 2025AB/META/` (UMLS RRF files)
   - `LOINC source/Loinc_2.81/` (LOINC CSV files)
   - `SNOMED source/` (SNOMED RF2 files)
4. **Language** for the output description (default: English)

The GitHub URLs for the concept set files are:
- OHDSI definition: `https://raw.githubusercontent.com/indicate-eu/data-dictionary-content/refs/heads/main/concept_sets/{id}.json`
- Resolved concepts: `https://raw.githubusercontent.com/indicate-eu/data-dictionary-content/refs/heads/main/concept_sets_resolved/{id}.json`

### Step 2: Fetch Concept Set Data

Fetch both JSON files:
1. The **OHDSI concept set definition** (contains included/excluded items with `isExcluded`, `includeDescendants`, `includeMapped` flags)
2. The **resolved concept set** (contains the final list of concepts after resolution)

Parse these to identify:
- **Included concepts**: Items in the resolved list that are standard concepts (`standardConcept: "S"`)
- **Excluded concepts**: Items in the OHDSI definition where `isExcluded: true`
- **Hierarchy nodes**: LOINC Hierarchy items used to include descendants

### Step 3: Search Vocabulary Sources

For each vocabulary source, search for definitions and metadata.

#### 3a. UMLS (most important for definitions)

**Find the parent CUI** — Search `MRCONSO.RRF` for the general concept (not individual LOINC codes, which rarely have definitions):
```bash
grep "|ENG|" MRCONSO.RRF | grep "|MSH|" | grep -i "concept_name"
```

**Get definitions** from `MRDEF.RRF` — Search by CUI for definitions from these sources (in priority order):
- **MSH** (MeSH) — most authoritative medical definitions
- **NCI** (NCI Thesaurus) — good clinical definitions
- **CSP** (CRISP Thesaurus) — concise definitions
- **ICF** (International Classification of Functioning) — functional perspective
- **HPO** (Human Phenotype Ontology) — for phenotype-related concepts
- **MEDLINEPLUS** — patient-friendly definitions

```bash
grep "^CUI_HERE|" MRDEF.RRF
```

**Get semantic type** from `MRSTY.RRF`:
```bash
grep "^CUI_HERE|" MRSTY.RRF
```

**Get MeSH hierarchy** from `MRREL.RRF` — find parent/child MeSH concepts:
```bash
grep "^CUI_HERE|" MRREL.RRF | grep "|MSH|"
```

**Get synonyms** from `MRCONSO.RRF` — find all English terms for the CUI:
```bash
grep "^CUI_HERE|" MRCONSO.RRF | grep "|ENG|"
```

#### 3b. LOINC Source (for LOINC-based concept sets)

Search `LoincTable/Loinc.csv` for each LOINC code to get:
- **COMPONENT**: What is measured (e.g., "Heart rate")
- **PROPERTY**: Type of measurement (e.g., "NRat" = Number Rate)
- **TIME_ASPCT**: Temporal aspect (e.g., "Pt" = Point in time)
- **SYSTEM**: Where measured (e.g., "XXX", "Peripheral artery", "Heart")
- **SCALE_TYP**: Scale type (e.g., "Qn" = Quantitative)
- **METHOD_TYP**: Method (e.g., "Pulse oximetry", "Palpation", "EKG")
- **LONG_COMMON_NAME**: Full descriptive name
- **CONSUMER_NAME**: Patient-friendly name
- **RELATEDNAMES2**: Related keywords
- **EXAMPLE_UCUM_UNITS**: Expected units
- **CLASS**: LOINC class
- **ORDER_OBS**: Order vs Observation

Also check:
- `AccessoryFiles/ConsumerName/ConsumerName.csv` for simplified names
- `AccessoryFiles/PartFile/Part.csv` for component definitions

#### 3c. SNOMED Source (for SNOMED-based concept sets)

If the concept set contains SNOMED concepts, extract from the RF2 ZIP:
- `sct2_Description_Snapshot-en_INT_20251101.txt` — FSN and synonyms
- `sct2_TextDefinition_Snapshot-en_INT_20251101.txt` — Full text definitions (only ~3.6% of concepts have them)

```bash
unzip -p "path/to/zip" "*/sct2_Description_Snapshot-en*" | grep "SNOMED_ID"
unzip -p "path/to/zip" "*/sct2_TextDefinition_Snapshot-en*" | grep "SNOMED_ID"
```

#### 3d. Web Search (complementary)

Use web search to find additional clinical information that vocabulary files may not provide:
- **Clinical guidelines** mentioning the concept (e.g., normal ranges, measurement protocols)
- **Wikipedia or medical references** for clear clinical context
- **LOINC.org** for official LOINC descriptions and usage notes
- **SNOMED browser** for concept details and hierarchy context

This is especially useful for:
- Concepts that lack definitions in UMLS/SNOMED (most LOINC codes)
- Understanding clinical nuances (e.g., when to use invasive vs non-invasive measurement)
- Normal ranges and units in specific clinical contexts (ICU, neonatal, etc.)

Present web sources found to the user and let them decide which to include in the final description.

### Step 4: Present Raw Data

Before generating the description, show the user what was found:

```
=== VOCABULARY DATA FOUND ===

UMLS DEFINITIONS:
- [Source]: "Definition text..."
- [Source]: "Definition text..."

SEMANTIC TYPE: T201 - Clinical Attribute

MESH HIERARCHY:
- Parents: ...
- Children: ...
- Related: ...

SYNONYMS: term1, term2, term3

LOINC DECOMPOSITION (for each included standard concept):
| LOINC Code | Long Name | System | Method | Condition | Units | Class |
|------------|-----------|--------|--------|-----------|-------|-------|
| ...        | ...       | ...    | ...    | ...       | ...   | ...   |

SNOMED DATA (if applicable):
- FSN: ...
- Synonyms: ...
- TextDefinition: ...

EXCLUDED CONCEPTS:
| Concept | Reason for exclusion (inferred) |
|---------|-------------------------------|
| ...     | ...                           |

WEB SOURCES (if found):
- [URL]: key information found
```

Ask the user if they want to adjust anything before generating the description.

### Step 5: Generate Description

Generate a structured description in Markdown with these sections:

#### Structure

```markdown
## Definition & Clinical Context

[2-4 paragraphs combining:]
- Authoritative definition (from MeSH/NCI/UMLS)
- What this measures and why it matters clinically
- UMLS semantic type
- Where this fits in the MeSH hierarchy (parents/related concepts)
- Key clinical contexts where this is used (ICU, emergency, routine monitoring...)
- Normal ranges if relevant and available
- Units of measurement

## Included Concepts

[Brief intro explaining the concept set structure — e.g., "This concept set includes X standard LOINC concepts covering heart rate measurements across different methods, sites, and clinical conditions."]

### [Group 1 name — e.g., "General heart rate"]
[For each concept:]
- **[LOINC code] — [Long common name]**: [1-2 sentence description explaining what makes this concept specific — the method, anatomical site, clinical condition, or timing. Use LOINC decomposition to explain clearly.]

### [Group 2 name — e.g., "Position-specific measurements"]
...

[Group concepts logically by method, site, condition, or clinical context — NOT just alphabetically]

## Excluded Concepts

[Brief intro explaining the exclusion strategy]

[For each excluded concept or group of excluded concepts:]
- **[Concept name/group]**: [Why excluded — e.g., "Pulse wave forms measure arterial waveform morphology, not heart rate frequency", "Fetal heart rate is a distinct clinical measurement with different normal ranges and clinical significance"]

## Mapping Notes

[Practical notes for data alignment:]
- Which concept to use as default when the source doesn't specify method/site
- Common source system names that map to specific concepts
- Disambiguation tips for similar-sounding concepts
- Any gotchas or common mapping errors

## Sources

[List all sources used to build this description:]
- Which UMLS definitions were used (e.g., "MeSH definition of Heart rate (CUI C0018810)")
- Which LOINC decompositions were used (e.g., "LOINC 8867-4 component/method/system fields")
- Which SNOMED descriptions or TextDefinitions were used
- Which web sources were consulted (with URLs), if any
- Mark each source clearly so a reviewer can verify the information
```

#### Writing Guidelines

- **Audience**: Clinicians, laboratory experts, data scientists, and clinical informaticians — some may be domain experts, others not
- **Goal**: Enable correct data alignment and provide a shared reference for multidisciplinary teams
- **Tone**: Precise, technical but accessible — a clinician should recognize the clinical reality, a data scientist should understand what to map
- **DO NOT** repeat general ETL/mapping rules (LOINC for labs, RxNorm for drugs, etc.) — these are documented elsewhere
- **DO NOT** include OHDSI concept IDs in the description — use LOINC/SNOMED codes instead
- **DO** explain clinical nuances that affect mapping decisions
- **DO** explain what distinguishes each concept from similar ones
- **DO** group concepts logically (by method, site, condition) rather than alphabetically
- **DO** highlight which concept is the most generic/default one

### Step 6: Output

Present the generated description to the user. Ask if they want any adjustments.

The description can then be:
1. Stored as the `longDescription` field in the concept set metadata
2. Used as documentation for the INDICATE data dictionary
3. Shared with data providers for alignment guidance
