# CCDM v0.1 — Proposed Design

*The Common Clinical Data Model for ARIA Phase 1. Seven tables. Small on purpose — covers what ARIA needs, nothing more. Everything below is a proposal to review with Reva.*

---

## The problem in one line

Every component in the POC uses its own field names and values for the same facts. The rules can't be written once, a miss can't be traced, and nothing can be reused. The CCDM is the one dictionary everything reads and writes.

---

## The seven tables

### Group A — reference tables (loaded once, changed rarely)

These define the dictionary. Silver reads them; it never writes them.

#### 1. `ccdm.vocabulary` — the allowed values

One row per allowed value for every controlled field. This is the heart of the CCDM.

| field | allowed_value | meaning | ccdm_version |
|---|---|---|---|
| assertion | ACTIVE | condition is current at this visit | 0.1 |
| assertion | HISTORY | past condition, "history of" | 0.1 |
| assertion | NEGATED | ruled out, absent | 0.1 |
| assertion | SUSPECTED | "possible", "rule out", not confirmed | 0.1 |
| provider_role | ATTENDING | physician of record | 0.1 |
| provider_role | CONSULTING | specialist | 0.1 |
| provider_role | NURSE | nursing note | 0.1 |
| provider_role | OTHER | anything else | 0.1 |
| section | ASSESSMENT_PLAN | assessment and plan | 0.1 |
| section | HPI | history of present illness | 0.1 |
| section | MEDICATIONS | medication list | 0.1 |
| section | PROBLEM_LIST | standing problem list | 0.1 |
| section | LABS | lab results | 0.1 |
| section | RADIOLOGY | imaging | 0.1 |
| meat_criterion | M | monitoring | 0.1 |
| meat_criterion | E | evaluation | 0.1 |
| meat_criterion | A | assessment | 0.1 |
| meat_criterion | T | treatment | 0.1 |
| disposition | CODEABLE | passes all gates | 0.1 |
| disposition | CODER_QUEUE | needs a coder | 0.1 |
| disposition | DROP | not codeable | 0.1 |

**Solves:** the "present vs affirmed vs active" problem. There is exactly one spelling of each idea, and everything is checked against this table.

#### 2. `ccdm.source_map` — translation from each input

How each source's words become CCDM words. One row per translation.

| source | source_field | source_value | ccdm_field | ccdm_value |
|---|---|---|---|---|
| nlp_v3 | status | present | assertion | ACTIVE |
| nlp_v3 | status | history | assertion | HISTORY |
| nlp_v3 | status | negated | assertion | NEGATED |
| nlp_v3 | status | possible | assertion | SUSPECTED |
| nlp_v3 | role | MD | provider_role | ATTENDING |
| nlp_v3 | role | RN | provider_role | NURSE |
| nlp_v3 | sec | A/P | section | ASSESSMENT_PLAN |
| nlp_v3 | sec | MEDS | section | MEDICATIONS |

**Solves:** adding a second source (the LLM extractor in Reva's ensemble, an external NLP) is adding rows here, not changing code or schema.

#### 3. `ccdm.code_reference` — the codes (this is the "CMS reference data" box)

| icd10 | description | hcc | raf_weight | model_year | valid_from | valid_to |
|---|---|---|---|---|---|---|
| E11.22 | T2 diabetes with diabetic CKD | 37 | 0.30 | 2026 | 2025-10-01 | 2026-09-30 |
| N18.30 | CKD stage 3, unspecified | 329 | 0.13 | 2026 | 2025-10-01 | 2026-09-30 |
| I50.9 | Heart failure, unspecified | 226 | 0.33 | 2026 | 2025-10-01 | 2026-09-30 |
| I50.22 | Chronic systolic heart failure | 226 | 0.33 | 2026 | 2025-10-01 | 2026-09-30 |
| I10 | Essential hypertension | *(null)* | 0.00 | 2026 | 2025-10-01 | 2026-09-30 |

**Solves:** one answer to "is this a valid code and what HCC is it," by year, so last year's chart replays against last year's crosswalk.

---

### Group B — chart tables (populated from bronze, every chart)

These are silver. Every row carries `chart_id` and `ccdm_version`.

#### 4. `ccdm.encounter` — one row per visit

| chart_id | encounter_id | patient_id | client_id | dos | provider_id | ccdm_version |
|---|---|---|---|---|---|---|
| CH-8841 | ENC-5510 | P-2201 | HUMANA | 2026-03-14 | PR-310 | 0.1 |

#### 5. `ccdm.provider` — one row per doctor (upsert by NPI)

| provider_id | npi | name | provider_role |
|---|---|---|---|
| PR-310 | 1234567890 | Dr. Patel | ATTENDING |

#### 6. `ccdm.condition_mention` — one row per place a condition is mentioned

| chart_id | mention_id | encounter_id | icd10 | hcc | page | char_start | char_end | text_span | assertion | section | source | source_conf | ccdm_version |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| CH-8841 | M-1 | ENC-5510 | E11.22 | 37 | 3 | 412 | 454 | Type 2 diabetes with kidney complications | ACTIVE | ASSESSMENT_PLAN | nlp_v3 | 0.94 | 0.1 |
| CH-8841 | M-2 | ENC-5510 | N18.30 | 329 | 3 | 588 | 599 | CKD stage 3 | ACTIVE | ASSESSMENT_PLAN | nlp_v3 | 0.91 | 0.1 |
| CH-8841 | M-3 | ENC-5510 | I50.9 | 226 | 3 | 640 | 664 | History of heart failure | HISTORY | ASSESSMENT_PLAN | nlp_v3 | 0.72 | 0.1 |

Every value in `assertion` and `section` must exist in `ccdm.vocabulary`. Every `icd10` must exist in `ccdm.code_reference` for the chart's year. Otherwise the row goes to `ccdm.quarantine` with a reason.

#### 7. `ccdm.evidence_span` — one row per supporting sentence

| chart_id | evidence_id | encounter_id | page | char_start | char_end | text | section | ccdm_version |
|---|---|---|---|---|---|---|---|---|
| CH-8841 | EV-1 | ENC-5510 | 3 | 456 | 468 | on metformin | ASSESSMENT_PLAN | 0.1 |
| CH-8841 | EV-2 | ENC-5510 | 3 | 481 | 488 | A1c 7.8 | ASSESSMENT_PLAN | 0.1 |
| CH-8841 | EV-3 | ENC-5510 | 3 | 601 | 636 | will recheck labs in 3 months | ASSESSMENT_PLAN | 0.1 |
| CH-8841 | EV-4 | ENC-5510 | 3 | 666 | 690 | stable on lisinopril | MEDICATIONS | 0.1 |

**Solves (4–7 together):** the Abstractor gets a fixed, predictable input. Positions are exact so citations work. Nothing malformed gets past silver.

---

## What silver does with them

```
bronze payload
   → parse header      → encounter (4), provider (5)
   → for each mention  → translate via source_map (2)
                       → look up hcc in code_reference (3)
                       → check assertion/section in vocabulary (1)
                       → pass: condition_mention (6), evidence_span (7)
                       → fail: ccdm.quarantine (chart_id, mention, reason)
```

---

## What changes in every other box

The CCDM is not only a silver change. This is the part to be upfront about with Reva.

| box | change | who |
|---|---|---|
| **3 · Silver** | Build the seven tables, the mapping and validation job, quarantine | Data eng |
| **4 · Abstractor** | Input: read `condition_mention` + `evidence_span` instead of raw NLP JSON. Output: use CCDM names — `assertion`, `provider_role`, `meat_criterion` — and vocabulary values. Today it uses its own (e.g. `status: affirmed`) | AI team changes the code, or data eng writes a thin adapter on each side so the POC runs unchanged for now |
| **5 · CTG store** | `ctg.node` and `ctg.edge` columns named per CCDM; every enum value must be in `ccdm.vocabulary` | Data eng |
| **6 · Auditor** | Rules reference CCDM field names. Gate L1 reads `code_reference`; gate L2 reads `assertion`; gate L5 reads `meat_criterion` edges | AI team (rule syntax); data eng (the tables it reads) |
| **7 · Gold** | `disposition` values from vocabulary; carries `ccdm_version` | Data eng |
| **Policy-to-Logic** | Its "validate against the CCDM registry" step points at `ccdm.vocabulary`. A rule with a field or value not in the registry is rejected at compile time | AI team — one config change |
| **Policy Store** | Each rule set records the `ccdm_version` it was written against | Data eng |
| **CIS** | No change. CIS keeps sending NLP output in its own format; the source map translates it | — |
| **CMS reference box** | Becomes `ccdm.code_reference`; same yearly load | Data eng |

---

## Versioning rule

- `ccdm_version` on every row in every table.
- Adding a table, a column, or an allowed value = minor bump (0.1 → 0.2). Old rows still valid.
- Renaming or removing anything = major bump (0.x → 1.0). Requires a golden-dataset replay and a rule-set review before promotion.
- A rule set says which version it was written against. The Auditor refuses to run a rule set against a CTG of a different major version.

---

## What to say to Reva

"You said the shared schema is the hardest part. Here's a concrete v0.1: seven tables, a vocabulary of allowed values, a source map for translation, a version number. I'd build the reference tables and silver first — about two weeks. The Abstractor and the rules then move onto these names; we can do that by changing the components or by wrapping the POC with adapters, your call. Once this exists, the CTG columns, the rule validation, and gold all point at the same dictionary. That's what makes the second use case a mapping exercise instead of a rebuild."

---

## Open questions for that session

1. Is there an existing vocabulary in the 2M-concept taxonomy for assertion / section / role that we should adopt instead of inventing values?
2. Does the Abstractor need any field silver doesn't produce (e.g. the full chart text, temporal expressions)?
3. Should `evidence_span` come from the NLP, or does the Abstractor find evidence itself? Decides whether table 7 is silver or CTG.
4. Adapter now, or change the POC now?
5. Who owns the vocabulary table going forward — change control process?
