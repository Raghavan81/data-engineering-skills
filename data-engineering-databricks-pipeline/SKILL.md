---
name: data-engineering-pipeline
description: >
  Use when designing, developing, or migrating ETL/data pipelines on Databricks.
  Supports net-new development and migration from existing systems (Redshift, Glue,
  Step Functions, dbt, etc.). Features: interactive one-question-at-a-time flow,
  multi-feed/multi-BRD handling, pattern analysis, parameterized workflow generation,
  manual code change detection (preserve/merge/regenerate/custom), dry-run approval,
  job-level state resumability, timestamped Q&A logging, RAG-based context retrieval,
  Genie Code/Spaces backend routing, and versioned HTML audit/efficiency reporting.
version: 4.0.0
author: Hermes Agent
license: MIT
metadata:
  hermes:
    tags: [databricks, data-engineering, etl, migration, pyspark, delta-lake, unity-catalog, redshift, glue, rag, genie]
    related_skills: [databricks-qa-pipeline-v1, databricks-qa-pipeline, databricks-de]
    supersedes: [databricks-de v3.0.0, data-engineering-pipeline v1.0.0]
---

# Databricks Data Engineering Pipeline Skill (v4)

## Overview

This skill guides you through designing, developing, and migrating ETL/data pipelines on Databricks. It supports two modes — **net-new development** and **migration from an existing system** — forked during Initial Preferences. Both modes share the same 7-phase structure but accept different input artifacts.

**Key principles:**

- One question at a time — never ask multiple questions in a single turn.
- Dry run before every artifact — show the plan, get approval, then generate.
- Job-level resumability — state is saved after every job, not just every phase.
- Manual change detection — detects if you've modified generated code and offers preserve/merge/regenerate/custom options.
- Multi-feed / multi-BRD aware — hybrid BRD-to-feed mapping, conflict resolution, processing order.
- Pattern analysis — group similar feeds for reusable templates vs. per-feed code.
- Document-format agnostic — accepts any format; LLM extracts what it needs.
- Auditing & efficiency — tracks project duration, LLM calls, retries, overrides, and manual rework.
- Living Audit Report — generates versioned HTML audit reports after every run.
- RAG integration — uses Databricks Genie internal RAG / vector tools for context retrieval; cite sources in artifacts.

---

## When to Use

- User wants to build a new Databricks ETL pipeline from requirements.
- User wants to migrate an existing pipeline (Redshift, Glue, dbt, Step Functions, Airflow, etc.) to Databricks.
- User wants to generate PySpark scripts, Unity Catalog DDLs, or Databricks Workflow configs.
- User wants unit tests generated alongside pipeline artifacts.
- User has multiple feeds/BRDs and needs conflict resolution and parameterized workflows.

**Do NOT use for:**

- QA, integration testing, or UAT — use `databricks-qa-pipeline-v1` for that.
- Infrastructure provisioning only (Terraform, networking) — this skill focuses on pipeline logic.

---

## EXECUTION RULES (MUST FOLLOW)

1. Ask ONE question at a time. Wait for the user's answer before asking the next.
2. Always show a dry run (plan) before generating or writing any artifact.
3. Never overwrite existing state — always resume from saved `state.json`.
4. Save `state.json` after every job completion (not just phase completion).
5. Log every Q&A pair with an ISO-8601 timestamp if logging is enabled.
6. If the user says "skip" or "no" to any optional step, move on without asking again.
7. If an LLM call fails, retry up to 3 times, then offer fallback options.
8. Never log or echo credentials (PAT tokens, secrets).
9. On phase re-run, mark downstream jobs/artifacts as `stale`.
10. Before any LLM task, run RAG retrieval when configured; cite sources in generated artifacts.

---

## WORKSPACE SETUP

### On First Invocation

1. Extract feature/project name from the user's request → convert to kebab-case.
2. Ask: **"Start new project"** or **"Select existing project"**.
3. Load or create `{workspace}/.de/{feature-name}/state.json`.
4. **If existing project:** Ask: **"Resume from last checkpoint"** OR **"Re-run specific phase"** (offer each of the 7 phases).
5. **If new project:** Proceed with Initial Preferences (one at a time), then Phase 1.

On resume, display: `Resuming {feature-name} at Phase {N}, Job {job_name}` (from `current_phase` and `current_job_index`).

Default workspace path (override if user specifies): `/home/ragkirth/workspace`

Project root: `{workspace}/.de/{feature_name}/`

### State File Schema (Comprehensive)

```json
{
  "feature_name": "string (kebab-case)",
  "mode": "new_development | migration",
  "current_phase": "1 | 2 | 3 | 4 | 5 | 6 | 7 | complete",
  "current_job_index": 0,
  "workspace_path": "/home/ragkirth/workspace",
  "project_path": "{workspace}/.de/{feature_name}",
  "phase_status": {
    "pre_flight": "not_started | in_progress | completed",
    "discovery": "not_started | in_progress | completed",
    "inventory_mapping": "not_started | in_progress | completed",
    "architecture_decision": "not_started | in_progress | completed",
    "artifact_generation": "not_started | in_progress | completed",
    "unit_testing": "not_started | in_progress | completed",
    "finalization": "not_started | in_progress | completed"
  },
  "context": {
    "save_qa_log": true,
    "llm_code_backend": "genie_code | foundation_models",
    "llm_analysis_backend": "genie_spaces | foundation_models",
    "llm_auth_mode": "pat_token | sdk_auto",
    "genie_code_space_id": "string (if genie_code)",
    "genie_spaces_id": "string (if genie_spaces)",
    "foundation_model_endpoint": "string (if foundation_models)",
    "workspace_url": "https://...",
    "rag_vector_db_path": "string | null",
    "cluster_id": "string | null",
    "dq_framework": "dlt_expectations | pyspark_assertions | great_expectations | user_choice | decide_per_job",
    "qa_logging_enabled": true
  },
  "inputs": {
    "documents": [
      {
        "doc_name": "string",
        "doc_type": "brd | fsd | stt | data_catalog | sample_data | other",
        "feeds_covered": ["feed_a"],
        "extracted_context": {}
      }
    ],
    "technical_artifacts": [
      {
        "artifact_name": "string",
        "artifact_type": "ddl | glue_script | feed_config | dag | terraform | dbt | qc_script | other",
        "feeds_covered": ["feed_a"],
        "extracted_context": {}
      }
    ]
  },
  "project": {
    "feeds": [
      {
        "feed_id": "string",
        "feed_name": "string",
        "source_system": "string",
        "brd_reference": "string",
        "brd_sources": ["doc_name"],
        "brd_rules": [],
        "fsd_logic": {},
        "stt_mappings": [],
        "tables": [],
        "validation_rules": [],
        "processing_order": 0,
        "conflicts": [],
        "conflicts_resolved": []
      }
    ],
    "multi_feed_rules": [],
    "conflict_log": []
  },
  "architecture": {
    "recommended": {},
    "approved": {},
    "layer_mapping": {},
    "compute_config": {},
    "orchestration_type": "databricks_workflows | delta_live_tables",
    "dq_framework": "string",
    "catalog_schema_strategy": {}
  },
  "inventory": {
    "tables": [],
    "columns": {},
    "jobs": []
  },
  "jobs": [
    {
      "phase": "5 | 6",
      "job_name": "string",
      "feed_name": "string",
      "status": "not_started | dry_run_approved | in_progress | completed | skipped | stale | failed",
      "artifacts": {
        "ddl": "path",
        "script": "path",
        "workflow_config": "path",
        "dq_checks": "path",
        "unit_tests": "path"
      },
      "artifact_metadata": {
        "script_checksum": "sha256",
        "ddl_checksum": "sha256",
        "workflow_checksum": "sha256",
        "dq_checksum": "sha256",
        "generated_at": "ISO-8601",
        "last_modified": "ISO-8601",
        "modified_by_user": false,
        "resolution": "preserve | merge | regenerate | custom | null"
      },
      "checksums": {},
      "modified_flags": {},
      "unit_tests": {},
      "dry_run_approved": false,
      "timestamp": "ISO-8601"
    }
  ],
  "artifacts": {
    "structure": "mirrored | recommended | custom",
    "base_path": "string",
    "manifest": [],
    "workflow_configs": [],
    "ddl_scripts": [],
    "pyspark_jobs": [],
    "unit_tests": [],
    "audit_reports": [],
    "workflows": [
      {
        "workflow_name": "string",
        "workflow_file": "path",
        "feeds_using_this_workflow": ["feed_a"],
        "parameter_configs": {}
      }
    ],
    "checksums_file": "checksums.json"
  },
  "audit_metrics": {
    "project_start_time": "ISO-8601",
    "project_end_time": "ISO-8601 | null",
    "total_llm_calls": 0,
    "llm_revision_requests": 0,
    "llm_call_retries": 0,
    "user_overrides_count": 0,
    "phase_re_runs": 0,
    "manual_rework_count": 0,
    "total_user_approvals": 0,
    "total_artifacts_generated": 0,
    "manual_code_changes_detected": 0,
    "manual_code_changes_resolved_preserve": 0,
    "manual_code_changes_resolved_merge": 0,
    "manual_code_changes_resolved_regenerate": 0,
    "manual_code_changes_resolved_custom": 0
  },
  "rework_history": [
    {
      "timestamp": "ISO-8601",
      "event_type": "phase_re_run | manual_change | conflict_resolution | override | rag_refresh",
      "phase": "string",
      "job_name": "string | null",
      "details": "string"
    }
  ]
}
```

**Rules for state:**

- Never store PAT tokens or secrets in `state.json`.
- On phase re-run, mark downstream jobs/artifacts as `stale`.
- Append significant events to `rework_history`.
- Increment `audit_metrics` on LLM calls, retries, approvals, and manual-change resolutions.

---

## INITIAL PREFERENCES SETUP

Ask ALL preference questions **ONE AT A TIME** before Phase 1.

### Q1: Project Selection

```
"Would you like to start a new project or select an existing one?

1. Start new project
2. Select existing project"
```

If existing: load `state.json` and offer resume or phase re-run.

### Q2: Mode Selection (FORK POINT)

```
"Is this a net-new pipeline development or a migration from an existing system?

1. New Development — I have requirements documents (BRD, FSD, STT, etc.)
2. Migration — I have an existing pipeline to port to Databricks"
```

Store as: `mode` → `new_development` or `migration`

### Q3: Q&A Logging

```
"Would you like me to save a timestamped log of all questions and answers?
This creates a de-log.md file useful for team handoffs and audits.

1. Yes — Save Q&A log
2. No — Skip logging"
```

Store as: `context.save_qa_log`

If `true`: create and maintain `.de/{feature-name}/de-log.md`. Every entry MUST use:

```
### [YYYY-MM-DDTHH:MM:SS] Q: <question asked>
**A:** <user's answer>
```

### Q4: LLM Backend (Code Generation)

```
"Which LLM backend would you like to use for code generation (PySpark, DDL, workflows, DQ scripts)?

1. Genie Code — optimized for Databricks code generation
2. Databricks Foundation Models — direct model API, faster, more control"
```

Store as: `context.llm_code_backend`

### Q5: LLM Backend (Analysis & Planning)

```
"Which LLM backend would you like to use for analysis, discovery, and architecture recommendations?

1. Genie Spaces — conversational, data-aware, uses your Databricks data context
2. Databricks Foundation Models — direct model API"
```

Store as: `context.llm_analysis_backend`

### Q6: RAG Context

```
"Where is your RAG vector DB stored (path, Unity Catalog volume, or Genie knowledge source)?
If you do not use RAG, reply 'skip'."
```

Store as: `context.rag_vector_db_path` (null if skipped)

### Q7: LLM Authentication

```
"How would you like to authenticate with Databricks?

1. PAT Token — I have a personal access token
2. SDK Auto — I am running inside a Databricks notebook (auto-credentials)"
```

Store as: `context.llm_auth_mode`

**If `pat_token`:** Ask for PAT (never log it), workspace URL, and backend-specific IDs:
- Genie Code space ID (if `llm_code_backend` = genie_code)
- Genie Spaces ID (if `llm_analysis_backend` = genie_spaces)
- Foundation Model endpoint name (if either backend uses foundation_models)

**If `sdk_auto`:** Ask for backend-specific IDs only (SDK resolves credentials automatically).

Test connectivity immediately after credentials are provided. On failure: re-prompt.

### Q8: Additional Instructions

```
"Any additional architectural or development instructions for this project?
Reply 'none' to skip."
```

Save state after Initial Preferences.

---

## PHASE 1: PRE-FLIGHT CHECK

**Objective:** Validate workspace, auth, LLM connectivity, cluster readiness, and RAG access.

Run these checks automatically, display results, ask user to confirm:

```
Pre-Flight Checks:
✓ Databricks workspace reachable
✓ Authentication confirmed (PAT / SDK)
✓ LLM code backend connectivity confirmed (Genie Code / Foundation Models)
✓ LLM analysis backend connectivity confirmed (Genie Spaces / Foundation Models)
✓ RAG vector DB accessible (if configured)
✓ Source system accessible (if migration)
✓ Write access to output path (.de/{feature-name}/ and artifact base path)
✓ Compute cluster available (or serverless configured)
✓ Project directory structure created
```

**Branching:**

- **All confirmed** → proceed to Phase 2
- **Partial access** → list what works; ask user to proceed with limitations or fix first
- **Critical failure** (workspace, auth, LLM) → show fix instructions; do not proceed until resolved

**Artifacts:**

- `pre_flight_report.md` (connectivity, versions, cluster info, backend status)

Save state:

```json
{
  "current_phase": "2",
  "phase_status": { "pre_flight": "completed" },
  "audit_metrics": { "project_start_time": "ISO-8601" }
}
```

---

## PHASE 2: DISCOVERY

**Objective:** Ingest documents, detect multi-feed patterns, extract atomic processes, resolve conflicts, and recommend processing order.

### Input Collection (one at a time; at least one document required for new development)

For **both modes**, ask for documents:

```
"Do you have any of the following documents? Provide as many as you have.
You can upload a file, paste content, or provide a file path. Skip any you don't have.

- Business Requirements Document (BRD)
- Functional Specification Document (FSD)
- Source-to-Target Mapping (STT)
- Data Catalog / Schema Definitions
- Sample Data Files"
```

For **migration mode**, additionally ask:

```
"Do you have any of the following technical artifacts?
- SQL DDLs / Stored Procedures
- Glue / Spark scripts
- Feed Configuration JSONs
- Airflow DAGs / Step Function definitions
- Terraform / IaC files
- dbt models
- QC / Validation scripts
- EventBridge / Lambda configs
- Other (describe)"
```

If new development and user provides no documents: ask again before proceeding.

### RAG Retrieval (if configured)

Before extraction for each document batch:

1. Trigger RAG search using Databricks Genie internal vector tools / configured vector path.
2. Synthesize results; prioritize project docs and migration artifacts.
3. Merge RAG context into `extracted_context`; cite sources in `discovery_report.md`.

### Extraction

For each provided artifact, the LLM extracts and stores under `inputs` / `project.feeds`:

| Artifact Type | Extracted Information |
|---|---|
| BRD | Business rules, data domains, SLAs, feed boundaries |
| FSD | Transformation logic, field mappings, business logic |
| STT | Source→target column mappings, type casts, derivations |
| Data Catalog | Schemas, PII flags, lineage hints |
| Sample Data | Column inference, formats, anomalies |
| DDL | Table names, columns, types, constraints, distribution keys |
| Stored Proc | Transformation logic, source/target tables, business rules |
| Glue script | Job name, source, target, transformations, dependencies |
| Feed config JSON | Feed name, format, delimiter, column count, validation rules |
| DAG / Step Function | Execution order, triggers, dependencies, error handling |
| Terraform | Compute config, IAM, storage, network topology |
| dbt models | Model names, refs, transformations, tests |
| QC scripts | Existing check logic to port as DQ rules |

### Multi-Feed / Multi-BRD Handling

1. **BRD-to-Feed Mapping (Hybrid):** LLM infers which BRD sections apply to which feeds; user confirms or corrects (one feed at a time if many).
2. **Conflicting Rules:** Flag conflicts across BRDs/feeds; ask user to resolve (precedence order, merge rule, or feed-specific override). Log in `conflicts_resolved`, `conflict_log`, and `rework_history`.
3. **Processing Order:** LLM recommends feed/job order based on dependencies; user approves or adjusts.

Display extracted summary per feed. Ask: **"Does this look correct? Any corrections?"**

**Artifacts:**

- `discovery_report.md` (feeds, conflicts, processing order, RAG citations)
- `feeds_manifest.json` (structured feed definitions)

Save state after Phase 2 (`phase_status.discovery`: `completed`).

---

## PHASE 3: INVENTORY & MAPPING

**Objective:** Create table-level, column-level, and job-level mappings; infer layers; document lineage.

Build a complete inventory at three levels:

### Table-Level Mapping

```
| Source Table         | Target Delta Table        | Layer  | Update Type | Feed      |
|----------------------|---------------------------|--------|-------------|-----------|
| stg.l_keycode        | silver.l_keycode          | Silver | Upsert      | feed_a    |
| dwh.fact_orders      | gold.fact_orders          | Gold   | Overwrite   | feed_b    |
```

### Column-Level Mapping

```
| Source Column   | Source Type | Target Column   | Target Type | Transform / Cast     |
|-----------------|-------------|-----------------|-------------|----------------------|
| client_code     | VARCHAR(2)  | client_code     | STRING      | None                 |
| order_date      | DATE        | order_date      | DATE        | to_date() cast       |
| revenue         | NUMERIC     | revenue         | DECIMAL     | CAST(revenue AS DEC) |
```

### Job/Script-Level Mapping

```
| Source Job           | Source Type  | Target Job              | Target Type         | Feed   |
|----------------------|--------------|-------------------------|---------------------|--------|
| LoadToStage.py       | Glue ETL     | load_to_bronze.py       | Databricks Workflow | feed_a |
| StageToCore.py       | Glue ETL     | bronze_to_silver.py     | Databricks Workflow | feed_a |
| ValidationEngine.py  | Glue PyShell | dq_checks.py            | DLT Expectations    | feed_b |
```

### Layer Naming & Lineage

1. Infer layer names from source system (e.g., L1/L2/L3/DWH, RAW/STG/DWH).
2. Recommend Databricks equivalent (Bronze/Silver/Gold or keep source names).
3. Ask: **"Do you approve this layer mapping, or would you like to rename any layers?"**
4. Document lineage: source → layer → layer → target (Mermaid or text in `lineage_diagram.md`).
5. Identify SCD types (Type 1, 2, or 3) where applicable.

Populate `inventory` and `architecture.layer_mapping`.

**Artifacts:**

- `inventory_mapping.csv`
- `layer_definitions.json` (layer names, purposes, retention policies)
- `lineage_diagram.md`

Save state after Phase 3 (`phase_status.inventory_mapping`: `completed`).

---

## PHASE 4: ARCHITECTURE DECISION

**Objective:** Recommend and finalize architecture, compute, orchestration, DQ strategy, and Unity Catalog design.

### Recommendation

Based on discovery and inventory, the LLM recommends (use `purpose="analysis"` backend):

```
Recommended Architecture for {feature_name}:

Pattern:         {medallion | star schema | data vault — with rationale}
Compute:         {Small | Medium | Large | Serverless | Custom — ask user}
Storage:         Unity Catalog + Delta Lake
Layer structure: {mapped layers}
DQ Framework:    {recommendation}
Orchestration:   Databricks Workflows OR Delta Live Tables
File format:     Delta (Parquet + transaction log)
Update pattern:  MERGE / Overwrite / Append per feed
Catalog/Schema:  {Unity Catalog naming and access strategy}

Rationale:
- {feed/job mapping summary}
- {DQ porting summary}
- {orchestration rationale}
```

### User Decision

```
"Does this architecture work for you?
1. Yes — proceed with recommended architecture
2. Modify — I want to change specific components (ask one at a time)
3. Override — I will specify the full architecture"
```

Also ask (one at a time):

- **Compute allocation:** Small, Medium, Large, Serverless, or Custom (capture specs in `compute_config`).
- **Orchestration choice:** Databricks Workflows OR Delta Live Tables (pros/cons per feed if mixed).
- **DQ framework:**
  1. DLT Expectations
  2. PySpark Assertions
  3. Great Expectations
  4. Decide per job

Store in `architecture.approved`.

**Artifacts:**

- `architecture_decision_record.md` (ADR: recommendation, rationale, alternatives)
- `compute_sizing.json`
- `orchestration_plan.json`
- `dq_framework.md`

Save state after Phase 4 (`phase_status.architecture_decision`: `completed`).

---

## PHASE 5: ARTIFACT GENERATION

**Objective:** Generate PySpark scripts, DDLs, Workflow configs, DQ checks, and unit test stubs via pattern-aware, per-job dry-run flow.

### Artifact Structure

If source code was provided:

- Mirror source folder structure by default.
- Ask: **"Mirror your source folder structure, or recommend a new one?"**

If no source code:

- LLM recommends structure from architecture.
- Display recommendation; ask for approval before creating files.

Set `artifacts.structure` and `artifacts.base_path`.

### Pattern Analysis (before per-job loop)

1. Analyze all feeds for similarities (source system, transforms, update pattern, validation, schema shape).
2. Group feeds with concise reasoning.
3. Per group, ask:

```
"For this group ({feed_names}), would you like to:
1. Generate ONE reusable template + parameter configs (recommended for similar feeds)
2. Generate separate code for each feed"
```

If template: create shared script/workflow with `parameter_configs` per feed in `artifacts.workflows`.

### Per-Job Loop (one job at a time)

Use `current_job_index` to resume. For each job in `inventory.jobs`:

#### Step A — Dry Run (MANDATORY)

```
Dry Run for Job: {job_name} (Feed: {feed_name})

Will generate:
  📄 scripts/{layer}/{job_name}.py          — PySpark transformation script
  📄 ddl/{layer}/{table_name}.sql           — Unity Catalog DDL
  📄 workflows/{job_name}_config.json       — Databricks Workflow config (or shared parameterized workflow)
  📄 dq/{job_name}_checks.py               — Data quality checks
  📄 tests/unit/test_{job_name}.py          — Unit test file

Source mapping:
  Input:  {source_table} ({source_type})
  Output: {target_table} (Delta, {update_type})
  Logic:  {transformation_summary}
  DQ:     {framework for this job}

Proceed?
1. Yes — generate these artifacts
2. Modify plan — change something before generating
3. Skip this job"
```

Only proceed after explicit approval. Set `dry_run_approved: true` and increment `total_user_approvals`.

#### Step B — Generate Artifacts

Route LLM calls with `purpose="code_gen"`. Generate in order; show each; ask **1. Yes  2. Revise  3. Skip**:

1. Unity Catalog DDL — `CREATE SCHEMA/TABLE IF NOT EXISTS`, grants as needed.
2. PySpark script — schema enforcement, MERGE/overwrite/append, error handling, parameterization.
3. Databricks Workflow config JSON — tasks, cluster, dependencies (or parameterize shared workflow).
4. DQ checks — per approved framework.

After each artifact: compute SHA-256 checksum, set `generated_at`, `modified_by_user: false`.

#### Step C — Unit Test Stubs

Generate tests for:

- Schema validation (columns, types, nullability)
- Row count checks (source vs target range)
- Business rules (BRD/FSD/feed config)
- Transformation correctness (sample input → expected output)

Save to `tests/unit/test_{job_name}.py`

#### Step D — Save Job State

Update job entry: `status: completed`, artifact paths, checksums, `current_job_index++`, `total_artifacts_generated++`.

Save `state.json` after **every** job.

#### Manual Change Detection

Before regenerating any artifact for a job with existing files:

1. Compare on-disk file checksum to `artifact_metadata.*_checksum`.
2. If mismatch and `modified_by_user` was false → set `manual_code_changes_detected++`, alert user:

```
"Manual changes detected in {artifact_path}. How would you like to proceed?
1. Preserve — keep your edits; do not regenerate this artifact
2. Merge — show diff; LLM merges your changes with new generation
3. Regenerate — replace with newly generated artifact (backup old file)
4. Mark custom — skip future auto-regeneration for this artifact"
```

Log resolution in `artifact_metadata.resolution` and appropriate `audit_metrics.manual_code_changes_resolved_*` counter; append `rework_history`.

After all jobs: write `checksums.json`; set `phase_status.artifact_generation`: `completed`; proceed to Phase 6.

**Phase 5 artifact directories:**

- `ddl_scripts/`
- `pyspark_jobs/`
- `workflow_configs/`
- `dq_checks/`
- `tests/unit/`
- `checksums.json`

---

## PHASE 6: UNIT TESTING

**Objective:** Run and validate unit tests for schema, row counts, business rules, and transformation correctness.

Run all generated unit tests; display results:

```
Unit Test Results:

Job: load_to_bronze
  ✓ Schema validation       PASS
  ✓ Row count check         PASS (source: 1.2M, target: 1.2M)
  ✗ Business rule: keycode  FAIL — 42 rows with invalid keycode format
  ✓ Transformation check    PASS
```

For any FAIL, ask: **"Fix now"** or **"Log as known gap"**

- **Fix now** → revise artifact (Phase 5 regen for that job), re-run test, update checksums.
- **Known gap** → add to `artifacts.manifest` known_gaps section.

**Artifacts:**

- `test_results.json` (test status, pass/fail, coverage per job)

Save state; set `phase_status.unit_testing`: `completed`.

---

## PHASE 7: FINALIZATION

**Objective:** Generate audit report, summarize metrics, archive project, and provide efficiency insights.

### Deliverables

1. **Artifact Manifest** — list all generated files with paths under `artifacts.manifest`.
2. **Source → Target Mapping Summary** — condensed from Phase 3.
3. **Unit Test Results Summary** — pass/fail counts per job; links to test files.
4. **Known Gaps** — items logged in Phases 5–6 with recommended next steps.
5. **Living Audit Report** — versioned HTML: `reports/audit_report_YYYYMMDD_HHMMSS.html`

Include in audit report:

- Project timeline (`project_start_time` → `project_end_time`)
- Phase completion status
- Feed/job inventory summary
- Artifact manifest with checksums
- Manual change events and resolutions
- LLM metrics: calls, retries, revision requests, overrides
- RAG sources cited (if used)
- Efficiency insights (interpret metrics — e.g., high retries vs. overrides vs. manual edits)

### Metrics (update `audit_metrics`)

- Project duration
- Total artifacts, approvals, phase re-runs
- Manual rework breakdown
- LLM efficiency (calls per artifact, retry rate)

### Archive

Zip all artifacts, state, and logs → `project_archive.zip` (optional; ask user).

Save state:

```json
{
  "current_phase": "complete",
  "phase_status": { "finalization": "completed" },
  "audit_metrics": { "project_end_time": "ISO-8601" }
}
```

If Q&A logging enabled, ensure `de-log.md` is complete with ISO-8601 entries.

---

## LLM CALL WRAPPER

### Routing Strategy

| Purpose | Backend field | Options |
|---|---|---|
| `code_gen` | `context.llm_code_backend` | `genie_code`, `foundation_models` |
| `analysis` | `context.llm_analysis_backend` | `genie_spaces`, `foundation_models` |

**Fallback:** If preferred Genie backend unavailable, offer Foundation Models or manual artifact upload.

**Supported formats:** `expected_format`: `python`, `json`, `sql`, `text`, `yaml`

### RAG Integration

Before any `analysis` or `code_gen` task when `rag_vector_db_path` is set:

1. Trigger RAG search (Genie vector tools or configured path).
2. Synthesize results; prioritize project docs.
3. Include citations in generated artifacts and `discovery_report.md`.

### Implementation Reference

Use this unified wrapper pattern (configure module-level vars during Initial Preferences; never persist PAT in `state.json`):

```python
import hashlib
import json
import logging
import re
import time

import requests

logger = logging.getLogger(__name__)

# Set during Initial Preferences — in-memory only for PAT
llm_code_backend = "genie_code"       # or foundation_models
llm_analysis_backend = "genie_spaces"  # or foundation_models
llm_auth_mode = "pat_token"            # or sdk_auto
workspace_url = ""
genie_code_space_id = ""
genie_spaces_id = ""
foundation_model_endpoint = ""
databricks_token = ""  # never write to state.json


def _strip_markdown_fences(text: str) -> str:
    text = re.sub(r"^```[a-z]*\s*", "", text.strip())
    text = re.sub(r"```\s*$", "", text)
    return text.strip()


def _extract_code_block(text: str, expected_format: str) -> str:
    pattern = rf"```(?:{expected_format})?\s*([\s\S]*?)```"
    match = re.search(pattern, text, re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return _strip_markdown_fences(text)


def call_llm(
    prompt: str,
    label: str,
    purpose: str = "analysis",
    expected_format: str = "text",
) -> str:
    """
    Unified LLM entry point.
    purpose: code_gen | analysis
    expected_format: python | json | sql | text | yaml
    Retries 3x with backoff; raises with fallback options on final failure.
    """
    raw = _call_llm_raw(prompt, label, purpose)
    if expected_format == "text":
        return raw
    return _extract_code_block(raw, expected_format)


def _resolve_backend(purpose: str) -> str:
    if purpose == "code_gen":
        return llm_code_backend
    return llm_analysis_backend


def _resolve_genie_space_id(backend: str) -> str:
    if backend == "genie_code":
        return genie_code_space_id
    return genie_spaces_id


def _call_llm_raw(prompt: str, label: str, purpose: str) -> str:
    backend = _resolve_backend(purpose)
    for attempt in range(1, 4):
        try:
            if backend in ("genie_code", "genie_spaces"):
                if llm_auth_mode == "pat_token":
                    return _call_llm_genie_pat(prompt, label, backend)
                return _call_llm_genie_sdk(prompt, label, backend)
            if llm_auth_mode == "pat_token":
                return _call_llm_foundation_pat(prompt, label)
            return _call_llm_foundation_sdk(prompt, label)
        except Exception as exc:
            logger.warning("[%s] attempt %d failed: %s", label, attempt, exc)
            if attempt == 3:
                raise RuntimeError(
                    f"LLM unavailable after 3 attempts for '{label}': {exc}\n"
                    "Options: 1. Retry  2. Switch backend  3. Provide artifact manually"
                ) from exc
            time.sleep([5, 15][attempt - 1])
    raise RuntimeError("unreachable")


def _call_llm_genie_pat(prompt: str, label: str, backend: str) -> str:
    space_id = _resolve_genie_space_id(backend)
    url = f"{workspace_url}/api/2.0/genie/spaces/{space_id}/start-conversation"
    resp = requests.post(
        url,
        headers={"Authorization": f"Bearer {databricks_token}", "Content-Type": "application/json"},
        json={"content": prompt},
        timeout=60,
    )
    resp.raise_for_status()
    data = resp.json()
    return _genie_poll(space_id, data["conversation"]["id"], data["message"]["id"], label)


def _genie_poll(space_id, conversation_id, message_id, label, timeout=120, interval=2):
    start = time.time()
    while time.time() - start < timeout:
        url = (
            f"{workspace_url}/api/2.0/genie/spaces/{space_id}/conversations/"
            f"{conversation_id}/messages/{message_id}"
        )
        resp = requests.get(
            url,
            headers={"Authorization": f"Bearer {databricks_token}"},
            timeout=30,
        )
        resp.raise_for_status()
        msg = resp.json()
        if msg["status"] == "COMPLETED":
            return _strip_markdown_fences(msg.get("content") or msg.get("text") or str(msg))
        if msg["status"] == "FAILED":
            raise RuntimeError(f"Genie failed: {msg.get('error')}")
        time.sleep(interval)
    raise TimeoutError(f"Genie timed out for {label}")


def _call_llm_genie_sdk(prompt: str, label: str, backend: str) -> str:
    from databricks.sdk import WorkspaceClient

    space_id = _resolve_genie_space_id(backend)
    w = WorkspaceClient()
    path = f"/api/2.0/genie/spaces/{space_id}/start-conversation"
    data = w.api_client.do("POST", path=path, body={"content": prompt})
    conversation_id = data["conversation"]["id"]
    message_id = data["message"]["id"]
    start = time.time()
    while time.time() - start < 120:
        msg = w.api_client.do(
            "GET",
            path=(
                f"/api/2.0/genie/spaces/{space_id}/conversations/"
                f"{conversation_id}/messages/{message_id}"
            ),
        )
        if msg["status"] == "COMPLETED":
            return _strip_markdown_fences(msg.get("content") or msg.get("text") or str(msg))
        if msg["status"] == "FAILED":
            raise RuntimeError(f"Genie SDK failed: {msg.get('error')}")
        time.sleep(2)
    raise TimeoutError(f"Genie SDK timed out for {label}")


def _call_llm_foundation_pat(prompt: str, label: str) -> str:
    url = f"{workspace_url}/serving-endpoints/{foundation_model_endpoint}/invocations"
    resp = requests.post(
        url,
        headers={"Authorization": f"Bearer {databricks_token}", "Content-Type": "application/json"},
        json={"messages": [{"role": "user", "content": prompt}], "max_tokens": 8192},
        timeout=120,
    )
    resp.raise_for_status()
    return _strip_markdown_fences(resp.json()["choices"][0]["message"]["content"])


def _call_llm_foundation_sdk(prompt: str, label: str) -> str:
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    resp = w.serving_endpoints.query(
        name=foundation_model_endpoint,
        messages=[{"role": "user", "content": prompt}],
        max_tokens=8192,
    )
    return _strip_markdown_fences(resp.choices[0].message.content)


def artifact_checksum(path: str) -> str:
    with open(path, "rb") as f:
        return hashlib.sha256(f.read()).hexdigest()
```

**Usage examples:**

```python
# Discovery / architecture
summary = call_llm(prompt, "discovery-extract", purpose="analysis", expected_format="json")

# PySpark generation
script = call_llm(prompt, "job-bronze-load", purpose="code_gen", expected_format="python")
```

---

## COMMON PITFALLS

1. **Never ask multiple questions at once** — one question per turn, always.
2. **Never skip the dry run** — mandatory before every artifact generation (DDL, scripts, workflows, DQ, unit tests).
3. **Never overwrite a completed job's artifacts** without explicit user approval.
4. **Ignoring manual changes** — always checksum before regenerate; offer preserve/merge/regenerate/custom.
5. **Multi-BRD / multi-feed** — confirm BRD-to-feed mapping and resolve conflicts before Phase 5.
6. **Parameterized workflows** — when feeds are grouped, do not duplicate identical workflow JSONs.
7. **Document format agnostic** — if parsing fails, ask user to paste the relevant section.
8. **Layer naming** — infer from source first; never assume Bronze/Silver/Gold without user approval.
9. **PAT token** — never write to `state.json`, logs, `de-log.md`, audit HTML, or chat echo.
10. **Foundation Models endpoint** — verify endpoint exists during Pre-Flight.
11. **Genie Space IDs** — must be in the same workspace as `workspace_url`; use correct ID per backend (Code vs Spaces).
12. **Job-level resumability** — always read `current_job_index` before Phase 5 loop.
13. **New development** — at least one requirements document required.
14. **Phase re-run** — mark downstream jobs `stale`; do not silently use outdated artifacts.
15. **Wrong LLM backend for task** — use `code_gen` for artifacts, `analysis` for discovery/architecture.
16. **Skipping RAG** — when configured, retrieve and cite; do not hallucinate project-specific rules.
17. **Credential leakage** — never echo PAT in chat, reports, or error messages.
18. **Losing state** — save `state.json` after every job, not only at phase boundaries.

---

## VERIFICATION CHECKLIST

- [ ] `state.json` exists and is valid JSON after every job
- [ ] Dry run shown and approved before every artifact
- [ ] All artifacts listed in `artifacts.manifest`
- [ ] Per-artifact checksums stored in `jobs[].artifact_metadata` and `checksums.json`
- [ ] Unit tests generated for every non-skipped job
- [ ] Unit tests executed in Phase 6 with results in `test_results.json`
- [ ] Known gaps logged in finalization manifest
- [ ] `de-log.md` has ISO-8601 timestamped Q&A entries (if logging enabled)
- [ ] No credentials written to any file
- [ ] Layer mapping approved by user before Phase 5
- [ ] Architecture and DQ framework approved before Phase 5
- [ ] Multi-feed conflicts resolved and recorded in `project.feeds`
- [ ] Pattern analysis completed; template vs. per-feed decision recorded
- [ ] Parameterized workflow `parameter_configs` documented when using shared templates
- [ ] Manual changes detected and resolved via user-approved option
- [ ] Downstream artifacts marked `stale` on upstream phase re-run
- [ ] `audit_metrics` updated (LLM calls, retries, manual change counters)
- [ ] Versioned HTML audit report generated in Phase 7
- [ ] `rework_history` captures phase re-runs, overrides, and manual change events
- [ ] RAG context retrieved and cited when configured
- [ ] Efficiency insights included in audit report
- [ ] One-question-at-a-time flow maintained throughout

---

## QUICK START

1. Invoke the skill with project name and requirements (or migration artifacts).
2. Answer Initial Preferences — one question at a time (project, mode, logging, backends, RAG, auth).
3. Complete Phase 1 Pre-Flight — fix critical failures before continuing.
4. Phase 2 Discovery — provide documents; confirm feed mapping and processing order.
5. Phases 3–4 — approve inventory, layers, and architecture.
6. Phase 5 — dry-run and generate artifacts job-by-job; save state after each job.
7. Phase 6 — run unit tests; fix or log known gaps.
8. Phase 7 — audit report, manifest, archive.

**Resume anytime:** load `state.json`, display checkpoint, continue from `current_phase` / `current_job_index`.

---

## REFERENCES

- Databricks Workflows: https://docs.databricks.com/workflows/
- Delta Live Tables: https://docs.databricks.com/delta-live-tables/
- Unity Catalog: https://docs.databricks.com/data-governance/unity-catalog/
- PySpark API: https://spark.apache.org/docs/latest/api/python/
- Databricks Genie: https://docs.databricks.com/genie/
- Redshift Migration: https://docs.databricks.com/migration/
- Glue to Databricks: https://docs.databricks.com/integrations/
