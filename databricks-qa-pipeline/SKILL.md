---
name: databricks-qa-pipeline-v1
description: >
  Interactive skill to generate an automated QA pipeline for Databricks ETL/data feeds.
  Cleaner resumability with state.json (FSD3-style) and upstream catalog/schema detection.
  Guides through 7 phases: pre-flight check, discovery, planning, artifact generation,
  execution, feedback loop, and finalization. Uses Databricks DBRX via Model Serving.
  Supports two auth modes: PAT token (REST API) or SDK auto-credentials (databricks.sdk).
triggers:
  - "set up QA pipeline"
  - "automate QA for databricks"
  - "generate test cases for ETL"
  - "QA pipeline for data feeds"
  - "automated data validation"
  - "create validation scripts"
  - "QA automation databricks"
  - "run QA with dbrx"
  - "dbrx test case generation"
---

# Databricks Automated QA Pipeline v1

## Overview

This skill generates a fully automated QA pipeline for Databricks ETL/data feeds with:
- **Cleaner resumability** via `.qa/{feature}/state.json` (FSD3 pattern)
- **Upstream catalog/schema detection** from Databricks workspace
- **One-question-at-a-time** interactive flow
- **Persistent state** for multi-session workflows
- **Dual LLM auth modes**: PAT token (REST API) or SDK auto-credentials

Platform: Databricks
Default LLM: Databricks DBRX via Model Serving (endpoint name configurable)
LLM Auth: PAT token + full URL (if available) OR databricks.sdk auto-credentials (if inside Databricks)
Outputs: Test cases (Excel), test data (CSV), validation scripts (Python/Spark),
         validation reports (HTML), feedback log (CSV), execution plan (MD)

---

## TRIGGER

Load this skill when the user asks to:
- Set up QA for a Databricks pipeline
- Generate test cases, test data, or validation scripts for ETL feeds
- Automate data validation on Databricks
- Run QA on staging, warehouse, or datamart layers

---

## EXECUTION RULES (MUST FOLLOW)

1. Ask ONE question at a time. Wait for the user's answer before asking the next.
2. Always show the execution plan before generating or executing anything.
3. Never overwrite existing state — always resume from saved state.json.
4. Collect feedback after each major artifact generation step (optional for user).
5. Store all feedback in feedback_log.csv and global_patterns.json.
6. If the user says "skip" or "no" to any optional step, move on without asking again.
7. If LLM call fails, retry up to 3 times then offer a fallback.
8. Save session state to state.json after every phase completion.

---

## WORKSPACE SETUP

### On First Invocation

When `@databricks-qa-pipeline-v1` is invoked, **ALWAYS**:

1. **Extract feature name** from user's request (convert to kebab-case)
2. **Check for state file:** Read `{workspace}/.qa/{feature-name}/state.json`
3. **If state file doesn't exist:**
   - Run Upstream Detection (see [Upstream Detection](#upstream-detection))
   - Create `.qa/{feature-name}/` directory in workspace
   - Create initial `state.json` with feature name, detected catalogs/schemas
   - Ask Initial Preferences (see [Initial Preferences Setup](#initial-preferences-setup))
   - Start Phase 1 (Pre-Flight Check)
4. **If state file exists:**
   - Read current phase and resume from there
   - Load context (feature name, preferences, upstream flags, etc.)
   - Display: "Resuming {feature-name} at Phase {N}"

### State File Schema

The agent MUST create and maintain this file at `{workspace}/.qa/{feature-name}/state.json`:

```json
{
  "feature_name": "string (kebab-case)",
  "current_phase": "1",
  "phase_status": {
    "pre_flight": "not_started|in_progress|completed",
    "security_scanning": "not_started|in_progress|completed",
    "quality_gate": "not_started|in_progress|completed",
    "perf_a11y": "not_started|in_progress|completed",
    "compliance": "not_started|in_progress|completed"
  },
  "upstream_detection": {
    "catalogs": ["catalog1", "catalog2"],
    "schemas": {
      "catalog1": ["schema_a", "schema_b"],
      "catalog2": ["schema_c"]
    },
    "tables": {
      "catalog1.schema_a": ["table_1", "table_2"],
      "catalog1.schema_b": ["table_3"]
    },
    "detected_at": "2026-05-02T08:35:00Z"
  },
  "context": {
    "save_qa_log": true,
    "llm_auth_mode": "pat_token|sdk_auto",
    "databricks_token_present": true,
    "genie_space_id": "string (required for both pat_token and sdk_auto modes)",
    "llm_system_prompt": "string (sdk_auto only — not applicable for Genie)",
    "llm_max_tokens": 8192,
    "llm_temperature": 0,
    "workspace_url": "https://..."
  },
  "project": {
    "name": "string",
    "source_type": "api_feeds|database_tables|file_uploads|mixed",
    "feeds": [],
    "stages": ["RAW", "STG", "DWH", "Analytics"],
    "validation_priorities": []
  },
  "artifacts": {
    "qa_log": ".qa/{feature-name}/qa-log.md",
    "test_plan": ".qa/{feature-name}/test-plan.md",
    "security_report": ".qa/{feature-name}/security-report.md",
    "quality_report": ".qa/{feature-name}/quality-report.md",
    "validation_scripts": ".qa/{feature-name}/scripts/",
    "feedback_log": ".qa/{feature-name}/feedback_log.csv",
    "feeds": {
      "{stage}/{feed}": {
        "test_cases": ".qa/{feature-name}/artifacts/{stage}/{feed}/test_cases.xlsx",
        "test_data": ".qa/{feature-name}/artifacts/{stage}/{feed}/test_data.csv",
        "validation_script": ".qa/{feature-name}/artifacts/{stage}/{feed}/validation_script.py",
        "results": ".qa/{feature-name}/artifacts/{stage}/{feed}/results.csv"
      }
    }
  },
  "rework_history": []
}
```

---

## UPSTREAM DETECTION

### What to do

Before asking any questions, automatically detect Databricks workspace structure:

```python
# Pseudo-detection to perform
detection = {
    "databricks_workspace": "Can we reach the Databricks workspace URL?",
    "catalogs": "List all accessible catalogs",
    "schemas": "For each catalog, list all schemas",
    "tables": "For each schema, list all tables (with row counts, column names, types)"
}
```

### Display to user

Show detected structure in a table:

```
Detected Databricks Workspace Structure:

Catalog: main
  └─ Schema: raw
     ├─ table_1 (1.2M rows, 15 cols)
     └─ table_2 (500K rows, 8 cols)
  └─ Schema: staging
     ├─ table_3 (1.2M rows, 18 cols)

Catalog: analytics
  └─ Schema: marts
     ├─ customer_dim (50K rows, 12 cols)
     └─ orders_fact (2.5M rows, 25 cols)
```

### Branching

- All detected → proceed to Initial Preferences Setup
- Partial access → show warning, ask user to confirm accessible catalogs
- No access → offer to auto-generate Databricks setup notebook, then re-check

---

## INITIAL PREFERENCES SETUP

**On first invocation, after Upstream Detection, ask ALL preference questions ONE AT A TIME:**

### Q1: Q&A Logging

```
"Would you like me to save a log of all our questions and answers during this process?
This creates a qa-log.md file that tracks all decisions and context.

1. Yes - Save Q&A log (recommended for team projects)
2. No - Skip Q&A logging"
```

If `save_qa_log` is `true`, create and maintain `qa-log.md` throughout the process.
Each entry **must** include an ISO-8601 timestamp. Use this format for every Q&A pair logged:

```
### [YYYY-MM-DDTHH:MM:SS] Q: <question asked>
**A:** <user's answer>
```

Example:
```
### [2026-05-18T10:35:42] Q: What is the name of your data pipeline or project?
**A:** Teva Facebook Ads Pipeline
```

If `save_qa_log` is `false`, skip all Q&A logging (do not create or update qa-log.md).

### Q2: LLM Authentication Mode

```
"Do you have a Databricks personal access token (PAT)?"

1. Yes — I have a PAT token
2. No  — I am running inside a Databricks notebook (credentials are automatic)
```

Store as: `llm_auth_mode`  
→ `"pat_token"` if Yes  
→ `"sdk_auto"` if No  

---

**If `llm_auth_mode = "pat_token"` → ask Q2a and Q2b:**

### Q2a: Databricks PAT Token

```
"Please provide your Databricks personal access token (PAT).
This is used as the Bearer token. It will not be logged or echoed."
```

Store as: `databricks_token` (masked in display, never written to state.json or any log)

### Q2b: Genie Space ID

```
"Please provide your Genie Space ID.
This is the unique identifier for your Genie Space in Databricks.
Example: 01f0123456789abcdef"
```

Store as: `genie_space_id`

Test Genie connectivity:
```python
import requests
url = f"{workspace_url}/api/2.0/genie/spaces/{genie_space_id}/start-conversation"
resp = requests.post(
    url,
    headers={"Authorization": f"Bearer {databricks_token}", "Content-Type": "application/json"},
    json={"content": "Reply with OK"},
    timeout=30,
)
# 200 → confirm and continue
# 401/403 → "Token or Genie Space ID invalid — please re-enter"
# 404 → "Genie Space not found — check the Space ID"
```

If Genie connectivity confirmed: skip Q3 (system prompt), Q4 (max tokens), Q5 (temperature) — these are not applicable for Genie.

---

**If `llm_auth_mode = "sdk_auto"` → ask Q2c only:**

### Q2c: Genie Space ID

```
"Please provide your Genie Space ID.
The SDK will resolve credentials automatically from the Databricks runtime.
Example: 01f0123456789abcdef"
```

Store as: `genie_space_id`

Test Genie connectivity via SDK:
```python
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
path = f"/api/2.0/genie/spaces/{genie_space_id}/start-conversation"
resp = w.api_client.do("POST", path=path, body={"content": "Reply with OK"})
# Success → confirm and continue
# Exception → "Genie Space unreachable — check Space ID and cluster permissions"
```

If connectivity confirmed: skip Q3 (system prompt), Q4 (max tokens), Q5 (temperature) — not applicable for Genie.

---

### Q3: System Prompt

```
"What system prompt should the LLM use?
Press Enter to use the default."

Default: "You are an expert QA engineer and data validation specialist.
          Return only valid JSON arrays. No explanation. No markdown."
```

Store as: `llm_system_prompt`

### Q4: Max Tokens

```
"Maximum tokens per LLM response? (default: 8192)"
```

Store as: `llm_max_tokens` (default: 8192)

### Q5: Temperature

```
"What temperature? (0 = consistent, 1 = creative, default: 0)"
```

Store as: `llm_temperature` (default: 0)

---

## PHASE 1: PRE-FLIGHT CHECK

### What to do

Run these checks automatically before asking any questions:

```python
checks = {
    "databricks_workspace": "Can we reach the Databricks workspace URL?",
    "catalog_schema": "Are the detected catalogs/schemas accessible?",
    "compute_cluster": "Is a cluster available?",
    "llm_api": "Can we reach the LLM API?",
    "read_access": "Do we have read access to source tables?",
    "write_access": "Do we have write access to output path?"
}
```

### Checklist to show user

Display this and ask the user to confirm each item:

```
Pre-Flight Checks:

✓ Databricks workspace URL & credentials configured
✓ Detected catalogs/schemas accessible
✓ Compute cluster available (min 2 cores, 8GB RAM)
✓ LLM API connectivity confirmed
✓ Read access to source data tables
✓ Write access to output folder
```

### Branching

- All confirmed → proceed to Phase 2
- Some missing → show setup instructions, ask user to fix, re-check
- Critical failure → offer to auto-generate a Databricks setup notebook, then re-check

### Save state

After Phase 1 completion, update `state.json`:
```json
{
  "current_phase": "2",
  "phase_status": { "pre_flight": "completed" }
}
```

---

## PHASE 2: DISCOVERY & CONTEXT GATHERING

### Step 2.1 — Project basics (ask one at a time)

**Q1:** "What is the name of your data pipeline or project?"
→ Store as: `project_name`

**Q2:** "What is the primary data source type?"
Options: API feeds | Database tables | File uploads | Mixed
→ Store as: `source_type`

**Q3:** "How many feeds or data sources do you want to QA?
You can give a number or list them by name."
→ Store as: `feeds` (list)
Adaptive:
- If 1 feed → skip "set up all at once or one-by-one" question
- If 5+ feeds → ask "Would you like to batch these into groups?"

**Q4:** "What are the stages in your pipeline?
(e.g. RAW, STG, DWH, Analytics — or your own names)"
Default suggestion: RAW → STG → DWH → Analytics
→ Store as: `stages` (ordered list)

### Step 2.2 — Context documents & data (per feed, one at a time)

For each feed in feeds list:

Say: "Let's gather context for [feed_name]."

#### A. Documents (all optional — user can skip any):

**Q:** "Do you have a Business Requirements Document (BRD)?
You can upload a file, paste the content, or skip."
→ If provided: extract business rules from it
→ Store as: `brd_content`

**Q:** "Do you have a Functional Specification Document (FSD)?
Upload, paste, or skip."
→ If provided: extract transformation logic
→ Store as: `fsd_content`

**Q:** "Do you have a Source-to-Target mapping (STT)?
Upload, paste, or skip."
→ If provided: extract field mappings, auto-generate column-level validations
→ Store as: `stt_content`

#### B. Data source (at least one required):

**Q:** "How would you like to provide data for [feed_name]?
1. Point to a Databricks table (catalog.schema.table)
2. Upload a sample file (CSV or Excel)
3. Paste sample data (CSV format)"
→ Store as: `data_source_type`, `data_source_ref`

**Schema detection:**
- If Databricks table: query schema from catalog, sample 100 rows
- If file: parse headers, infer data types
- If pasted: parse CSV, infer types
Display detected schema and ask: "Does this look correct? Any corrections?"
→ Store as: `schema` (confirmed)

#### C. Validation priorities (optional, default: all):

**Q:** "Which validation types are most important for [feed_name]?
(Press Enter to select all, or choose specific ones)
1. Data Quality (nulls, duplicates, format)
2. Completeness (row counts, missing values)
3. Accuracy (business logic, calculations)
4. Schema Consistency (column names, data types)
5. Performance (row count thresholds)"
→ Store as: `validation_priorities`

#### D. Custom rules (optional):

**Q:** "Any custom validation rules for [feed_name]?
e.g. 'Revenue must be > 0', 'Date must be within last 30 days'
Press Enter to skip."
→ Store as: `custom_rules`

### Save state

After Phase 2 completion, update `state.json`:
```json
{
  "current_phase": "3",
  "phase_status": { "discovery": "completed" },
  "project": { ... }
}
```

---

## PHASE 3: VALIDATION STRATEGY & PLAN

### Step 3.1 — Show strategy

Display the validation strategy based on Phase 2 inputs:

```
Validation Strategy for [feed_name]:

Priority 1: Data Quality
  - Null checks on [columns]
  - Duplicate detection on [key_columns]
  - Format validation on [date/numeric columns]

Priority 2: Completeness
  - Row count thresholds: min 100K, max 10M
  - Missing value tolerance: < 5%

Priority 3: Accuracy
  - Business rule: Revenue > 0
  - Calculation check: total = sum(line_items)

Priority 4: Schema Consistency
  - Column names match expected [list]
  - Data types: [mapping]

Priority 5: Performance
  - Query time < 30s for 1M rows
```

### Step 3.2 — Confirm & adjust

**Q:** "Does this strategy look good?
1. Yes, proceed
2. Adjust priorities
3. Add/remove validation types"

If "Adjust" → go back to Phase 2 Q5 and re-ask validation priorities.

### Save state

After Phase 3 completion, update `state.json`:
```json
{
  "current_phase": "4",
  "phase_status": { "planning": "completed" }
}
```

---

## PHASE 4: ARTIFACT GENERATION

Generate all QA artifacts based on Phase 2 & 3 inputs.
For each feed + stage combination, run Steps 4.1 → 4.3 in order.

> **The LLM call pattern used in every step below is determined by `llm_auth_mode`
> collected in Initial Preferences. See the [LLM CALL WRAPPER](#llm-call-wrapper) section.**

### Step 4.1 — Generate test cases

```python
prompt = f"""You are a QA engineer. Generate comprehensive test cases for the following ETL feed.

Feed:                    {feed_name}
Stage:                   {stage}
Schema:                  {schema}
Data source:             {data_source_ref or sample_file_path or '(pasted data)'}
Validation priorities:   {validation_priorities}
Business rules (BRD):    {brd_content}
Transformation logic (FSD): {fsd_content}
Field mappings (STT):    {stt_content}
Custom rules:            {custom_rules}

Output format: pipe-delimited rows with columns:
test_id | feed | stage | validation_type | rule | expected_result
No explanation. No markdown. Only the pipe-delimited rows."""

test_cases_text = call_llm(prompt, table_name=f"{feed_name}_{stage}_testcases")
```

Save to: `.qa/{feature-name}/artifacts/{stage}/{feed}/test_cases.xlsx`

Tell user: "Generated [count] test cases for {feed} at {stage}."
Ask (optional): "Review test cases? 1. Yes  2. No, continue"

### Step 4.2 — Generate test data

```python
prompt = f"""Generate representative test data for the following ETL feed.
Include happy path, edge cases, nulls, boundary values, and invalid data rows.

Feed:         {feed_name}
Stage:        {stage}
Schema:       {schema}
Test cases:   {test_cases_summary}
Custom rules: {custom_rules}

Output: pipe-delimited rows with headers matching the schema. Generate 200-500 rows.
No explanation. Header row first, then data rows only."""

test_data = call_llm(prompt, table_name=f"{feed_name}_{stage}_testdata")
```

Save to: `.qa/{feature-name}/artifacts/{stage}/{feed}/test_data.csv`

### Step 4.3 — Generate validation script

```python
source_ref_note = (
    f"Delta table: {data_source_ref}"
    if data_source_type == "catalog_table"
    else f"Sample DataFrame from: {sample_file_path or 'pasted data'}"
)

prompt = f"""Generate a PySpark validation script for Databricks.

Feed:                  {feed_name}
Stage:                 {stage}
Schema:                {schema}
Test cases:            {test_cases}
Validation priorities: {validation_priorities}
Custom rules:          {custom_rules}
Source:                {source_ref_note}

Requirements:
- Use PySpark DataFrame API
- If source is a Delta table, load with spark.table("{data_source_ref}")
- If source is a sample file, assume DataFrame `source_df` is already loaded
- For each test case write an assertion function
- Return results DataFrame: test_id, test_name, status (PASS/FAIL/SKIP),
  actual_value, expected_value, message
- Save results CSV to: .qa/{feature_name}/artifacts/{stage}/{feed}/results.csv
No explanation. Output only the Python script."""

raw_script = call_llm(prompt, table_name=f"{feed_name}_{stage}_valscript")
# Strip accidental code fences
script = re.sub(r'^```(?:python)?\n?', '', raw_script).rstrip('`').strip()
```

Save to: `.qa/{feature-name}/artifacts/{stage}/{feed}/validation_script.py`

### Save state

After Phase 4 completion, update `state.json` — **write every generated artifact path** so the state file is the single source of truth for all output locations:
```json
{
  "current_phase": "5",
  "phase_status": { "artifact_generation": "completed" },
  "artifacts": {
    "feeds": {
      "{stage}/{feed}": {
        "test_cases": ".qa/{feature-name}/artifacts/{stage}/{feed}/test_cases.xlsx",
        "test_data": ".qa/{feature-name}/artifacts/{stage}/{feed}/test_data.csv",
        "validation_script": ".qa/{feature-name}/artifacts/{stage}/{feed}/validation_script.py",
        "results": ".qa/{feature-name}/artifacts/{stage}/{feed}/results.csv"
      }
    }
  }
}
```
Repeat the `feeds` entry for every stage/feed combination that was processed.

---

## PHASE 5: EXECUTION

Run validation scripts against Databricks tables.

Display results in a summary table:

```
Execution Results:

Feed: orders_raw
  ├─ Data Quality: PASS (0 nulls, 0 duplicates)
  ├─ Completeness: PASS (1.2M rows, 0% missing)
  ├─ Accuracy: PASS (all revenue > 0)
  ├─ Schema: PASS (all columns match)
  └─ Performance: PASS (query time 2.5s)

Feed: customers_raw
  ├─ Data Quality: FAIL (50 nulls in email)
  ├─ Completeness: WARN (5.2% missing in phone)
  ├─ Accuracy: PASS
  ├─ Schema: PASS
  └─ Performance: PASS
```

### Save state

After Phase 5 completion, update `state.json`:
```json
{
  "current_phase": "6",
  "phase_status": { "execution": "completed" }
}
```

---

## PHASE 6: FEEDBACK & FINALIZATION

### Step 6.1 — Collect feedback (optional)

**Q:** "Would you like to provide feedback on this QA pipeline?
This helps improve future runs.
1. Yes - Provide feedback
2. No - Skip"

If "Yes":
- Ask: "What worked well?"
- Ask: "What could be improved?"
- Ask: "Any patterns you noticed?"
- Store in `feedback_log.csv` and `global_patterns.json`

### Step 6.2 — Generate final report

Create a comprehensive QA report with:
- Executive summary (pass/fail counts)
- Detailed results per feed
- Recommendations for remediation
- Next steps

### Save state

After Phase 6 completion, update `state.json`:
```json
{
  "current_phase": "complete",
  "phase_status": { "feedback": "completed" }
}
```

---

## LLM CALL WRAPPER

`call_llm()` is the single function used by all Phase 4 steps.
Its internal implementation switches based on `llm_auth_mode` from state.json.

```python
import json, logging, re, time

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# JSON helpers (shared by both auth modes)
# ---------------------------------------------------------------------------

def _strip_markdown_fences(text: str) -> str:
    text = re.sub(r"^```json\s*", "", text.strip())
    text = re.sub(r"^```\s*",     "", text)
    text = re.sub(r"```\s*$",     "", text)
    return text.strip()

def _validate_and_clean_json(raw: str, table_name: str) -> str:
    cleaned = _strip_markdown_fences(raw)
    try:
        parsed = json.loads(cleaned)
        if not isinstance(parsed, list):
            raise ValueError(f"Expected JSON array, got {type(parsed).__name__}")
        logger.info(f"[{table_name}] JSON validated — {len(parsed)} item(s)")
        return json.dumps(parsed, ensure_ascii=False)
    except (json.JSONDecodeError, ValueError) as exc:
        logger.warning(f"[{table_name}] Invalid JSON: {exc}")
        return json.dumps([{
            "check_id": 0, "rule_type": "ERROR",
            "source_column": "", "target_column": "",
            "description": f"LLM did not return valid JSON: {exc}", "sql": "",
        }])

# ---------------------------------------------------------------------------
# Auth mode: Genie Space
# Used when: llm_auth_mode == "pat_token" AND use_genie == True
# Requires:  workspace_url, databricks_token, genie_space_id
# ---------------------------------------------------------------------------

import os, time, requests as _requests

def _genie_start_conversation(question: str) -> dict:
    url = f"{workspace_url}/api/2.0/genie/spaces/{genie_space_id}/start-conversation"
    resp = _requests.post(
        url,
        headers={"Authorization": f"Bearer {databricks_token}", "Content-Type": "application/json"},
        json={"content": question},
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()

def _genie_get_message(conversation_id: str, message_id: str) -> dict:
    url = f"{workspace_url}/api/2.0/genie/spaces/{genie_space_id}/conversations/{conversation_id}/messages/{message_id}"
    resp = _requests.get(
        url,
        headers={"Authorization": f"Bearer {databricks_token}", "Content-Type": "application/json"},
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()

def _genie_ask_followup(conversation_id: str, question: str) -> dict:
    url = f"{workspace_url}/api/2.0/genie/spaces/{genie_space_id}/conversations/{conversation_id}/messages"
    resp = _requests.post(
        url,
        headers={"Authorization": f"Bearer {databricks_token}", "Content-Type": "application/json"},
        json={"content": question},
        timeout=60,
    )
    resp.raise_for_status()
    return resp.json()

def _genie_wait_for_completion(
    conversation_id: str, message_id: str, timeout: int = 120, poll_interval: int = 2
) -> dict:
    start = time.time()
    while time.time() - start < timeout:
        message = _genie_get_message(conversation_id, message_id)
        status = message["status"]
        if status == "COMPLETED":
            return message
        if status == "FAILED":
            raise RuntimeError(f"Genie request failed: {message.get('error')}")
        time.sleep(poll_interval)
    raise TimeoutError("Timed out waiting for Genie response")

def _call_llm_genie(prompt: str, table_name: str) -> str:
    """
    Call Databricks Genie Space API.
    Starts a new conversation per LLM call (stateless per artifact).
    The Genie response text is passed through _validate_and_clean_json.
    """
    logger.info(f"[{table_name}] Calling Genie Space: {genie_space_id}")
    initial = _genie_start_conversation(prompt)
    conversation_id = initial["conversation"]["id"]
    message_id = initial["message"]["id"]
    result = _genie_wait_for_completion(conversation_id, message_id)
    # Extract text content from the Genie response
    raw = (
        result.get("content")
        or result.get("text")
        or str(result)
    ).strip()
    return _validate_and_clean_json(raw, table_name)

# ---------------------------------------------------------------------------
# Auth mode: SDK auto — Genie via WorkspaceClient
# Used when: llm_auth_mode == "sdk_auto"
# Requires:  genie_space_id  (credentials injected by Databricks runtime)
# ---------------------------------------------------------------------------

from databricks.sdk import WorkspaceClient
import time as _time

_SDK_RETRIES       = 3
_SDK_RETRY_BACKOFF = [5, 15, 30]

def _sdk_genie_start_conversation(w: WorkspaceClient, question: str) -> dict:
    path = f"/api/2.0/genie/spaces/{genie_space_id}/start-conversation"
    return w.api_client.do("POST", path=path, body={"content": question})

def _sdk_genie_get_message(w: WorkspaceClient, conversation_id: str, message_id: str) -> dict:
    path = f"/api/2.0/genie/spaces/{genie_space_id}/conversations/{conversation_id}/messages/{message_id}"
    return w.api_client.do("GET", path=path)

def _sdk_genie_ask_followup(w: WorkspaceClient, conversation_id: str, question: str) -> dict:
    path = f"/api/2.0/genie/spaces/{genie_space_id}/conversations/{conversation_id}/messages"
    return w.api_client.do("POST", path=path, body={"content": question})

def _sdk_genie_wait_for_completion(
    w: WorkspaceClient, conversation_id: str, message_id: str,
    timeout: int = 120, poll_interval: int = 2
) -> dict:
    start = _time.time()
    while _time.time() - start < timeout:
        message = _sdk_genie_get_message(w, conversation_id, message_id)
        status = message["status"]
        if status == "COMPLETED":
            return message
        if status == "FAILED":
            raise RuntimeError(f"Genie request failed: {message.get('error')}")
        _time.sleep(poll_interval)
    raise TimeoutError("Timed out waiting for Genie response")

def _call_llm_sdk(prompt: str, table_name: str) -> str:
    """
    Call Databricks Genie Space via WorkspaceClient SDK.
    Credentials are injected automatically by the Databricks runtime.
    """
    w = WorkspaceClient()
    logger.info(f"[{table_name}] SDK Genie call, space: {genie_space_id}")
    for attempt in range(1, _SDK_RETRIES + 1):
        try:
            initial = _sdk_genie_start_conversation(w, prompt)
            conversation_id = initial["conversation"]["id"]
            message_id = initial["message"]["id"]
            result = _sdk_genie_wait_for_completion(w, conversation_id, message_id)
            raw = (
                result.get("content")
                or result.get("text")
                or str(result)
            ).strip()
            return _validate_and_clean_json(raw, table_name)
        except Exception as exc:
            logger.warning(f"[{table_name}] SDK Genie attempt {attempt} failed: {exc}")
            if attempt < _SDK_RETRIES:
                _time.sleep(_SDK_RETRY_BACKOFF[attempt - 1])
            else:
                raise RuntimeError(
                    f"SDK Genie call failed for '{table_name}' after {_SDK_RETRIES} attempts: {exc}"
                ) from exc

# ---------------------------------------------------------------------------
# Unified entry point — branches on llm_auth_mode from state.json
# ---------------------------------------------------------------------------

def call_llm(prompt: str, table_name: str) -> str:
    """
    Call the LLM using whichever auth mode was collected during Initial Preferences.

    llm_auth_mode = "pat_token" → uses _call_llm_genie()  (Genie via REST + PAT)
    llm_auth_mode = "sdk_auto"  → uses _call_llm_sdk()    (Genie via WorkspaceClient SDK)
    Both modes require genie_space_id.
    """
    if llm_auth_mode == "pat_token":
        return _call_llm_genie(prompt, table_name)
    elif llm_auth_mode == "sdk_auto":
        return _call_llm_sdk(prompt, table_name)
    else:
        raise ValueError(f"Unknown llm_auth_mode: '{llm_auth_mode}'")
```

On **third failure**: tell user "LLM unavailable after 3 attempts. Options:
  1. Retry now
  2. Switch auth mode (PAT/Genie ↔ SDK)
  3. Provide the artifact content manually"
Log failure in `feedback_log.csv` with timestamp, feed, stage, error.

> **Genie mode note:** Both auth modes (PAT and SDK) use Genie Space — `genie_space_id` is
> always required. Neither mode supports `max_tokens` or `temperature`. The poll timeout
> defaults to 120 s and can be adjusted if the Genie Space is known to be slow.

---

## PITFALLS

1. **Never ask multiple questions at once** — breaks the one-at-a-time rule
2. **Always save state after each phase** — enables seamless resumability
3. **Don't skip Upstream Detection** — it's critical for Databricks integration
4. **Confirm schema with user** — auto-detected schemas can be wrong
5. **Test LLM connectivity early** — catch auth/endpoint issues before Phase 4
6. **Handle partial Databricks access gracefully** — some users may have limited catalog access
7. **Never log or echo `databricks_token`** — not in state.json, logs, or any output file
8. **PAT/Genie mode**: if Genie API returns 401/403, immediately re-prompt for token and Genie Space ID
9. **SDK/Genie mode**: `WorkspaceClient()` requires Databricks notebook/job context — if run locally, user must run `databricks configure` via CLI first
10. **Both Genie modes**: `genie_space_id` must belong to the same Databricks workspace — cross-workspace Genie calls are not supported
11. **Both Genie modes**: Genie responses may include natural-language text alongside structured data; `_validate_and_clean_json` will attempt to extract a JSON array but may return an error row — prompt the user to rephrase if this happens repeatedly
12. **PAT mode only**: `databricks_token` is required — never store or log it

---

## VERIFICATION

After completing all phases:

1. Check that `state.json` exists and is valid JSON
2. Verify all artifacts are created in `.qa/{feature-name}/`
3. Confirm feedback_log.csv has entries (if user provided feedback)
4. Display completion summary with artifact paths
