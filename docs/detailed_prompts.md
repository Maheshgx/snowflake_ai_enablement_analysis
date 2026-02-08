# Snowflake AI Readiness Agent - System Instructions

> **Generated On:** 2026-02-06 07:14 UTC  
> **Agent Version:** 2.0  
> **Document Type:** Agent System Prompt & Instructions

---

## AGENT SYSTEM PROMPT

```
===============================================================================
SNOWFLAKE AI READINESS AGENT - SYSTEM INSTRUCTIONS
===============================================================================

You are the Snowflake AI Readiness Agent, an autonomous utility for analyzing
Snowflake environments to identify AI/ML enablement opportunities.

CORE DIRECTIVES:
----------------
1. EXECUTION MODE: You operate as a structured application with defined entry point
   - Entry: main() → parse_arguments() → run_agent()
   - All execution flows through the run_agent() function

2. OPERATION MODE: Read-only analysis ONLY
   - Execute only SELECT statements
   - Never modify data or schema
   - Maintain complete audit trail

3. TIMESTAMP STANDARD (CRITICAL):
   - ALL timestamps in output documentation MUST use UTC format
   - Format: "YYYY-MM-DD HH:MM UTC" (e.g., "2026-02-06 07:14 UTC")
   - Use get_utc_timestamp() function for all timestamp generation
   - Never use local timezone in any output

4. METADATA STANDARD:
   - ALL generated documents MUST include "Generated On" field
   - Use format_doc_header() for markdown documents
   - Use get_generated_metadata() for JSON outputs
   - Include agent name and version in all outputs

5. OUTPUT REQUIREMENTS:
   - Markdown files: Use format_doc_header(title, description)
   - JSON files: Include "_metadata" key with get_generated_metadata()
   - SQL files: Include header comment block with UTC timestamp
   - CSV files: No header required (data only)

6. OUTPUT DESTINATION (CRITICAL):
   - Standard output folder: snowflake-ai-enablement-reports/
   - ALL generated reports MUST be written to this directory
   - Directory structure:
     * snowflake-ai-enablement-reports/metadata/     - JSON/CSV data files
     * snowflake-ai-enablement-reports/reports/      - Analysis reports
     * snowflake-ai-enablement-reports/profiles/     - Schema profiles
     * snowflake-ai-enablement-reports/logs/         - Audit trail & logs
   - Configurable via config.yaml: output.directory

7. RUN MODE HANDLING (CRITICAL):
   - Two modes available: "fresh" and "append"
   
   FRESH MODE (default):
     - Overwrites existing analysis data
     - Best for: First-time runs, complete re-analysis
     - Optional backup before overwrite
   
   APPEND MODE (incremental):
     - Preserves existing analysis data
     - Loads existing candidates from previous runs
     - Merges new candidates with existing (avoids duplicates)
     - Tracks run history in run_history.json
     - Best for: Analyzing databases incrementally
     - Workflow: Run multiple times with different target_databases
       to build comprehensive report across all databases
   
   - Functions for append mode:
     * load_existing_candidates() - Load previous candidates
     * load_existing_metadata() - Load run history context
     * merge_candidates() - Merge new with existing (deduplicated)
     * save_run_history() - Track incremental runs
   
   - Configurable via config.yaml: run_mode.mode, run_mode.append_strategy

8. PROGRESS TRACKING (USER EXPERIENCE):
   - Display real-time progress during long-running operations
   - Use print_progress() for in-place progress updates
   - Use print_progress_complete() for phase completion summaries
   
   Progress Display Format:
     [████████░░░░░░░░░░░░░░░░░░░░░░] 1,234/3,379 (36.5%) | Current: DB.TABLE.COL | OK:800 Cache:650 Err:34
   
   - Visual progress bar with percentage
   - Current object being processed
   - Running statistics (OK/New/Cache/Err counts)
   
   Applied to phases:
     * Phase 2B: Data analysis (OK/New/Cache/Err)
     * Phase 2E: Full scan analysis (OK/Err)
     * Phase 5B: Candidate confirmation (Confirmed/Unconfirmed)

9. REPORT INDEX (COMPLETION OUTPUT):
   - Generate README.md as main report entry point
   - Show link to README.md at agent completion
   - README.md contains:
     * Quick navigation to all reports
     * Analysis summary (environment, candidates, statistics)
     * Objects analyzed by database breakdown
     * Generated files listing
     * Next steps guidance
   
   Completion Output Format:
     ======================================================================
     📋 VIEW FULL REPORT: snowflake-ai-enablement-reports/README.md
     ======================================================================

EXECUTION PHASES:
-----------------
Phase 1:  Metadata Discovery (databases, schemas, tables, columns)
Phase 2:  AI Candidate Identification
Phase 2A: Load Analysis Cache
Phase 2B: Sampling Pass (data quality analysis)
Phase 2C: Save Analysis Cache
Phase 2D: Identify Top Candidates
Phase 2E: Full Scan Analysis
Phase 2F: Generate Data Analysis Reports
Phase 3:  Enhanced Analysis (text-rich columns, education tables, PII)
Phase 4:  Data Profiling (text and variant column profiling)
Phase 5:  Scoring Candidates
Phase 5B: Flagging Confirmed Candidates
Phase 6:  Report Generation (all with UTC timestamps)

STAGE RESTART CAPABILITY:
-------------------------
The agent supports restarting from any stage using --start-stage option.

Valid stages: 1, 2, 2A, 2B, 2C, 2D, 2E, 2F, 3, 4, 5, 5B, 6

Usage: python scripts/snowflake_full_analysis.py --start-stage 2B

When restarting from a later stage:
- Agent loads intermediate state from previous run files
- Skipped stages display "[SKIPPED - Loading from cache]"
- Required files must exist in snowflake-ai-enablement-reports/

Key functions for stage restart:
- should_run_stage(current, start) - Determines if stage should execute
- load_intermediate_state(start_stage) - Loads cached data for resume
- VALID_STAGES constant - Defines execution order
- STAGE_ORDER dict - Maps stage to execution sequence

SAFETY CONSTRAINTS:
-------------------
- Reject any non-SELECT queries
- Log all queries to audit trail
- Use SAMPLE/LIMIT for efficiency
- Respect configured timeouts
- Cache results to avoid redundant analysis

===============================================================================
```

---

## Feature Generation Prompts

The following prompts were used to generate the agent features:

---

### Agent Architecture Transformation Prompt (v2.0)

```
Please refactor the existing utility code and its internal instructions with the following changes:

1.  **Architecture Transformation:**
    * **Application Mode:** Change the code to run as a structured application (using a main entry point).
    * **Agent Conversion:** Refactor the logic so the utility functions as an autonomous agent.

2.  **Standardized Metadata & Time:**
    * Include a "Generated On" metadata field in all output documentation.
    * **Constraint:** All timestamps must be in UTC format with the timezone explicitly labeled (e.g., 2026-02-06 12:44 UTC).

3.  **Documentation Update:**
    * Revise the README/internal docs to reflect the new agent-based workflow and the UTC timestamp requirement.

4.  **Prompt Self-Update (Crucial):**
    * Locate the system prompt or "instruction" section of this utility.
    * Rewrite it to incorporate all the above changes so that future iterations of this agent are aware they are running as an application and must follow the UTC documentation standard.
```

**Implementation Result:**
- Added `AGENT_VERSION` and `AGENT_NAME` constants
- Created `get_utc_timestamp()` for standardized UTC formatting
- Created `get_generated_metadata()` for consistent output metadata
- Created `format_doc_header()` for markdown document headers
- Refactored entry point: `main()` → `parse_arguments()` → `run_agent()`
- Added CLI argument support: `--config`, `--dry-run`, `--start-stage`, `--version`, `--quiet`
- Updated all output functions to include UTC timestamps
- Updated this document with agent system instructions

---

### Stage Restart Feature Prompt (v2.1)

```
Update code to restart from any stage like from 2B or 2C.
Also update documentation what are prerequisites if one has to start from stage like 2b, 2c etc.
```

**Implementation Result:**
- Added `VALID_STAGES` constant defining execution order: `['1', '2', '2A', '2B', '2C', '2D', '2E', '2F', '3', '4', '5', '5B', '6']`
- Added `STAGE_ORDER` dict mapping stage to sequence number
- Added `should_run_stage(current, start)` function for conditional execution
- Added `load_intermediate_state(start_stage)` to restore cached data when resuming
- Added `--start-stage` / `-s` CLI argument to `parse_arguments()`
- Modified `run_agent()` to accept `start_stage` parameter
- Wrapped all phases with `if should_run_stage('X', start_stage):` conditionals
- Added `json_serializer()` to handle Decimal types from Snowflake queries
- Fixed `json.JSONEncodeError` → `TypeError` in exception handling
- Updated `docs/USER_GUIDE.md` with comprehensive stage restart documentation

**Stage Restart Prerequisites:**
| Start Stage | Required Files |
|-------------|----------------|
| 2, 2A, 2B | `metadata/full_inventory.csv` |
| 2C | Above + `metadata/all_candidates.json` |
| 2D-2F | Above + `metadata/data_analysis_cache.json` |
| 3 | All Phase 2 outputs |
| 4 | Above + `metadata/enhanced_text_candidates.json` |
| 5, 5B | Above + `profiles/text_column_profiles.json` |
| 6 | All above with scores and confirmation data |

---

### Original Requirements Prompt (v1.0)

```
Role: Expert Snowflake Developer & Data Architect 
Task: Refactor the existing Snowflake AI Readiness Utility repository to enhance connectivity, filtering, and data analysis depth.

Objective 1: Configuration & Connectivity Overhaul

Refactor Connection Logic: Modify the main connection module to read credentials and settings from a YAML configuration file.

File Standard: Create a config.example.yaml template file in the root directory. This template should include placeholders for Snowflake account details (account, user, password/key, warehouse, role) and analysis parameters.

Implementation: Ensure the script looks for a config.yaml file (and includes this in .gitignore) for local execution.

Database Filtering: Implement a specific parameter in the config.yaml (e.g., target_databases: [DB1, DB2]) that allows the user to specify a list of databases to analyze.

If the list is empty or commented out, the script should default to analyzing all accessible databases.

The code must apply this filter early in the execution process to avoid unnecessary metadata fetching for irrelevant databases.

Objective 2: Deep Data Analysis (Data Profiling)

Expand Analysis Scope: Move beyond the current "metadata-only" approach. The script must now perform actual data profiling on high-potential columns identified during the metadata phase.

Validation Logic:

For columns identified as candidates for AI/GenAI (e.g., based on naming conventions like _TEXT, _JSON, or data types like VARIANT, VARCHAR), generate and execute queries to sample the actual data.

Heuristics: Implement logic to check:

Sparsity: Is the column mostly null?

Cardinality: Is it high/low cardinality?

Content Type: Does the text field actually contain meaningful natural language (for GenAI) or just system codes? Does the VARIANT column contain valid JSON structure?

Output: Update the final report to flag "Confirmed Candidates" where the physical data supports the metadata hypothesis.

Objective 3: Documentation

Prompt Documentation: Create a new markdown file in the docs/ folder (e.g., docs/detailed_prompts.md).

Content: Inside this file, include the exact prompt text used to generate these features (i.e., this prompt) to serve as a record of the requirements and design intent.

Constraints & Best Practices:

Ensure all data profiling queries are efficient (use SAMPLE, LIMIT, or Snowflake's HLL functions where appropriate) to minimize compute costs.

The solution must remain strictly read-only regarding database schemas; do not alter any existing tables or data.

Maintain existing logging and error handling standards.

Output Deliverables:

Updated Python source code files.

config.example.yaml file.

docs/detailed_prompts.md file.
```

---

## Implementation Summary

### Objective 1: Configuration & Connectivity Overhaul

**To Run**

**Dry Run (Recommended First):**
1. Copy `config.example.yaml` to `config.yaml`
2. Fill in your Snowflake credentials
3. Set `dry_run.enabled: true` in config.yaml
4. Run: `python scripts/snowflake_full_analysis.py`
5. Review the scope and estimated runtime

**Full Analysis:**
1. After successful dry run, set `dry_run.enabled: false`
2. Optionally set `target_databases` to filter which databases to analyze
3. Run: `python scripts/snowflake_full_analysis.py`

**Files Modified/Created:**
- `config.example.yaml` - YAML configuration template with all settings
- `scripts/snowflake_full_analysis.py` - Refactored connection logic
- `.gitignore` - Added `config.yaml` to ignore list

**Key Features Implemented:**
1. **YAML Configuration Loading** (`load_yaml_config()`)
   - Loads from `config.yaml` in repo root
   - Falls back to `config.example.yaml` with warning
   - Falls back to environment variables for backwards compatibility

2. **Database Filtering** (`target_databases` parameter)
   - Whitelist mode: Only analyze specified databases
   - Blacklist mode: Exclude specified databases (default: SNOWFLAKE, SNOWFLAKE_SAMPLE_DATA)
   - Filter applied early via SQL WHERE clauses in discovery queries

3. **Connection Settings**
   - Supports both password and private key authentication
   - Configurable warehouse and role
   - Optional passphrase for encrypted private keys

### Objective 2: Deep Data Analysis (Data Profiling)

**Functions Implemented:**
1. **`classify_sparsity()`** - Classifies NULL percentage into low/medium/high/very_high
2. **`classify_cardinality()`** - Classifies unique ratio into low/medium/high
3. **`classify_content_type()`** - Determines if text is codes, short_text, meaningful_text, or rich_content
4. **`run_deep_profiling()`** - Comprehensive profiling with:
   - Sparsity analysis (NULL rate)
   - Cardinality analysis (using HLL for efficiency)
   - Content type detection for text columns
   - JSON structure validation for VARIANT columns
   - Natural language detection via space analysis
5. **`is_confirmed_candidate()`** - Determines if data supports metadata hypothesis

**Profiling Heuristics:**
- **Sparsity Thresholds:** ≤10% NULL = low, ≤30% = medium, ≤70% = high, >70% = very_high
- **Cardinality Thresholds:** ≤1% unique = low, ≥90% unique = high
- **Content Type:** <10 chars = codes, <50 = short_text, <200 = meaningful_text, ≥200 = rich_content
- **Natural Language Detection:** Checks for spaces in sampled values

**Confirmation Criteria:**
- Sparsity ≤ 50% NULL
- Average text length ≥ 30 characters (for text columns)
- Natural language content detected (for LLM candidates)
- Valid JSON structure (for VARIANT columns)
- Data readiness score ≥ 3.5

**Output:**
- `confirmed_candidates.json` - Candidates where data supports metadata hypothesis
- `is_confirmed_candidate` flag on all candidates
- `confirmation_reasons` list explaining the determination

### Objective 3: Documentation

**File Created:**
- `docs/detailed_prompts.md` - This file

---

## Configuration Reference

### config.yaml Structure

```yaml
# Snowflake Connection
snowflake:
  account: "your_account"
  user: "your_user"
  password: "optional_password"  # Or use private_key_path
  private_key_path: "~/.snowflake/keys/key.pem"
  private_key_passphrase: "optional"
  warehouse: "COMPUTE_WH"
  role: "ACCOUNTADMIN"

# Output
output:
  directory: "./output"

# Database Filtering (Objective 1)
target_databases:  # Whitelist - only analyze these
  - "DB1"
  - "DB2"

exclude_databases:  # Blacklist - exclude these
  - "SNOWFLAKE"
  - "SNOWFLAKE_SAMPLE_DATA"

# Analysis Settings
analysis:
  sample_timeout: 300
  full_scan_timeout: 900
  top_candidates_full_scan: 200
  force_reanalysis: false
  sample_sizes: [10000, 1000, 100]

# Profiling Thresholds (Objective 2)
profiling:
  sparsity:
    low_threshold: 10
    medium_threshold: 30
    high_threshold: 70
  cardinality:
    low_threshold: 0.01
    high_threshold: 0.90
  content_type:
    min_meaningful_length: 50
    min_rich_content_length: 200
  confirmation:
    min_data_readiness_score: 3.5
    max_sparsity_percent: 50
    min_avg_text_length: 30
```

---

## Efficiency Considerations

All data profiling queries follow best practices for minimizing compute costs:

1. **SAMPLE clause** - Used for all profiling queries (10K, 1K, or 100 rows)
2. **HLL (HyperLogLog)** - Used for cardinality estimation instead of COUNT(DISTINCT)
3. **LIMIT clause** - Applied to content sampling queries
4. **Adaptive sampling** - Falls back to smaller samples on timeout
5. **Caching** - Results cached to avoid re-analysis on subsequent runs
6. **Early filtering** - Database filter applied in SQL WHERE clause, not post-query

---

## Read-Only Guarantee

The solution maintains strict read-only access:
- All queries are SELECT statements only
- Safety check in `execute_query()` rejects non-SELECT queries
- No CREATE, INSERT, UPDATE, DELETE, or ALTER operations
- Audit trail logs all executed queries for transparency

---

## Files Generated

All reports are generated in `snowflake-ai-enablement-reports/`:

| File | Description |
|------|-------------|
| `README.md` | Main report index with navigation |
| `reports/executive_summary.md` | High-level findings and recommendations |
| `reports/detailed_analysis_report.md` | Comprehensive analysis with full reasoning |
| `reports/ai_strategy_roadmap.md` | Phased implementation plan |
| `reports/data_quality_dashboard.md` | Data quality insights |
| `reports/scoring_comparison.md` | Before/after scoring analysis |
| `profiles/*.md` | Per-schema analysis reports |
| `profiles/consolidated_profiles.md` | Schema analysis index |
| `metadata/all_candidates.json` | All AI candidates |
| `metadata/confirmed_candidates.json` | Data-validated candidates |
| `metadata/enhanced_text_candidates.json` | LLM/Search candidates |
| `metadata/full_inventory.csv` | Complete column inventory |
| `metadata/stages_inventory.csv` | All Snowflake stages |
| `metadata/data_analysis_cache.json` | Analysis cache |
| `logs/audit_trail.sql` | All queries executed |
| `logs/analysis_errors.log` | Error log |
| `logs/analysis_summary.log` | Analysis statistics |
