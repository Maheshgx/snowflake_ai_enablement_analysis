# Snowflake AI Readiness Agent - System Instructions

> **Generated On:** 2026-02-09 UTC  
> **Agent Version:** 2.1  
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
   - Configurable via config/config.yaml: output.directory

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
   
   - Configurable via config/config.yaml: run_mode.mode, run_mode.append_strategy

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
Phase 2B: Metadata-Based Analysis (no table scans - uses ACCOUNT_USAGE metadata)
Phase 2C: Save Analysis Cache
Phase 2D: Identify Top Candidates
Phase 2E: Metadata-Based Enhanced Scoring (no table scans)
Phase 2F: Generate Data Analysis Reports (incl. AI Readiness Metadata Report)
Phase 3:  Enhanced Analysis (text-rich columns, education tables, PII)
Phase 4:  Metadata-Based Data Profiling (no table scans)
Phase 5:  Scoring Candidates
Phase 5B: Flagging Confirmed Candidates
Phase 6:  Report Generation (all with UTC timestamps)

STAGE RESTART CAPABILITY:
-------------------------
The agent supports restarting from any stage using --start-stage option.

Valid stages: 1, 2, 2A, 2B, 2C, 2D, 2E, 2F, 3, 4, 5, 5B, 6

Usage: python3 scripts/snowflake_full_analysis.py --start-stage 2B

When restarting from a later stage:
- Agent loads intermediate state from previous run files
- Skipped stages display "[SKIPPED - Loading from cache]"
- Required files must exist in snowflake-ai-enablement-reports/

Key functions for stage restart:
- should_run_stage(current, start) - Determines if stage should execute
- load_intermediate_state(start_stage) - Loads cached data for resume
- VALID_STAGES constant - Defines execution order
- STAGE_ORDER dict - Maps stage to execution sequence

METADATA-ONLY ANALYSIS (v2.1):
-------------------------------
All data evaluation uses metadata views instead of table scans:
- SNOWFLAKE.ACCOUNT_USAGE.TABLES (freshness, clustering, row count)
- SNOWFLAKE.ACCOUNT_USAGE.COLUMNS (data types, comments, nullability)
- SNOWFLAKE.ACCOUNT_USAGE.TABLE_CONSTRAINTS (PK, FK, UNIQUE)
- SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS (storage, clustering depth)
- INFORMATION_SCHEMA.COLUMNS / TABLES (real-time per-database checks)

Module: scripts/snowflake_ai_readiness_metadata.py
- run_metadata_analysis() - Orchestrates all metadata fetching and scoring
- compute_table_readiness_score() - Per-table AI readiness (0-100)
- compute_column_metadata_stats() - Replaces run_adaptive_sample()
- enhance_data_readiness_score_metadata() - Replaces enhance_data_readiness_score()
- generate_readiness_report_markdown() - New readiness report

SAFETY CONSTRAINTS:
-------------------
- Reject any non-SELECT queries
- Log all queries to audit trail
- Metadata-only queries (no production table access)
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

### Metadata-Only Refactoring Prompt (v2.1)

```
I have a Python-based utility that evaluates Snowflake tables for 'AI Readiness'
(checking for descriptions, compatible data types, and data freshness). Currently,
it executes SELECT queries on production tables, which is driving up credit
consumption. Please refactor the logic to use Snowflake Metadata instead of
table scans. Specifically:

Replace Table Scans: Instead of querying actual tables, query
INFORMATION_SCHEMA.COLUMNS, INFORMATION_SCHEMA.TABLES, and
SNOWFLAKE.ACCOUNT_USAGE.TABLE_CONSTRAINTS.

Evaluation Criteria: Use metadata to check for:
- Presence of Comments: (e.g., COMMENT column) to ensure LLMs have context.
- Data Types: Identify unsupported types for vectorization or LLM processing.
- Freshness: Use LAST_ALTERED or STALE_STATS_INFO to ensure the data is current.
- Clustering/Partitioning: Check if the table is optimized for large-scale retrieval.
- Efficiency: Ensure the queries target the INFORMATION_SCHEMA for real-time checks
  or ACCOUNT_USAGE for historical/account-wide audits.

Output: Provide the refactored SQL queries and a Python snippet showing how to
parse these metadata results into a 'Readiness Score'.
```

**Implementation Result:**
- Created `scripts/snowflake_ai_readiness_metadata.py` — standalone metadata evaluation module
- 6 SQL query templates targeting ACCOUNT_USAGE and INFORMATION_SCHEMA views
- Per-table AI Readiness Score (0–100) across 5 weighted dimensions:
  - Comments (25%), Data Types (20%), Freshness (25%), Clustering (15%), Constraints (15%)
- Lookup builders: `build_table_metadata_lookup()`, `build_column_metadata_lookup()`, `build_constraints_lookup()`, `build_storage_lookup()`
- `compute_column_metadata_stats()` replaces `run_adaptive_sample()` (zero table scans)
- `enhance_data_readiness_score_metadata()` replaces `enhance_data_readiness_score()`
- `run_metadata_analysis()` orchestrates all 4 metadata queries and scoring
- `generate_readiness_report_markdown()` produces the new readiness report
- Refactored `run_agent()` in main script:
  - Phase 2B: Replaced per-candidate table scans with `run_metadata_analysis()`
  - Phase 2E: Replaced `run_full_scan_analysis()` with metadata-based enhanced scoring
  - Phase 4: Replaced `profile_sample_text_columns()` / `profile_variant_columns()` with metadata estimates
  - Phase 2F: Added `ai_readiness_metadata_report.md` and `table_readiness_scores.json` outputs
- SELECT access to individual tables no longer required
- ~95%+ credit reduction (4 metadata queries vs. N×3 table scans)

---

### Stage Restart Feature Prompt (v2.2)

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

File Standard: Create a config/config.example.yaml template file in the config/ directory. This template should include placeholders for Snowflake account details (account, user, password/key, warehouse, role) and analysis parameters.

Implementation: Ensure the script looks for a config/config.yaml file (and includes this in .gitignore) for local execution.

Database Filtering: Implement a specific parameter in the config/config.yaml (e.g., target_databases: [DB1, DB2]) that allows the user to specify a list of databases to analyze.

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

config/config.example.yaml file.

docs/detailed_prompts.md file.
```

---

## Implementation Summary

### Objective 1: Configuration & Connectivity Overhaul

**To Run**

**Dry Run (Recommended First):**
1. Copy `config/config.example.yaml` to `config/config.yaml`
2. Fill in your Snowflake credentials
3. Set `dry_run.enabled: true` in config/config.yaml
4. Run: `python3 scripts/snowflake_full_analysis.py`
5. Review the scope and estimated runtime

**Full Analysis:**
1. After successful dry run, set `dry_run.enabled: false`
2. Optionally set `target_databases` to filter which databases to analyze
3. Run: `python3 scripts/snowflake_full_analysis.py`

**Files Modified/Created:**
- `config/config.example.yaml` - YAML configuration template with all settings
- `scripts/snowflake_full_analysis.py` - Refactored connection logic
- `.gitignore` - Added `config/config.yaml` to ignore list

**Key Features Implemented:**
1. **YAML Configuration Loading** (`load_yaml_config()`)
   - Loads from `config/config.yaml` (primary)
   - Falls back to repo root `config.yaml` (backward compatibility)
   - Falls back to `config/config.example.yaml` with warning
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

**Functions Implemented (Metadata-Based, v2.1):**

In `scripts/snowflake_ai_readiness_metadata.py`:
1. **`fetch_tables_metadata()`** - Queries ACCOUNT_USAGE.TABLES for freshness, clustering, row count
2. **`fetch_columns_metadata()`** - Queries ACCOUNT_USAGE.COLUMNS for data types, comments, nullability
3. **`fetch_table_constraints()`** - Queries ACCOUNT_USAGE.TABLE_CONSTRAINTS for PK/FK/UNIQUE
4. **`fetch_table_storage_metrics()`** - Queries ACCOUNT_USAGE.TABLE_STORAGE_METRICS for storage info
5. **`build_table_metadata_lookup()`** - Parses table rows into (db, schema, table) keyed dict
6. **`build_column_metadata_lookup()`** - Parses column rows into (db, schema, table) keyed dict
7. **`build_constraints_lookup()`** - Parses constraint rows into lookup dict
8. **`build_storage_lookup()`** - Parses storage metrics into lookup dict
9. **`score_comments()`** - Scores table/column comment presence (0–100)
10. **`score_data_types()`** - Scores data type AI compatibility (0–100)
11. **`score_freshness()`** - Scores data currency via LAST_ALTERED (0–100)
12. **`score_clustering()`** - Scores clustering/partitioning optimization (0–100)
13. **`score_constraints()`** - Scores constraint presence (0–100)
14. **`compute_table_readiness_score()`** - Weighted total across all 5 dimensions
15. **`compute_column_metadata_stats()`** - Replaces `run_adaptive_sample()` (zero table scans)
16. **`enhance_data_readiness_score_metadata()`** - Replaces `enhance_data_readiness_score()`
17. **`run_metadata_analysis()`** - Orchestrates full metadata-based analysis
18. **`generate_readiness_report_markdown()`** - Generates readiness report

Legacy functions preserved as dead code in main script:
- `classify_sparsity()`, `classify_cardinality()`, `classify_content_type()`
- `run_deep_profiling()`, `run_adaptive_sample()`, `run_full_scan_analysis()`
- `profile_sample_text_columns()`, `profile_variant_columns()`
- `is_confirmed_candidate()` (still called for confirmation logic)

**AI Readiness Scoring (Per-Table, 0–100):**

| Dimension | Weight | Source |
|-----------|--------|--------|
| Comments | 25% | COMMENT on tables/columns |
| Data Types | 20% | DATA_TYPE compatibility |
| Freshness | 25% | LAST_ALTERED |
| Clustering | 15% | CLUSTERING_KEY |
| Constraints | 15% | TABLE_CONSTRAINTS |

**Confirmation Criteria (metadata-derived):**
- Estimated sparsity ≤ 50% (from IS_NULLABLE)
- Estimated text length ≥ 30 characters (from CHARACTER_MAXIMUM_LENGTH)
- Data readiness score ≥ 3.5

**Output:**
- `confirmed_candidates.json` - Candidates where metadata supports AI hypothesis
- `table_readiness_scores.json` - Per-table readiness scores (NEW)
- `ai_readiness_metadata_report.md` - Readiness report (NEW)
- `is_confirmed_candidate` flag on all candidates
- `confirmation_reasons` list explaining the determination

### Objective 3: Documentation

**File Created:**
- `docs/detailed_prompts.md` - This file

---

## Configuration Reference

### config/config.yaml Structure

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
  top_candidates_full_scan: 200
  force_reanalysis: false

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

Since v2.1, all analysis uses metadata-only queries for maximum efficiency:

1. **Metadata-only queries** - ACCOUNT_USAGE and INFORMATION_SCHEMA views only (no table scans)
2. **4 total queries** - Replaces N×3 per-table SAMPLE/SELECT queries
3. **~95%+ credit reduction** - Metadata views consume minimal compute
4. **In-memory lookups** - Parsed metadata stored in dicts keyed by (db, schema, table)
5. **Caching** - Results cached to avoid re-analysis on subsequent runs
6. **Early filtering** - Database filter applied in SQL WHERE clause, not post-query
7. **No SELECT on tables required** - Simplified permission model

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
| `reports/ai_readiness_metadata_report.md` | Per-table AI readiness scores (NEW) |
| `reports/data_quality_dashboard.md` | Data quality insights |
| `reports/scoring_comparison.md` | Before/after scoring analysis |
| `profiles/*.md` | Per-schema analysis reports |
| `profiles/consolidated_profiles.md` | Schema analysis index |
| `metadata/all_candidates.json` | All AI candidates |
| `metadata/confirmed_candidates.json` | Data-validated candidates |
| `metadata/enhanced_text_candidates.json` | LLM/Search candidates |
| `metadata/table_readiness_scores.json` | Per-table AI readiness scores (NEW) |
| `metadata/full_inventory.csv` | Complete column inventory |
| `metadata/stages_inventory.csv` | All Snowflake stages |
| `metadata/data_analysis_cache.json` | Analysis cache |
| `logs/audit_trail.sql` | All queries executed |
| `logs/analysis_errors.log` | Error log |
| `logs/analysis_summary.log` | Analysis statistics |
