# Snowflake AI Readiness Agent - User Guide

> **Version:** 2.1 (Metadata-Only Analysis)  
> **Last Updated:** February 2026  
> **Timestamp Standard:** All outputs use UTC format  
> **Analysis Mode:** Metadata-only — zero table scans

---

## Table of Contents

1. [Introduction](#introduction)
2. [Prerequisites](#prerequisites)
3. [Installation](#installation)
4. [Configuration](#configuration)
5. [Usage & Execution](#usage--execution)
6. [Modes of Operation](#modes-of-operation)
7. [Restarting from a Specific Stage](#restarting-from-a-specific-stage)
8. [Output Interpretation](#output-interpretation)
9. [Troubleshooting](#troubleshooting)
10. [FAQ](#faq)

---

## Introduction

The Snowflake AI Readiness Agent is an autonomous utility that analyzes your Snowflake environment to identify opportunities for AI/ML enablement using Snowflake's native Cortex AI features. The agent performs:

- **Metadata Inspection** - Analyzes schema information (column names, types, sizes) via ACCOUNT_USAGE and INFORMATION_SCHEMA
- **AI Readiness Scoring** - Per-table readiness score (0–100) across 5 dimensions using metadata only
- **Candidate Scoring** - Ranks opportunities by business potential and metadata-derived data readiness
- **Report Generation** - Produces actionable reports for stakeholders
- **Zero Table Scans** - All analysis via metadata views, reducing credit consumption by ~95%+

### Agent Architecture (v2.0)

The utility operates as an **autonomous agent** with the following characteristics:

| Aspect | Description |
|--------|-------------|
| **Entry Point** | Structured `main()` → `parse_arguments()` → `run_agent()` flow |
| **CLI Interface** | Full command-line argument support (`--config`, `--dry-run`, etc.) |
| **Timestamps** | All output uses **UTC format** (e.g., `2026-02-06 07:14 UTC`) |
| **Metadata** | All documents include "Generated On" field with UTC timestamp |
| **Operation** | Read-only, autonomous execution with complete audit trail |

### What's New in Version 2.1

| Feature | Description |
|---------|-------------|
| **Metadata-Only Analysis** | All data evaluation via INFORMATION_SCHEMA and ACCOUNT_USAGE — no table scans |
| **AI Readiness Scoring** | Per-table score (0–100) across 5 dimensions: comments, types, freshness, clustering, constraints |
| **~95%+ Credit Reduction** | 4 metadata queries replace thousands of per-table SAMPLE/SELECT queries |
| **New Module** | `snowflake_ai_readiness_metadata.py` — standalone metadata evaluation module |
| **New Reports** | `ai_readiness_metadata_report.md` and `table_readiness_scores.json` |
| **Simplified Permissions** | SELECT access to individual tables no longer required |

### What Was in Version 2.0

| Feature | Description |
|---------|-------------|
| **Agent Architecture** | Structured application with CLI and autonomous execution |
| **YAML Configuration** | Replace environment variables with simple `config/config.yaml` |
| **Database Filtering** | Analyze specific databases with `target_databases` parameter |
| **Run Modes** | Fresh (overwrite) or Append (incremental) for multi-database analysis |
| **Dry Run Mode** | Validate configuration before full analysis |
| **Confirmed Candidates** | Distinguish metadata-based from data-validated candidates |
| **Progress Tracking** | Real-time progress bar with current object and running stats |
| **Report Index** | Main README.md linking all reports with analysis breakdown |
| **UTC Timestamps** | Standardized UTC format in all generated documentation |

---

## Prerequisites

### System Requirements

| Requirement | Minimum | Recommended |
|-------------|---------|-------------|
| Python 3 | 3.8+ (use `python3` command) | 3.10+ |
| Memory | 4 GB | 8 GB |
| Disk Space | 500 MB | 2 GB (for large reports) |

### Snowflake Account Requirements

| Component | Requirement |
|-----------|-------------|
| Account Type | Any (Standard, Enterprise, Business Critical) |
| Authentication | Password or Private Key |
| Warehouse | XS or larger (metadata queries only — minimal compute) |

### Required Snowflake Privileges

The utility requires **read-only** access. No write permissions are needed.

#### Required Permissions

```sql
-- Access to ACCOUNT_USAGE views for metadata-based analysis (required)
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE <your_role>;

-- Warehouse for running metadata queries (required)
GRANT USAGE ON WAREHOUSE <warehouse_name> TO ROLE <your_role>;
```

#### Optional Permissions (Real-Time INFORMATION_SCHEMA Checks)

```sql
-- For real-time per-database metadata checks via INFORMATION_SCHEMA (optional)
GRANT USAGE ON DATABASE <database_name> TO ROLE <your_role>;
```

> **Note (v2.1):** SELECT access to individual tables is **no longer required**. All analysis is performed via `SNOWFLAKE.ACCOUNT_USAGE` and `INFORMATION_SCHEMA` metadata views. This significantly simplifies permission setup.

---

## Installation

### Step 1: Clone the Repository

```bash
git clone <repository-url>
cd snowflake_ai_enablement_analysis
```

### Step 2: Create a Virtual Environment

Creating an isolated Python 3 environment prevents dependency conflicts.

**macOS / Linux:**
```bash
python3 -m venv venv
source venv/bin/activate
```

**Windows:**
```bash
python3 -m venv venv
venv\Scripts\activate
```

> **Note:** Always use `python3` (not `python`) to ensure Python 3.x is used. On some systems, `python` may still point to Python 2.x.

### Step 3: Install Dependencies

```bash
pip install -r config/requirements.txt
```

**Installed packages:**

| Package | Purpose |
|---------|---------|
| `snowflake-connector-python` | Snowflake database connectivity |
| `cryptography` | Private key authentication support |
| `pandas` | Data processing and analysis |
| `PyYAML` | YAML configuration file parsing |

### Step 4: Verify Installation

```bash
python3 -c "import snowflake.connector; import yaml; print('Installation successful!')"
```

---

## Configuration

### Configuration File Workflow

The utility uses a YAML-based configuration system:

```
config/config.example.yaml  →  (copy)  →  config/config.yaml
       (template)                          (your secrets)
       (committed)                         (git-ignored)
```

### Step 1: Create Your Configuration File

```bash
cp config/config.example.yaml config/config.yaml
```

> ⚠️ **SECURITY WARNING**  
> `config/config.yaml` contains sensitive credentials (passwords, private key paths).  
> This file is listed in `.gitignore` and should **NEVER** be committed to version control.

### Step 2: Configure Snowflake Connection

Edit `config/config.yaml` with your Snowflake credentials:

```yaml
# =============================================================================
# SNOWFLAKE CONNECTION SETTINGS
# =============================================================================
snowflake:
  # Account identifier (REQUIRED)
  # Format: <orgname>-<account_name> or <account_locator>.<region>.<cloud>
  # Examples: 
  #   - "myorg-myaccount"
  #   - "xy12345.us-east-1"
  #   - "abc123.west-us-2.azure"
  account: "your_account_identifier"
  
  # Username (REQUIRED)
  user: "your_username"
  
  # ===========================================
  # AUTHENTICATION - Choose ONE method below
  # ===========================================
  
  # Option 1: Password Authentication (simplest)
  password: "your_password"
  
  # Option 2: Private Key Authentication (recommended for production)
  # private_key_path: "~/.snowflake/keys/rsa_key.pem"
  # private_key_passphrase: "optional_passphrase"  # Only if key is encrypted
  
  # Warehouse (REQUIRED for data profiling)
  warehouse: "COMPUTE_WH"
  
  # Role (optional - uses default role if not specified)
  role: "ACCOUNTADMIN"
```

### Step 3: Configure Database Filtering

Control which databases are analyzed:

```yaml
# =============================================================================
# DATABASE FILTERING
# =============================================================================

# OPTION A: Whitelist Mode - Analyze ONLY these databases
# Uncomment and list specific databases:
# target_databases:
#   - "PRODUCTION_DB"
#   - "ANALYTICS_DB"
#   - "DATA_WAREHOUSE"

# OPTION B: Blacklist Mode - Analyze ALL databases EXCEPT these
# System databases are excluded by default:
exclude_databases:
  - "SNOWFLAKE"
  - "SNOWFLAKE_SAMPLE_DATA"
```

**Filtering Behavior:**

| Configuration | Behavior |
|---------------|----------|
| `target_databases` specified | Only analyze listed databases (whitelist) |
| `target_databases` empty/commented | Analyze all databases except `exclude_databases` |
| Both specified | `target_databases` takes precedence |

### Step 4: Configure Dry Run Mode

Start with dry run enabled to validate your setup:

```yaml
# =============================================================================
# DRY RUN MODE
# =============================================================================
dry_run:
  # Enable dry run mode (RECOMMENDED for first run)
  enabled: true
  
  # Show sample queries that would be executed
  show_sample_queries: true
  
  # Validate access to target databases
  validate_access: true
```

### Step 5: Configure Analysis Settings (Optional)

Fine-tune the analysis and confirmation thresholds:

```yaml
# =============================================================================
# ANALYSIS SETTINGS
# =============================================================================
analysis:
  # Top candidates for enhanced metadata scoring
  top_candidates_full_scan: 200
  # Force re-analysis of cached candidates
  force_reanalysis: false

# =============================================================================
# PROFILING THRESHOLDS (used for candidate confirmation)
# =============================================================================
profiling:
  # Confirmation thresholds for "Confirmed Candidates"
  confirmation:
    min_data_readiness_score: 3.5  # Minimum score (0-5 scale)
    max_sparsity_percent: 50       # Maximum NULL percentage
    min_avg_text_length: 30        # Minimum average text length
```

> **Note (v2.1):** Sparsity and content type thresholds are now estimated from metadata (IS_NULLABLE, CHARACTER_MAXIMUM_LENGTH) rather than measured via table scans.

### Setting Up Private Key Authentication

If using private key authentication instead of password:

```bash
# 1. Create directory for keys
mkdir -p ~/.snowflake/keys

# 2. Generate private key (unencrypted)
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM \
  -out ~/.snowflake/keys/rsa_key.pem -nocrypt

# 3. Generate public key
openssl rsa -in ~/.snowflake/keys/rsa_key.pem -pubout \
  -out ~/.snowflake/keys/rsa_key.pub

# 4. Secure the private key
chmod 600 ~/.snowflake/keys/rsa_key.pem

# 5. Get the public key content (remove headers and newlines)
cat ~/.snowflake/keys/rsa_key.pub | grep -v "PUBLIC KEY" | tr -d '\n'
```

Then register the public key in Snowflake:

```sql
-- Run as SECURITYADMIN or ACCOUNTADMIN
ALTER USER your_username SET RSA_PUBLIC_KEY='MIIBIjANBgkqh...';

-- Verify
DESC USER your_username;
```

---

## Usage & Execution

### Command Line Execution

```bash
# Activate virtual environment first
source venv/bin/activate  # macOS/Linux
# or: venv\Scripts\activate  # Windows

# Run the analysis
python3 scripts/snowflake_full_analysis.py
```

### Execution Workflow

```
┌─────────────────────────────────────────────────────────────┐
│                    START EXECUTION                          │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
                   ┌─────────────────────┐
                   │ Load config/     │
                   │ config.yaml      │
                   └─────────────────────┘
                            │
                            ▼
                   ┌────────────────┐
                   │ Connect to     │
                   │ Snowflake      │
                   └────────────────┘
                            │
                            ▼
              ┌─────────────────────────┐
              │  Dry Run Enabled?       │
              └─────────────────────────┘
                     │           │
                    YES          NO
                     │           │
                     ▼           ▼
           ┌──────────────┐  ┌──────────────────┐
           │ Run Dry Run  │  │ Full Analysis    │
           │ Validation   │  │ (6 Phases)       │
           └──────────────┘  └──────────────────┘
                     │           │
                     ▼           ▼
              ┌─────────────────────────┐
              │       COMPLETE          │
              └─────────────────────────┘
```

---

## Modes of Operation

### Mode 1: Dry Run (Validation)

**Purpose:** Validate configuration and estimate scope without executing full analysis.

**Enable:** Set `dry_run.enabled: true` in `config/config.yaml`

**What it does:**

| Step | Action | Output |
|------|--------|--------|
| 1 | Validate connection | User, account, role, warehouse |
| 2 | Discover databases | List of databases to analyze (with filters) |
| 3 | Validate access | Check SELECT permissions on databases |
| 4 | Estimate scope | Count of tables, columns, candidates |
| 5 | Show queries | Sample queries that would be executed |

**Example Output:**

```
======================================================================
DRY RUN MODE - Configuration Validation
======================================================================

[Step 1/5] Validating Snowflake Connection...
  ✓ Connected as: ANALYSIS_USER
  ✓ Account: MYORG-MYACCOUNT
  ✓ Role: DATA_ANALYST
  ✓ Warehouse: COMPUTE_WH

[Step 2/5] Discovering Databases (with filters applied)...
  Found 5 database(s) to analyze:
    - ANALYTICS_DB
    - PRODUCTION_DB
    - STAGING_DB
    - DATA_WAREHOUSE
    - REPORTING_DB

[Step 3/5] Validating Database Access...
    ✓ ANALYTICS_DB: Accessible
    ✓ PRODUCTION_DB: Accessible
    ✓ STAGING_DB: Accessible
    ✓ DATA_WAREHOUSE: Accessible
    ✓ REPORTING_DB: Accessible

[Step 4/5] Estimating Analysis Scope...
  Databases to analyze: 5
  Estimated tables: 1,234
  Estimated columns: 45,678
  Estimated AI candidates: 890+

  Estimated runtime:
    - Metadata discovery: ~0m 10s
    - Data profiling: ~16m 40s
    - Total: ~16m 50s

======================================================================
DRY RUN COMPLETE
======================================================================

To run full analysis, set 'dry_run.enabled: false' in config/config.yaml
```

### Mode 2: Run Modes (Fresh vs Append)

The agent supports two run modes for handling existing analysis data:

**Configuration:**

```yaml
run_mode:
  mode: "fresh"        # or "append"
  append_strategy: "merge"  # or "add"
  backup_before_fresh: false
```

| Mode | Behavior | Best For |
|------|----------|----------|
| **fresh** | Clears/overwrites existing data | First run, complete re-analysis |
| **append** | Preserves and merges with existing | Incremental multi-database analysis |

**Append Mode Details:**

When `mode: "append"` is set:
1. Loads existing candidates from `all_candidates.json`
2. Loads run history from `run_history.json`
3. Analyzes new databases specified in `target_databases`
4. Merges new candidates with existing (deduplicates by database.schema.table.column)
5. Updates run history for tracking

**Incremental Analysis Workflow:**

```bash
# Run 1: Analyze production databases
# config/config.yaml:
#   target_databases: ["PROD_DB1", "PROD_DB2"]
#   run_mode.mode: "fresh"
python3 scripts/snowflake_full_analysis.py

# Run 2: Add analytics databases
# config/config.yaml:
#   target_databases: ["ANALYTICS_DB"]
#   run_mode.mode: "append"
python3 scripts/snowflake_full_analysis.py

# Run 3: Add more databases
# config/config.yaml:
#   target_databases: ["STAGING_DB", "DEV_DB"]
#   run_mode.mode: "append"
python3 scripts/snowflake_full_analysis.py

# Result: Comprehensive report covering all databases
```

**Run History Tracking:**

In append mode, the agent creates `run_history.json`:

```json
{
  "runs": [
    {"timestamp": "2026-02-06 07:30 UTC", "databases_analyzed": ["PROD_DB1", "PROD_DB2"]},
    {"timestamp": "2026-02-06 08:15 UTC", "databases_analyzed": ["ANALYTICS_DB"]}
  ],
  "databases_analyzed": ["PROD_DB1", "PROD_DB2", "ANALYTICS_DB"],
  "last_updated": "2026-02-06 08:15 UTC"
}
```

### Progress Tracking

During execution, the agent displays real-time progress for long-running operations:

**Progress Bar Format:**
```
  [████████████░░░░░░░░░░░░░░░░░░] 1,234/3,379 (36.5%) | Current: DB.SCHEMA.TABLE.COLUMN | OK:800 New:150 Cache:650 Err:34
```

**Progress Elements:**

| Element | Description |
|---------|-------------|
| **Progress Bar** | Visual indicator of completion percentage |
| **Count** | Current item / Total items with percentage |
| **Current** | Name of object currently being processed |
| **Stats** | Running counts (varies by phase) |

**Phases with Progress Tracking:**

| Phase | Statistics Displayed |
|-------|---------------------|
| **Phase 2B: Metadata Analysis** | OK (success), New (metadata analyses), Err (errors), Table scans avoided |
| **Phase 2E: Metadata Scoring** | Re-scored (count), Table scans avoided |
| **Phase 5B: Confirmation** | Confirmed, Unconfirmed |

**Completion Summary:**

After each phase completes, a summary is displayed:
```
  Phase 2B Complete:
    - Successfully analyzed: 3,200
    - From cache: 2,500
    - New analyses: 700
    - Skipped (errors): 179
```

### Restarting from a Specific Stage

The agent supports restarting from any stage using the `--start-stage` (or `-s`) option. This is useful when:
- A previous run was interrupted
- You want to re-run only later stages (e.g., regenerate reports)
- Debugging issues in specific phases

#### Available Stages

| Stage | Name | Description |
|-------|------|-------------|
| **1** | Metadata Discovery | Discover databases, schemas, tables, columns from ACCOUNT_USAGE |
| **2** | AI Candidate Identification | Identify potential AI candidates from metadata |
| **2A** | Load Analysis Cache | Load previously cached analysis results |
| **2B** | Metadata-Based Analysis | Analyze candidate data quality via ACCOUNT_USAGE metadata (no table scans) |
| **2C** | Save Analysis Cache | Save analysis results to cache file |
| **2D** | Identify Top Candidates | Select top N candidates for enhanced scoring |
| **2E** | Metadata-Based Enhanced Scoring | Re-score top candidates with metadata-derived statistics (no table scans) |
| **2F** | Generate Data Analysis Reports | Create data quality dashboard and comparison reports |
| **3** | Enhanced Analysis | Find text-rich columns, education tables, PII columns |
| **4** | Metadata-Based Data Profiling | Profile text and variant columns using metadata (no table scans) |
| **5** | Scoring Candidates | Calculate scores for all candidates |
| **5B** | Flagging Confirmed Candidates | Mark candidates as confirmed based on data quality |
| **6** | Report Generation | Generate executive summary, roadmap, and profile reports |

#### Usage Examples

```bash
# Restart from Phase 2B (Metadata-Based Analysis)
python3 scripts/snowflake_full_analysis.py --start-stage 2B

# Restart from Phase 3 (Enhanced Analysis)
python3 scripts/snowflake_full_analysis.py --start-stage 3

# Just regenerate reports (Phase 6)
python3 scripts/snowflake_full_analysis.py -s 6

# Show help with all stage options
python3 scripts/snowflake_full_analysis.py --help
```

#### Prerequisites by Stage

**⚠️ IMPORTANT:** When restarting from a later stage, the agent loads data from previous runs. Ensure these files exist in `snowflake-ai-enablement-reports/`:

| Start Stage | Required Files (Prerequisites) |
|-------------|-------------------------------|
| **2, 2A, 2B** | `metadata/full_inventory.csv` |
| **2C** | `metadata/full_inventory.csv`, `metadata/all_candidates.json` |
| **2D, 2E, 2F** | `metadata/full_inventory.csv`, `metadata/all_candidates.json`, `metadata/data_analysis_cache.json` |
| **3** | All of above + completed Phase 2 |
| **4** | All of above + `metadata/enhanced_text_candidates.json` |
| **5, 5B** | All of above + `profiles/text_column_profiles.json` |
| **6** | All of above + confirmed candidates data |

#### Detailed Prerequisites

##### Starting from Stage 2B (Sampling Pass)
```
Required files:
├── metadata/
│   ├── full_inventory.csv          ← Columns metadata from Phase 1
│   └── all_candidates.json         ← AI candidates from Phase 2 (optional, will re-identify)
```

##### Starting from Stage 2C (Save Cache)
```
Required files:
├── metadata/
│   ├── full_inventory.csv          ← Columns metadata
│   ├── all_candidates.json         ← AI candidates with analysis data
│   └── data_analysis_cache.json    ← Analysis cache (will be updated)
```

##### Starting from Stage 3 (Enhanced Analysis)
```
Required files:
├── metadata/
│   ├── full_inventory.csv          ← Columns metadata
│   ├── all_candidates.json         ← AI candidates
│   └── data_analysis_cache.json    ← Analysis cache
```

##### Starting from Stage 5 or 5B (Scoring/Confirmation)
```
Required files:
├── metadata/
│   ├── full_inventory.csv
│   ├── all_candidates.json
│   └── enhanced_text_candidates.json  ← From Phase 3
├── profiles/
│   └── text_column_profiles.json      ← From Phase 4
```

##### Starting from Stage 6 (Report Generation Only)
```
Required files:
├── metadata/
│   ├── full_inventory.csv
│   ├── all_candidates.json          ← Must include scores and confirmation status
│   ├── enhanced_text_candidates.json
│   └── stages_inventory.csv         ← Optional, for Document AI info
├── profiles/
│   └── text_column_profiles.json
```

#### What Happens When Files Are Missing

If required files are missing:
1. The agent will print a warning: `"Warning: Could not load [file]: [error]"`
2. Empty/default values will be used for the missing data
3. The stage will run but may produce incomplete results

**Recommendation:** Always complete a full run first before using stage restart for debugging or regeneration.

#### Common Restart Scenarios

| Scenario | Command |
|----------|---------|
| Analysis interrupted at 2B, want to resume | `--start-stage 2B` |
| Want to regenerate reports with existing data | `--start-stage 6` |
| Re-run scoring with updated thresholds | `--start-stage 5` |
| Re-run data profiling only | `--start-stage 4` |
| Analysis completed but want different confirmation criteria | `--start-stage 5B` |

### Report Index (Main Summary)

Upon completion, the agent generates a **README.md** in the output directory that serves as the main entry point for all reports:

**Location:** `snowflake-ai-enablement-reports/README.md`

**Contents:**

| Section | Description |
|---------|-------------|
| **Quick Navigation** | Links to all major reports (Executive Summary, Roadmap, etc.) |
| **Analysis Summary** | Environment metrics, AI candidates, analysis statistics |
| **Objects by Database** | Breakdown of tables, candidates, and confirmation rates per database |
| **Generated Files** | Complete listing of all output files |
| **Next Steps** | Recommended actions for reviewing the analysis |

**Completion Output:**
```
======================================================================
📋 VIEW FULL REPORT: snowflake-ai-enablement-reports/README.md
======================================================================
```

### Mode 3: Metadata Inspection (Phase 1-2)

**What it analyzes:**

The tool first examines schema metadata from `SNOWFLAKE.ACCOUNT_USAGE`:

| Metadata | Source | Purpose |
|----------|--------|---------|
| Databases | `DATABASES` view | Scope of analysis |
| Schemas | `SCHEMATA` view | Organization structure |
| Tables/Views | `TABLES` view | Row counts, sizes |
| Columns | `COLUMNS` view | Data types, lengths, names |
| Stages | `STAGES` view | Document AI opportunities |

**AI Candidate Identification:**

Candidates are identified based on metadata heuristics:

| AI Feature | Detection Criteria |
|------------|-------------------|
| **Cortex LLM** | VARCHAR/TEXT columns with length ≥500 or names containing: DESCRIPTION, CONTENT, MESSAGE, NOTE, SUMMARY, TEXT, COMMENT, FEEDBACK, REVIEW |
| **Cortex Search/RAG** | Tables with 2+ text columns suitable for semantic search |
| **Cortex ML** | Numeric columns with time-series patterns (forecasting, anomaly detection) |
| **Cortex Extract** | VARIANT, OBJECT, ARRAY columns (JSON/XML processing) |
| **Document AI** | External/internal stages containing documents |

### Mode 3: Metadata-Based Data Profiling (Phase 2B-4)

**What it evaluates (metadata-only):**

Since v2.1, all data evaluation is performed via metadata views — no queries touch production tables:

| Evaluation | Metadata Source | What It Derives |
|------------|----------------|-----------------|
| **Comments** | `COLUMNS.COMMENT`, `TABLES.COMMENT` | LLM context availability |
| **Data Types** | `COLUMNS.DATA_TYPE` | AI/vectorization compatibility |
| **Freshness** | `TABLES.LAST_ALTERED` | Data currency |
| **Clustering** | `TABLES.CLUSTERING_KEY` | Large-scale retrieval optimization |
| **Constraints** | `TABLE_CONSTRAINTS` | Data quality / relationships |
| **Nullability** | `COLUMNS.IS_NULLABLE` | Estimated NULL rate |
| **Content Size** | `COLUMNS.CHARACTER_MAXIMUM_LENGTH` | Estimated text richness |

**Metadata Queries Used:**

```sql
-- Table metadata (freshness, clustering, row count)
SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE,
       ROW_COUNT, BYTES, COMMENT, CREATED, LAST_ALTERED, CLUSTERING_KEY
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES
WHERE DELETED IS NULL;

-- Column metadata (data types, comments, nullability)
SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME,
       DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE, COMMENT
FROM SNOWFLAKE.ACCOUNT_USAGE.COLUMNS
WHERE DELETED IS NULL;

-- Table constraints (PK, FK, UNIQUE)
SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME,
       CONSTRAINT_TYPE, CONSTRAINT_NAME
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_CONSTRAINTS
WHERE DELETED IS NULL;

-- Storage metrics for clustering analysis
SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME,
       ACTIVE_BYTES, TIME_TRAVEL_BYTES, FAILSAFE_BYTES
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS;
```

**Efficiency Features:**

| Feature | Implementation | Benefit |
|---------|----------------|--------|
| Metadata-only queries | ACCOUNT_USAGE + INFORMATION_SCHEMA | Zero production table access |
| 4 total queries | Replaces N×3 per-table scans | ~95%+ credit reduction |
| Caching | `data_analysis_cache.json` | Skip re-analysis |
| Lookup structures | In-memory dicts keyed by (db, schema, table) | Fast per-candidate scoring |

---

## Output Interpretation

### Output Directory Structure

All reports are generated in the `snowflake-ai-enablement-reports/` folder:

```
snowflake-ai-enablement-reports/
├── README.md                        ← START HERE (Main Report Index)
├── reports/
│   ├── executive_summary.md         ← High-level findings
│   ├── detailed_analysis_report.md  ← Comprehensive analysis with reasoning
│   ├── ai_strategy_roadmap.md       ← Phased implementation plan
│   ├── ai_readiness_metadata_report.md  ← Per-table AI readiness scores (NEW)
│   ├── data_quality_dashboard.md
│   └── scoring_comparison.md
├── profiles/
│   ├── consolidated_profiles.md     ← Per-schema analysis index
│   └── *_analysis.md                ← Individual schema reports
├── metadata/
│   ├── all_candidates.json
│   ├── confirmed_candidates.json
│   ├── enhanced_text_candidates.json
│   ├── table_readiness_scores.json  ← Per-table AI readiness scores (NEW)
│   ├── full_inventory.csv
│   ├── stages_inventory.csv
│   └── data_analysis_cache.json
└── logs/
    ├── audit_trail.sql
    ├── analysis_errors.log
    └── analysis_summary.log
```

### Understanding Candidate Types

#### Potential Candidate (Metadata Only)

A column identified as a potential AI candidate based solely on metadata:

```json
{
  "database": "ANALYTICS",
  "schema": "CONTENT",
  "table": "ARTICLES",
  "column": "BODY_TEXT",
  "data_type": "VARCHAR(16777216)",
  "ai_feature": "Cortex LLM",
  "reason": "Text column 'BODY_TEXT' with length 16777216",
  "is_confirmed_candidate": false,
  "confirmation_reasons": [
    "High sparsity (72.3% NULL)",
    "Short content (avg 15.2 chars)"
  ]
}
```

**Interpretation:** Metadata suggests this could be an LLM candidate, but data profiling revealed issues (mostly NULL, short content when present).

#### Confirmed Candidate (Data Validated)

A column where data profiling confirms AI suitability:

```json
{
  "database": "ANALYTICS",
  "schema": "CONTENT",
  "table": "BLOG_POSTS",
  "column": "CONTENT",
  "data_type": "VARCHAR(16777216)",
  "ai_feature": "Cortex LLM",
  "reason": "Text column 'CONTENT' with length 16777216",
  "is_confirmed_candidate": true,
  "confirmation_reasons": [
    "Good completeness (95.2% populated)",
    "Substantial content (avg 1,523.4 chars)",
    "Natural language content detected",
    "Good data readiness (4.2)"
  ],
  "statistics": {
    "null_percentage": 4.8,
    "avg_length": 1523.4,
    "max_length": 45678,
    "sample_size": 10000
  },
  "scores": {
    "business_potential": 4.5,
    "data_readiness": 4.2,
    "metadata_quality": 3.8,
    "governance_risk": 1.2,
    "total": 16.7
  }
}
```

**Interpretation:** Both metadata AND data profiling confirm this is an excellent LLM candidate with rich content and high completeness.

### Confirmation Criteria

A candidate becomes "Confirmed" when:

| Criterion | Threshold | Rationale |
|-----------|-----------|-----------|
| Sparsity | ≤50% NULL | Sufficient data available |
| Avg Text Length | ≥30 chars | Meaningful content exists |
| Natural Language | Detected | Not codes/IDs |
| JSON Structure | ≥90% valid | For VARIANT columns |
| Data Readiness | ≥3.5/5.0 | Overall quality score |

### Scoring System

Each candidate is scored on four dimensions (0-5 scale):

| Dimension | Weight | Factors |
|-----------|--------|---------|
| **Business Potential** | High | Column name relevance, use case fit |
| **Data Readiness** | High | NULL rate, content length, data quality |
| **Metadata Quality** | Medium | Comments, documentation |
| **Governance Risk** | Low (inverted) | PII indicators, sensitivity |

**Total Score:** 0-20 (sum of all dimensions)

### Key Reports

#### 1. README.md (Main Report Index)

**Location:** `snowflake-ai-enablement-reports/README.md`

**Purpose:** Central navigation hub linking all reports with analysis summary.

**Use for:** Starting point for reviewing analysis results.

#### 2. Executive Summary (`reports/executive_summary.md`)

**Location:** `snowflake-ai-enablement-reports/reports/executive_summary.md`

**Purpose:** High-level findings and recommendations for stakeholders.

**Sections:**
- Environment overview
- AI candidate summary
- Top 10 high-value opportunities
- Quick wins
- Data readiness assessment
- Governance considerations

#### 3. Detailed Analysis Report (`reports/detailed_analysis_report.md`)

**Location:** `snowflake-ai-enablement-reports/reports/detailed_analysis_report.md`

**Purpose:** Comprehensive analysis with full reasoning for each AI candidate.

**Sections:**
- Scoring methodology explanation
- Top 25 LLM candidates with score breakdowns
- Top 15 Search/RAG candidates with SQL examples
- Top 10 ML and Extract candidates
- Data quality assessment
- PII categories and governance
- Implementation recommendations (3 phases)

#### 4. AI Strategy Roadmap (`reports/ai_strategy_roadmap.md`)

**Location:** `snowflake-ai-enablement-reports/reports/ai_strategy_roadmap.md`

**Purpose:** Phased implementation plan with priorities and timelines.

**Sections:**
- Executive overview with key findings
- Priority matrix (P1: Immediate, P2: Short-term, P3: Medium-term)
- Detailed Phase 1 recommendations with score breakdowns
- Quick win SQL examples
- Feature implementation guides (LLM, Search, ML, Extract)
- Governance checklist
- Success metrics and resource planning

#### 5. Schema Profiles (`profiles/*.md`)

**Location:** `snowflake-ai-enablement-reports/profiles/`

**Purpose:** Per-schema detailed analysis grouped by AI feature.

**Use for:** Understanding AI opportunities within specific schemas.

#### 6. Confirmed Candidates (`metadata/confirmed_candidates.json`)

**Purpose:** Production-ready AI candidates validated by data profiling.

**Use for:** Immediate AI enablement projects, POC planning.

#### 7. Audit Trail (`logs/audit_trail.sql`)

**Purpose:** Complete log of all queries executed.

**Use for:** Security audit, troubleshooting, compliance.

---

## Troubleshooting

### Common Issues

#### Connection Failed

```
Error: Failed to connect to Snowflake
```

**Solutions:**
1. Verify `account` format (e.g., `xy12345.us-east-1`)
2. Check username and password/key
3. Ensure warehouse exists and is running
4. Verify network connectivity

#### Permission Denied

```
Error: Insufficient privileges to operate on database 'XYZ'
```

**Solutions:**
1. Run dry run with `validate_access: true` to identify issues
2. Grant required permissions (see [Prerequisites](#required-snowflake-privileges))
3. Use `target_databases` to exclude inaccessible databases

#### Timeout During Metadata Query

```
Warning: Query timeout on metadata view
```

**This is rare** since metadata queries are lightweight. If it occurs:
1. Check that your warehouse is running
2. Verify ACCOUNT_USAGE access permissions
3. For very large accounts, consider using `target_databases` to reduce scope

#### No Candidates Found

**Possible causes:**
1. `target_databases` filtering too restrictive
2. No text/variant columns in analyzed databases
3. All columns filtered out by system database exclusion

**Solutions:**
1. Run dry run to see scope
2. Broaden `target_databases` filter
3. Check `exclude_databases` list

### Debug Mode

Enable verbose logging by setting in `config/config.yaml`:

```yaml
logging:
  level: "DEBUG"
  detailed_audit: true
```

---

## FAQ

### Q: Is this tool safe to run on production?

**A:** Yes. The tool is strictly read-only:
- Only SELECT queries are executed
- All queries are validated before execution
- No INSERT, UPDATE, DELETE, or ALTER operations
- Complete audit trail is maintained

### Q: How long does the analysis take?

**A:** Since v2.1, all analysis is metadata-only, so it's significantly faster:
- Small environments (<1,000 tables): 1-3 minutes
- Medium environments (1,000-10,000 tables): 3-10 minutes
- Large environments (10,000+ tables): 10-20 minutes

The 4 metadata queries execute quickly regardless of actual data volume. Use dry run to estimate before full analysis.

### Q: Can I run this on specific databases only?

**A:** Yes. Use the `target_databases` parameter:

```yaml
target_databases:
  - "ANALYTICS_DB"
  - "REPORTING_DB"
```

### Q: What's the difference between "Potential" and "Confirmed" candidates?

**A:**
- **Potential:** Identified by metadata heuristics (column name, type, size)
- **Confirmed:** Validated by metadata-derived quality assessment (comments present, compatible types, fresh data, good constraints)

Always prioritize Confirmed candidates for AI projects.

### Q: Why did you switch from table scans to metadata-only?

**A:** The previous approach executed `SELECT ... SAMPLE` queries on every candidate column, which:
- Consumed significant Snowflake credits proportional to data volume
- Required SELECT access to every table being analyzed
- Could take hours for large environments

The metadata-only approach uses 4 queries against `ACCOUNT_USAGE` views, reducing credit consumption by ~95%+ while still providing comprehensive readiness assessment.

### Q: How do I re-run analysis on new databases?

**A:** The tool uses caching:
1. Already-analyzed columns are skipped (loaded from cache)
2. New columns are analyzed and added to cache
3. To force re-analysis: set `analysis.force_reanalysis: true`

### Q: Can I restart the analysis from a specific stage?

**A:** Yes! Use the `--start-stage` option to resume from any stage:

```bash
python3 scripts/snowflake_full_analysis.py --start-stage 2B
python3 scripts/snowflake_full_analysis.py --start-stage 3
python3 scripts/snowflake_full_analysis.py -s 6  # Short form
```

See [Restarting from a Specific Stage](#restarting-from-a-specific-stage) for detailed prerequisites

### Q: Can I customize the AI candidate detection?

**A:** Yes. Edit in `config/config.yaml`:

```yaml
ai_candidates:
  text_indicators:
    - "DESCRIPTION"
    - "CONTENT"
    - "YOUR_CUSTOM_INDICATOR"
  min_text_column_length: 500
```

---

## Support

For issues and feature requests:
1. Check the [Troubleshooting](#troubleshooting) section
2. Review `snowflake-ai-enablement-reports/logs/analysis_errors.log`
3. Open an issue in the repository with:
   - Error message
   - Relevant config (redact credentials)
   - Dry run output

---

*This guide is maintained alongside the codebase. For the latest version, see `docs/USER_GUIDE.md` in the repository.*
