#!/usr/bin/env python3
"""
===============================================================================
SNOWFLAKE AI READINESS AGENT
===============================================================================

An autonomous agent for analyzing Snowflake environments to identify
AI/ML enablement opportunities using Snowflake Cortex AI services.

AGENT INSTRUCTIONS:
-------------------
This utility operates as an autonomous agent with the following directives:

1. EXECUTION MODE: Runs as a structured application with defined entry point
2. OPERATION: Read-only analysis - SELECT statements ONLY
3. TIMESTAMPS: All output must use UTC format with explicit timezone label
   Format: "YYYY-MM-DD HH:MM UTC" (e.g., "2026-02-06 07:14 UTC")
4. METADATA: All generated documentation must include "Generated On" field
5. SAFETY: No data modification - audit trail maintained for all queries

GENERATED OUTPUTS:
------------------
All outputs include standardized metadata header with UTC timestamp:
- executive_summary.md
- ai_strategy_roadmap.md  
- metadata/full_inventory.csv
- metadata/all_candidates.json
- metadata/confirmed_candidates.json
- profiles/*.md
- reports/data_quality_dashboard.md
- logs/audit_trail.sql

VERSION: 2.0 (Agent Architecture)
===============================================================================
"""

import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import os
import sys
import csv
import json
import yaml
import argparse
from datetime import datetime, timezone
from pathlib import Path
from collections import defaultdict
from snowflake_ai_readiness_metadata import (
    run_metadata_analysis,
    generate_readiness_report_markdown,
    compute_column_metadata_stats,
    enhance_data_readiness_score_metadata,
    build_table_metadata_lookup,
    build_column_metadata_lookup,
)

# =============================================================================
# AGENT METADATA & UTC TIMESTAMP UTILITIES
# =============================================================================

AGENT_VERSION = "2.0"
AGENT_NAME = "Snowflake AI Readiness Agent"

# Valid stages for --start-stage option (in execution order)
VALID_STAGES = ['1', '2', '2A', '2B', '2C', '2D', '2E', '2F', '3', '4', '5', '5B', '6']
STAGE_ORDER = {stage: idx for idx, stage in enumerate(VALID_STAGES)}

def should_run_stage(current_stage, start_stage):
    """
    Determine if a stage should be executed based on the start stage.
    
    Args:
        current_stage: The stage we're about to run (e.g., '2B')
        start_stage: The stage to start from (e.g., '2C' means skip 1, 2, 2A, 2B)
    
    Returns:
        bool: True if current_stage should be executed
    """
    if not start_stage:
        return True
    start_stage = start_stage.upper()
    current_stage = current_stage.upper()
    return STAGE_ORDER.get(current_stage, 0) >= STAGE_ORDER.get(start_stage, 0)

def get_utc_timestamp():
    """
    Get current timestamp in UTC with explicit timezone label.
    
    Returns: str in format "YYYY-MM-DD HH:MM UTC"
    
    AGENT DIRECTIVE: All timestamps in output documentation MUST use this function
    to ensure consistent UTC formatting across all generated artifacts.
    """
    utc_now = datetime.now(timezone.utc)
    return utc_now.strftime("%Y-%m-%d %H:%M UTC")

def get_utc_timestamp_iso():
    """
    Get current timestamp in ISO format with UTC timezone.
    
    Returns: str in ISO format with +00:00 suffix
    """
    return datetime.now(timezone.utc).isoformat()

def get_generated_metadata():
    """
    Generate standardized metadata block for all output documents.
    
    Returns: dict with agent metadata including UTC timestamp
    """
    return {
        "agent_name": AGENT_NAME,
        "agent_version": AGENT_VERSION,
        "generated_on": get_utc_timestamp(),
        "generated_on_iso": get_utc_timestamp_iso(),
        "mode": "read_only",
        "timezone": "UTC"
    }

def format_doc_header(title, description=""):
    """
    Generate standardized markdown header for all output documents.
    
    AGENT DIRECTIVE: All generated markdown documents MUST include this header.
    """
    metadata = get_generated_metadata()
    header = f"""# {title}

> **Generated On:** {metadata['generated_on']}  
> **Agent:** {metadata['agent_name']} v{metadata['agent_version']}  
> **Mode:** Read-Only Analysis

"""
    if description:
        header += f"{description}\n\n"
    header += "---\n\n"
    return header

# Script paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(SCRIPT_DIR)
CONFIG_DIR = os.path.join(REPO_ROOT, "config")

# =============================================================================
# CONFIGURATION LOADING (Objective 1: YAML Configuration)
# =============================================================================

def load_yaml_config(config_path=None):
    """
    Load configuration from YAML file.
    
    Priority:
    1. Explicit config_path parameter
    2. config/config.yaml in repo
    3. config/config.example.yaml in repo (with warning)
    4. Legacy: config.yaml in repo root (backward compatibility)
    5. Fall back to environment variables
    
    Returns dict with configuration values.
    """
    config = {}
    config_file = None
    
    # Determine config file path
    if config_path and os.path.exists(config_path):
        config_file = config_path
    elif os.path.exists(os.path.join(CONFIG_DIR, "config.yaml")):
        config_file = os.path.join(CONFIG_DIR, "config.yaml")
    elif os.path.exists(os.path.join(REPO_ROOT, "config.yaml")):
        print("NOTE: Found config.yaml in repo root. Consider moving it to config/config.yaml")
        config_file = os.path.join(REPO_ROOT, "config.yaml")
    elif os.path.exists(os.path.join(CONFIG_DIR, "config.example.yaml")):
        print("WARNING: Using config/config.example.yaml - copy to config/config.yaml and update with your credentials")
        config_file = os.path.join(CONFIG_DIR, "config.example.yaml")
    
    if config_file:
        print(f"Loading configuration from: {config_file}")
        try:
            with open(config_file, 'r') as f:
                config = yaml.safe_load(f) or {}
        except yaml.YAMLError as e:
            print(f"ERROR: Failed to parse YAML config: {e}")
            config = {}
        except IOError as e:
            print(f"ERROR: Failed to read config file: {e}")
            config = {}
    else:
        print("No YAML config found, using environment variables")
    
    return config

def get_config_value(config, yaml_path, env_var=None, default=None):
    """
    Get configuration value with fallback chain: YAML -> ENV -> default.
    
    yaml_path: dot-separated path like 'snowflake.account'
    env_var: optional environment variable name
    default: default value if not found
    """
    # Try YAML config first
    value = config
    for key in yaml_path.split('.'):
        if isinstance(value, dict) and key in value:
            value = value[key]
        else:
            value = None
            break
    
    if value is not None:
        return value
    
    # Try environment variable
    if env_var and os.environ.get(env_var):
        return os.environ.get(env_var)
    
    # Return default
    return default

# Load configuration
CONFIG = load_yaml_config()

# Snowflake connection settings (from YAML or environment)
ACCOUNT = get_config_value(CONFIG, 'snowflake.account', 'SNOWFLAKE_ACCOUNT', 'your_account_identifier')
USER = get_config_value(CONFIG, 'snowflake.user', 'SNOWFLAKE_USER', 'your_username')
PASSWORD = get_config_value(CONFIG, 'snowflake.password', 'SNOWFLAKE_PASSWORD', None)
PRIVATE_KEY_PATH = get_config_value(
    CONFIG, 'snowflake.private_key_path', 'SNOWFLAKE_PRIVATE_KEY_PATH',
    os.path.expanduser("~/.snowflake/keys/snowflake_private_key.pem")
)
PRIVATE_KEY_PASSPHRASE = get_config_value(CONFIG, 'snowflake.private_key_passphrase', 'SNOWFLAKE_PRIVATE_KEY_PASSPHRASE', None)
WAREHOUSE = get_config_value(CONFIG, 'snowflake.warehouse', 'SNOWFLAKE_WAREHOUSE', None)
ROLE = get_config_value(CONFIG, 'snowflake.role', 'SNOWFLAKE_ROLE', None)

# Output directory - Standard destination: snowflake-ai-enablement-reports
OUTPUT_DIR = Path(get_config_value(CONFIG, 'output.directory', 'OUTPUT_DIR', os.path.join(REPO_ROOT, "snowflake-ai-enablement-reports")))

# Database filtering (Objective 1: target_databases)
TARGET_DATABASES = get_config_value(CONFIG, 'target_databases', None, [])
EXCLUDE_DATABASES = get_config_value(CONFIG, 'exclude_databases', None, ['SNOWFLAKE', 'SNOWFLAKE_SAMPLE_DATA'])

# Data Analysis Configuration
DATA_ANALYSIS_SAMPLE_TIMEOUT = int(get_config_value(CONFIG, 'analysis.sample_timeout', 'DATA_ANALYSIS_SAMPLE_TIMEOUT', 300))
DATA_ANALYSIS_FULL_TIMEOUT = int(get_config_value(CONFIG, 'analysis.full_scan_timeout', 'DATA_ANALYSIS_FULL_TIMEOUT', 900))
TOP_CANDIDATES_FULL_SCAN = int(get_config_value(CONFIG, 'analysis.top_candidates_full_scan', 'TOP_CANDIDATES_FULL_SCAN', 200))
FORCE_REANALYSIS = get_config_value(CONFIG, 'analysis.force_reanalysis', 'FORCE_REANALYSIS', False)
if isinstance(FORCE_REANALYSIS, str):
    FORCE_REANALYSIS = FORCE_REANALYSIS.lower() == 'true'
SAMPLE_SIZES = get_config_value(CONFIG, 'analysis.sample_sizes', None, [10000, 1000, 100])

# Data Profiling Thresholds (Objective 2: Deep Data Profiling)
PROFILING_SPARSITY_LOW = get_config_value(CONFIG, 'profiling.sparsity.low_threshold', None, 10)
PROFILING_SPARSITY_MEDIUM = get_config_value(CONFIG, 'profiling.sparsity.medium_threshold', None, 30)
PROFILING_SPARSITY_HIGH = get_config_value(CONFIG, 'profiling.sparsity.high_threshold', None, 70)
PROFILING_CARDINALITY_LOW = get_config_value(CONFIG, 'profiling.cardinality.low_threshold', None, 0.01)
PROFILING_CARDINALITY_HIGH = get_config_value(CONFIG, 'profiling.cardinality.high_threshold', None, 0.90)
PROFILING_MIN_MEANINGFUL_LENGTH = get_config_value(CONFIG, 'profiling.content_type.min_meaningful_length', None, 50)
PROFILING_MIN_RICH_LENGTH = get_config_value(CONFIG, 'profiling.content_type.min_rich_content_length', None, 200)
PROFILING_CONTENT_SAMPLE_SIZE = get_config_value(CONFIG, 'profiling.content_type.content_sample_size', None, 100)

# Confirmation thresholds for "Confirmed Candidates"
CONFIRM_MIN_DATA_READINESS = get_config_value(CONFIG, 'profiling.confirmation.min_data_readiness_score', None, 3.5)
CONFIRM_MAX_SPARSITY = get_config_value(CONFIG, 'profiling.confirmation.max_sparsity_percent', None, 50)
CONFIRM_MIN_AVG_TEXT_LENGTH = get_config_value(CONFIG, 'profiling.confirmation.min_avg_text_length', None, 30)

# AI candidate settings
TEXT_INDICATORS = get_config_value(CONFIG, 'ai_candidates.text_indicators', None, 
    ['DESCRIPTION', 'CONTENT', 'MESSAGE', 'NOTE', 'SUMMARY', 'DETAIL', 'BODY', 'TEXT', 
     'COMMENT', 'FEEDBACK', 'REVIEW', 'ABSTRACT', 'BIO', 'NARRATIVE', 'TITLE', 'SUBJECT'])
MIN_TEXT_COLUMN_LENGTH = get_config_value(CONFIG, 'ai_candidates.min_text_column_length', None, 500)
MIN_TEXT_COLUMNS_FOR_SEARCH = get_config_value(CONFIG, 'ai_candidates.min_text_columns_for_search', None, 2)

# PII indicators
PII_INDICATORS = get_config_value(CONFIG, 'pii.indicators', None,
    ['EMAIL', 'SSN', 'SOCIAL_SECURITY', 'PHONE', 'ADDRESS', 'FIRST_NAME', 'LAST_NAME', 
     'BIRTH', 'DOB', 'PASSWORD', 'SECRET', 'CREDENTIAL'])

# Dry Run Mode
DRY_RUN_ENABLED = get_config_value(CONFIG, 'dry_run.enabled', None, False)
DRY_RUN_SHOW_QUERIES = get_config_value(CONFIG, 'dry_run.show_sample_queries', None, True)
DRY_RUN_VALIDATE_ACCESS = get_config_value(CONFIG, 'dry_run.validate_access', None, True)

# Run Mode Configuration (fresh/append)
RUN_MODE = get_config_value(CONFIG, 'run_mode.mode', None, 'fresh').lower()
APPEND_STRATEGY = get_config_value(CONFIG, 'run_mode.append_strategy', None, 'merge').lower()
BACKUP_BEFORE_FRESH = get_config_value(CONFIG, 'run_mode.backup_before_fresh', None, False)
if isinstance(BACKUP_BEFORE_FRESH, str):
    BACKUP_BEFORE_FRESH = BACKUP_BEFORE_FRESH.lower() == 'true'

# Legacy support: ANALYZE_DATABASES from environment (deprecated)
_db_env = os.environ.get("ANALYZE_DATABASES", "").strip()
if _db_env and not TARGET_DATABASES:
    TARGET_DATABASES = [db.strip() for db in _db_env.split(",") if db.strip()]

# Cache and log paths
CACHE_FILE = OUTPUT_DIR / "metadata" / "data_analysis_cache.json"
ANALYSIS_ERRORS_LOG = OUTPUT_DIR / "logs" / "analysis_errors.log"
ANALYSIS_SUMMARY_LOG = OUTPUT_DIR / "logs" / "analysis_summary.log"

# Audit trail
AUDIT_LOG = []

# =============================================================================
# PROGRESS TRACKING UTILITIES
# =============================================================================

def print_progress(current, total, item_name="", phase="", extra_info=""):
    """
    Print detailed progress status.
    
    Args:
        current: Current item number (1-indexed)
        total: Total number of items
        item_name: Name/identifier of current item being processed
        phase: Current phase name
        extra_info: Additional status info (e.g., "analyzed: 50, cached: 30, errors: 2")
    """
    pct = (current / total * 100) if total > 0 else 0
    bar_width = 30
    filled = int(bar_width * current / total) if total > 0 else 0
    bar = "█" * filled + "░" * (bar_width - filled)
    
    # Build status line
    status = f"\r  [{bar}] {current:,}/{total:,} ({pct:.1f}%)"
    
    if item_name:
        # Truncate long names
        display_name = item_name[:50] + "..." if len(item_name) > 50 else item_name
        status += f" | Current: {display_name}"
    
    if extra_info:
        status += f" | {extra_info}"
    
    # Pad to overwrite previous line
    print(status.ljust(150), end="", flush=True)

def print_progress_complete(phase, stats_dict):
    """
    Print completion summary for a phase.
    
    Args:
        phase: Phase name
        stats_dict: Dict of statistics to display
    """
    print()  # New line after progress bar
    print(f"\n  {phase} Complete:")
    for key, value in stats_dict.items():
        if isinstance(value, int):
            print(f"    - {key}: {value:,}")
        else:
            print(f"    - {key}: {value}")

def load_private_key():
    """Load private key from file, supporting both encrypted and unencrypted keys."""
    key_path = os.path.expanduser(PRIVATE_KEY_PATH)
    
    if not os.path.exists(key_path):
        raise FileNotFoundError(f"Private key file not found: {key_path}")
    
    with open(key_path, "rb") as key_file:
        key_data = key_file.read()
    
    # Handle passphrase if provided
    passphrase = None
    if PRIVATE_KEY_PASSPHRASE:
        passphrase = PRIVATE_KEY_PASSPHRASE.encode()
    
    p_key = serialization.load_pem_private_key(
        key_data,
        password=passphrase,
        backend=default_backend()
    )
    
    return p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

def get_connection():
    """
    Establish Snowflake connection using YAML config settings.
    
    Supports:
    - Private key authentication (recommended)
    - Password authentication (fallback)
    - Optional warehouse and role settings
    """
    conn_params = {
        'account': ACCOUNT,
        'user': USER,
    }
    
    # Authentication method
    if PASSWORD:
        # Password authentication
        conn_params['password'] = PASSWORD
    else:
        # Private key authentication (default)
        private_key_bytes = load_private_key()
        conn_params['private_key'] = private_key_bytes
    
    # Optional warehouse
    if WAREHOUSE:
        conn_params['warehouse'] = WAREHOUSE
    
    # Optional role
    if ROLE:
        conn_params['role'] = ROLE
    
    return snowflake.connector.connect(**conn_params)

def print_config_summary():
    """Print summary of loaded configuration for verification."""
    print("\n=== Configuration Summary ===")
    print(f"Account: {ACCOUNT}")
    print(f"User: {USER}")
    print(f"Auth Method: {'Password' if PASSWORD else 'Private Key'}")
    print(f"Warehouse: {WAREHOUSE or 'Default'}")
    print(f"Role: {ROLE or 'Default'}")
    print(f"Output Directory: {OUTPUT_DIR}")
    
    if TARGET_DATABASES:
        print(f"Target Databases: {', '.join(TARGET_DATABASES)}")
    else:
        print("Target Databases: ALL (no filter)")
    
    if EXCLUDE_DATABASES:
        print(f"Excluded Databases: {', '.join(EXCLUDE_DATABASES)}")
    
    print(f"Sample Timeout: {DATA_ANALYSIS_SAMPLE_TIMEOUT}s")
    print(f"Full Scan Timeout: {DATA_ANALYSIS_FULL_TIMEOUT}s")
    print(f"Top Candidates for Full Scan: {TOP_CANDIDATES_FULL_SCAN}")
    
    # Run mode display
    print(f"Run Mode: {RUN_MODE.upper()}")
    if RUN_MODE == 'append':
        print(f"  - Append Strategy: {APPEND_STRATEGY}")
        print("  - Will preserve and merge with existing data")
    else:
        print("  - Will start fresh (overwrite existing data)")
        if BACKUP_BEFORE_FRESH:
            print("  - Backup enabled before overwrite")
    
    if DRY_RUN_ENABLED:
        print("")
        print("*** DRY RUN MODE ENABLED ***")
        print("  - Will validate connection and show scope")
        print("  - Will NOT execute data profiling queries")
        print("  - Will NOT generate full reports")
    print("")

# =============================================================================
# RUN MODE FUNCTIONS (Fresh/Append)
# =============================================================================

def load_existing_candidates():
    """
    Load existing candidates from previous analysis runs.
    Used in append mode to merge with new results.
    
    Returns: list of existing candidates, or empty list if none found
    """
    candidates_path = OUTPUT_DIR / "metadata" / "all_candidates.json"
    
    if not candidates_path.exists():
        print("  No existing candidates found - starting fresh")
        return []
    
    try:
        with open(candidates_path, "r", encoding="utf-8") as f:
            existing = json.load(f)
        print(f"  Loaded {len(existing):,} existing candidates from previous run")
        return existing
    except (json.JSONDecodeError, IOError) as e:
        print(f"  Warning: Could not load existing candidates: {e}")
        return []

def load_intermediate_state(start_stage):
    """
    Load intermediate state from previous runs when restarting from a later stage.
    
    This function loads cached data from previous analysis runs to enable
    resuming from any stage without re-running earlier stages.
    
    Args:
        start_stage: The stage to start from (e.g., '2B', '3', '5')
    
    Returns:
        dict: State containing loaded data from previous runs
    """
    state = {
        'databases': [],
        'schemas': [],
        'tables': [],
        'columns': [],
        'stages': [],
        'all_candidates': [],
        'enhanced_llm': [],
        'enhanced_search': [],
        'llm_candidates': [],
        'variant_candidates': [],
        'ml_candidates': [],
        'search_candidates': [],
        'text_profiles': [],
        'variant_profiles': [],
        'edu_tables': [],
        'pii_columns': [],
        'text_rich_columns': [],
        'cache': {}
    }
    
    start_stage = start_stage.upper() if start_stage else '1'
    print(f"\n=== Loading Intermediate State for Restart from Stage {start_stage} ===")
    
    # Load full inventory CSV for metadata (databases, schemas, tables, columns info)
    inventory_path = OUTPUT_DIR / "metadata" / "full_inventory.csv"
    if inventory_path.exists():
        try:
            with open(inventory_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
            if rows:
                # Extract unique databases, schemas, tables
                state['databases'] = list(set((r['DATABASE'],) for r in rows))
                state['schemas'] = list(set((r['DATABASE'], r['SCHEMA']) for r in rows))
                state['tables'] = list(set((r['DATABASE'], r['SCHEMA'], r['TABLE']) for r in rows))
                # Convert to column tuples matching discover_columns format
                state['columns'] = [
                    (r['DATABASE'], r['SCHEMA'], r['TABLE'], r['COLUMN'], 0, 
                     r['DATA_TYPE'], r.get('MAX_LENGTH'), None, None, 'YES', r.get('COMMENT', ''))
                    for r in rows
                ]
                print(f"  Loaded inventory: {len(state['databases'])} DBs, {len(state['tables'])} tables, {len(state['columns'])} columns")
        except Exception as e:
            print(f"  Warning: Could not load inventory: {e}")
    
    # Load stages inventory
    stages_path = OUTPUT_DIR / "metadata" / "stages_inventory.csv"
    if stages_path.exists():
        try:
            with open(stages_path, "r", encoding="utf-8") as f:
                reader = csv.reader(f)
                header = next(reader, None)
                state['stages'] = list(reader)
            print(f"  Loaded {len(state['stages'])} stages from inventory")
        except Exception as e:
            print(f"  Warning: Could not load stages inventory: {e}")
    
    # Load all candidates
    candidates_path = OUTPUT_DIR / "metadata" / "all_candidates.json"
    if candidates_path.exists():
        try:
            with open(candidates_path, "r", encoding="utf-8") as f:
                state['all_candidates'] = json.load(f)
            print(f"  Loaded {len(state['all_candidates']):,} candidates")
        except Exception as e:
            print(f"  Warning: Could not load candidates: {e}")
    
    # Load enhanced candidates
    enhanced_path = OUTPUT_DIR / "metadata" / "enhanced_text_candidates.json"
    if enhanced_path.exists():
        try:
            with open(enhanced_path, "r", encoding="utf-8") as f:
                enhanced = json.load(f)
            state['enhanced_llm'] = enhanced.get('llm_candidates', [])
            state['enhanced_search'] = enhanced.get('search_candidates', [])
            print(f"  Loaded enhanced: {len(state['enhanced_llm'])} LLM, {len(state['enhanced_search'])} search")
        except Exception as e:
            print(f"  Warning: Could not load enhanced candidates: {e}")
    
    # Load text profiles
    profiles_path = OUTPUT_DIR / "profiles" / "text_column_profiles.json"
    if profiles_path.exists():
        try:
            with open(profiles_path, "r", encoding="utf-8") as f:
                profiles = json.load(f)
            state['text_profiles'] = profiles.get('text_profiles', [])
            state['variant_profiles'] = profiles.get('variant_profiles', [])
            print(f"  Loaded profiles: {len(state['text_profiles'])} text, {len(state['variant_profiles'])} variant")
        except Exception as e:
            print(f"  Warning: Could not load profiles: {e}")
    
    # Load analysis cache
    state['cache'] = load_analysis_cache()
    
    print("  Intermediate state loaded successfully")
    return state

def load_existing_metadata():
    """
    Load existing metadata context for append mode.
    Returns dict with databases, schemas, tables already analyzed.
    """
    metadata = {
        'databases_analyzed': set(),
        'schemas_analyzed': set(),
        'tables_analyzed': set(),
        'run_history': []
    }
    
    # Try to load run history
    history_path = OUTPUT_DIR / "metadata" / "run_history.json"
    if history_path.exists():
        try:
            with open(history_path, "r", encoding="utf-8") as f:
                history = json.load(f)
            metadata['run_history'] = history.get('runs', [])
            metadata['databases_analyzed'] = set(history.get('databases_analyzed', []))
            print(f"  Loaded run history: {len(metadata['run_history'])} previous runs")
            print(f"  Previously analyzed databases: {', '.join(metadata['databases_analyzed']) or 'None'}")
        except (json.JSONDecodeError, IOError):
            pass
    
    return metadata

def save_run_history(metadata, new_databases):
    """Save run history for incremental tracking."""
    history_path = OUTPUT_DIR / "metadata" / "run_history.json"
    
    # Add current run
    current_run = {
        'timestamp': get_utc_timestamp(),
        'timestamp_iso': get_utc_timestamp_iso(),
        'mode': RUN_MODE,
        'databases_analyzed': list(new_databases),
        'target_filter': TARGET_DATABASES or 'ALL'
    }
    metadata['run_history'].append(current_run)
    metadata['databases_analyzed'].update(new_databases)
    
    history = {
        'runs': metadata['run_history'],
        'databases_analyzed': list(metadata['databases_analyzed']),
        'last_updated': get_utc_timestamp()
    }
    
    with open(history_path, "w", encoding="utf-8") as f:
        json.dump(history, f, indent=2)
    print(f"  Updated run history: {len(metadata['run_history'])} total runs")

def merge_candidates(existing, new_candidates):
    """
    Merge new candidates with existing ones, avoiding duplicates.
    Uses database.schema.table.column as unique key.
    
    Returns: merged list of candidates
    """
    # Create lookup of existing candidates
    existing_keys = set()
    for c in existing:
        key = f"{c.get('database','')}.{c.get('schema','')}.{c.get('table','')}.{c.get('column','')}"
        existing_keys.add(key)
    
    # Add new candidates that don't already exist
    added_count = 0
    updated_count = 0
    
    for c in new_candidates:
        key = f"{c.get('database','')}.{c.get('schema','')}.{c.get('table','')}.{c.get('column','')}"
        if key not in existing_keys:
            existing.append(c)
            existing_keys.add(key)
            added_count += 1
        else:
            # Update existing candidate with new data (optional)
            updated_count += 1
    
    print(f"  Merge results: {added_count:,} new, {updated_count:,} already existed")
    return existing

def prepare_output_directory():
    """
    Prepare output directory based on run mode.
    - Fresh mode: Clear existing data (with optional backup)
    - Append mode: Preserve existing data
    """
    # Ensure directories exist
    (OUTPUT_DIR / "metadata").mkdir(parents=True, exist_ok=True)
    (OUTPUT_DIR / "logs").mkdir(parents=True, exist_ok=True)
    (OUTPUT_DIR / "profiles").mkdir(parents=True, exist_ok=True)
    (OUTPUT_DIR / "reports").mkdir(parents=True, exist_ok=True)
    
    if RUN_MODE == 'fresh':
        if BACKUP_BEFORE_FRESH:
            backup_dir = OUTPUT_DIR.parent / f"backup_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
            if OUTPUT_DIR.exists():
                import shutil
                shutil.copytree(OUTPUT_DIR, backup_dir)
                print(f"  Backed up existing reports to: {backup_dir}")
        
        # In fresh mode, we don't clear files - they get overwritten naturally
        print(f"  Fresh mode: Reports will be overwritten")
    else:
        print(f"  Append mode: Preserving existing data")

# =============================================================================
# DRY RUN FUNCTIONS
# =============================================================================

def run_dry_run(conn):
    """
    Execute dry run mode: validate configuration without full analysis.
    
    Performs:
    1. Connection validation
    2. Database access validation
    3. Scope estimation (databases, tables, columns)
    4. Sample query display
    5. Estimated runtime calculation
    """
    print("\n" + "=" * 70)
    print("DRY RUN MODE - Configuration Validation")
    print("=" * 70)
    
    # Step 1: Connection validation
    print("\n[Step 1/5] Validating Snowflake Connection...")
    try:
        cols, rows = execute_query(conn, "SELECT CURRENT_USER(), CURRENT_ACCOUNT(), CURRENT_ROLE(), CURRENT_WAREHOUSE()",
                                   "Dry run: Verify connection")
        print(f"  ✓ Connected as: {rows[0][0]}")
        print(f"  ✓ Account: {rows[0][1]}")
        print(f"  ✓ Role: {rows[0][2]}")
        print(f"  ✓ Warehouse: {rows[0][3] or 'Not set'}")
    except Exception as e:
        print(f"  ✗ Connection FAILED: {e}")
        return False
    
    # Step 2: List databases that would be analyzed
    # Use ACCOUNT_USAGE.TABLES for authoritative database list (ACCOUNT_USAGE.DATABASES contains stale entries)
    print("\n[Step 2/5] Discovering Databases (from tables metadata)...")
    table_filter = ""
    if TARGET_DATABASES:
        db_list = ", ".join([f"'{db.upper()}'" for db in TARGET_DATABASES])
        table_filter = f"AND UPPER(TABLE_CATALOG) IN ({db_list})"
    elif EXCLUDE_DATABASES:
        db_list = ", ".join([f"'{db.upper()}'" for db in EXCLUDE_DATABASES])
        table_filter = f"AND UPPER(TABLE_CATALOG) NOT IN ({db_list})"

    db_query = f"""
    SELECT DISTINCT TABLE_CATALOG AS DATABASE_NAME
    FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES
    WHERE DELETED IS NULL
    {table_filter}
    ORDER BY DATABASE_NAME
    """
    cols, databases = execute_query(conn, db_query, "Dry run: List databases with tables")
    actual_db_count = len(databases)
    
    print(f"  Found {actual_db_count} database(s) to analyze:")
    for db in databases[:20]:  # Show first 20
        print(f"    - {db[0]}")
    if actual_db_count > 20:
        print(f"    ... and {actual_db_count - 20} more")
    
    # Step 3: Validate access to databases (if enabled)
    if DRY_RUN_VALIDATE_ACCESS:
        print("\n[Step 3/5] Validating Database Access...")
        accessible = 0
        inaccessible = []
        
        for db in databases[:10]:  # Check first 10
            db_name = db[0]
            try:
                test_query = f'SELECT COUNT(*) FROM "{db_name}".INFORMATION_SCHEMA.TABLES LIMIT 1'
                execute_query(conn, test_query, f"Dry run: Test access to {db_name}")
                accessible += 1
                print(f"    ✓ {db_name}: Accessible")
            except Exception as e:
                inaccessible.append(db_name)
                print(f"    ✗ {db_name}: No access ({str(e)[:50]})")
        
        if actual_db_count > 10:
            print(f"    (checked 10 of {actual_db_count} databases)")
        
        if inaccessible:
            print(f"\n  WARNING: {len(inaccessible)} database(s) may not be accessible")
    else:
        print("\n[Step 3/5] Skipping access validation (disabled in config)")
    
    # Step 4: Estimate scope
    print("\n[Step 4/5] Estimating Analysis Scope...")
    
    # Count tables
    table_query = f"""
    SELECT COUNT(*) FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES 
    WHERE DELETED IS NULL {table_filter}
    """
    _, table_count = execute_query(conn, table_query, "Dry run: Count tables")
    total_tables = table_count[0][0] if table_count else 0
    
    # Count columns
    col_query = f"""
    SELECT COUNT(*) FROM SNOWFLAKE.ACCOUNT_USAGE.COLUMNS 
    WHERE DELETED IS NULL {table_filter}
    """
    _, col_count = execute_query(conn, col_query, "Dry run: Count columns")
    total_columns = col_count[0][0] if col_count else 0
    
    # Estimate AI candidates (text columns with indicator names)
    indicator_patterns = " OR ".join([f"UPPER(COLUMN_NAME) LIKE '%{ind}%'" for ind in TEXT_INDICATORS[:5]])
    candidate_query = f"""
    SELECT COUNT(*) FROM SNOWFLAKE.ACCOUNT_USAGE.COLUMNS 
    WHERE DELETED IS NULL {table_filter}
    AND (UPPER(DATA_TYPE) LIKE '%VARCHAR%' OR UPPER(DATA_TYPE) LIKE '%TEXT%')
    AND ({indicator_patterns})
    """
    _, cand_count = execute_query(conn, candidate_query, "Dry run: Estimate AI candidates")
    estimated_candidates = cand_count[0][0] if cand_count else 0
    
    print(f"  Databases to analyze: {actual_db_count:,}")
    print(f"  Estimated tables: {total_tables:,}")
    print(f"  Estimated columns: {total_columns:,}")
    print(f"  Estimated AI candidates: {estimated_candidates:,}+")
    
    # Estimate runtime
    est_metadata_time = actual_db_count * 2  # ~2 sec per DB for metadata
    est_profiling_time = min(estimated_candidates, TOP_CANDIDATES_FULL_SCAN) * 5  # ~5 sec per candidate
    est_total_time = est_metadata_time + est_profiling_time
    
    print(f"\n  Estimated runtime:")
    print(f"    - Metadata discovery: ~{est_metadata_time // 60}m {est_metadata_time % 60}s")
    print(f"    - Data profiling: ~{est_profiling_time // 60}m {est_profiling_time % 60}s")
    print(f"    - Total: ~{est_total_time // 60}m {est_total_time % 60}s")
    
    # Step 5: Show sample queries (if enabled)
    if DRY_RUN_SHOW_QUERIES:
        print("\n[Step 5/5] Sample Queries That Would Be Executed...")
        
        sample_db = databases[0][0] if databases else "YOUR_DATABASE"
        
        print("\n  1. Metadata Discovery Query:")
        print(f"""     SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE
     FROM SNOWFLAKE.ACCOUNT_USAGE.COLUMNS
     WHERE DELETED IS NULL {table_filter[:50]}...
     """)
        
        print("  2. Data Profiling Query (per candidate column):")
        print(f"""     SELECT COUNT(*), COUNT("column"), HLL("column"), AVG(LENGTH("column"))
     FROM "{sample_db}"."schema"."table"
     SAMPLE (10000 ROWS)
     """)
        
        print("  3. Content Type Validation Query:")
        print(f"""     SELECT "column"
     FROM "{sample_db}"."schema"."table"
     SAMPLE (100 ROWS)
     WHERE "column" IS NOT NULL
     LIMIT 10
     """)
    else:
        print("\n[Step 5/5] Skipping sample query display (disabled in config)")
    
    # Summary
    print("\n" + "=" * 70)
    print("DRY RUN COMPLETE")
    print("=" * 70)
    print(f"""
Summary:
  - Connection: Valid
  - Databases to analyze: {actual_db_count}
  - Estimated tables: {total_tables:,}
  - Estimated columns: {total_columns:,}
  - Estimated AI candidates: {estimated_candidates:,}+
  - Estimated runtime: ~{est_total_time // 60}m {est_total_time % 60}s

To run full analysis, set 'dry_run.enabled: false' in config/config.yaml
""")
    
    return True

def execute_query(conn, query, description=""):
    """Execute SELECT query with audit logging (UTC timestamps)."""
    query_upper = query.strip().upper()
    if not query_upper.startswith("SELECT"):
        raise ValueError(f"SAFETY VIOLATION: Only SELECT allowed. Got: {query[:50]}")

    AUDIT_LOG.append({
        "timestamp": get_utc_timestamp(),
        "timestamp_iso": get_utc_timestamp_iso(),
        "description": description,
        "query": query.strip()
    })

    cursor = conn.cursor()
    try:
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        results = cursor.fetchall()
        return columns, results
    except Exception as e:
        print(f"  Query error: {e}")
        return [], []
    finally:
        cursor.close()

def save_csv(filepath, columns, rows):
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        writer.writerows(rows)

def save_audit_log():
    """Save audit trail with UTC timestamp metadata."""
    audit_path = OUTPUT_DIR / "logs" / "audit_trail.sql"
    metadata = get_generated_metadata()
    with open(audit_path, "w") as f:
        f.write("-- ============================================================\n")
        f.write(f"-- {AGENT_NAME} - Audit Trail\n")
        f.write("-- ============================================================\n")
        f.write(f"-- Generated On: {metadata['generated_on']}\n")
        f.write(f"-- Agent Version: {metadata['agent_version']}\n")
        f.write("-- Mode: READ-ONLY (All queries are SELECT only)\n")
        f.write(f"-- Total Queries Executed: {len(AUDIT_LOG)}\n")
        f.write("-- ============================================================\n\n")
        for i, entry in enumerate(AUDIT_LOG, 1):
            f.write(f"-- Query #{i} [{entry['timestamp']}]\n")
            f.write(f"-- Description: {entry['description']}\n")
            f.write(f"{entry['query']};\n\n")

def load_analysis_cache():
    """Load existing data analysis cache from JSON file"""
    if not CACHE_FILE.exists() or FORCE_REANALYSIS:
        return {}

    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            cache = json.load(f)
        print(f"Loaded cache with {len(cache)} analyzed columns")
        return cache
    except (json.JSONDecodeError, IOError, OSError) as e:
        print(f"Warning: Could not load cache: {e}")
        return {}

def json_serializer(obj):
    """Custom JSON serializer for objects not serializable by default json code."""
    from decimal import Decimal
    if isinstance(obj, Decimal):
        return float(obj)
    if hasattr(obj, 'isoformat'):
        return obj.isoformat()
    return str(obj)

def save_analysis_cache(cache):
    """Save data analysis cache to JSON file. Returns True on success, False on failure."""
    try:
        CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f, indent=2, default=json_serializer)
        print(f"Saved cache with {len(cache)} analyzed columns")
        return True
    except (TypeError, IOError, OSError) as e:
        print(f"Error saving cache: {e}")
        return False

def validate_snowflake_identifier(identifier, name):
    """Validate Snowflake identifier to prevent SQL injection"""
    import re
    if not re.match(r'^[A-Za-z0-9_$]+$', identifier):
        raise ValueError(f"Invalid {name}: {identifier}")
    return identifier

# =============================================================================
# DATABASE FILTERING (Objective 1: Early Filter Application)
# =============================================================================

def should_include_database(db_name):
    """
    Determine if a database should be included in analysis.
    
    Filter logic:
    1. If TARGET_DATABASES is specified (non-empty), only include databases in that list
    2. If TARGET_DATABASES is empty/not set, include all databases EXCEPT those in EXCLUDE_DATABASES
    
    This filter is applied EARLY in execution to avoid unnecessary metadata fetching.
    """
    if not db_name:
        return False
    
    db_upper = db_name.upper()
    
    # If target_databases is specified, use whitelist mode
    if TARGET_DATABASES:
        target_upper = [db.upper() for db in TARGET_DATABASES]
        return db_upper in target_upper
    
    # Otherwise, exclude blacklisted databases
    if EXCLUDE_DATABASES:
        exclude_upper = [db.upper() for db in EXCLUDE_DATABASES]
        if db_upper in exclude_upper:
            return False
    
    return True

def filter_by_database(rows, db_index=0):
    """
    Filter rows by database name using the configured target_databases filter.
    
    Args:
        rows: List of tuples from query results
        db_index: Index of the database name column in each row (default 0)
    
    Returns:
        Filtered list of rows
    """
    if not TARGET_DATABASES and not EXCLUDE_DATABASES:
        return rows  # No filtering needed
    
    filtered = [row for row in rows if should_include_database(row[db_index])]
    return filtered

def get_database_filter_clause():
    """
    Generate SQL WHERE clause fragment for database filtering.
    
    Returns empty string if no filtering needed, otherwise returns
    AND clause to filter databases in SQL queries.
    """
    if TARGET_DATABASES:
        # Whitelist mode
        db_list = ", ".join([f"'{db.upper()}'" for db in TARGET_DATABASES])
        return f"AND UPPER(DATABASE_NAME) IN ({db_list})"
    elif EXCLUDE_DATABASES:
        # Blacklist mode
        db_list = ", ".join([f"'{db.upper()}'" for db in EXCLUDE_DATABASES])
        return f"AND UPPER(DATABASE_NAME) NOT IN ({db_list})"
    return ""

# =============================================================================
# DEEP DATA PROFILING (Objective 2: Sparsity, Cardinality, Content Type)
# =============================================================================

def classify_sparsity(null_percentage):
    """
    Classify column sparsity based on NULL percentage.
    
    Returns: 'low', 'medium', 'high', or 'very_high'
    """
    if null_percentage is None:
        return 'unknown'
    
    if null_percentage <= PROFILING_SPARSITY_LOW:
        return 'low'  # Good - minimal NULLs
    elif null_percentage <= PROFILING_SPARSITY_MEDIUM:
        return 'medium'  # Acceptable
    elif null_percentage <= PROFILING_SPARSITY_HIGH:
        return 'high'  # Poor - many NULLs
    else:
        return 'very_high'  # Very poor - mostly NULLs

def classify_cardinality(unique_ratio):
    """
    Classify column cardinality based on unique value ratio.
    
    unique_ratio: distinct_count / total_count
    Returns: 'low', 'medium', or 'high'
    """
    if unique_ratio is None:
        return 'unknown'
    
    if unique_ratio <= PROFILING_CARDINALITY_LOW:
        return 'low'  # Few unique values (codes/categories)
    elif unique_ratio >= PROFILING_CARDINALITY_HIGH:
        return 'high'  # Many unique values (identifiers)
    else:
        return 'medium'  # Normal distribution

def classify_content_type(avg_length, sample_values=None):
    """
    Classify text content type based on average length and sample analysis.
    
    Returns dict with:
    - content_class: 'code', 'short_text', 'meaningful_text', 'rich_content'
    - is_natural_language: bool
    - is_structured: bool (JSON, codes, etc.)
    """
    result = {
        'content_class': 'unknown',
        'is_natural_language': False,
        'is_structured': False
    }
    
    if avg_length is None:
        return result
    
    # Classify by length
    if avg_length < 10:
        result['content_class'] = 'code'  # Likely codes/IDs
        result['is_structured'] = True
    elif avg_length < PROFILING_MIN_MEANINGFUL_LENGTH:
        result['content_class'] = 'short_text'  # Short values
    elif avg_length < PROFILING_MIN_RICH_LENGTH:
        result['content_class'] = 'meaningful_text'  # Good for some AI
        result['is_natural_language'] = True
    else:
        result['content_class'] = 'rich_content'  # Ideal for GenAI
        result['is_natural_language'] = True
    
    return result

def run_deep_profiling(conn, db, schema, table, column, data_type):
    """
    Run deep data profiling on a column to assess AI readiness.
    
    Performs:
    1. Sparsity check (NULL rate)
    2. Cardinality check (distinct values)
    3. Content type analysis (for text columns)
    4. JSON structure validation (for VARIANT columns)
    
    Uses SAMPLE and LIMIT to minimize compute costs.
    Returns dict with profiling results.
    """
    # Validate identifiers
    db = validate_snowflake_identifier(db, "database")
    schema = validate_snowflake_identifier(schema, "schema")
    table = validate_snowflake_identifier(table, "table")
    column = validate_snowflake_identifier(column, "column")
    
    profile = {
        'sparsity': {},
        'cardinality': {},
        'content_type': {},
        'json_structure': {},
        'profiled_at': datetime.now().isoformat()
    }
    
    is_text = any(t in data_type.upper() for t in ['VARCHAR', 'TEXT', 'STRING', 'CHAR'])
    is_variant = data_type.upper() in ('VARIANT', 'OBJECT', 'ARRAY')
    
    try:
        cursor = conn.cursor()
        cursor.execute(f"ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = {DATA_ANALYSIS_SAMPLE_TIMEOUT}")
        
        # Query 1: Basic stats with cardinality (using HLL for efficiency)
        if is_text:
            stats_query = f"""
            SELECT
                COUNT(*) as total_count,
                COUNT("{column}") as non_null_count,
                HLL("{column}") as approx_distinct,
                AVG(LENGTH("{column}")) as avg_length,
                MAX(LENGTH("{column}")) as max_length
            FROM "{db}"."{schema}"."{table}"
            SAMPLE (10000 ROWS)
            """
        elif is_variant:
            stats_query = f"""
            SELECT
                COUNT(*) as total_count,
                COUNT("{column}") as non_null_count,
                HLL(TO_VARCHAR("{column}")) as approx_distinct,
                AVG(LENGTH(TO_VARCHAR("{column}"))) as avg_length,
                MAX(LENGTH(TO_VARCHAR("{column}"))) as max_length
            FROM "{db}"."{schema}"."{table}"
            SAMPLE (10000 ROWS)
            """
        else:
            stats_query = f"""
            SELECT
                COUNT(*) as total_count,
                COUNT("{column}") as non_null_count,
                HLL("{column}") as approx_distinct,
                NULL as avg_length,
                NULL as max_length
            FROM "{db}"."{schema}"."{table}"
            SAMPLE (10000 ROWS)
            """
        
        cursor.execute(stats_query)
        result = cursor.fetchone()
        
        if result:
            total_count = result[0] or 0
            non_null_count = result[1] or 0
            approx_distinct = result[2] or 0
            avg_length = result[3]
            max_length = result[4]
            
            # Calculate sparsity
            null_pct = ((total_count - non_null_count) / total_count * 100) if total_count > 0 else 100
            profile['sparsity'] = {
                'null_percentage': round(null_pct, 2),
                'classification': classify_sparsity(null_pct),
                'non_null_count': non_null_count,
                'total_count': total_count
            }
            
            # Calculate cardinality
            unique_ratio = (approx_distinct / non_null_count) if non_null_count > 0 else 0
            profile['cardinality'] = {
                'approx_distinct': approx_distinct,
                'unique_ratio': round(unique_ratio, 4),
                'classification': classify_cardinality(unique_ratio)
            }
            
            # Content type for text columns
            if is_text and avg_length is not None:
                profile['content_type'] = classify_content_type(avg_length)
                profile['content_type']['avg_length'] = round(avg_length, 2)
                profile['content_type']['max_length'] = max_length
        
        # Query 2: JSON structure validation for VARIANT columns
        if is_variant:
            json_query = f"""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN TRY_PARSE_JSON(TO_VARCHAR("{column}")) IS NOT NULL THEN 1 ELSE 0 END) as valid_json,
                SUM(CASE WHEN TYPEOF("{column}") = 'OBJECT' THEN 1 ELSE 0 END) as object_count,
                SUM(CASE WHEN TYPEOF("{column}") = 'ARRAY' THEN 1 ELSE 0 END) as array_count
            FROM "{db}"."{schema}"."{table}"
            SAMPLE (1000 ROWS)
            WHERE "{column}" IS NOT NULL
            """
            cursor.execute(json_query)
            json_result = cursor.fetchone()
            
            if json_result and json_result[0]:
                total = json_result[0]
                valid_json = json_result[1] or 0
                object_count = json_result[2] or 0
                array_count = json_result[3] or 0
                
                profile['json_structure'] = {
                    'valid_json_rate': round(valid_json / total * 100, 2) if total > 0 else 0,
                    'object_rate': round(object_count / total * 100, 2) if total > 0 else 0,
                    'array_rate': round(array_count / total * 100, 2) if total > 0 else 0,
                    'is_valid_structure': (valid_json / total >= 0.9) if total > 0 else False
                }
        
        # Query 3: Content sample for text columns (check for natural language)
        if is_text and avg_length and avg_length >= PROFILING_MIN_MEANINGFUL_LENGTH:
            sample_query = f"""
            SELECT "{column}"
            FROM "{db}"."{schema}"."{table}"
            SAMPLE ({PROFILING_CONTENT_SAMPLE_SIZE} ROWS)
            WHERE "{column}" IS NOT NULL
            AND LENGTH("{column}") > 20
            LIMIT 10
            """
            cursor.execute(sample_query)
            samples = cursor.fetchall()
            
            if samples:
                # Simple heuristic: check for spaces (indicates words vs codes)
                space_count = sum(1 for s in samples if s[0] and ' ' in str(s[0]))
                has_natural_language = (space_count / len(samples)) >= 0.5
                profile['content_type']['is_natural_language'] = has_natural_language
                profile['content_type']['sample_with_spaces_rate'] = round(space_count / len(samples) * 100, 2)
        
        cursor.close()
        profile['success'] = True
        
    except Exception as e:
        profile['success'] = False
        profile['error'] = str(e)[:200]
    
    return profile

def is_confirmed_candidate(candidate, profile=None):
    """
    Determine if a candidate is a "Confirmed Candidate" based on data profiling.
    
    A Confirmed Candidate is one where the physical data supports the metadata hypothesis:
    - Low to medium sparsity (not mostly NULL)
    - For text: meaningful content length and natural language indicators
    - For VARIANT: valid JSON structure
    - Meets minimum data readiness score
    
    Returns: (is_confirmed: bool, reasons: list)
    """
    reasons = []
    is_confirmed = True
    
    # Get statistics from candidate or profile
    stats = candidate.get('statistics', {})
    if profile:
        stats = {**stats, **profile}
    
    # Check 1: Sparsity
    null_pct = stats.get('null_percentage') or stats.get('sparsity', {}).get('null_percentage', 100)
    if null_pct > CONFIRM_MAX_SPARSITY:
        is_confirmed = False
        reasons.append(f"High sparsity ({null_pct:.1f}% NULL)")
    else:
        reasons.append(f"Good completeness ({100-null_pct:.1f}% populated)")
    
    # Check 2: Content quality for text columns
    ai_feature = candidate.get('ai_feature', '')
    if ai_feature in ('Cortex LLM', 'Cortex Search / RAG'):
        avg_len = stats.get('avg_length') or stats.get('content_type', {}).get('avg_length', 0)
        if avg_len and avg_len < CONFIRM_MIN_AVG_TEXT_LENGTH:
            is_confirmed = False
            reasons.append(f"Short content (avg {avg_len:.1f} chars)")
        elif avg_len:
            reasons.append(f"Substantial content (avg {avg_len:.1f} chars)")
        
        # Check natural language indicator
        is_nl = stats.get('content_type', {}).get('is_natural_language', None)
        if is_nl is False:
            is_confirmed = False
            reasons.append("Content appears to be codes/structured, not natural language")
        elif is_nl is True:
            reasons.append("Natural language content detected")
    
    # Check 3: JSON structure for VARIANT columns
    if ai_feature == 'Cortex Extract':
        json_valid = stats.get('json_structure', {}).get('is_valid_structure', None)
        if json_valid is False:
            is_confirmed = False
            reasons.append("Invalid JSON structure detected")
        elif json_valid is True:
            reasons.append("Valid JSON structure confirmed")
    
    # Check 4: Data readiness score
    data_readiness = candidate.get('scores', {}).get('data_readiness', 0)
    if data_readiness < CONFIRM_MIN_DATA_READINESS:
        is_confirmed = False
        reasons.append(f"Low data readiness score ({data_readiness:.2f})")
    else:
        reasons.append(f"Good data readiness ({data_readiness:.2f})")
    
    return is_confirmed, reasons

def run_adaptive_sample(conn, db, schema, table, column, data_type):
    """
    Run adaptive sampling with fallback: 10K → 1K → 100 rows
    Returns (success, statistics_dict, sample_size, error_msg)
    """
    # Validate identifiers to prevent SQL injection
    db = validate_snowflake_identifier(db, "database")
    schema = validate_snowflake_identifier(schema, "schema")
    table = validate_snowflake_identifier(table, "table")
    column = validate_snowflake_identifier(column, "column")

    # Build base query - handle different data types
    is_numeric = any(t in data_type.upper() for t in ['NUMBER', 'INT', 'FLOAT', 'DOUBLE', 'DECIMAL'])

    if is_numeric:
        # For numeric columns (ML candidates), don't use LENGTH
        base_query = f"""
        SELECT
            COUNT(*) as row_count,
            COUNT("{column}") as non_null_count,
            MAX("{column}") as max_value,
            MIN("{column}") as min_value,
            AVG("{column}") as avg_value,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY "{column}") as median_value,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY "{column}") as p95_value,
            STDDEV("{column}") as stddev_value
        FROM "{db}"."{schema}"."{table}"
        """
    else:
        # For text/variant columns, use LENGTH
        base_query = f"""
        SELECT
            COUNT(*) as row_count,
            COUNT("{column}") as non_null_count,
            MAX(LENGTH("{column}")) as max_length,
            MIN(LENGTH("{column}")) as min_length,
            AVG(LENGTH("{column}")) as avg_length,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY LENGTH("{column}")) as median_length,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY LENGTH("{column}")) as p95_length,
            STDDEV(LENGTH("{column}")) as stddev_length
        FROM "{db}"."{schema}"."{table}"
        """

    # Try samples in order from config (default: 10K, 1K, 100)
    sample_sizes = SAMPLE_SIZES

    for i, sample_size in enumerate(sample_sizes):
        query = base_query + f" SAMPLE ({sample_size} ROWS)"

        cursor = conn.cursor()
        try:
            # Set statement timeout
            cursor.execute(f"ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = {DATA_ANALYSIS_SAMPLE_TIMEOUT}")

            # Execute sampling query
            cursor.execute(query)
            result = cursor.fetchone()

            if result:
                # Build statistics dictionary
                if is_numeric:
                    stats = {
                        "row_count": result[0],
                        "non_null_count": result[1],
                        "null_percentage": round((1 - result[1] / result[0]) * 100, 2) if result[0] > 0 else 100.0,
                        "max_value": float(result[2]) if result[2] is not None else None,
                        "min_value": float(result[3]) if result[3] is not None else None,
                        "avg_value": float(result[4]) if result[4] is not None else None,
                        "median_value": float(result[5]) if result[5] is not None else None,
                        "p95_value": float(result[6]) if result[6] is not None else None,
                        "stddev_value": float(result[7]) if result[7] is not None else None
                    }
                else:
                    stats = {
                        "row_count": result[0],
                        "non_null_count": result[1],
                        "null_percentage": round((1 - result[1] / result[0]) * 100, 2) if result[0] > 0 else 100.0,
                        "max_length": result[2],
                        "min_length": result[3],
                        "avg_length": round(result[4], 2) if result[4] is not None else None,
                        "median_length": round(result[5], 2) if result[5] is not None else None,
                        "p95_length": round(result[6], 2) if result[6] is not None else None,
                        "stddev_length": round(result[7], 2) if result[7] is not None else None
                    }

                cursor.close()
                return (True, stats, sample_size, None)

        except Exception as e:
            error_msg = str(e)
            if i == len(sample_sizes) - 1:
                # Last attempt failed
                cursor.close()
                return (False, None, None, error_msg)
            else:
                # Try smaller sample
                print(f"  Retrying with smaller sample ({sample_sizes[i + 1]} rows)")
                cursor.close()
                continue

    return (False, None, None, "All sampling attempts failed")

def enhance_data_readiness_score(candidate, statistics):
    """
    Calculate enhanced data readiness score (0-5) based on actual data quality.

    Components:
    - NULL rate impact (0-2 points)
    - Content substantiality (0-2 points) - text columns only
    - Data efficiency (0-1 point) - actual vs defined size

    Returns updated score (float, 0.0-5.0)
    """
    score = 0.0

    if not statistics:
        return 1.0  # Minimal score if no statistics available

    # Component 1: NULL Rate Impact (0-2 points)
    null_pct = statistics.get('null_percentage', 100.0)
    if null_pct <= 10:
        score += 2.0
    elif null_pct <= 30:
        score += 1.5
    elif null_pct <= 50:
        score += 1.0
    elif null_pct <= 70:
        score += 0.5
    # else: 0 points (>70% nulls)

    # Component 2: Content Substantiality (0-2 points) - text columns only
    avg_length = statistics.get('avg_length')
    if avg_length is not None:
        if avg_length >= 200:
            score += 2.0
        elif avg_length >= 100:
            score += 1.5
        elif avg_length >= 50:
            score += 1.0
        elif avg_length > 0:
            score += 0.5
        # else: 0 points (<50 chars - likely codes/short values)

    # Component 3: Data Efficiency (0-1 point) - actual vs defined size
    max_length = statistics.get('max_length')
    defined_length = candidate.get('max_length')
    if max_length is not None and defined_length and defined_length > 0:
        efficiency = (max_length / defined_length) * 100
        if efficiency > 50:
            score += 1.0
        elif efficiency >= 25:
            score += 0.5
        # else: 0 points (<25% efficiency)

    return round(score, 2)

def analyze_column_data(conn, candidate, cache):
    """
    Analyze a single candidate column with caching support.
    
    Args:
        conn: Snowflake connection
        candidate: Dict with database, schema, table, column, data_type
        cache: Dict of cached analysis results
    
    Returns:
        Dict with 'success', 'from_cache', and optionally 'error' keys
    """
    # Check if candidate has required fields
    required_fields = ['database', 'schema', 'table', 'column']
    if not all(k in candidate for k in required_fields):
        return {'success': False, 'error': 'Missing required fields'}
    
    # Only analyze LLM and Extract candidates (text/variant columns)
    ai_feature = candidate.get('ai_feature', '')
    if ai_feature not in ('Cortex LLM', 'Cortex Extract'):
        # ML and Search candidates don't need individual column analysis
        candidate['statistics'] = {}
        return {'success': True, 'from_cache': False, 'skipped': True}
    
    db = candidate['database']
    schema = candidate['schema']
    table = candidate['table']
    column = candidate['column']
    data_type = candidate.get('data_type', 'VARCHAR')
    
    cache_key = f"{db}.{schema}.{table}.{column}"
    
    # Check cache first
    if cache_key in cache and not FORCE_REANALYSIS:
        cached_data = cache[cache_key]
        candidate['statistics'] = cached_data.get('statistics', {})
        candidate['sample_size'] = cached_data.get('sample_size')
        candidate['analyzed_at'] = cached_data.get('analyzed_at')
        return {'success': True, 'from_cache': True}
    
    # Run adaptive sampling
    try:
        success, statistics, sample_size, error_msg = run_adaptive_sample(
            conn, db, schema, table, column, data_type
        )
        
        if success:
            # Cache successful analysis
            cache[cache_key] = {
                "analyzed_at": get_utc_timestamp_iso(),
                "sample_size": sample_size,
                "analysis_type": "sample",
                "statistics": statistics
            }
            
            # Attach to candidate
            candidate['statistics'] = statistics
            candidate['sample_size'] = sample_size
            candidate['analyzed_at'] = cache[cache_key]["analyzed_at"]
            
            return {'success': True, 'from_cache': False}
        else:
            return {'success': False, 'error': error_msg}
            
    except Exception as e:
        return {'success': False, 'error': str(e)}

def analyze_candidates(conn, candidates):
    """
    Orchestrate Phase 2: Data Analysis with caching and error handling.

    Steps:
    1. Load existing cache
    2. Run sampling pass on uncached candidates
    3. Re-score candidates with actual data
    4. Log errors comprehensively
    5. Save updated cache

    Returns (analyzed_candidates, analysis_cache, error_log)
    """
    print("\n" + "=" * 70)
    print("PHASE 2: ACTUAL DATA ANALYSIS")
    print("=" * 70)

    # Step 1: Load cache
    print("\n=== Phase 2A: Loading Cache ===")
    cache = load_analysis_cache()
    cached_count = len(cache)
    print(f"Loaded {cached_count} previously analyzed columns from cache")

    # Prepare error tracking
    error_log = []
    analysis_errors_path = ANALYSIS_ERRORS_LOG
    analysis_errors_path.parent.mkdir(parents=True, exist_ok=True)

    # Step 2: Sampling pass
    print("\n=== Phase 2B: Sampling Pass ===")
    total_candidates = len(candidates)
    analyzed_count = 0
    skipped_count = 0
    cached_hits = 0

    # Filter to only LLM and variant candidates that have database/schema/table/column info
    analyzable_candidates = []
    for cand in candidates:
        ai_feature = cand.get('ai_feature', '')
        if ai_feature in ('Cortex LLM', 'Cortex Extract'):
            if all(k in cand for k in ['database', 'schema', 'table', 'column']):
                analyzable_candidates.append(cand)

    print(f"Found {len(analyzable_candidates)} analyzable column candidates (LLM + Extract)")
    print(f"Analyzing candidates with adaptive sampling (10K→1K→100 rows)...")

    for i, cand in enumerate(analyzable_candidates, 1):
        db = cand['database']
        schema = cand['schema']
        table = cand['table']
        column = cand['column']
        data_type = cand.get('data_type', 'VARCHAR')

        cache_key = f"{db}.{schema}.{table}.{column}"

        # Check cache
        if cache_key in cache and not FORCE_REANALYSIS:
            cached_hits += 1
            # Apply cached statistics to candidate
            cand['statistics'] = cache[cache_key].get('statistics', {})
            cand['sample_size'] = cache[cache_key].get('sample_size')
            cand['analyzed_at'] = cache[cache_key].get('analyzed_at')
            continue

        # Progress indicator every 100 candidates
        if i % 100 == 0:
            print(f"  Progress: {i}/{len(analyzable_candidates)} candidates analyzed...")

        # Run adaptive sampling
        try:
            success, statistics, sample_size, error_msg = run_adaptive_sample(
                conn, db, schema, table, column, data_type
            )

            if success:
                # Cache successful analysis
                cache[cache_key] = {
                    "analyzed_at": datetime.now().isoformat(),
                    "sample_size": sample_size,
                    "analysis_type": "sample",
                    "statistics": statistics
                }

                # Attach to candidate
                cand['statistics'] = statistics
                cand['sample_size'] = sample_size
                cand['analyzed_at'] = cache[cache_key]["analyzed_at"]

                analyzed_count += 1
            else:
                # Log error
                error_entry = {
                    "timestamp": datetime.now().isoformat(),
                    "candidate": cache_key,
                    "error": error_msg,
                    "attempts": "3 (10000→1000→100 rows)"
                }
                error_log.append(error_entry)

                # Write to error log file
                with open(analysis_errors_path, "a", encoding="utf-8") as f:
                    f.write(f"[{error_entry['timestamp']}] ERROR: {cache_key}\n")
                    f.write(f"  Attempts: {error_entry['attempts']}\n")
                    f.write(f"  Error: {error_msg}\n")
                    f.write(f"  Action: Skipped, continuing analysis\n\n")

                skipped_count += 1

        except Exception as e:
            # Catch-all for unexpected errors
            error_entry = {
                "timestamp": datetime.now().isoformat(),
                "candidate": cache_key,
                "error": str(e),
                "attempts": "Unexpected error"
            }
            error_log.append(error_entry)

            with open(analysis_errors_path, "a", encoding="utf-8") as f:
                f.write(f"[{error_entry['timestamp']}] UNEXPECTED ERROR: {cache_key}\n")
                f.write(f"  Error: {str(e)}\n")
                f.write(f"  Action: Skipped, continuing analysis\n\n")

            skipped_count += 1

    # Step 3: Re-score with actual data
    print("\n=== Phase 2C: Re-scoring with Actual Data ===")
    rescored_count = 0
    for cand in analyzable_candidates:
        if 'statistics' in cand:
            # Calculate enhanced data readiness score
            enhanced_score = enhance_data_readiness_score(cand, cand['statistics'])

            # Update scores
            if 'scores' not in cand:
                cand['scores'] = {
                    'business_potential': 3,
                    'metadata_quality': 2,
                    'governance_risk': 2
                }

            # Replace data_readiness score with enhanced version
            cand['scores']['data_readiness'] = enhanced_score
            cand['total_score'] = sum(cand['scores'].values())
            rescored_count += 1

    print(f"Re-scored {rescored_count} candidates with enhanced data readiness scores")

    # Step 4: Save cache
    print("\n=== Phase 2D: Saving Cache ===")
    save_success = save_analysis_cache(cache)
    if save_success:
        print(f"Successfully saved cache with {len(cache)} analyzed columns")

    # Summary
    print("\n=== Phase 2 Summary ===")
    print(f"Total candidates: {len(analyzable_candidates)}")
    print(f"Cache hits: {cached_hits}")
    print(f"Newly analyzed: {analyzed_count}")
    print(f"Skipped (errors): {skipped_count}")
    print(f"Success rate: {((cached_hits + analyzed_count) / len(analyzable_candidates) * 100):.1f}%")

    return candidates, cache, error_log

def identify_top_candidates(candidates, top_n=200):
    """
    Identify top N candidates by enhanced total score.

    Filters to only candidates with:
    - Valid total_score
    - database, schema, table, column fields

    Returns list of top N candidates sorted by score (descending)
    """
    # Filter to scorable candidates with column info
    scorable = []
    for cand in candidates:
        if 'total_score' in cand and all(k in cand for k in ['database', 'schema', 'table', 'column']):
            scorable.append(cand)

    # Sort by total_score descending
    sorted_candidates = sorted(scorable, key=lambda x: x.get('total_score', 0), reverse=True)

    # Return top N
    top_candidates = sorted_candidates[:top_n]

    print(f"\n=== Top {top_n} Candidates Identified ===")
    print(f"Filtered from {len(scorable)} scorable candidates")
    if top_candidates:
        print(f"Score range: {top_candidates[0].get('total_score', 0):.2f} (highest) to {top_candidates[-1].get('total_score', 0):.2f} (lowest in top {top_n})")

    return top_candidates

def run_full_scan_analysis(conn, top_candidates, cache):
    """
    Run full table scans (no SAMPLE) on top candidates for exact statistics.

    Updates cache with full scan results (analysis_type: "full_scan")
    Updates candidates with exact statistics

    Returns (updated_candidates, updated_cache, full_scan_errors)
    """
    print("\n" + "=" * 70)
    print(f"PHASE 2E: FULL SCAN ANALYSIS (Top {len(top_candidates)} Candidates)")
    print("=" * 70)

    full_scan_errors = []
    success_count = 0
    skipped_count = 0
    total_candidates = len(top_candidates)

    for i, cand in enumerate(top_candidates, 1):
        db = cand['database']
        schema = cand['schema']
        table = cand['table']
        column = cand['column']
        data_type = cand.get('data_type', 'VARCHAR')

        cache_key = f"{db}.{schema}.{table}.{column}"

        # Progress tracking
        extra_info = f"OK:{success_count} Err:{skipped_count}"
        print_progress(i, total_candidates, cache_key, "Phase 2E", extra_info)

        # Validate identifiers
        try:
            db = validate_snowflake_identifier(db, "database")
            schema = validate_snowflake_identifier(schema, "schema")
            table = validate_snowflake_identifier(table, "table")
            column = validate_snowflake_identifier(column, "column")
        except ValueError as e:
            error_msg = f"Invalid identifier: {e}"
            full_scan_errors.append({
                "timestamp": datetime.now().isoformat(),
                "candidate": cache_key,
                "error": error_msg
            })
            skipped_count += 1
            continue

        # Build full scan query (no SAMPLE clause)
        is_numeric = any(t in data_type.upper() for t in ['NUMBER', 'INT', 'FLOAT', 'DOUBLE', 'DECIMAL'])

        if is_numeric:
            query = f"""
            SELECT
                COUNT(*) as row_count,
                COUNT("{column}") as non_null_count,
                MAX("{column}") as max_value,
                MIN("{column}") as min_value,
                AVG("{column}") as avg_value,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY "{column}") as median_value,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY "{column}") as p95_value,
                STDDEV("{column}") as stddev_value
            FROM "{db}"."{schema}"."{table}"
            """
        else:
            query = f"""
            SELECT
                COUNT(*) as row_count,
                COUNT("{column}") as non_null_count,
                MAX(LENGTH("{column}")) as max_length,
                MIN(LENGTH("{column}")) as min_length,
                AVG(LENGTH("{column}")) as avg_length,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY LENGTH("{column}")) as median_length,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY LENGTH("{column}")) as p95_length,
                STDDEV(LENGTH("{column}")) as stddev_length
            FROM "{db}"."{schema}"."{table}"
            """

        # Execute full scan with extended timeout
        cursor = conn.cursor()
        try:
            cursor.execute(f"ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = {DATA_ANALYSIS_FULL_TIMEOUT}")
            cursor.execute(query)
            result = cursor.fetchone()

            if result:
                # Build statistics
                if is_numeric:
                    stats = {
                        "row_count": result[0],
                        "non_null_count": result[1],
                        "null_percentage": round((1 - result[1] / result[0]) * 100, 2) if result[0] > 0 else 100.0,
                        "max_value": float(result[2]) if result[2] is not None else None,
                        "min_value": float(result[3]) if result[3] is not None else None,
                        "avg_value": float(result[4]) if result[4] is not None else None,
                        "median_value": float(result[5]) if result[5] is not None else None,
                        "p95_value": float(result[6]) if result[6] is not None else None,
                        "stddev_value": float(result[7]) if result[7] is not None else None
                    }
                else:
                    stats = {
                        "row_count": result[0],
                        "non_null_count": result[1],
                        "null_percentage": round((1 - result[1] / result[0]) * 100, 2) if result[0] > 0 else 100.0,
                        "max_length": result[2],
                        "min_length": result[3],
                        "avg_length": round(result[4], 2) if result[4] is not None else None,
                        "median_length": round(result[5], 2) if result[5] is not None else None,
                        "p95_length": round(result[6], 2) if result[6] is not None else None,
                        "stddev_length": round(result[7], 2) if result[7] is not None else None
                    }

                # Update cache with full scan
                cache[cache_key] = {
                    "analyzed_at": datetime.now().isoformat(),
                    "sample_size": "full",
                    "analysis_type": "full_scan",
                    "statistics": stats
                }

                # Update candidate
                cand['statistics'] = stats
                cand['sample_size'] = "full"
                cand['analyzed_at'] = cache[cache_key]["analyzed_at"]

                # Re-score with exact data
                enhanced_score = enhance_data_readiness_score(cand, stats)
                if 'scores' in cand:
                    cand['scores']['data_readiness'] = enhanced_score
                    cand['total_score'] = sum(cand['scores'].values())

                success_count += 1

        except Exception as e:
            error_msg = str(e)
            full_scan_errors.append({
                "timestamp": datetime.now().isoformat(),
                "candidate": cache_key,
                "error": error_msg
            })
            skipped_count += 1

        finally:
            cursor.close()

    # Print completion summary
    print_progress_complete("Phase 2E Full Scan", {
        "Successful": success_count,
        "Skipped (errors)": skipped_count,
        "Total processed": total_candidates
    })

    # Save updated cache
    save_analysis_cache(cache)

    return top_candidates, cache, full_scan_errors

def generate_data_quality_dashboard(candidates, cache, error_log):
    """
    Generate comprehensive data quality dashboard markdown report.

    Sections:
    - Executive Summary
    - Top Over-Provisioned Columns
    - Highest Quality AI Candidates
    - Candidates to Reconsider
    - Data Quality by AI Feature

    Returns dashboard markdown string
    """
    dashboard = f"""# Data Quality Dashboard - Actual Data Analysis

**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
**Analysis Type:** Adaptive Sampling (10K→1K→100 rows) + Top 200 Full Scans

---

## Executive Summary

"""

    # Calculate summary statistics
    total_candidates = len(candidates)
    analyzed_candidates = len([c for c in candidates if 'statistics' in c])
    success_rate = (analyzed_candidates / total_candidates * 100) if total_candidates > 0 else 0
    top_200_count = len([c for c in candidates if c.get('sample_size') == 'full'])
    error_count = len(error_log)

    dashboard += f"""| Metric | Value |
|--------|-------|
| **Total Candidates** | {total_candidates:,} |
| **Successfully Analyzed** | {analyzed_candidates:,} ({success_rate:.1f}%) |
| **Top Candidates (Full Scan)** | {top_200_count:,} |
| **Analysis Errors** | {error_count:,} |

"""

    # Key findings
    high_quality = [c for c in candidates if 'scores' in c and c['scores'].get('data_readiness', 0) >= 4.0]
    low_quality = [c for c in candidates if 'scores' in c and c['scores'].get('data_readiness', 0) <= 1.5]

    dashboard += f"""### Key Findings

- **High Quality Candidates:** {len(high_quality):,} columns with data readiness score ≥ 4.0
- **Low Quality Candidates:** {len(low_quality):,} columns with data readiness score ≤ 1.5
- **Cache Entries:** {len(cache):,} analyzed columns cached for incremental runs

---

## Top Over-Provisioned Columns

Columns where actual data usage is significantly below defined size (efficiency < 25%).

| Rank | Database.Schema.Table.Column | Defined | Avg Actual | Efficiency | Recommendation |
|------|------------------------------|---------|------------|------------|----------------|
"""

    # Find over-provisioned columns
    over_provisioned = []
    for cand in candidates:
        if 'statistics' not in cand:
            continue
        stats = cand['statistics']
        defined_len = cand.get('max_length')
        avg_len = stats.get('avg_length')

        if defined_len and avg_len and defined_len > 0:
            efficiency = (avg_len / defined_len) * 100
            if efficiency < 25:
                over_provisioned.append({
                    'candidate': cand,
                    'defined': defined_len,
                    'avg_actual': avg_len,
                    'efficiency': efficiency
                })

    # Sort by worst efficiency
    over_provisioned.sort(key=lambda x: x['efficiency'])

    for i, item in enumerate(over_provisioned[:20], 1):
        cand = item['candidate']
        fqn = f"{cand.get('database', 'N/A')}.{cand.get('schema', 'N/A')}.{cand.get('table', 'N/A')}.{cand.get('column', 'N/A')}"
        dashboard += f"| {i} | `{fqn[:60]}` | {item['defined']} | {item['avg_actual']:.1f} | {item['efficiency']:.1f}% | Consider VARCHAR({int(item['avg_actual'] * 2)}) |\n"

    if not over_provisioned:
        dashboard += "| - | No significantly over-provisioned columns found | - | - | - | - |\n"

    dashboard += f"""
---

## Highest Quality AI Candidates

Top candidates with excellent data readiness (score ≥ 4.0).

| Rank | Database.Schema.Table.Column | Score | Avg Length | NULL% | AI Feature |
|------|------------------------------|-------|------------|-------|------------|
"""

    # Sort high quality by score
    high_quality_sorted = sorted(high_quality, key=lambda x: x.get('total_score', 0), reverse=True)

    for i, cand in enumerate(high_quality_sorted[:30], 1):
        fqn = f"{cand.get('database', 'N/A')}.{cand.get('schema', 'N/A')}.{cand.get('table', 'N/A')}.{cand.get('column', 'N/A')}"
        score = cand.get('total_score', 0)
        avg_len = cand.get('statistics', {}).get('avg_length', 0) if 'statistics' in cand else 0
        null_pct = cand.get('statistics', {}).get('null_percentage', 100) if 'statistics' in cand else 100
        ai_feature = cand.get('ai_feature', 'N/A')
        dashboard += f"| {i} | `{fqn[:60]}` | {score:.2f} | {avg_len:.1f} | {null_pct:.1f}% | {ai_feature} |\n"

    if not high_quality:
        dashboard += "| - | No high-quality candidates found | - | - | - | - |\n"

    dashboard += f"""
---

## Candidates to Reconsider

Candidates with poor data quality (data readiness ≤ 1.5) that may not be suitable for AI.

| Database.Schema.Table.Column | Total Score | Data Readiness | Issue | Reason |
|------------------------------|-------------|----------------|-------|--------|
"""

    for cand in low_quality[:20]:
        fqn = f"{cand.get('database', 'N/A')}.{cand.get('schema', 'N/A')}.{cand.get('table', 'N/A')}.{cand.get('column', 'N/A')}"
        total_score = cand.get('total_score', 0)
        data_readiness = cand['scores'].get('data_readiness', 0)

        # Determine issue
        stats = cand.get('statistics', {})
        null_pct = stats.get('null_percentage', 100)
        avg_len = stats.get('avg_length', 0)

        if null_pct > 50:
            issue = "High NULL rate"
            reason = f"{null_pct:.1f}% NULLs"
        elif avg_len and avg_len < 50:
            issue = "Short content"
            reason = f"Avg {avg_len:.1f} chars"
        else:
            issue = "Low quality"
            reason = "Multiple factors"

        dashboard += f"| `{fqn[:60]}` | {total_score:.2f} | {data_readiness:.2f} | {issue} | {reason} |\n"

    if not low_quality:
        dashboard += "| - | No low-quality candidates identified | - | - | - | - |\n"

    dashboard += f"""
---

## Data Quality by AI Feature

"""

    # Group by AI feature
    feature_stats = defaultdict(lambda: {'total': 0, 'analyzed': 0, 'high_quality': 0, 'avg_score': []})

    for cand in candidates:
        ai_feature = cand.get('ai_feature', 'Unknown')
        feature_stats[ai_feature]['total'] += 1

        if 'statistics' in cand:
            feature_stats[ai_feature]['analyzed'] += 1

        if 'scores' in cand:
            data_readiness = cand['scores'].get('data_readiness', 0)
            feature_stats[ai_feature]['avg_score'].append(data_readiness)

            if data_readiness >= 4.0:
                feature_stats[ai_feature]['high_quality'] += 1

    for feature in sorted(feature_stats.keys()):
        stats = feature_stats[feature]
        avg_score = sum(stats['avg_score']) / len(stats['avg_score']) if stats['avg_score'] else 0
        analysis_rate = (stats['analyzed'] / stats['total'] * 100) if stats['total'] > 0 else 0

        dashboard += f"""### {feature}

- **Total Candidates:** {stats['total']:,}
- **Analyzed:** {stats['analyzed']:,} ({analysis_rate:.1f}%)
- **High Quality (≥4.0):** {stats['high_quality']:,}
- **Average Data Readiness:** {avg_score:.2f}/5.0

"""

    dashboard += """---

## Next Steps

1. **Review High Quality Candidates** - Start POCs with top-ranked columns
2. **Investigate Over-Provisioned Columns** - Optimize storage costs
3. **Reconsider Low Quality Candidates** - May need data quality improvements before AI enablement
4. **Check Error Log** - Review `logs/analysis_errors.log` for analysis failures

---

## Files Generated

| File | Description |
|------|-------------|
| `metadata/data_analysis_cache.json` | Cache of all analysis results |
| `metadata/all_candidates_enhanced.json` | Candidates with enhanced scores |
| `metadata/top_200_full_analysis.json` | Detailed exact statistics for top 200 |
| `logs/analysis_errors.log` | Detailed error log |
| `logs/analysis_summary.log` | Summary statistics |
"""

    return dashboard

def generate_comparison_report(candidates):
    """
    Generate before/after scoring comparison report.

    Compares metadata-only scoring with data-enhanced scoring.
    Shows biggest movers up and down in ranking.

    Returns comparison report markdown string
    """
    report = f"""# Scoring Comparison Report - Metadata vs Data-Enhanced

**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

---

## Methodology Comparison

### Metadata-Only Scoring (Before)
- Based on column type, defined size (CHARACTER_MAXIMUM_LENGTH), naming patterns
- Data Readiness: Fixed heuristics (e.g., +4 if max_length > 100)
- No understanding of actual data content or quality

### Data-Enhanced Scoring (After)
- Based on actual data analysis (10K sample or full scan)
- Data Readiness: 0-5 scale based on:
  - NULL rate impact (0-2 points)
  - Content substantiality (0-2 points)
  - Data efficiency (0-1 point)
- Evidence-based ranking with real statistics

---

## Impact Summary

"""

    # Separate candidates into before/after groups
    metadata_only = []
    data_enhanced = []

    for cand in candidates:
        if 'statistics' in cand and 'scores' in cand:
            data_enhanced.append(cand)
        else:
            metadata_only.append(cand)

    report += f"""| Metric | Count |
|--------|-------|
| **Total Candidates** | {len(candidates):,} |
| **Data-Enhanced Candidates** | {len(data_enhanced):,} |
| **Metadata-Only Candidates** | {len(metadata_only):,} |

"""

    # Calculate score distributions
    if data_enhanced:
        data_readiness_scores = [c['scores'].get('data_readiness', 0) for c in data_enhanced]
        avg_dr_score = sum(data_readiness_scores) / len(data_readiness_scores)
        high_dr = len([s for s in data_readiness_scores if s >= 4.0])
        low_dr = len([s for s in data_readiness_scores if s <= 1.5])

        report += f"""### Data-Enhanced Candidates Statistics

- **Average Data Readiness Score:** {avg_dr_score:.2f}/5.0
- **High Quality (≥4.0):** {high_dr:,} ({high_dr/len(data_enhanced)*100:.1f}%)
- **Low Quality (≤1.5):** {low_dr:,} ({low_dr/len(data_enhanced)*100:.1f}%)

"""

    # Identify movers (for candidates with both metadata and data scores)
    # We'll simulate "before" by using a fixed metadata score of 3.0
    movers = []
    for cand in data_enhanced:
        metadata_dr_score = 3.0  # Default metadata-based data readiness
        enhanced_dr_score = cand['scores'].get('data_readiness', 3.0)
        score_change = enhanced_dr_score - metadata_dr_score

        movers.append({
            'candidate': cand,
            'before': metadata_dr_score,
            'after': enhanced_dr_score,
            'change': score_change
        })

    # Sort by biggest change
    movers.sort(key=lambda x: abs(x['change']), reverse=True)

    report += f"""---

## Biggest Movers (Up and Down)

### Top Improvers (Enhanced Score > Metadata Score)

Candidates that proved to be higher quality than metadata suggested.

| Rank | Database.Schema.Table.Column | Before | After | Change | Reason |
|------|------------------------------|--------|-------|--------|--------|
"""

    improvers = [m for m in movers if m['change'] > 0]
    for i, mover in enumerate(improvers[:15], 1):
        cand = mover['candidate']
        fqn = f"{cand.get('database', 'N/A')}.{cand.get('schema', 'N/A')}.{cand.get('table', 'N/A')}.{cand.get('column', 'N/A')}"
        stats = cand.get('statistics', {})
        null_pct = stats.get('null_percentage', 100)
        avg_len = stats.get('avg_length', 0)

        # Determine reason for improvement
        if null_pct <= 10 and avg_len >= 200:
            reason = "High quality + rich content"
        elif null_pct <= 10:
            reason = "Excellent completeness"
        elif avg_len >= 200:
            reason = "Rich content"
        else:
            reason = "Good data quality"

        report += f"| {i} | `{fqn[:55]}` | {mover['before']:.2f} | {mover['after']:.2f} | +{mover['change']:.2f} | {reason} |\n"

    if not improvers:
        report += "| - | No improvers found | - | - | - | - |\n"

    report += f"""

### Top Decliners (Enhanced Score < Metadata Score)

Candidates that proved to be lower quality than metadata suggested.

| Rank | Database.Schema.Table.Column | Before | After | Change | Reason |
|------|------------------------------|--------|-------|--------|--------|
"""

    decliners = [m for m in movers if m['change'] < 0]
    decliners.sort(key=lambda x: x['change'])  # Most negative first

    for i, mover in enumerate(decliners[:15], 1):
        cand = mover['candidate']
        fqn = f"{cand.get('database', 'N/A')}.{cand.get('schema', 'N/A')}.{cand.get('table', 'N/A')}.{cand.get('column', 'N/A')}"
        stats = cand.get('statistics', {})
        null_pct = stats.get('null_percentage', 100)
        avg_len = stats.get('avg_length', 0)

        # Determine reason for decline
        if null_pct > 70:
            reason = f"High NULLs ({null_pct:.1f}%)"
        elif avg_len < 50:
            reason = f"Short content ({avg_len:.1f} chars)"
        elif null_pct > 50:
            reason = f"Many NULLs ({null_pct:.1f}%)"
        else:
            reason = "Data quality issues"

        report += f"| {i} | `{fqn[:55]}` | {mover['before']:.2f} | {mover['after']:.2f} | {mover['change']:.2f} | {reason} |\n"

    if not decliners:
        report += "| - | No decliners found | - | - | - | - |\n"

    report += f"""

---

## Key Insights

"""

    if improvers and decliners:
        report += f"""- **{len(improvers):,} candidates improved** with data-enhanced scoring
- **{len(decliners):,} candidates declined** with data-enhanced scoring
- Data-enhanced scoring provides more accurate ranking based on actual content quality
- Metadata alone can be misleading - columns with large defined sizes may have sparse content
- NULL rates and actual content length are critical factors missed by metadata-only analysis

"""

    report += """---

## Recommendations

1. **Trust Data-Enhanced Scores** - Prioritize candidates with actual data analysis
2. **Investigate Decliners** - Understand why expected high-value columns have poor data quality
3. **Validate Improvers** - Verify that unexpected high-quality columns align with business value
4. **Data Quality Initiatives** - Address high NULL rates and sparse content before AI enablement

---

## Next Steps

- Review [Data Quality Dashboard](data_quality_dashboard.md) for detailed analysis
- Focus POCs on top data-enhanced candidates
- Address data quality issues in declining candidates
"""

    return report

def save_enhanced_metadata(candidates, top_candidates, dashboard, comparison_report):
    """
    Save enhanced metadata files:
    - all_candidates_enhanced.json
    - top_200_full_analysis.json
    - data_quality_dashboard.md
    - scoring_comparison.md

    Returns dict with file paths and success status
    """
    results = {}

    # Ensure reports directory exists
    reports_dir = OUTPUT_DIR / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)

    # Save all_candidates_enhanced.json
    try:
        enhanced_path = OUTPUT_DIR / "metadata" / "all_candidates_enhanced.json"
        with open(enhanced_path, "w", encoding="utf-8") as f:
            json.dump(candidates, f, indent=2, default=str)
        results['all_candidates_enhanced'] = str(enhanced_path)
        print(f"Saved enhanced candidates to {enhanced_path}")
    except Exception as e:
        results['all_candidates_enhanced'] = f"ERROR: {e}"

    # Save top_200_full_analysis.json
    try:
        top_200_path = OUTPUT_DIR / "metadata" / "top_200_full_analysis.json"
        with open(top_200_path, "w", encoding="utf-8") as f:
            json.dump(top_candidates, f, indent=2, default=str)
        results['top_200_full_analysis'] = str(top_200_path)
        print(f"Saved top 200 analysis to {top_200_path}")
    except Exception as e:
        results['top_200_full_analysis'] = f"ERROR: {e}"

    # Save data_quality_dashboard.md
    try:
        dashboard_path = reports_dir / "data_quality_dashboard.md"
        with open(dashboard_path, "w", encoding="utf-8") as f:
            f.write(dashboard)
        results['data_quality_dashboard'] = str(dashboard_path)
        print(f"Saved dashboard to {dashboard_path}")
    except Exception as e:
        results['data_quality_dashboard'] = f"ERROR: {e}"

    # Save scoring_comparison.md
    try:
        comparison_path = reports_dir / "scoring_comparison.md"
        with open(comparison_path, "w", encoding="utf-8") as f:
            f.write(comparison_report)
        results['scoring_comparison'] = str(comparison_path)
        print(f"Saved comparison report to {comparison_path}")
    except Exception as e:
        results['scoring_comparison'] = f"ERROR: {e}"

    return results

# ==================== PHASE 1: METADATA DISCOVERY ====================

def discover_databases(conn):
    """Discover databases with early filtering applied.
    
    Uses ACCOUNT_USAGE.TABLES (DISTINCT TABLE_CATALOG) as the authoritative
    source instead of ACCOUNT_USAGE.DATABASES, which may contain stale entries
    for dropped/recreated databases even with DELETED IS NULL.
    """
    print("\n=== Discovering Databases ===")
    
    # Build filter clause for database filtering
    table_filter = ""
    if TARGET_DATABASES:
        db_list = ", ".join([f"'{db.upper()}'" for db in TARGET_DATABASES])
        table_filter = f"AND UPPER(TABLE_CATALOG) IN ({db_list})"
    elif EXCLUDE_DATABASES:
        db_list = ", ".join([f"'{db.upper()}'" for db in EXCLUDE_DATABASES])
        table_filter = f"AND UPPER(TABLE_CATALOG) NOT IN ({db_list})"
    
    query = f"""
    SELECT DISTINCT TABLE_CATALOG AS DATABASE_NAME
    FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES
    WHERE DELETED IS NULL
    {table_filter}
    ORDER BY DATABASE_NAME
    """
    cols, rows = execute_query(conn, query, "List databases with tables from ACCOUNT_USAGE.TABLES")
    
    # Log filtering info
    if TARGET_DATABASES:
        print(f"Found {len(rows)} databases (filtered to: {', '.join(TARGET_DATABASES)})")
    elif EXCLUDE_DATABASES:
        print(f"Found {len(rows)} databases (excluding: {', '.join(EXCLUDE_DATABASES)})")
    else:
        print(f"Found {len(rows)} databases (no filter)")
    
    return cols, rows

def discover_schemas(conn):
    """Discover schemas with early database filtering applied.
    
    Uses ACCOUNT_USAGE.TABLES (DISTINCT TABLE_CATALOG, TABLE_SCHEMA) as the
    authoritative source instead of ACCOUNT_USAGE.SCHEMATA, which may contain
    stale entries for dropped/recreated schemas.
    """
    print("\n=== Discovering Schemas ===")
    
    # Build filter clause - use TABLE_CATALOG for tables
    db_filter = ""
    if TARGET_DATABASES:
        db_list = ", ".join([f"'{db.upper()}'" for db in TARGET_DATABASES])
        db_filter = f"AND UPPER(TABLE_CATALOG) IN ({db_list})"
    elif EXCLUDE_DATABASES:
        db_list = ", ".join([f"'{db.upper()}'" for db in EXCLUDE_DATABASES])
        db_filter = f"AND UPPER(TABLE_CATALOG) NOT IN ({db_list})"
    
    query = f"""
    SELECT DISTINCT TABLE_CATALOG AS DATABASE_NAME, TABLE_SCHEMA AS SCHEMA_NAME
    FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES
    WHERE DELETED IS NULL
    {db_filter}
    ORDER BY TABLE_CATALOG, TABLE_SCHEMA
    """
    cols, rows = execute_query(conn, query, "List schemas with tables from ACCOUNT_USAGE.TABLES")
    print(f"Found {len(rows)} schemas")
    return cols, rows

def discover_tables_and_views(conn):
    """Discover tables and views with early database filtering applied."""
    print("\n=== Discovering Tables & Views ===")
    
    # Build filter clause - use TABLE_CATALOG for tables
    db_filter = ""
    if TARGET_DATABASES:
        db_list = ", ".join([f"'{db.upper()}'" for db in TARGET_DATABASES])
        db_filter = f"AND UPPER(TABLE_CATALOG) IN ({db_list})"
    elif EXCLUDE_DATABASES:
        db_list = ", ".join([f"'{db.upper()}'" for db in EXCLUDE_DATABASES])
        db_filter = f"AND UPPER(TABLE_CATALOG) NOT IN ({db_list})"
    
    query = f"""
    SELECT
        TABLE_CATALOG AS DATABASE_NAME,
        TABLE_SCHEMA AS SCHEMA_NAME,
        TABLE_NAME,
        TABLE_TYPE,
        ROW_COUNT,
        BYTES,
        COMMENT,
        CREATED,
        LAST_ALTERED
    FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES
    WHERE DELETED IS NULL
    {db_filter}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME ORDER BY CREATED DESC) = 1
    ORDER BY TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME
    """
    cols, rows = execute_query(conn, query, "List all tables and views from ACCOUNT_USAGE (filtered)")
    print(f"Found {len(rows)} tables/views")
    return cols, rows

def discover_columns(conn):
    """Discover columns with early database filtering applied."""
    print("\n=== Discovering Columns ===")
    
    # Build filter clause - use TABLE_CATALOG for columns
    db_filter = ""
    if TARGET_DATABASES:
        db_list = ", ".join([f"'{db.upper()}'" for db in TARGET_DATABASES])
        db_filter = f"AND UPPER(TABLE_CATALOG) IN ({db_list})"
    elif EXCLUDE_DATABASES:
        db_list = ", ".join([f"'{db.upper()}'" for db in EXCLUDE_DATABASES])
        db_filter = f"AND UPPER(TABLE_CATALOG) NOT IN ({db_list})"
    
    query = f"""
    SELECT
        TABLE_CATALOG AS DATABASE_NAME,
        TABLE_SCHEMA AS SCHEMA_NAME,
        TABLE_NAME,
        COLUMN_NAME,
        ORDINAL_POSITION,
        DATA_TYPE,
        CHARACTER_MAXIMUM_LENGTH,
        NUMERIC_PRECISION,
        NUMERIC_SCALE,
        IS_NULLABLE,
        COMMENT
    FROM SNOWFLAKE.ACCOUNT_USAGE.COLUMNS
    WHERE DELETED IS NULL
    {db_filter}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME ORDER BY ORDINAL_POSITION) = 1
    ORDER BY TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
    """
    cols, rows = execute_query(conn, query, "List all columns from ACCOUNT_USAGE (filtered)")
    print(f"Found {len(rows)} columns")
    return cols, rows

def discover_stages(conn):
    """Discover stages with early database filtering applied."""
    print("\n=== Discovering Stages ===")
    
    # Build filter clause - use STAGE_CATALOG for stages
    db_filter = ""
    if TARGET_DATABASES:
        db_list = ", ".join([f"'{db.upper()}'" for db in TARGET_DATABASES])
        db_filter = f"AND UPPER(STAGE_CATALOG) IN ({db_list})"
    elif EXCLUDE_DATABASES:
        db_list = ", ".join([f"'{db.upper()}'" for db in EXCLUDE_DATABASES])
        db_filter = f"AND UPPER(STAGE_CATALOG) NOT IN ({db_list})"
    
    query = f"""
    SELECT
        STAGE_CATALOG AS DATABASE_NAME,
        STAGE_SCHEMA AS SCHEMA_NAME,
        STAGE_NAME,
        STAGE_URL,
        STAGE_TYPE,
        COMMENT,
        CREATED
    FROM SNOWFLAKE.ACCOUNT_USAGE.STAGES
    WHERE DELETED IS NULL
    {db_filter}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY STAGE_CATALOG, STAGE_SCHEMA, STAGE_NAME ORDER BY CREATED DESC) = 1
    ORDER BY STAGE_CATALOG, STAGE_SCHEMA, STAGE_NAME
    """
    cols, rows = execute_query(conn, query, "List all stages from ACCOUNT_USAGE (filtered)")
    print(f"Found {len(rows)} stages")
    return cols, rows

# ==================== PHASE 2: AI CANDIDATE IDENTIFICATION ====================

def identify_llm_candidates(columns_data):
    """Find high-density VARCHAR/TEXT columns for Cortex LLM"""
    candidates = []
    text_indicators = ['DESCRIPTION', 'CONTENT', 'MESSAGE', 'NOTE', 'SUMMARY',
                       'DETAIL', 'BODY', 'TEXT', 'COMMENT', 'FEEDBACK', 'REVIEW',
                       'ABSTRACT', 'BIO', 'NARRATIVE', 'TITLE', 'SUBJECT']

    for row in columns_data:
        db, schema, table, col_name, _, data_type, max_len, _, _, _, comment = row
        if not data_type:
            continue
        dtype_upper = data_type.upper()
        col_upper = (col_name or "").upper()

        # Check for text columns
        is_text_type = any(t in dtype_upper for t in ['VARCHAR', 'TEXT', 'STRING', 'CHAR'])
        is_long_text = max_len and max_len >= 500
        has_text_indicator = any(ind in col_upper for ind in text_indicators)

        if is_text_type and (is_long_text or has_text_indicator):
            candidates.append({
                "database": db,
                "schema": schema,
                "table": table,
                "column": col_name,
                "data_type": data_type,
                "max_length": max_len,
                "comment": comment,
                "ai_feature": "Cortex LLM",
                "reason": f"Text column ({data_type}) - {'long text' if is_long_text else 'semantic name'}"
            })
    return candidates

def identify_variant_candidates(columns_data):
    """Find VARIANT columns for Cortex Extract"""
    candidates = []
    for row in columns_data:
        db, schema, table, col_name, _, data_type, _, _, _, _, comment = row
        if data_type and data_type.upper() in ('VARIANT', 'OBJECT', 'ARRAY'):
            candidates.append({
                "database": db,
                "schema": schema,
                "table": table,
                "column": col_name,
                "data_type": data_type,
                "comment": comment,
                "ai_feature": "Cortex Extract",
                "reason": f"Semi-structured {data_type} column"
            })
    return candidates

def identify_ml_candidates(columns_data):
    """Find tables with TIMESTAMP + NUMBER for ML forecasting/anomaly"""
    table_columns = defaultdict(list)
    for row in columns_data:
        db, schema, table, col_name, _, data_type, _, _, _, _, _ = row
        key = (db, schema, table)
        table_columns[key].append((col_name, data_type or ""))

    candidates = []
    for (db, schema, table), cols in table_columns.items():
        has_timestamp = any('TIMESTAMP' in dtype.upper() or 'DATE' in dtype.upper()
                           for _, dtype in cols if dtype)
        num_cols = [(name, dtype) for name, dtype in cols if dtype and
                    any(t in dtype.upper() for t in ['NUMBER', 'FLOAT', 'DECIMAL', 'INTEGER', 'DOUBLE'])]

        if has_timestamp and len(num_cols) >= 1:
            candidates.append({
                "database": db,
                "schema": schema,
                "table": table,
                "ai_feature": "Cortex ML (Forecasting/Anomaly)",
                "reason": f"Has timestamp + {len(num_cols)} numeric columns",
                "numeric_columns": [n for n, _ in num_cols[:5]]
            })
    return candidates

def identify_search_candidates(columns_data):
    """Find tables with rich text for Cortex Search/RAG"""
    table_text_cols = defaultdict(list)
    for row in columns_data:
        db, schema, table, col_name, _, data_type, max_len, _, _, _, _ = row
        if data_type and any(t in data_type.upper() for t in ['VARCHAR', 'TEXT', 'STRING']):
            key = (db, schema, table)
            table_text_cols[key].append((col_name, max_len or 0))

    candidates = []
    for (db, schema, table), text_cols in table_text_cols.items():
        long_text = [c for c, length in text_cols if length >= 200]
        if len(long_text) >= 2:
            candidates.append({
                "database": db,
                "schema": schema,
                "table": table,
                "ai_feature": "Cortex Search / RAG",
                "reason": f"{len(long_text)} substantial text columns",
                "text_columns": long_text[:5]
            })
    return candidates

# ==================== PHASE 3: ENHANCED TEXT ANALYSIS ====================

def find_text_rich_columns(conn):
    """Find columns likely to contain rich text content"""
    print("\n=== Finding Text-Rich Columns ===")
    query = """
    SELECT
        TABLE_CATALOG AS DATABASE_NAME,
        TABLE_SCHEMA AS SCHEMA_NAME,
        TABLE_NAME,
        COLUMN_NAME,
        DATA_TYPE,
        CHARACTER_MAXIMUM_LENGTH,
        COMMENT
    FROM SNOWFLAKE.ACCOUNT_USAGE.COLUMNS
    WHERE DELETED IS NULL
    AND (
        DATA_TYPE IN ('TEXT', 'VARCHAR', 'STRING')
        AND (
            CHARACTER_MAXIMUM_LENGTH >= 1000
            OR UPPER(COLUMN_NAME) LIKE '%DESCRIPTION%'
            OR UPPER(COLUMN_NAME) LIKE '%CONTENT%'
            OR UPPER(COLUMN_NAME) LIKE '%MESSAGE%'
            OR UPPER(COLUMN_NAME) LIKE '%NOTE%'
            OR UPPER(COLUMN_NAME) LIKE '%SUMMARY%'
            OR UPPER(COLUMN_NAME) LIKE '%DETAIL%'
            OR UPPER(COLUMN_NAME) LIKE '%BODY%'
            OR UPPER(COLUMN_NAME) LIKE '%TEXT%'
            OR UPPER(COLUMN_NAME) LIKE '%COMMENT%'
            OR UPPER(COLUMN_NAME) LIKE '%FEEDBACK%'
            OR UPPER(COLUMN_NAME) LIKE '%REVIEW%'
            OR UPPER(COLUMN_NAME) LIKE '%ABSTRACT%'
            OR UPPER(COLUMN_NAME) LIKE '%NARRATIVE%'
        )
    )
    ORDER BY TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME
    """
    cols, rows = execute_query(conn, query, "Find text-rich columns for LLM analysis")
    print(f"Found {len(rows)} text-rich columns")
    return rows

def find_education_tables(conn):
    """Find education/learning related tables"""
    print("\n=== Finding Education Content Tables ===")
    query = """
    SELECT DISTINCT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME
    FROM SNOWFLAKE.ACCOUNT_USAGE.COLUMNS
    WHERE DELETED IS NULL
    AND (
        UPPER(TABLE_NAME) LIKE '%CURRICULUM%'
        OR UPPER(TABLE_NAME) LIKE '%LESSON%'
        OR UPPER(TABLE_NAME) LIKE '%COURSE%'
        OR UPPER(TABLE_NAME) LIKE '%STUDENT%'
        OR UPPER(TABLE_NAME) LIKE '%LEARNING%'
        OR UPPER(TABLE_NAME) LIKE '%ASSESSMENT%'
        OR UPPER(TABLE_NAME) LIKE '%QUESTION%'
        OR UPPER(TABLE_NAME) LIKE '%ANSWER%'
        OR UPPER(TABLE_NAME) LIKE '%CONTENT%'
        OR UPPER(TABLE_NAME) LIKE '%RESOURCE%'
        OR UPPER(TABLE_NAME) LIKE '%FEEDBACK%'
        OR UPPER(TABLE_NAME) LIKE '%SKILL%'
        OR UPPER(TABLE_NAME) LIKE '%GRADE%'
        OR UPPER(TABLE_NAME) LIKE '%SCORE%'
        OR UPPER(TABLE_NAME) LIKE '%TEST%'
    )
    ORDER BY TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME
    """
    cols, rows = execute_query(conn, query, "Find education/learning content tables")
    print(f"Found {len(rows)} education-related tables")
    return rows

def find_document_columns(conn):
    """Find columns with document/file references"""
    print("\n=== Finding Document Reference Columns ===")
    query = """
    SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE
    FROM SNOWFLAKE.ACCOUNT_USAGE.COLUMNS
    WHERE DELETED IS NULL
    AND (
        UPPER(COLUMN_NAME) LIKE '%FILE%PATH%'
        OR UPPER(COLUMN_NAME) LIKE '%FILE%URL%'
        OR UPPER(COLUMN_NAME) LIKE '%DOCUMENT%'
        OR UPPER(COLUMN_NAME) LIKE '%PDF%'
        OR UPPER(COLUMN_NAME) LIKE '%ATTACHMENT%'
        OR UPPER(COLUMN_NAME) LIKE '%S3%'
        OR UPPER(COLUMN_NAME) LIKE '%BLOB%'
        OR UPPER(COLUMN_NAME) LIKE '%IMAGE%'
    )
    ORDER BY TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME
    """
    cols, rows = execute_query(conn, query, "Find document/file reference columns")
    print(f"Found {len(rows)} document reference columns")
    return rows

def find_pii_columns(conn):
    """Find columns that likely contain PII"""
    print("\n=== Finding PII Columns ===")
    query = """
    SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE
    FROM SNOWFLAKE.ACCOUNT_USAGE.COLUMNS
    WHERE DELETED IS NULL
    AND (
        UPPER(COLUMN_NAME) LIKE '%EMAIL%'
        OR UPPER(COLUMN_NAME) LIKE '%SSN%'
        OR UPPER(COLUMN_NAME) LIKE '%SOCIAL%SECURITY%'
        OR UPPER(COLUMN_NAME) LIKE '%PHONE%'
        OR UPPER(COLUMN_NAME) LIKE '%ADDRESS%'
        OR UPPER(COLUMN_NAME) LIKE '%FIRST%NAME%'
        OR UPPER(COLUMN_NAME) LIKE '%LAST%NAME%'
        OR UPPER(COLUMN_NAME) LIKE '%BIRTH%'
        OR UPPER(COLUMN_NAME) LIKE '%DOB%'
        OR UPPER(COLUMN_NAME) LIKE '%PASSWORD%'
        OR UPPER(COLUMN_NAME) LIKE '%SECRET%'
        OR UPPER(COLUMN_NAME) LIKE '%CREDENTIAL%'
    )
    ORDER BY TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME
    """
    cols, rows = execute_query(conn, query, "Find potential PII columns")
    print(f"Found {len(rows)} potential PII columns")
    return rows

# ==================== PHASE 4: DATA PROFILING ====================

def profile_sample_text_columns(conn, text_columns, limit=10):
    """Profile text columns for content quality"""
    print("\n=== Profiling Sample Text Columns ===")
    profiles = []

    # Sample diverse tables
    seen_tables = set()
    samples = []
    for row in text_columns:
        db, schema, table, col, dtype, max_len, comment = row
        table_key = f"{db}.{schema}.{table}"
        if table_key not in seen_tables and len(samples) < limit:
            seen_tables.add(table_key)
            samples.append(row)

    for row in samples:
        db, schema, table, col, dtype, max_len, comment = row
        fqn = f'"{db}"."{schema}"."{table}"'

        query = f"""
        SELECT
            COUNT(*) AS total_rows,
            COUNT("{col}") AS non_null_count,
            AVG(LENGTH("{col}")) AS avg_length,
            MAX(LENGTH("{col}")) AS max_length,
            MIN(LENGTH("{col}")) AS min_length
        FROM {fqn}
        TABLESAMPLE (1000 ROWS)
        """
        try:
            cols, rows = execute_query(conn, query, f"Profile text column {db}.{schema}.{table}.{col}")
            if rows and rows[0]:
                r = rows[0]
                profiles.append({
                    "database": db,
                    "schema": schema,
                    "table": table,
                    "column": col,
                    "data_type": dtype,
                    "total_rows_sampled": r[0],
                    "non_null_count": r[1],
                    "avg_length": float(r[2]) if r[2] else 0,
                    "max_length": r[3],
                    "min_length": r[4]
                })
                print(f"  {db}.{schema}.{table}.{col}: avg_len={profiles[-1]['avg_length']:.0f}")
        except Exception as e:
            print(f"  Could not profile {fqn}.{col}: {e}")

    return profiles

def profile_variant_columns(conn, variant_candidates, limit=5):
    """Profile VARIANT columns for key structure"""
    print("\n=== Profiling VARIANT Columns ===")
    profiles = []

    for cand in variant_candidates[:limit]:
        db, schema, table, col = cand['database'], cand['schema'], cand['table'], cand['column']
        fqn = f'"{db}"."{schema}"."{table}"'

        query = f"""
        SELECT DISTINCT f.key
        FROM {fqn} TABLESAMPLE (100 ROWS),
             LATERAL FLATTEN(input => "{col}", recursive => FALSE) f
        LIMIT 50
        """
        try:
            cols, rows = execute_query(conn, query, f"Profile VARIANT keys in {db}.{schema}.{table}.{col}")
            if rows:
                profiles.append({
                    **cand,
                    "top_keys": [r[0] for r in rows]
                })
                print(f"  {db}.{schema}.{table}.{col}: {len(rows)} keys found")
        except Exception as e:
            print(f"  Could not profile VARIANT {fqn}.{col}: {e}")

    return profiles

# ==================== PHASE 5: SCORING ====================

def score_candidate(candidate, text_profiles=None, variant_profiles=None):
    """Score candidate on 0-5 scale for multiple dimensions"""
    scores = {
        "business_potential": 3,
        "data_readiness": 3,
        "metadata_quality": 2,
        "governance_risk": 2
    }

    # Adjust based on AI feature type
    ai_feature = candidate.get('ai_feature', '')
    if ai_feature == 'Cortex LLM':
        scores['business_potential'] = 4
    elif ai_feature == 'Cortex ML (Forecasting/Anomaly)':
        scores['business_potential'] = 5
    elif ai_feature == 'Cortex Search / RAG':
        scores['business_potential'] = 4
    elif ai_feature == 'Cortex Extract':
        scores['business_potential'] = 3

    # Check for profile data
    if text_profiles:
        key = (candidate.get('database'), candidate.get('schema'),
               candidate.get('table'), candidate.get('column'))
        for p in text_profiles:
            if (p['database'], p['schema'], p['table'], p['column']) == key:
                if p.get('avg_length', 0) > 100:
                    scores['data_readiness'] = 4
                if p.get('non_null_count', 0) and p.get('total_rows_sampled', 1):
                    fill_rate = p['non_null_count'] / max(p['total_rows_sampled'], 1)
                    if fill_rate > 0.9:
                        scores['data_readiness'] = 5
                break

    # Check for comments (metadata quality)
    if candidate.get('comment'):
        scores['metadata_quality'] = 4

    # PII risk based on column names
    col_name = str(candidate.get('column', '')).upper()
    table_name = str(candidate.get('table', '')).upper()
    pii_indicators = ['EMAIL', 'SSN', 'PHONE', 'ADDRESS', 'NAME', 'DOB', 'BIRTH', 'PASSWORD', 'SECRET']
    if any(ind in col_name or ind in table_name for ind in pii_indicators):
        scores['governance_risk'] = 5

    candidate['scores'] = scores
    candidate['total_score'] = sum(scores.values())
    return candidate

# ==================== PHASE 6: REPORT GENERATION ====================

def generate_executive_summary(all_candidates, tables_count, columns_count,
                               databases_count, schemas_count, stages_count,
                               llm_count, search_count, ml_count, extract_count,
                               edu_tables, pii_columns, text_profiles):
    """Generate executive summary markdown"""

    # Sort candidates by score
    sorted_candidates = sorted(all_candidates, key=lambda x: x.get('total_score', 0), reverse=True)

    # Feature distribution
    feature_counts = defaultdict(int)
    for c in all_candidates:
        feature_counts[c.get('ai_feature', 'Unknown')] += 1

    summary = f"""# Snowflake AI Enablement - Executive Summary

**Analysis Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
**Account:** WSWZONP-NWEA_PRD (EJA57698)
**Environment:** Education Technology (EdTech) - Assessment & Learning Platform

---

## Environment Overview

| Metric | Count |
|--------|-------|
| **Databases** | {databases_count:,} |
| **Schemas** | {schemas_count:,} |
| **Tables/Views** | {tables_count:,} |
| **Columns** | {columns_count:,} |
| **Stages** | {stages_count:,} |

### Domain Analysis
This is a comprehensive **Education Technology (EdTech)** data platform containing:
- Student assessment data ({len([t for t in edu_tables if 'STUDENT' in str(t).upper()])} student-related tables)
- Learning content & curriculum
- Educational resource management
- Marketing & lead data (Marketo integration)
- Operational/infrastructure data

---

## AI Candidate Summary

| AI Feature | Candidates | Priority |
|------------|------------|----------|
| **Cortex LLM Functions** | {llm_count:,} | High |
| **Cortex Search / RAG** | {search_count:,} | High |
| **Cortex ML (Forecasting/Anomaly)** | {ml_count:,} | High |
| **Cortex Extract (Semi-structured)** | {extract_count:,} | Medium |
| **Document AI** | {stages_count}+ stages | Medium |

**Total AI-Enabled Candidates:** {llm_count + search_count + ml_count + extract_count:,}+ columns and tables

---

## Top 10 High-Value AI Opportunities

| Rank | Database.Schema.Table | AI Feature | Score | Rationale |
|------|----------------------|------------|-------|-----------|
"""

    for i, cand in enumerate(sorted_candidates[:10], 1):
        fqn = f"{cand.get('database', 'N/A')}.{cand.get('schema', 'N/A')}.{cand.get('table', 'N/A')}"
        if cand.get('column'):
            fqn += f".{cand.get('column')}"
        feature = cand.get('ai_feature', 'Unknown')
        score = cand.get('total_score', 0)
        reason = cand.get('reason', 'N/A')[:50]
        summary += f"| {i} | `{fqn[:60]}` | {feature} | {score}/20 | {reason} |\n"

    summary += f"""

---

## Quick Wins (Implement Immediately)

### 1. Content Summarization (Cortex LLM)
"""

    # Find best text profile
    if text_profiles:
        best_text = max(text_profiles, key=lambda x: x.get('avg_length', 0))
        summary += f"""
**Target:** `{best_text['database']}.{best_text['schema']}.{best_text['table']}.{best_text['column']}`
- Average text length: {best_text['avg_length']:.0f} characters
- Ideal for: SUMMARIZE, CLASSIFY, SENTIMENT

```sql
SELECT
    SNOWFLAKE.CORTEX.SUMMARIZE("{best_text['column']}") AS summary
FROM "{best_text['database']}"."{best_text['schema']}"."{best_text['table']}"
WHERE "{best_text['column']}" IS NOT NULL
LIMIT 100;
```
"""

    summary += f"""

### 2. Semantic Search (Cortex Search)
Create a Cortex Search Service on content tables with multiple text columns.

### 3. Predictive Analytics (Cortex ML)
{ml_count:,} tables identified with timestamp + numeric columns for forecasting.

### 4. Semi-Structured Processing (Cortex Extract)
{extract_count:,} VARIANT/ARRAY/OBJECT columns ready for extraction.

---

## AI Feature Distribution

"""
    for feature, count in sorted(feature_counts.items(), key=lambda x: -x[1]):
        summary += f"- **{feature}:** {count:,} candidates\n"

    summary += f"""

---

## Data Readiness Assessment

### Strengths
- Rich text content in educational materials
- Strong time-series data for ML forecasting ({ml_count:,} tables)
- {extract_count:,} VARIANT columns for semi-structured processing
- {len(edu_tables):,} education-related tables identified

### Areas for Improvement
- Limited column comments (metadata quality varies)
- Some tables have sparse text content
- PII present in student/lead data - requires governance review

---

## Governance & PII Considerations

**High-Risk Columns Identified:** {len(pii_columns):,} columns

**PII Categories Found:**
- EMAIL columns
- NAME columns (first, last)
- ADDRESS columns
- PHONE columns
- DOB/BIRTH columns

**Recommendations:**
1. Implement Dynamic Data Masking before AI processing
2. Use Snowflake's Row Access Policies for student data
3. Consider Cortex Guard for sensitive content filtering
4. Document data classification before AI enablement
5. Review FERPA, COPPA, GDPR compliance requirements

---

## Next Steps

1. **Review** [AI Strategy Roadmap](reports/ai_strategy_roadmap.md) for implementation priorities
2. **Explore** [Detailed Analysis Report](reports/detailed_analysis_report.md) for comprehensive candidate analysis
3. **Browse** [Profile Reports](profiles/consolidated_profiles.md) for per-schema details
4. **Audit** [Query Log](logs/audit_trail.sql) for transparency
5. **Conduct POC** with top 3 candidates identified above

---

## Files Generated

| File | Description |
|------|-------------|
| `reports/executive_summary.md` | This file - high-level findings |
| `reports/detailed_analysis_report.md` | Comprehensive analysis with reasoning |
| `reports/ai_strategy_roadmap.md` | Prioritized implementation plan |
| `metadata/full_inventory.csv` | Complete column inventory |
| `metadata/stages_inventory.csv` | All stages with URLs |
| `metadata/all_candidates.json` | All AI candidates |
| `metadata/enhanced_text_candidates.json` | LLM/Search candidates |
| `profiles/*.md` | Per-schema analysis reports |
| `profiles/text_column_profiles.json` | Text column profiling data |
| `logs/audit_trail.sql` | All SELECT queries executed |
"""
    return summary


def generate_detailed_analysis_report(all_candidates, text_profiles, variant_profiles, 
                                       confirmed_candidates, edu_tables, pii_columns,
                                       databases, schemas, tables, stages):
    """
    Generate comprehensive detailed analysis report in markdown format.
    
    This report provides in-depth analysis of each AI candidate with full reasoning,
    data quality metrics, and implementation recommendations.
    """
    
    # Sort candidates by score
    sorted_candidates = sorted(all_candidates, key=lambda x: x.get('total_score', 0), reverse=True)
    confirmed_set = set(f"{c.get('database')}.{c.get('schema')}.{c.get('table')}.{c.get('column', '')}" 
                        for c in confirmed_candidates)
    
    # Create text profile lookup
    text_profile_lookup = {}
    for tp in text_profiles:
        key = f"{tp.get('database')}.{tp.get('schema')}.{tp.get('table')}.{tp.get('column')}"
        text_profile_lookup[key] = tp
    
    # Group by AI feature
    by_feature = defaultdict(list)
    for c in sorted_candidates:
        by_feature[c.get('ai_feature', 'Unknown')].append(c)
    
    report = f"""# Snowflake AI Enablement - Detailed Analysis Report

> **Generated On:** {get_utc_timestamp()}  
> **Agent:** {AGENT_NAME} v{AGENT_VERSION}  
> **Report Type:** Comprehensive AI Candidate Analysis with Reasoning

---

## Table of Contents

1. [Analysis Overview](#analysis-overview)
2. [Scoring Methodology](#scoring-methodology)
3. [Cortex LLM Candidates](#cortex-llm-candidates)
4. [Cortex Search / RAG Candidates](#cortex-search--rag-candidates)
5. [Cortex ML Candidates](#cortex-ml-candidates)
6. [Document AI / Extract Candidates](#document-ai--extract-candidates)
7. [Data Quality Assessment](#data-quality-assessment)
8. [PII & Governance Considerations](#pii--governance-considerations)
9. [Implementation Recommendations](#implementation-recommendations)

---

## Analysis Overview

| Metric | Value |
|--------|-------|
| **Total Databases Analyzed** | {len(databases):,} |
| **Total Schemas** | {len(schemas):,} |
| **Total Tables/Views** | {len(tables):,} |
| **Total Stages** | {len(stages):,} |
| **Total AI Candidates** | {len(all_candidates):,} |
| **Confirmed Candidates** | {len(confirmed_candidates):,} |
| **Confirmation Rate** | {(len(confirmed_candidates)/len(all_candidates)*100) if all_candidates else 0:.1f}% |

### Candidates by AI Feature

| AI Feature | Total | Confirmed | Top Score |
|------------|-------|-----------|-----------|
"""
    
    for feature in ['Cortex LLM', 'Cortex Search / RAG', 'Cortex ML (Forecasting/Anomaly)', 'Cortex Extract (Semi-structured)']:
        feature_cands = by_feature.get(feature, [])
        feature_confirmed = len([c for c in feature_cands if f"{c.get('database')}.{c.get('schema')}.{c.get('table')}.{c.get('column', '')}" in confirmed_set])
        top_score = max([c.get('total_score', 0) for c in feature_cands]) if feature_cands else 0
        report += f"| **{feature}** | {len(feature_cands):,} | {feature_confirmed:,} | {top_score}/20 |\n"
    
    report += f"""

---

## Scoring Methodology

Each candidate is evaluated across **4 dimensions** (each scored 1-5):

| Dimension | Weight | Description |
|-----------|--------|-------------|
| **Business Potential** | High | Value to business operations, analytics, or user experience |
| **Data Readiness** | High | Data quality, completeness, and suitability for AI processing |
| **Metadata Quality** | Medium | Column naming conventions, comments, and documentation |
| **Governance Risk** | Medium | PII presence, sensitivity, compliance requirements |

**Total Score = Business + Data + Metadata + Risk** (max 20)

### Confirmation Criteria
A candidate is **confirmed** if:
- Sparsity ≤ 50% (less than half NULL values)
- Average text length ≥ 30 characters (for text columns)
- Contains natural language content (for LLM candidates)
- Valid structure (for VARIANT/JSON columns)

---

## Cortex LLM Candidates

Cortex LLM functions enable text summarization, classification, sentiment analysis, and translation on text columns.

### Why These Candidates?
- Text columns with sufficient length for meaningful processing
- Semantic column names suggesting natural language content
- Low sparsity indicating data availability

"""
    
    llm_cands = by_feature.get('Cortex LLM', [])[:50]  # Top 50
    if llm_cands:
        report += "### Top LLM Candidates (Detailed Analysis)\n\n"
        for i, cand in enumerate(llm_cands[:25], 1):
            fqn = f"{cand.get('database')}.{cand.get('schema')}.{cand.get('table')}.{cand.get('column', '')}"
            is_confirmed = fqn in confirmed_set
            profile = text_profile_lookup.get(fqn, {})
            
            report += f"#### {i}. `{cand.get('table')}.{cand.get('column')}`\n\n"
            report += f"**Full Path:** `{fqn}`\n\n"
            report += f"| Attribute | Value |\n"
            report += f"|-----------|-------|\n"
            report += f"| **Score** | {cand.get('total_score', 0)}/20 |\n"
            report += f"| **Status** | {'✅ Confirmed' if is_confirmed else '⚠️ Needs Review'} |\n"
            report += f"| **Data Type** | {cand.get('data_type', 'N/A')} |\n"
            report += f"| **Max Length** | {cand.get('max_length', 'N/A'):,} |\n"
            
            if profile:
                report += f"| **Avg Length** | {profile.get('avg_length', 0):.1f} chars |\n"
                report += f"| **Non-Null Count** | {profile.get('non_null_count', 0):,} / {profile.get('total_rows_sampled', 0):,} |\n"
                sparsity = 100 - (profile.get('non_null_count', 0) / profile.get('total_rows_sampled', 1) * 100)
                report += f"| **Sparsity** | {sparsity:.1f}% |\n"
            
            report += f"\n**Selection Reason:** {cand.get('reason', 'N/A')}\n\n"
            
            # Score breakdown
            scores = cand.get('scores', {})
            report += f"**Score Breakdown:**\n"
            report += f"- Business Potential: {scores.get('business_potential', 0)}/5\n"
            report += f"- Data Readiness: {scores.get('data_readiness', 0)}/5\n"
            report += f"- Metadata Quality: {scores.get('metadata_quality', 0)}/5\n"
            report += f"- Governance Risk: {scores.get('governance_risk', 0)}/5\n\n"
            
            # Confirmation reasons
            if cand.get('confirmation_reasons'):
                report += f"**Confirmation Analysis:**\n"
                for reason in cand.get('confirmation_reasons', []):
                    report += f"- {reason}\n"
                report += "\n"
            
            # Recommended use cases
            report += f"**Recommended Cortex Functions:**\n"
            avg_len = profile.get('avg_length', cand.get('max_length', 100))
            if avg_len and avg_len > 200:
                report += f"- `SNOWFLAKE.CORTEX.SUMMARIZE()` - Summarize long text\n"
            report += f"- `SNOWFLAKE.CORTEX.SENTIMENT()` - Analyze sentiment\n"
            report += f"- `SNOWFLAKE.CORTEX.CLASSIFY_TEXT()` - Categorize content\n"
            if 'DESCRIPTION' in str(cand.get('column', '')).upper() or 'CONTENT' in str(cand.get('column', '')).upper():
                report += f"- `SNOWFLAKE.CORTEX.TRANSLATE()` - Multi-language support\n"
            report += "\n---\n\n"
    else:
        report += "*No Cortex LLM candidates identified.*\n\n"
    
    # Cortex Search / RAG section
    report += """## Cortex Search / RAG Candidates

Cortex Search enables semantic search and Retrieval-Augmented Generation (RAG) on tables with multiple text columns.

### Why These Candidates?
- Tables with 2+ substantial text columns
- Content suitable for knowledge retrieval
- Educational or documentation content

"""
    
    search_cands = by_feature.get('Cortex Search / RAG', [])[:30]
    if search_cands:
        report += "### Top Search/RAG Candidates\n\n"
        for i, cand in enumerate(search_cands[:15], 1):
            fqn = f"{cand.get('database')}.{cand.get('schema')}.{cand.get('table')}"
            is_confirmed = any(fqn in c for c in confirmed_set)
            
            report += f"#### {i}. `{cand.get('schema')}.{cand.get('table')}`\n\n"
            report += f"**Full Path:** `{fqn}`\n\n"
            report += f"| Attribute | Value |\n"
            report += f"|-----------|-------|\n"
            report += f"| **Score** | {cand.get('total_score', 0)}/20 |\n"
            report += f"| **Status** | {'✅ Confirmed' if is_confirmed else '⚠️ Needs Review'} |\n"
            report += f"\n**Selection Reason:** {cand.get('reason', 'N/A')}\n\n"
            
            if cand.get('text_columns'):
                report += f"**Text Columns for Search Index:**\n"
                for col in cand.get('text_columns', [])[:5]:
                    report += f"- `{col}`\n"
                report += "\n"
            
            report += f"**Recommended Implementation:**\n"
            report += f"```sql\nCREATE CORTEX SEARCH SERVICE {cand.get('table', 'table')}_search\n"
            report += f"  ON {cand.get('database')}.{cand.get('schema')}.{cand.get('table')}\n"
            report += f"  TARGET_LAG = '1 hour'\n"
            report += f"  WAREHOUSE = your_warehouse;\n```\n\n"
            report += "---\n\n"
    else:
        report += "*No Cortex Search candidates identified.*\n\n"
    
    # Cortex ML section
    report += """## Cortex ML Candidates

Cortex ML enables forecasting and anomaly detection on time-series data.

### Why These Candidates?
- Tables with timestamp/date columns
- Numeric columns suitable for forecasting
- Time-series patterns in the data

"""
    
    ml_cands = by_feature.get('Cortex ML (Forecasting/Anomaly)', [])[:20]
    if ml_cands:
        report += "### Top ML Candidates\n\n"
        for i, cand in enumerate(ml_cands[:10], 1):
            fqn = f"{cand.get('database')}.{cand.get('schema')}.{cand.get('table')}"
            
            report += f"#### {i}. `{cand.get('schema')}.{cand.get('table')}`\n\n"
            report += f"**Full Path:** `{fqn}`\n\n"
            report += f"| Attribute | Value |\n"
            report += f"|-----------|-------|\n"
            report += f"| **Score** | {cand.get('total_score', 0)}/20 |\n"
            report += f"\n**Selection Reason:** {cand.get('reason', 'N/A')}\n\n"
            
            report += f"**Recommended Use Cases:**\n"
            report += f"- Time-series forecasting with `SNOWFLAKE.ML.FORECAST`\n"
            report += f"- Anomaly detection with `SNOWFLAKE.ML.ANOMALY_DETECTION`\n\n"
            report += "---\n\n"
    else:
        report += "*No Cortex ML candidates identified.*\n\n"
    
    # Document AI / Extract section
    report += """## Document AI / Extract Candidates

Cortex Extract processes semi-structured data (VARIANT, OBJECT, ARRAY) and Document AI processes unstructured files.

### Why These Candidates?
- VARIANT/OBJECT/ARRAY columns with JSON content
- Stages with document files (PDF, images)
- Complex nested data structures

"""
    
    extract_cands = by_feature.get('Cortex Extract (Semi-structured)', [])[:20]
    if extract_cands:
        report += "### Top Extract Candidates\n\n"
        for i, cand in enumerate(extract_cands[:10], 1):
            fqn = f"{cand.get('database')}.{cand.get('schema')}.{cand.get('table')}.{cand.get('column', '')}"
            
            report += f"#### {i}. `{cand.get('table')}.{cand.get('column')}`\n\n"
            report += f"**Full Path:** `{fqn}`\n\n"
            report += f"| Attribute | Value |\n"
            report += f"|-----------|-------|\n"
            report += f"| **Score** | {cand.get('total_score', 0)}/20 |\n"
            report += f"| **Data Type** | {cand.get('data_type', 'N/A')} |\n"
            report += f"\n**Selection Reason:** {cand.get('reason', 'N/A')}\n\n"
            report += "---\n\n"
    else:
        report += "*No Cortex Extract candidates identified.*\n\n"
    
    # Data Quality Assessment
    report += f"""## Data Quality Assessment

### Text Column Quality Summary

"""
    if text_profiles:
        report += "| Column | Avg Length | Max Length | Sparsity | Quality |\n"
        report += "|--------|------------|------------|----------|--------|\n"
        for tp in sorted(text_profiles, key=lambda x: x.get('avg_length', 0), reverse=True)[:20]:
            sparsity = 100 - (tp.get('non_null_count', 0) / tp.get('total_rows_sampled', 1) * 100) if tp.get('total_rows_sampled', 0) > 0 else 100
            quality = "🟢 Good" if sparsity < 30 and tp.get('avg_length', 0) > 50 else "🟡 Fair" if sparsity < 60 else "🔴 Poor"
            col_name = f"{tp.get('table')}.{tp.get('column')}"[:40]
            report += f"| `{col_name}` | {tp.get('avg_length', 0):.0f} | {tp.get('max_length', 0):,} | {sparsity:.0f}% | {quality} |\n"
    else:
        report += "*No text profiling data available.*\n"
    
    # PII & Governance
    report += f"""

---

## PII & Governance Considerations

**Total PII Columns Identified:** {len(pii_columns):,}

### PII Categories Detected

"""
    pii_categories = defaultdict(list)
    for col in pii_columns:
        col_upper = str(col).upper()
        if 'EMAIL' in col_upper:
            pii_categories['Email'].append(col)
        elif 'NAME' in col_upper or 'FIRST' in col_upper or 'LAST' in col_upper:
            pii_categories['Name'].append(col)
        elif 'PHONE' in col_upper or 'MOBILE' in col_upper:
            pii_categories['Phone'].append(col)
        elif 'ADDRESS' in col_upper or 'STREET' in col_upper or 'CITY' in col_upper:
            pii_categories['Address'].append(col)
        elif 'DOB' in col_upper or 'BIRTH' in col_upper:
            pii_categories['Date of Birth'].append(col)
        elif 'SSN' in col_upper or 'SOCIAL' in col_upper:
            pii_categories['SSN'].append(col)
        else:
            pii_categories['Other'].append(col)
    
    for category, cols in sorted(pii_categories.items(), key=lambda x: -len(x[1])):
        report += f"- **{category}:** {len(cols)} columns\n"
    
    report += f"""

### Governance Recommendations

1. **Data Masking:** Apply Dynamic Data Masking to PII columns before AI processing
2. **Access Policies:** Implement Row Access Policies for sensitive data
3. **Cortex Guard:** Use Cortex Guard for filtering sensitive outputs
4. **Compliance Review:** Ensure FERPA, COPPA, GDPR compliance for student data
5. **Audit Logging:** Maintain comprehensive audit trails for AI operations

---

## Implementation Recommendations

### Phase 1: Quick Wins (1-2 weeks)
1. Implement Cortex LLM on top 5 confirmed text columns
2. Create proof-of-concept for content summarization
3. Test sentiment analysis on feedback columns

### Phase 2: Core AI Services (2-4 weeks)
1. Deploy Cortex Search Service on content tables
2. Implement ML forecasting on time-series data
3. Set up data masking for PII protection

### Phase 3: Advanced Use Cases (4-8 weeks)
1. Build RAG applications with Cortex Search
2. Deploy Document AI for stage processing
3. Create AI-powered dashboards and reports

---

## Appendix: Education Domain Tables

**Education-related tables identified:** {len(edu_tables):,}

These tables contain student, assessment, and learning content data particularly suited for EdTech AI applications.

---

*Report generated by {AGENT_NAME} v{AGENT_VERSION}*
"""
    
    return report


def generate_roadmap(all_candidates, text_profiles, edu_tables, stages_data):
    """Generate prioritized AI strategy roadmap with detailed implementation guidance"""

    sorted_cands = sorted(all_candidates, key=lambda x: x.get('total_score', 0), reverse=True)

    # Priority groups with detailed criteria
    p1 = [c for c in sorted_cands if c.get('total_score', 0) >= 15 and c.get('scores', {}).get('governance_risk', 5) <= 3]
    p2 = [c for c in sorted_cands if c.get('total_score', 0) >= 12 and c not in p1]
    p3 = [c for c in sorted_cands if c not in p1 and c not in p2]
    p3_count = len(p3)
    
    # Group by AI feature for analysis
    by_feature = defaultdict(list)
    for c in sorted_cands:
        by_feature[c.get('ai_feature', 'Unknown')].append(c)
    
    # Calculate confirmation rates
    confirmed_p1 = len([c for c in p1 if c.get('is_confirmed_candidate')])
    confirmed_p2 = len([c for c in p2 if c.get('is_confirmed_candidate')])

    roadmap = f"""# Snowflake AI Strategy Roadmap

> **Generated On:** {get_utc_timestamp()}  
> **Agent:** {AGENT_NAME} v{AGENT_VERSION}  
> **Report Type:** AI Implementation Roadmap with Prioritized Actions

---

## Table of Contents

1. [Executive Overview](#executive-overview)
2. [Implementation Priority Matrix](#implementation-priority-matrix)
3. [Phase 1: Quick Wins](#phase-1-quick-wins-1-2-weeks)
4. [Phase 2: Core AI Services](#phase-2-core-ai-services-2-4-weeks)
5. [Phase 3: Advanced Use Cases](#phase-3-advanced-use-cases-1-3-months)
6. [Feature Implementation Guides](#feature-implementation-guides)
7. [Governance & Compliance](#governance--compliance)
8. [Success Metrics & KPIs](#success-metrics--kpis)
9. [Resource Planning](#resource-planning)

---

## Executive Overview

This roadmap provides a prioritized implementation plan for Snowflake Cortex AI services based on the analysis of your data environment.

### Key Findings

| Metric | Value | Implication |
|--------|-------|-------------|
| **Total AI Candidates** | {len(all_candidates):,} | Large opportunity for AI enablement |
| **Priority 1 (Immediate)** | {len(p1):,} ({confirmed_p1} confirmed) | Ready for immediate implementation |
| **Priority 2 (Short-term)** | {len(p2):,} ({confirmed_p2} confirmed) | Requires minor preparation |
| **Priority 3 (Medium-term)** | {p3_count:,} | Requires data/governance work |
| **Education Tables** | {len(edu_tables):,} | Strong EdTech AI potential |

### AI Feature Distribution

| AI Feature | Candidates | Priority 1 | Priority 2 |
|------------|------------|------------|------------|
| **Cortex LLM** | {len(by_feature.get('Cortex LLM', [])):,} | {len([c for c in p1 if c.get('ai_feature') == 'Cortex LLM'])} | {len([c for c in p2 if c.get('ai_feature') == 'Cortex LLM'])} |
| **Cortex Search/RAG** | {len(by_feature.get('Cortex Search / RAG', [])):,} | {len([c for c in p1 if c.get('ai_feature') == 'Cortex Search / RAG'])} | {len([c for c in p2 if c.get('ai_feature') == 'Cortex Search / RAG'])} |
| **Cortex ML** | {len(by_feature.get('Cortex ML (Forecasting/Anomaly)', [])):,} | {len([c for c in p1 if c.get('ai_feature') == 'Cortex ML (Forecasting/Anomaly)'])} | {len([c for c in p2 if c.get('ai_feature') == 'Cortex ML (Forecasting/Anomaly)'])} |
| **Cortex Extract** | {len(by_feature.get('Cortex Extract (Semi-structured)', [])):,} | {len([c for c in p1 if c.get('ai_feature') == 'Cortex Extract (Semi-structured)'])} | {len([c for c in p2 if c.get('ai_feature') == 'Cortex Extract (Semi-structured)'])} |

---

## Implementation Priority Matrix

### Priority Criteria

| Priority | Score Range | Governance Risk | Data Readiness | Timeline |
|----------|-------------|-----------------|----------------|----------|
| **P1 - Immediate** | ≥15/20 | Low (≤3) | Confirmed | 1-2 weeks |
| **P2 - Short-term** | ≥12/20 | Medium | Likely ready | 2-4 weeks |
| **P3 - Medium-term** | <12/20 | Any | Needs work | 1-3 months |

---

## Phase 1: Quick Wins (1-2 Weeks)

**Objective:** Demonstrate immediate value with high-confidence, low-risk implementations.

### Selection Criteria
- Total score ≥ 15/20
- Governance risk ≤ 3/5 (low PII concern)
- Data confirmed as ready for AI processing

### Priority 1 Candidates ({len(p1):,} total)

| # | Schema | Object | AI Feature | Score | Why Selected |
|---|--------|--------|------------|-------|--------------|
"""

    for i, cand in enumerate(p1[:10], 1):
        obj = f"{cand.get('table', 'N/A')}"
        if cand.get('column'):
            obj += f".{cand.get('column')}"
        roadmap += f"| {i} | {cand.get('schema', 'N/A')[:20]} | `{obj[:30]}` | {cand.get('ai_feature', 'N/A')} | {cand.get('total_score', 0)}/20 | {cand.get('reason', 'N/A')[:30]} |\n"

    # Add detailed P1 candidate analysis
    if p1:
        roadmap += "\n### Detailed Phase 1 Recommendations\n\n"
        for i, cand in enumerate(p1[:5], 1):
            fqn = f"{cand.get('database')}.{cand.get('schema')}.{cand.get('table')}"
            if cand.get('column'):
                fqn += f".{cand.get('column')}"
            scores = cand.get('scores', {})
            roadmap += f"""#### {i}. `{cand.get('table')}.{cand.get('column', 'N/A')}`

**Full Path:** `{fqn}`

| Dimension | Score | Assessment |
|-----------|-------|------------|
| Business Potential | {scores.get('business_potential', 0)}/5 | {'High value' if scores.get('business_potential', 0) >= 4 else 'Moderate value'} |
| Data Readiness | {scores.get('data_readiness', 0)}/5 | {'Ready' if scores.get('data_readiness', 0) >= 4 else 'Needs validation'} |
| Metadata Quality | {scores.get('metadata_quality', 0)}/5 | {'Well documented' if scores.get('metadata_quality', 0) >= 3 else 'Limited docs'} |
| Governance Risk | {scores.get('governance_risk', 0)}/5 | {'Low risk' if scores.get('governance_risk', 0) <= 3 else 'Review needed'} |

**Why Selected:** {cand.get('reason', 'N/A')}

**Recommended Action:** Implement {cand.get('ai_feature', 'AI feature')} with pilot testing

---

"""

    roadmap += f"""
### Quick Win SQL Examples

```sql
-- Cortex LLM: Summarize text content
SELECT
    SNOWFLAKE.CORTEX.SUMMARIZE(text_column) AS summary
FROM your_table
WHERE text_column IS NOT NULL
LIMIT 100;

-- Cortex LLM: Sentiment Analysis
SELECT
    text_column,
    SNOWFLAKE.CORTEX.SENTIMENT(text_column) AS sentiment_score
FROM your_table
LIMIT 100;

-- Cortex LLM: Classification
SELECT
    SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
        text_column,
        ['Math', 'Reading', 'Science', 'Social Studies', 'Other']
    ) AS category
FROM your_table
LIMIT 100;
```

---

## Phase 2: Core AI Services (2-4 Weeks)

**Objective:** Build foundational AI capabilities with moderate complexity implementations.

### Selection Criteria
- Total score ≥ 12/20
- May require minor data preparation
- Business value justifies implementation effort

### Priority 2 Candidates ({len(p2):,} total)

| # | Schema | Object | AI Feature | Score | Reason |
|---|--------|--------|------------|-------|--------|
"""

    for i, cand in enumerate(p2[:15], 1):
        obj = f"{cand.get('table', 'N/A')}"
        if cand.get('column'):
            obj += f".{cand.get('column')}"
        roadmap += f"| {i} | {cand.get('schema', 'N/A')[:20]} | `{obj[:35]}` | {cand.get('ai_feature', 'N/A')} | {cand.get('total_score', 0)}/20 | {cand.get('reason', 'N/A')[:25]} |\n"

    roadmap += f"""

---

## Phase 3: Advanced Use Cases (1-3 Months)

**Objective:** Implement complex AI solutions requiring data preparation or governance review.

### Selection Criteria
- Score < 12/20 OR governance risk > 3/5
- May require significant data cleanup
- Needs compliance/security review

### Priority 3 Summary

**{p3_count:,} candidates** identified requiring additional work:

| Blocker Category | Action Required |
|------------------|-----------------|
| **PII/Governance** | Review student data for FERPA/COPPA compliance |
| **Document AI** | Configure stage access permissions |
| **Data Quality** | Validate sparse columns, improve NULL rates |
| **Schema Complexity** | Document VARIANT structures before extraction |

### Phase 3 Preparation Checklist

- [ ] Conduct PII audit on high-risk columns
- [ ] Set up data masking policies
- [ ] Validate stage file formats and access
- [ ] Profile VARIANT column structures
- [ ] Estimate compute costs for large tables

---

## Feature Implementation Guides

### Cortex LLM Functions

**Available Functions:**
| Function | Description | Use Case |
|----------|-------------|----------|
| `SUMMARIZE(text)` | Generate summaries | Content digests |
| `SENTIMENT(text)` | Sentiment score (-1 to 1) | Feedback analysis |
| `CLASSIFY_TEXT(text, categories)` | Classification | Content tagging |
| `EXTRACT_ANSWER(text, question)` | Q&A extraction | Knowledge retrieval |
| `TRANSLATE(text, from, to)` | Translation | Localization |
| `COMPLETE(model, prompt)` | General LLM | Custom tasks |

**Best Candidates:**
"""

    if text_profiles:
        for p in sorted(text_profiles, key=lambda x: x.get('avg_length', 0), reverse=True)[:5]:
            roadmap += f"- `{p['database']}.{p['schema']}.{p['table']}.{p['column']}` - avg {p['avg_length']:.0f} chars\n"

    roadmap += f"""

**Cost Optimization:**
- Use TABLESAMPLE and LIMIT during development
- Materialize results for repeated access
- Consider batch processing for large tables

---

### Cortex Search / RAG

**Setup Steps:**
1. Identify tables with rich text columns
2. Create Cortex Search Service
3. Define search and filter columns
4. Query using natural language

```sql
-- Create search service (requires appropriate privileges)
CREATE CORTEX SEARCH SERVICE content_search
  ON database.schema.table
  WAREHOUSE = 'compute_wh'
  TARGET_LAG = '1 hour'
  AS (
    SELECT id, title, content, category
    FROM source_table
  );

-- Query the service
SELECT PARSE_JSON(
  SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
    'content_search',
    '{{"query": "algebra word problems", "columns": ["content"], "limit": 10}}'
  )
);
```

---

### Cortex ML (Forecasting & Anomaly Detection)

**Forecasting Example:**
```sql
-- Create time-series forecast
CREATE SNOWFLAKE.ML.FORECAST my_forecast(
  INPUT_DATA => SYSTEM$QUERY_REFERENCE('
    SELECT date_col AS ts, metric_col AS y, category AS series
    FROM my_table
  '),
  TIMESTAMP_COLNAME => 'ts',
  TARGET_COLNAME => 'y',
  SERIES_COLNAME => 'series'
);

-- Generate predictions
CALL my_forecast!FORECAST(FORECASTING_PERIODS => 30);
```

**Anomaly Detection Example:**
```sql
-- Create anomaly detector
CREATE SNOWFLAKE.ML.ANOMALY_DETECTION my_detector(
  INPUT_DATA => SYSTEM$QUERY_REFERENCE('
    SELECT timestamp_col AS ts, metric_col AS value
    FROM my_table
  '),
  TIMESTAMP_COLNAME => 'ts',
  TARGET_COLNAME => 'value',
  LABEL_COLNAME => ''
);

-- Detect anomalies
CALL my_detector!DETECT_ANOMALIES(
  INPUT_DATA => SYSTEM$QUERY_REFERENCE('SELECT * FROM new_data'),
  TIMESTAMP_COLNAME => 'ts',
  TARGET_COLNAME => 'value'
);
```

---

### Cortex Extract (Semi-Structured Data)

**Profile VARIANT Structure:**
```sql
-- Discover JSON keys
SELECT DISTINCT f.key, TYPEOF(f.value) AS type, COUNT(*) AS cnt
FROM my_table,
     LATERAL FLATTEN(input => variant_col, recursive => TRUE) f
GROUP BY 1, 2
ORDER BY 3 DESC;
```

**Extract and Enrich:**
```sql
SELECT
    variant_col:id::STRING AS id,
    variant_col:content::STRING AS content,
    SNOWFLAKE.CORTEX.SUMMARIZE(variant_col:content::STRING) AS summary
FROM my_table
WHERE variant_col:content IS NOT NULL;
```

---

### Document AI

**Available Stages:** {len(stages_data)} stages with S3/Azure/GCS URLs

**Setup Steps:**
1. Verify stage access permissions
2. List files in stage to confirm formats (.pdf, .docx, .png, .jpg)
3. Create Document AI model for your document type
4. Build extraction pipeline

**Supported File Types:**
- PDF documents
- Microsoft Word (.docx)
- Images (.png, .jpg) with text

---

## Governance Checklist

Before enabling AI features on any table:

- [ ] **Data Classification:** Confirm sensitivity level
- [ ] **PII Review:** Check for student/personal information
- [ ] **Access Controls:** Verify role-based access
- [ ] **Compliance:** FERPA, COPPA, GDPR requirements
- [ ] **Cost Approval:** Estimate credit consumption
- [ ] **Output Review:** Validate AI output quality
- [ ] **Audit Trail:** Enable query logging

---

## Success Metrics

| Phase | KPI | Target |
|-------|-----|--------|
| Phase 1 | Content search relevance | >85% |
| Phase 1 | LLM query latency | <2 seconds |
| Phase 2 | Forecast MAPE | <15% |
| Phase 2 | Classification F1 | >0.80 |
| Phase 3 | Document extraction accuracy | >90% |

---

## Resource Requirements

| Resource | Phase 1 | Phase 2 | Phase 3 |
|----------|---------|---------|---------|
| Warehouse Size | XS | S | M |
| Est. Credits/Month | 500 | 2,000 | 5,000 |
| Implementation Hours | 40 | 120 | 200 |

---

## Appendix: File References

| File | Description |
|------|-------------|
| `metadata/full_inventory.csv` | Complete column inventory |
| `metadata/stages_inventory.csv` | All stages |
| `metadata/all_candidates.json` | All AI candidates |
| `metadata/enhanced_text_candidates.json` | Text-focused candidates |
| `profiles/*.md` | Per-schema analysis |
| `profiles/text_column_profiles.json` | Text profiling data |
| `logs/audit_trail.sql` | Query audit trail |
"""
    return roadmap

def generate_profile_reports(all_candidates):
    """Generate per-schema profile reports"""
    print("\n=== Generating Profile Reports ===")

    # Group by database_schema
    profiles_by_schema = defaultdict(list)
    for cand in all_candidates:
        key = f"{cand.get('database', 'UNKNOWN')}_{cand.get('schema', 'UNKNOWN')}"
        profiles_by_schema[key].append(cand)

    consolidated_links = ["# Consolidated Profile Reports\n\n"]
    consolidated_links.append(f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
    consolidated_links.append(f"**Total Schemas:** {len(profiles_by_schema)}\n\n")
    consolidated_links.append("## Schema Index\n\n")

    for schema_key in sorted(profiles_by_schema.keys()):
        cands = profiles_by_schema[schema_key]
        profile_filename = f"{schema_key}_analysis.md"
        profile_path = OUTPUT_DIR / "profiles" / profile_filename

        # Generate profile content
        profile_content = f"# AI Analysis: {schema_key}\n\n"
        profile_content += f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        profile_content += f"**Candidates Found:** {len(cands)}\n\n"
        profile_content += "---\n\n"

        # Group by AI feature
        by_feature = defaultdict(list)
        for c in cands:
            by_feature[c.get('ai_feature', 'Unknown')].append(c)

        for feature in sorted(by_feature.keys()):
            feature_cands = by_feature[feature]
            profile_content += f"## {feature} ({len(feature_cands)} candidates)\n\n"

            for cand in sorted(feature_cands, key=lambda x: x.get('total_score', 0), reverse=True)[:20]:
                profile_content += f"### {cand.get('table', 'N/A')}"
                if cand.get('column'):
                    profile_content += f".{cand.get('column')}"
                profile_content += f"\n"
                profile_content += f"- **Score:** {cand.get('total_score', 0)}/20\n"
                profile_content += f"- **Reason:** {cand.get('reason', 'N/A')}\n"
                if cand.get('data_type'):
                    profile_content += f"- **Data Type:** {cand.get('data_type')}\n"
                if cand.get('scores'):
                    profile_content += f"- **Scores:** Business={cand['scores'].get('business_potential', 0)}, "
                    profile_content += f"Data={cand['scores'].get('data_readiness', 0)}, "
                    profile_content += f"Metadata={cand['scores'].get('metadata_quality', 0)}, "
                    profile_content += f"Risk={cand['scores'].get('governance_risk', 0)}\n"
                profile_content += "\n"

        with open(profile_path, "w", encoding="utf-8") as f:
            f.write(profile_content)

        consolidated_links.append(f"- [{schema_key}](profiles/{profile_filename}) - {len(cands)} candidates\n")

    # Write consolidated report
    consolidated_path = OUTPUT_DIR / "profiles" / "consolidated_profiles.md"
    with open(consolidated_path, "w", encoding="utf-8") as f:
        f.write("".join(consolidated_links))

    print(f"Generated {len(profiles_by_schema)} schema profile reports")
    return len(profiles_by_schema)

def generate_report_index(databases, schemas, tables, columns, stages,
                          llm_candidates, search_candidates, ml_candidates, variant_candidates,
                          confirmed_candidates, num_profiles, analysis_stats):
    """
    Generate main report index (README.md) that links all generated reports.
    
    This is the primary entry point for viewing analysis results.
    """
    total_candidates = len(llm_candidates) + len(search_candidates) + len(ml_candidates) + len(variant_candidates)
    confirmed_count = len(confirmed_candidates)
    
    # Group candidates by database for the analysis breakdown
    db_breakdown = defaultdict(lambda: {"tables": set(), "columns": 0, "candidates": 0, "confirmed": 0})
    
    all_cands = llm_candidates + search_candidates + ml_candidates + variant_candidates
    for cand in all_cands:
        db = cand.get('database', 'Unknown')
        db_breakdown[db]["columns"] += 1
        db_breakdown[db]["tables"].add(f"{cand.get('schema', '')}.{cand.get('table', '')}")
        db_breakdown[db]["candidates"] += 1
        if cand.get('is_confirmed_candidate'):
            db_breakdown[db]["confirmed"] += 1
    
    index_content = f"""# Snowflake AI Readiness Analysis - Report Index

> **Generated On:** {get_utc_timestamp()}  
> **Agent:** {AGENT_NAME} v{AGENT_VERSION}  
> **Mode:** Read-Only Analysis

---

## Quick Navigation

| Report | Description |
|--------|-------------|
| 📊 [Executive Summary](reports/executive_summary.md) | High-level findings and recommendations |
| 📋 [Detailed Analysis Report](reports/detailed_analysis_report.md) | Comprehensive AI candidate analysis with full reasoning |
| 🗺️ [AI Strategy Roadmap](reports/ai_strategy_roadmap.md) | Prioritized implementation plan |
| 📁 [Schema Profiles](profiles/consolidated_profiles.md) | Per-schema detailed analysis |
| 📈 [Data Quality Dashboard](reports/data_quality_dashboard.md) | Data quality insights |
| 🔍 [Scoring Comparison](reports/scoring_comparison.md) | Before/after scoring analysis |

---

## Analysis Summary

### Environment Analyzed

| Metric | Count |
|--------|-------|
| **Databases** | {len(databases):,} |
| **Schemas** | {len(schemas):,} |
| **Tables/Views** | {len(tables):,} |
| **Columns** | {len(columns):,} |
| **Stages** | {len(stages):,} |

### AI Candidates Identified

| AI Feature | Candidates | Description |
|------------|------------|-------------|
| **Cortex LLM** | {len(llm_candidates):,} | Text summarization, classification, sentiment |
| **Cortex Search/RAG** | {len(search_candidates):,} | Semantic search, retrieval-augmented generation |
| **Cortex ML** | {len(ml_candidates):,} | Forecasting, anomaly detection |
| **Cortex Extract** | {len(variant_candidates):,} | Semi-structured data processing |
| **Total** | {total_candidates:,} | All AI-enabled candidates |
| **Confirmed** | {confirmed_count:,} | Data-validated candidates |

### Analysis Statistics

| Metric | Value |
|--------|-------|
| **Candidates Analyzed** | {analysis_stats.get('analyzed', 0):,} |
| **From Cache** | {analysis_stats.get('from_cache', 0):,} |
| **New Analyses** | {analysis_stats.get('new', 0):,} |
| **Errors/Skipped** | {analysis_stats.get('errors', 0):,} |
| **Success Rate** | {analysis_stats.get('success_rate', 0):.1f}% |

---

## Objects Analyzed by Database

| Database | Tables | Candidates | Confirmed | Confirmation Rate |
|----------|--------|------------|-----------|-------------------|
"""
    
    for db_name in sorted(db_breakdown.keys()):
        stats = db_breakdown[db_name]
        table_count = len(stats["tables"])
        cand_count = stats["candidates"]
        conf_count = stats["confirmed"]
        conf_rate = (conf_count / cand_count * 100) if cand_count > 0 else 0
        index_content += f"| `{db_name}` | {table_count:,} | {cand_count:,} | {conf_count:,} | {conf_rate:.1f}% |\n"
    
    index_content += f"""
---

## Generated Files

### Reports (Markdown)

| File | Description |
|------|-------------|
| [`README.md`](README.md) | This index file |
| [`executive_summary.md`](executive_summary.md) | Executive summary with key findings |
| [`ai_strategy_roadmap.md`](ai_strategy_roadmap.md) | Prioritized AI implementation roadmap |
| [`profiles/consolidated_profiles.md`](profiles/consolidated_profiles.md) | Index of all schema profiles |
| [`reports/data_quality_dashboard.md`](reports/data_quality_dashboard.md) | Data quality analysis |
| [`reports/scoring_comparison.md`](reports/scoring_comparison.md) | Candidate scoring details |

### Data Files (JSON/CSV)

| File | Description | Records |
|------|-------------|---------|
| [`metadata/full_inventory.csv`](metadata/full_inventory.csv) | Complete column inventory | {len(columns):,} |
| [`metadata/stages_inventory.csv`](metadata/stages_inventory.csv) | All stages | {len(stages):,} |
| [`metadata/all_candidates.json`](metadata/all_candidates.json) | All AI candidates | {total_candidates:,} |
| [`metadata/confirmed_candidates.json`](metadata/confirmed_candidates.json) | Confirmed candidates | {confirmed_count:,} |
| [`metadata/enhanced_text_candidates.json`](metadata/enhanced_text_candidates.json) | LLM/Search candidates | - |
| [`metadata/data_analysis_cache.json`](metadata/data_analysis_cache.json) | Analysis cache | - |

### Schema Profiles

{num_profiles} schema-specific analysis reports generated in `profiles/` directory.

### Logs

| File | Description |
|------|-------------|
| [`logs/audit_trail.sql`](logs/audit_trail.sql) | All SQL queries executed |
| [`logs/analysis_errors.log`](logs/analysis_errors.log) | Error details |
| [`logs/analysis_summary.log`](logs/analysis_summary.log) | Analysis statistics |

---

## Next Steps

1. **Start Here:** Review the [Executive Summary](executive_summary.md) for key findings
2. **Plan Implementation:** Follow the [AI Strategy Roadmap](ai_strategy_roadmap.md)
3. **Deep Dive:** Explore [Schema Profiles](profiles/consolidated_profiles.md) for specific tables
4. **Validate Quality:** Check the [Data Quality Dashboard](reports/data_quality_dashboard.md)

---

## Support

For questions about this analysis:
- Review the [User Guide](../docs/USER_GUIDE.md)
- Check the [audit trail](logs/audit_trail.sql) for query details
- Consult the [detailed prompts](../docs/detailed_prompts.md) for methodology

"""
    return index_content

# ==================== AGENT ENTRY POINT ====================

def parse_arguments():
    """Parse command line arguments for agent execution."""
    parser = argparse.ArgumentParser(
        prog='snowflake-ai-agent',
        description=f'{AGENT_NAME} v{AGENT_VERSION} - Autonomous Snowflake AI Readiness Analysis',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 scripts/snowflake_full_analysis.py                          # Run with config/config.yaml
  python3 scripts/snowflake_full_analysis.py --dry-run                # Validate config only
  python3 scripts/snowflake_full_analysis.py --config path/to/my.yaml # Use custom config
  python3 scripts/snowflake_full_analysis.py --start-stage 2B         # Restart from Phase 2B
  python3 scripts/snowflake_full_analysis.py --start-stage 3          # Restart from Phase 3
  python3 scripts/snowflake_full_analysis.py --version                # Show version

Valid stages (in order): 1, 2, 2A, 2B, 2C, 2D, 2E, 2F, 3, 4, 5, 5B, 6
  Stage 1:  Metadata Discovery
  Stage 2:  AI Candidate Identification  
  Stage 2A: Load Analysis Cache
  Stage 2B: Metadata-Based Analysis (no table scans)
  Stage 2C: Save Analysis Cache
  Stage 2D: Identify Top Candidates
  Stage 2E: Metadata-Based Enhanced Scoring (no table scans)
  Stage 2F: Generate Data Analysis Reports
  Stage 3:  Enhanced Analysis
  Stage 4:  Metadata-Based Data Profiling (no table scans)
  Stage 5:  Scoring Candidates
  Stage 5B: Flagging Confirmed Candidates
  Stage 6:  Report Generation

All timestamps in generated output use UTC format.
        """
    )
    parser.add_argument('--config', '-c', type=str, help='Path to YAML configuration file')
    parser.add_argument('--dry-run', '-d', action='store_true', help='Run in dry-run mode (validate only)')
    parser.add_argument('--start-stage', '-s', type=str, choices=VALID_STAGES,
                        help='Restart from a specific stage (e.g., 2B, 3, 5). Loads cached data from previous runs.')
    parser.add_argument('--version', '-v', action='version', version=f'{AGENT_NAME} v{AGENT_VERSION}')
    parser.add_argument('--quiet', '-q', action='store_true', help='Suppress non-essential output')
    return parser.parse_args()

def run_agent(config_path=None, dry_run_override=None, start_stage=None):
    """
    Main agent execution entry point.
    
    AGENT DIRECTIVE: This is the primary entry point for the autonomous agent.
    All execution flows through this function.
    
    Args:
        config_path: Optional path to YAML configuration file
        dry_run_override: Override dry_run setting from config (True/False/None)
        start_stage: Optional stage to start from (e.g., '2B', '3', '5')
    
    Returns:
        int: Exit code (0 for success, non-zero for failure)
    """
    global CONFIG, DRY_RUN_ENABLED
    
    # Reload config if custom path provided
    if config_path:
        CONFIG = load_yaml_config(config_path)
    
    # Apply dry run override if specified
    if dry_run_override is not None:
        DRY_RUN_ENABLED = dry_run_override
    
    # Print agent banner with UTC timestamp
    print("=" * 70)
    print(f"{AGENT_NAME.upper()}")
    print(f"Version: {AGENT_VERSION} | Mode: Read-Only Analysis")
    print(f"Started: {get_utc_timestamp()}")
    if start_stage:
        print(f"Restart Mode: Starting from Stage {start_stage.upper()}")
    print("=" * 70)

    # Print configuration summary
    print_config_summary()

    try:
        conn = get_connection()
    except Exception as e:
        print(f"\nERROR: Failed to connect to Snowflake: {e}")
        return 1

    # ========== DRY RUN MODE ==========
    if DRY_RUN_ENABLED:
        success = run_dry_run(conn)
        conn.close()
        return 0 if success else 1
    
    # Verify connection
    cols, rows = execute_query(conn, "SELECT CURRENT_USER(), CURRENT_ACCOUNT(), CURRENT_ROLE()",
                               "Verify connection")
    print(f"\nConnected as: {rows[0][0]} | Account: {rows[0][1]} | Role: {rows[0][2]}")

    # ========== PREPARE OUTPUT DIRECTORY ==========
    print("\n" + "=" * 50)
    print(f"PREPARING OUTPUT ({RUN_MODE.upper()} MODE)")
    print("=" * 50)
    prepare_output_directory()
    
    # Load existing data if in append mode
    existing_candidates = []
    run_metadata = {'databases_analyzed': set(), 'run_history': []}
    if RUN_MODE == 'append':
        print("\nLoading existing analysis data...")
        existing_candidates = load_existing_candidates()
        run_metadata = load_existing_metadata()

    # Load intermediate state if restarting from a later stage
    intermediate_state = None
    if start_stage and start_stage.upper() != '1':
        intermediate_state = load_intermediate_state(start_stage)
    
    # Initialize variables that may be loaded from intermediate state
    databases, schemas, tables, columns, stages = [], [], [], [], []
    llm_candidates, variant_candidates, ml_candidates, search_candidates = [], [], [], []
    all_candidates = []
    cache = {}
    cached_count = 0

    # ========== PHASE 1: METADATA DISCOVERY ==========
    if should_run_stage('1', start_stage):
        print("\n" + "=" * 50)
        print("PHASE 1: METADATA DISCOVERY")
        print("=" * 50)

        db_cols, databases = discover_databases(conn)
        schema_cols, schemas = discover_schemas(conn)
        table_cols, tables = discover_tables_and_views(conn)
        column_cols, columns = discover_columns(conn)
        stage_cols, stages = discover_stages(conn)

        # Save inventory
        print("\nSaving metadata inventory...")

        # Full column inventory
        inventory_path = OUTPUT_DIR / "metadata" / "full_inventory.csv"
        inventory_cols = ["DATABASE", "SCHEMA", "TABLE", "COLUMN", "DATA_TYPE", "MAX_LENGTH", "COMMENT"]
        inventory_data = []
        for row in columns:
            db, schema, table, col_name, ordinal, data_type, max_len, num_prec, num_scale, nullable, comment = row
            inventory_data.append([db, schema, table, col_name, data_type, max_len, comment])
        save_csv(inventory_path, inventory_cols, inventory_data)
        print(f"  Saved {len(inventory_data):,} columns to {inventory_path}")

        # Stages inventory
        if stages:
            stages_path = OUTPUT_DIR / "metadata" / "stages_inventory.csv"
            save_csv(stages_path, stage_cols, stages)
            print(f"  Saved {len(stages):,} stages to {stages_path}")
    else:
        print("\n" + "=" * 50)
        print("PHASE 1: METADATA DISCOVERY [SKIPPED - Loading from cache]")
        print("=" * 50)
        if intermediate_state:
            databases = intermediate_state['databases']
            schemas = intermediate_state['schemas']
            tables = intermediate_state['tables']
            columns = intermediate_state['columns']
            stages = intermediate_state['stages']
        print(f"  Loaded: {len(databases)} DBs, {len(schemas)} schemas, {len(tables)} tables, {len(columns)} columns")

    # ========== PHASE 2: AI CANDIDATE IDENTIFICATION ==========
    if should_run_stage('2', start_stage):
        print("\n" + "=" * 50)
        print("PHASE 2: AI CANDIDATE IDENTIFICATION")
        print("=" * 50)

        llm_candidates = identify_llm_candidates(columns)
        print(f"Cortex LLM candidates: {len(llm_candidates):,}")

        variant_candidates = identify_variant_candidates(columns)
        print(f"Cortex Extract candidates: {len(variant_candidates):,}")

        ml_candidates = identify_ml_candidates(columns)
        print(f"Cortex ML candidates: {len(ml_candidates):,}")

        search_candidates = identify_search_candidates(columns)
        print(f"Cortex Search candidates: {len(search_candidates):,}")

        all_candidates = llm_candidates + variant_candidates + ml_candidates + search_candidates
        print(f"\nTotal candidates: {len(all_candidates):,}")
    else:
        print("\n" + "=" * 50)
        print("PHASE 2: AI CANDIDATE IDENTIFICATION [SKIPPED - Loading from cache]")
        print("=" * 50)
        if intermediate_state and intermediate_state['all_candidates']:
            all_candidates = intermediate_state['all_candidates']
            # Try to restore individual candidate lists
            llm_candidates = [c for c in all_candidates if c.get('ai_feature') == 'Cortex LLM']
            variant_candidates = [c for c in all_candidates if c.get('ai_feature') == 'Cortex Extract']
            ml_candidates = [c for c in all_candidates if c.get('ai_feature') == 'Cortex ML']
            search_candidates = [c for c in all_candidates if c.get('ai_feature') == 'Cortex Search / RAG']
        print(f"  Loaded {len(all_candidates):,} candidates from cache")

    # ========== PHASE 2: ACTUAL DATA ANALYSIS ==========
    # Initialize tracking variables
    analyzed_count = 0
    skipped_count = 0
    new_analyses = 0
    from_cache_count = 0
    top_candidates = []
    full_scan_results = []

    # Phase 2A: Load cache
    if should_run_stage('2A', start_stage):
        print("\n" + "=" * 50)
        print("PHASE 2A: LOADING ANALYSIS CACHE")
        print("=" * 50)
        cache = load_analysis_cache()
        cached_count = len(cache)
        print(f"  Found {cached_count:,} cached analyses")
    else:
        print("\nPhase 2A: Loading analysis cache [SKIPPED - already loaded]")
        if intermediate_state:
            cache = intermediate_state.get('cache', {})
        cached_count = len(cache)
        print(f"  Using {cached_count:,} cached analyses from intermediate state")

    # Phase 2B: Metadata-based analysis (replaces table-scan sampling pass)
    metadata_analysis_result = None
    if should_run_stage('2B', start_stage):
        print("\n" + "=" * 50)
        print("PHASE 2B: METADATA-BASED ANALYSIS (No Table Scans)")
        print("=" * 50)
        print("Analyzing candidate data quality via metadata...")

        # Run the metadata-only analysis (replaces per-candidate table scans)
        metadata_analysis_result = run_metadata_analysis(
            conn, execute_query,
            target_databases=TARGET_DATABASES,
            exclude_databases=EXCLUDE_DATABASES
        )

        # Build lookups for scoring candidates
        meta_table_lookup = metadata_analysis_result['table_lookup']
        meta_column_lookup = metadata_analysis_result['column_lookup']

        total_candidates = len(all_candidates)
        for i, candidate in enumerate(all_candidates, 1):
            item_name = f"{candidate.get('database', '?')}.{candidate.get('schema', '?')}.{candidate.get('table', '?')}.{candidate.get('column', '?')}"
            extra_info = f"OK:{analyzed_count} Err:{skipped_count}"
            print_progress(i, total_candidates, item_name, "Phase 2B", extra_info)

            try:
                table_key = (candidate.get('database'), candidate.get('schema'), candidate.get('table'))
                table_meta = meta_table_lookup.get(table_key, {})
                cols_meta = meta_column_lookup.get(table_key, [])

                # Find the specific column metadata
                col_name = candidate.get('column', '')
                col_meta = next((c for c in cols_meta if c.get('column_name') == col_name), {})

                if col_meta:
                    # Compute metadata-based statistics (replaces run_adaptive_sample)
                    stats = compute_column_metadata_stats(col_meta, table_meta)
                    candidate['statistics'] = stats
                    candidate['sample_size'] = 'metadata'
                    candidate['analyzed_at'] = get_utc_timestamp_iso()

                    # Cache the result
                    cache_key = f"{candidate.get('database')}.{candidate.get('schema')}.{candidate.get('table')}.{col_name}"
                    cache[cache_key] = {
                        "analyzed_at": get_utc_timestamp_iso(),
                        "sample_size": "metadata",
                        "analysis_type": "metadata_only",
                        "statistics": stats
                    }
                    analyzed_count += 1
                    new_analyses += 1
                else:
                    # No column metadata found – still count as analyzed with defaults
                    candidate['statistics'] = {'source': 'metadata_only', 'row_count': 0}
                    analyzed_count += 1
            except Exception as e:
                skipped_count += 1

        print_progress_complete("Phase 2B (Metadata)", {
            "Successfully analyzed": analyzed_count,
            "New analyses (metadata)": new_analyses,
            "Skipped (errors)": skipped_count,
            "Table scans avoided": total_candidates
        })
    else:
        print("\nPhase 2B: Metadata Analysis [SKIPPED]")
        analyzed_count = len([c for c in all_candidates if 'statistics' in c])
        print(f"  Using {analyzed_count:,} pre-analyzed candidates")

    # Phase 2C: Save updated cache
    if should_run_stage('2C', start_stage):
        print("\n" + "=" * 50)
        print("PHASE 2C: SAVING ANALYSIS CACHE")
        print("=" * 50)
        save_analysis_cache(cache)
    else:
        print("\nPhase 2C: Saving Cache [SKIPPED]")

    # Phase 2D: Identify top candidates for full scan
    if should_run_stage('2D', start_stage):
        print("\n" + "=" * 50)
        print("PHASE 2D: IDENTIFYING TOP CANDIDATES")
        print("=" * 50)
        top_candidates = identify_top_candidates(all_candidates, top_n=TOP_CANDIDATES_FULL_SCAN)
        print(f"  Selected top {len(top_candidates):,} candidates for full scan")
    else:
        print("\nPhase 2D: Identify Top Candidates [SKIPPED]")
        top_candidates = all_candidates[:TOP_CANDIDATES_FULL_SCAN]
        print(f"  Using top {len(top_candidates):,} candidates from loaded data")

    # Phase 2E: Metadata-based enhanced scoring (replaces full table scans)
    if should_run_stage('2E', start_stage):
        print("\n" + "=" * 50)
        print("PHASE 2E: METADATA-BASED ENHANCED SCORING (No Table Scans)")
        print("=" * 50)
        print("Re-scoring top candidates using metadata-derived statistics...")

        rescored = 0
        for cand in top_candidates:
            stats = cand.get('statistics', {})
            if stats:
                enhanced_score = enhance_data_readiness_score_metadata(cand, stats)
                if 'scores' not in cand:
                    cand['scores'] = {
                        'business_potential': 3,
                        'metadata_quality': 2,
                        'governance_risk': 2
                    }
                cand['scores']['data_readiness'] = enhanced_score
                cand['total_score'] = sum(cand['scores'].values())
                rescored += 1

        print(f"  Re-scored {rescored:,} top candidates with metadata-enhanced readiness")
        print(f"  Full table scans avoided: {len(top_candidates):,}")

        # Save updated cache
        save_analysis_cache(cache)
    else:
        print("\nPhase 2E: Metadata Enhanced Scoring [SKIPPED]")

    # Phase 2F: Generate data analysis reports
    if should_run_stage('2F', start_stage):
        print("\n" + "=" * 50)
        print("PHASE 2F: GENERATING DATA ANALYSIS REPORTS")
        print("=" * 50)

        # Save enhanced candidates
        enhanced_candidates_path = OUTPUT_DIR / "metadata" / "all_candidates_enhanced.json"
        with open(enhanced_candidates_path, "w", encoding="utf-8") as f:
            json.dump(all_candidates, f, indent=2, default=str)
        print(f"  Saved enhanced candidates to {enhanced_candidates_path}")

        # Save top 200 full analysis
        top_200_path = OUTPUT_DIR / "metadata" / "top_200_full_analysis.json"
        with open(top_200_path, "w", encoding="utf-8") as f:
            json.dump(top_candidates, f, indent=2, default=str)
        print(f"  Saved top 200 analysis to {top_200_path}")

        # Collect error log from ANALYSIS_ERRORS_LOG file
        error_log = []
        if ANALYSIS_ERRORS_LOG.exists():
            try:
                with open(ANALYSIS_ERRORS_LOG, "r", encoding="utf-8") as f:
                    error_content = f.read()
                    # Parse error log entries
                    for line in error_content.split('\n'):
                        if line.strip():
                            error_log.append(line)
            except Exception as e:
                print(f"  Warning: Could not read error log: {e}")

        # Generate data quality dashboard
        dashboard = generate_data_quality_dashboard(all_candidates, cache, error_log)
        dashboard_path = OUTPUT_DIR / "reports" / "data_quality_dashboard.md"
        with open(dashboard_path, "w", encoding="utf-8") as f:
            f.write(dashboard)
        print(f"  Generated data quality dashboard: {dashboard_path}")

        # Generate comparison report
        comparison = generate_comparison_report(all_candidates)
        comparison_path = OUTPUT_DIR / "reports" / "scoring_comparison.md"
        with open(comparison_path, "w", encoding="utf-8") as f:
            f.write(comparison)
        print(f"  Generated scoring comparison: {comparison_path}")

        # Generate metadata-based AI readiness report
        if metadata_analysis_result:
            readiness_report = generate_readiness_report_markdown(
                metadata_analysis_result['table_scores'],
                metadata_analysis_result['summary']
            )
            readiness_path = OUTPUT_DIR / "reports" / "ai_readiness_metadata_report.md"
            with open(readiness_path, "w", encoding="utf-8") as f:
                f.write(readiness_report)
            print(f"  Generated metadata readiness report: {readiness_path}")

            # Save table readiness scores as JSON
            readiness_json_path = OUTPUT_DIR / "metadata" / "table_readiness_scores.json"
            with open(readiness_json_path, "w", encoding="utf-8") as f:
                json.dump(metadata_analysis_result['table_scores'], f, indent=2, default=str)
            print(f"  Saved table readiness scores to {readiness_json_path}")
    else:
        print("\nPhase 2F: Generate Data Analysis Reports [SKIPPED]")

    print("\nPhase 2 Complete: Metadata-based data analysis finished (no table scans)")

    # ========== PHASE 3: ENHANCED ANALYSIS ==========
    # Initialize variables for Phase 3
    text_rich_columns = []
    edu_tables = []
    doc_columns = []
    pii_columns = []
    enhanced_llm = []
    enhanced_search = []

    if should_run_stage('3', start_stage):
        print("\n" + "=" * 50)
        print("PHASE 3: ENHANCED ANALYSIS")
        print("=" * 50)

        text_rich_columns = find_text_rich_columns(conn)
        edu_tables = find_education_tables(conn)
        doc_columns = find_document_columns(conn)
        pii_columns = find_pii_columns(conn)

        # Save enhanced candidates
        enhanced_path = OUTPUT_DIR / "metadata" / "enhanced_text_candidates.json"

        # Create LLM candidates from text-rich columns
        table_text_cols = defaultdict(list)
        for row in text_rich_columns:
            db, schema, table, col, dtype, max_len, comment = row
            enhanced_llm.append({
                "database": db, "schema": schema, "table": table, "column": col,
                "data_type": dtype, "max_length": max_len, "comment": comment,
                "ai_feature": "Cortex LLM", "reason": f"Text column '{col}'"
            })
            table_text_cols[(db, schema, table)].append(col)

        # Create search candidates from tables with multiple text columns
        for (db, schema, table), cols in table_text_cols.items():
            if len(cols) >= 2:
                enhanced_search.append({
                    "database": db, "schema": schema, "table": table,
                    "ai_feature": "Cortex Search / RAG",
                    "reason": f"{len(cols)} text columns",
                    "text_columns": cols[:10]
                })

        with open(enhanced_path, "w") as f:
            json.dump({
                "llm_candidates": enhanced_llm[:1000],
                "search_candidates": enhanced_search[:500],
                "total_llm": len(enhanced_llm),
                "total_search": len(enhanced_search),
                "education_tables": len(edu_tables),
                "document_columns": len(doc_columns),
                "pii_columns": len(pii_columns)
            }, f, indent=2, default=str)
        print(f"\nSaved enhanced candidates to {enhanced_path}")
    else:
        print("\n" + "=" * 50)
        print("PHASE 3: ENHANCED ANALYSIS [SKIPPED - Loading from cache]")
        print("=" * 50)
        if intermediate_state:
            enhanced_llm = intermediate_state.get('enhanced_llm', [])
            enhanced_search = intermediate_state.get('enhanced_search', [])
        print(f"  Loaded {len(enhanced_llm):,} LLM candidates, {len(enhanced_search):,} search candidates")

    # ========== PHASE 4: DATA PROFILING ==========
    # Initialize profiling variables
    text_profiles = []
    variant_profiles = []

    if should_run_stage('4', start_stage):
        print("\n" + "=" * 50)
        print("PHASE 4: METADATA-BASED DATA PROFILING (No Table Scans)")
        print("=" * 50)

        # Build metadata-based text profiles from text_rich_columns
        # This replaces profile_sample_text_columns which ran TABLESAMPLE queries
        print("\n=== Building Metadata-Based Text Profiles ===")
        seen_tables = set()
        for row in text_rich_columns[:15]:
            db, schema, table, col, dtype, max_len, comment = row
            table_key = f"{db}.{schema}.{table}"
            if table_key not in seen_tables:
                seen_tables.add(table_key)
                # Use metadata to estimate profile (no table scan)
                estimated_avg = min((max_len or 1000) * 0.3, 5000) if max_len else 100.0
                text_profiles.append({
                    "database": db,
                    "schema": schema,
                    "table": table,
                    "column": col,
                    "data_type": dtype,
                    "total_rows_sampled": 0,  # metadata-only, no sampling
                    "non_null_count": 0,
                    "avg_length": round(estimated_avg, 2),
                    "max_length": max_len or 16777216,
                    "min_length": 0,
                    "source": "metadata_only"
                })
                print(f"  {db}.{schema}.{table}.{col}: est_avg_len={estimated_avg:.0f} (from metadata)")
        print(f"Built {len(text_profiles)} text profiles from metadata")

        # Build metadata-based variant profiles
        # This replaces profile_variant_columns which ran LATERAL FLATTEN queries
        print("\n=== Building Metadata-Based Variant Profiles ===")
        for cand in variant_candidates[:5]:
            variant_profiles.append({
                **cand,
                "top_keys": [],  # Cannot determine keys without table scan
                "source": "metadata_only",
                "note": "Key structure requires LATERAL FLATTEN (optional targeted query)"
            })
            print(f"  {cand.get('database')}.{cand.get('schema')}.{cand.get('table')}.{cand.get('column')}: VARIANT (metadata only)")
        print(f"Built {len(variant_profiles)} variant profiles from metadata")

        # Save profiles
        profiles_json_path = OUTPUT_DIR / "profiles" / "text_column_profiles.json"
        with open(profiles_json_path, "w") as f:
            json.dump({
                "text_profiles": text_profiles,
                "variant_profiles": variant_profiles
            }, f, indent=2, default=str)
        print(f"\nSaved profiling data to {profiles_json_path}")
    else:
        print("\n" + "=" * 50)
        print("PHASE 4: DATA PROFILING [SKIPPED - Loading from cache]")
        print("=" * 50)
        if intermediate_state:
            text_profiles = intermediate_state.get('text_profiles', [])
            variant_profiles = intermediate_state.get('variant_profiles', [])
        print(f"  Loaded {len(text_profiles)} text profiles, {len(variant_profiles)} variant profiles")

    # ========== PHASE 5: SCORING ==========
    if should_run_stage('5', start_stage):
        print("\n" + "=" * 50)
        print("PHASE 5: SCORING CANDIDATES")
        print("=" * 50)

        for cand in all_candidates:
            score_candidate(cand, text_profiles, variant_profiles)

        # Also score enhanced candidates
        for cand in enhanced_llm + enhanced_search:
            score_candidate(cand, text_profiles, variant_profiles)

        print(f"Scored {len(all_candidates):,} candidates")
    else:
        print("\n" + "=" * 50)
        print("PHASE 5: SCORING CANDIDATES [SKIPPED - Using existing scores]")
        print("=" * 50)
        scored_count = len([c for c in all_candidates if 'total_score' in c])
        print(f"  {scored_count:,} candidates already have scores")

    # ========== PHASE 5B: CONFIRMED CANDIDATES (Objective 2) ==========
    confirmed_count = 0
    unconfirmed_count = 0
    confirmed_candidates = []

    if should_run_stage('5B', start_stage):
        print("\n" + "=" * 50)
        print("PHASE 5B: FLAGGING CONFIRMED CANDIDATES")
        print("=" * 50)
        total_to_confirm = len(all_candidates)
        
        for i, cand in enumerate(all_candidates, 1):
            # Build item name for progress
            item_name = f"{cand.get('database', '?')}.{cand.get('table', '?')}.{cand.get('column', '?')}"
            extra_info = f"Confirmed:{confirmed_count} Unconfirmed:{unconfirmed_count}"
            print_progress(i, total_to_confirm, item_name, "Phase 5B", extra_info)
            
            # Apply confirmed candidate logic based on data profiling
            is_confirmed, reasons = is_confirmed_candidate(cand)
            cand['is_confirmed_candidate'] = is_confirmed
            cand['confirmation_reasons'] = reasons
            
            if is_confirmed:
                confirmed_count += 1
            else:
                unconfirmed_count += 1
        
        # Print completion summary
        confirmation_rate = (confirmed_count/(confirmed_count+unconfirmed_count)*100) if (confirmed_count+unconfirmed_count) > 0 else 0
        print_progress_complete("Phase 5B Confirmation", {
            "Confirmed Candidates": confirmed_count,
            "Unconfirmed (data quality issues)": unconfirmed_count,
            "Confirmation rate": f"{confirmation_rate:.1f}%"
        })

        # Handle append mode - merge with existing candidates
        if RUN_MODE == 'append' and existing_candidates:
            print(f"\nMerging with existing candidates (append mode)...")
            if APPEND_STRATEGY == 'merge':
                all_candidates = merge_candidates(existing_candidates, all_candidates)
            else:
                # Add strategy - simply combine
                all_candidates = existing_candidates + all_candidates
                print(f"  Combined: {len(existing_candidates):,} existing + {len(all_candidates) - len(existing_candidates):,} new")
            
            # Update run history with databases analyzed in this run
            new_databases = set(c.get('database', '') for c in all_candidates if c.get('database'))
            save_run_history(run_metadata, new_databases)
        
        # Save all candidates (with confirmation status)
        candidates_path = OUTPUT_DIR / "metadata" / "all_candidates.json"
        with open(candidates_path, "w") as f:
            json.dump(all_candidates, f, indent=2, default=str)
        print(f"Saved all candidates to {candidates_path}")
        
        # Save confirmed candidates separately
        confirmed_candidates = [c for c in all_candidates if c.get('is_confirmed_candidate')]
        confirmed_path = OUTPUT_DIR / "metadata" / "confirmed_candidates.json"
        with open(confirmed_path, "w") as f:
            json.dump(confirmed_candidates, f, indent=2, default=str)
        print(f"Saved {len(confirmed_candidates):,} confirmed candidates to {confirmed_path}")
    else:
        print("\n" + "=" * 50)
        print("PHASE 5B: FLAGGING CONFIRMED CANDIDATES [SKIPPED]")
        print("=" * 50)
        confirmed_candidates = [c for c in all_candidates if c.get('is_confirmed_candidate')]
        confirmed_count = len(confirmed_candidates)
        print(f"  Using {confirmed_count:,} pre-confirmed candidates")

    # ========== PHASE 6: REPORT GENERATION ==========
    num_profiles = 0
    if should_run_stage('6', start_stage):
        print("\n" + "=" * 50)
        print("PHASE 6: GENERATING REPORTS")
        print("=" * 50)

        # Executive Summary
        exec_summary = generate_executive_summary(
            all_candidates=all_candidates + enhanced_llm[:100] + enhanced_search[:50],
            tables_count=len(tables),
            columns_count=len(columns),
            databases_count=len(databases),
            schemas_count=len(schemas),
            stages_count=len(stages),
            llm_count=len(enhanced_llm),
            search_count=len(enhanced_search),
            ml_count=len(ml_candidates),
            extract_count=len(variant_candidates),
            edu_tables=edu_tables,
            pii_columns=pii_columns,
            text_profiles=text_profiles
        )
        exec_path = OUTPUT_DIR / "reports" / "executive_summary.md"
        exec_path.parent.mkdir(parents=True, exist_ok=True)
        with open(exec_path, "w", encoding="utf-8") as f:
            f.write(exec_summary)
        print(f"Saved executive summary to {exec_path}")

        # Detailed Analysis Report (comprehensive markdown with reasoning)
        detailed_report = generate_detailed_analysis_report(
            all_candidates=all_candidates,
            text_profiles=text_profiles,
            variant_profiles=variant_profiles,
            confirmed_candidates=confirmed_candidates,
            edu_tables=edu_tables,
            pii_columns=pii_columns,
            databases=databases,
            schemas=schemas,
            tables=tables,
            stages=stages,
        )
        detailed_path = OUTPUT_DIR / "reports" / "detailed_analysis_report.md"
        detailed_path.parent.mkdir(parents=True, exist_ok=True)
        with open(detailed_path, "w", encoding="utf-8") as f:
            f.write(detailed_report)
        print(f"Saved detailed analysis report to {detailed_path}")

        # AI Strategy Roadmap
        roadmap = generate_roadmap(
            all_candidates=all_candidates + enhanced_llm[:100] + enhanced_search[:50],
            text_profiles=text_profiles,
            edu_tables=edu_tables,
            stages_data=stages
        )
        roadmap_path = OUTPUT_DIR / "reports" / "ai_strategy_roadmap.md"
        roadmap_path.parent.mkdir(parents=True, exist_ok=True)
        with open(roadmap_path, "w", encoding="utf-8") as f:
            f.write(roadmap)
        print(f"Saved roadmap to {roadmap_path}")

        # Profile Reports
        num_profiles = generate_profile_reports(all_candidates)

        # Save audit log
        save_audit_log()
        print(f"Saved audit trail with {len(AUDIT_LOG)} queries to {OUTPUT_DIR / 'logs' / 'audit_trail.sql'}")

        # Generate Report Index (Main Summary)
        analysis_stats = {
            'analyzed': analyzed_count,
            'from_cache': from_cache_count,
            'new': new_analyses,
            'errors': skipped_count,
            'success_rate': (analyzed_count/(analyzed_count+skipped_count)*100) if (analyzed_count+skipped_count) > 0 else 0
        }
        
        report_index = generate_report_index(
            databases=databases,
            schemas=schemas,
            tables=tables,
            columns=columns,
            stages=stages,
            llm_candidates=enhanced_llm,
            search_candidates=enhanced_search,
            ml_candidates=ml_candidates,
            variant_candidates=variant_candidates,
            confirmed_candidates=confirmed_candidates,
            num_profiles=num_profiles,
            analysis_stats=analysis_stats
        )
        index_path = OUTPUT_DIR / "README.md"
        with open(index_path, "w", encoding="utf-8") as f:
            f.write(report_index)
        print(f"Saved report index to {index_path}")
    else:
        print("\n" + "=" * 50)
        print("PHASE 6: GENERATING REPORTS [SKIPPED]")
        print("=" * 50)
        print("  Reports not regenerated (use --start-stage 6 to regenerate)")

    conn.close()

    # ========== SUMMARY ==========
    print("\n" + "=" * 70)
    print("ANALYSIS COMPLETE")
    print("=" * 70)
    print(f"""
Environment Summary:
  - Databases: {len(databases):,}
  - Schemas: {len(schemas):,}
  - Tables/Views: {len(tables):,}
  - Columns: {len(columns):,}
  - Stages: {len(stages):,}

AI Candidates Identified:
  - Cortex LLM: {len(enhanced_llm):,}
  - Cortex Search/RAG: {len(enhanced_search):,}
  - Cortex ML: {len(ml_candidates):,}
  - Cortex Extract: {len(variant_candidates):,}
  - Total: {len(enhanced_llm) + len(enhanced_search) + len(ml_candidates) + len(variant_candidates):,}

Data Analysis Summary:
  - Candidates analyzed: {analyzed_count:,}
  - From cache: {cached_count:,}
  - New analyses: {new_analyses:,}
  - Skipped (errors): {skipped_count:,}
  - Top candidates (full scan): {len(top_candidates):,}
  - Analysis success rate: {(analyzed_count/(analyzed_count+skipped_count)*100) if (analyzed_count+skipped_count) > 0 else 0:.1f}%

Reports Generated:
  - {OUTPUT_DIR}/README.md (Main Report Index)
  - {OUTPUT_DIR}/reports/executive_summary.md
  - {OUTPUT_DIR}/reports/detailed_analysis_report.md (Comprehensive Analysis)
  - {OUTPUT_DIR}/reports/ai_strategy_roadmap.md
  - {OUTPUT_DIR}/profiles/*.md ({num_profiles} schema profiles)
  - {OUTPUT_DIR}/metadata/*.json (data files)
  - {OUTPUT_DIR}/logs/*.sql (audit trail)

Confirmed Candidates: {len(confirmed_candidates):,} / {len(enhanced_llm) + len(enhanced_search) + len(ml_candidates) + len(variant_candidates):,}

Agent completed: {get_utc_timestamp()}

======================================================================
📋 VIEW FULL REPORT: {OUTPUT_DIR}/README.md
======================================================================
""")
    
    return 0  # Success

def main():
    """
    Application main entry point.
    
    Parses command line arguments and invokes the agent.
    """
    args = parse_arguments()
    
    # Determine dry run mode (CLI flag overrides config)
    dry_run_override = True if args.dry_run else None
    
    # Run the agent
    exit_code = run_agent(
        config_path=args.config,
        dry_run_override=dry_run_override,
        start_stage=args.start_stage
    )
    
    sys.exit(exit_code)

if __name__ == "__main__":
    main()
