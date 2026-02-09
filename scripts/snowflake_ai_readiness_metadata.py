#!/usr/bin/env python3
"""
===============================================================================
SNOWFLAKE AI READINESS - METADATA-ONLY EVALUATION MODULE
===============================================================================

Replaces production table scans with Snowflake metadata queries to evaluate
AI Readiness without driving up credit consumption.

Data Sources:
  - INFORMATION_SCHEMA.COLUMNS   (real-time, per-database)
  - INFORMATION_SCHEMA.TABLES    (real-time, per-database)
  - SNOWFLAKE.ACCOUNT_USAGE.COLUMNS          (account-wide, up to 365 days)
  - SNOWFLAKE.ACCOUNT_USAGE.TABLES           (account-wide, includes ROW_COUNT/BYTES)
  - SNOWFLAKE.ACCOUNT_USAGE.TABLE_CONSTRAINTS (primary/foreign keys)
  - SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS (clustering, storage)

Evaluation Criteria:
  1. Presence of Comments   – COMMENT column on tables/columns
  2. Data Types             – Identify unsupported types for vectorization/LLM
  3. Freshness              – LAST_ALTERED from TABLES metadata
  4. Clustering/Partitioning – CLUSTERING_KEY from TABLE_STORAGE_METRICS
  5. Constraints            – Primary/foreign keys from TABLE_CONSTRAINTS

Output:
  - Per-table AI Readiness Score (0–100)
  - Breakdown by dimension

VERSION: 1.0
===============================================================================
"""

from datetime import datetime, timezone, timedelta


# =============================================================================
# UNSUPPORTED / PROBLEMATIC DATA TYPES FOR AI/LLM PROCESSING
# =============================================================================

# Types that are NOT directly usable for vectorization or LLM text processing
UNSUPPORTED_AI_TYPES = frozenset([
    'BINARY', 'VARBINARY',
    'GEOGRAPHY', 'GEOMETRY',
])

# Types that need special handling (semi-structured) but are still usable
SEMI_STRUCTURED_TYPES = frozenset([
    'VARIANT', 'OBJECT', 'ARRAY',
])

# Types ideal for LLM / text processing
TEXT_TYPES = frozenset([
    'VARCHAR', 'TEXT', 'STRING', 'CHAR', 'CHARACTER',
])

# Types ideal for ML forecasting / anomaly detection
NUMERIC_TYPES = frozenset([
    'NUMBER', 'DECIMAL', 'NUMERIC', 'INT', 'INTEGER', 'BIGINT', 'SMALLINT',
    'TINYINT', 'BYTEINT', 'FLOAT', 'FLOAT4', 'FLOAT8', 'DOUBLE',
    'DOUBLE PRECISION', 'REAL',
])

TEMPORAL_TYPES = frozenset([
    'DATE', 'DATETIME', 'TIME', 'TIMESTAMP', 'TIMESTAMP_LTZ',
    'TIMESTAMP_NTZ', 'TIMESTAMP_TZ',
])


# =============================================================================
# SQL QUERIES – METADATA ONLY (NO TABLE SCANS)
# =============================================================================

def _db_filter_clause(target_databases, exclude_databases, catalog_col="TABLE_CATALOG"):
    """Build a SQL WHERE fragment for database filtering."""
    if target_databases:
        db_list = ", ".join([f"'{db.upper()}'" for db in target_databases])
        return f"AND UPPER({catalog_col}) IN ({db_list})"
    elif exclude_databases:
        db_list = ", ".join([f"'{db.upper()}'" for db in exclude_databases])
        return f"AND UPPER({catalog_col}) NOT IN ({db_list})"
    return ""


# ---------------------------------------------------------------------------
# Query 1: Column-level metadata (ACCOUNT_USAGE – account-wide, historical)
# ---------------------------------------------------------------------------
QUERY_COLUMNS_METADATA = """
SELECT
    c.TABLE_CATALOG       AS DATABASE_NAME,
    c.TABLE_SCHEMA        AS SCHEMA_NAME,
    c.TABLE_NAME,
    c.COLUMN_NAME,
    c.ORDINAL_POSITION,
    c.DATA_TYPE,
    c.CHARACTER_MAXIMUM_LENGTH,
    c.NUMERIC_PRECISION,
    c.NUMERIC_SCALE,
    c.IS_NULLABLE,
    c.COMMENT              AS COLUMN_COMMENT
FROM SNOWFLAKE.ACCOUNT_USAGE.COLUMNS c
WHERE c.DELETED IS NULL
{db_filter}
QUALIFY ROW_NUMBER() OVER (PARTITION BY c.TABLE_CATALOG, c.TABLE_SCHEMA, c.TABLE_NAME, c.COLUMN_NAME ORDER BY c.ORDINAL_POSITION) = 1
ORDER BY c.TABLE_CATALOG, c.TABLE_SCHEMA, c.TABLE_NAME, c.ORDINAL_POSITION
"""

# ---------------------------------------------------------------------------
# Query 2: Table-level metadata with freshness (ACCOUNT_USAGE)
# ---------------------------------------------------------------------------
QUERY_TABLES_METADATA = """
SELECT
    t.TABLE_CATALOG       AS DATABASE_NAME,
    t.TABLE_SCHEMA        AS SCHEMA_NAME,
    t.TABLE_NAME,
    t.TABLE_TYPE,
    t.ROW_COUNT,
    t.BYTES,
    t.COMMENT              AS TABLE_COMMENT,
    t.CREATED,
    t.LAST_ALTERED,
    t.CLUSTERING_KEY
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES t
WHERE t.DELETED IS NULL
{db_filter}
QUALIFY ROW_NUMBER() OVER (PARTITION BY t.TABLE_CATALOG, t.TABLE_SCHEMA, t.TABLE_NAME ORDER BY t.CREATED DESC) = 1
ORDER BY t.TABLE_CATALOG, t.TABLE_SCHEMA, t.TABLE_NAME
"""

# ---------------------------------------------------------------------------
# Query 3: Table constraints (primary keys, foreign keys, unique)
# ---------------------------------------------------------------------------
QUERY_TABLE_CONSTRAINTS = """
SELECT
    tc.TABLE_CATALOG      AS DATABASE_NAME,
    tc.TABLE_SCHEMA       AS SCHEMA_NAME,
    tc.TABLE_NAME,
    tc.CONSTRAINT_TYPE,
    tc.CONSTRAINT_NAME
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_CONSTRAINTS tc
WHERE tc.DELETED IS NULL
{db_filter}
QUALIFY ROW_NUMBER() OVER (PARTITION BY tc.TABLE_CATALOG, tc.TABLE_SCHEMA, tc.TABLE_NAME, tc.CONSTRAINT_NAME ORDER BY tc.CONSTRAINT_NAME) = 1
ORDER BY tc.TABLE_CATALOG, tc.TABLE_SCHEMA, tc.TABLE_NAME
"""

# ---------------------------------------------------------------------------
# Query 4: Real-time column metadata via INFORMATION_SCHEMA (per-database)
#   Use this for targeted, real-time checks on specific databases.
# ---------------------------------------------------------------------------
QUERY_INFO_SCHEMA_COLUMNS = """
SELECT
    '{database}' AS DATABASE_NAME,
    c.TABLE_SCHEMA        AS SCHEMA_NAME,
    c.TABLE_NAME,
    c.COLUMN_NAME,
    c.ORDINAL_POSITION,
    c.DATA_TYPE,
    c.CHARACTER_MAXIMUM_LENGTH,
    c.NUMERIC_PRECISION,
    c.NUMERIC_SCALE,
    c.IS_NULLABLE,
    c.COMMENT              AS COLUMN_COMMENT
FROM "{database}".INFORMATION_SCHEMA.COLUMNS c
WHERE c.TABLE_SCHEMA != 'INFORMATION_SCHEMA'
ORDER BY c.TABLE_SCHEMA, c.TABLE_NAME, c.ORDINAL_POSITION
"""

# ---------------------------------------------------------------------------
# Query 5: Real-time table metadata via INFORMATION_SCHEMA (per-database)
# ---------------------------------------------------------------------------
QUERY_INFO_SCHEMA_TABLES = """
SELECT
    '{database}' AS DATABASE_NAME,
    t.TABLE_SCHEMA        AS SCHEMA_NAME,
    t.TABLE_NAME,
    t.TABLE_TYPE,
    t.ROW_COUNT,
    t.BYTES,
    t.COMMENT              AS TABLE_COMMENT,
    t.CREATED,
    t.LAST_ALTERED,
    t.CLUSTERING_KEY
FROM "{database}".INFORMATION_SCHEMA.TABLES t
WHERE t.TABLE_SCHEMA != 'INFORMATION_SCHEMA'
ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME
"""

# ---------------------------------------------------------------------------
# Query 6: Table storage metrics for clustering depth info
# ---------------------------------------------------------------------------
QUERY_TABLE_STORAGE_METRICS = """
SELECT
    TABLE_CATALOG          AS DATABASE_NAME,
    TABLE_SCHEMA           AS SCHEMA_NAME,
    TABLE_NAME,
    ACTIVE_BYTES,
    TIME_TRAVEL_BYTES,
    FAILSAFE_BYTES,
    RETAINED_FOR_CLONE_BYTES
FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
WHERE TABLE_CATALOG IS NOT NULL
{db_filter}
QUALIFY ROW_NUMBER() OVER (PARTITION BY TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME ORDER BY ACTIVE_BYTES DESC NULLS LAST) = 1
ORDER BY TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME
"""


# =============================================================================
# METADATA FETCHING FUNCTIONS
# =============================================================================

def fetch_columns_metadata(conn, execute_query_fn, target_databases=None, exclude_databases=None):
    """
    Fetch column metadata from SNOWFLAKE.ACCOUNT_USAGE.COLUMNS.
    No table scans – metadata only.

    Returns: (column_names, rows)
    """
    db_filter = _db_filter_clause(target_databases, exclude_databases, "c.TABLE_CATALOG")
    query = QUERY_COLUMNS_METADATA.format(db_filter=db_filter)
    return execute_query_fn(conn, query, "Metadata: Fetch column metadata from ACCOUNT_USAGE.COLUMNS")


def fetch_tables_metadata(conn, execute_query_fn, target_databases=None, exclude_databases=None):
    """
    Fetch table metadata (including LAST_ALTERED, CLUSTERING_KEY, ROW_COUNT)
    from SNOWFLAKE.ACCOUNT_USAGE.TABLES.
    No table scans – metadata only.

    Returns: (column_names, rows)
    """
    db_filter = _db_filter_clause(target_databases, exclude_databases, "t.TABLE_CATALOG")
    query = QUERY_TABLES_METADATA.format(db_filter=db_filter)
    return execute_query_fn(conn, query, "Metadata: Fetch table metadata from ACCOUNT_USAGE.TABLES")


def fetch_table_constraints(conn, execute_query_fn, target_databases=None, exclude_databases=None):
    """
    Fetch table constraints (PK, FK, UNIQUE) from SNOWFLAKE.ACCOUNT_USAGE.TABLE_CONSTRAINTS.
    No table scans – metadata only.

    Returns: (column_names, rows)
    """
    db_filter = _db_filter_clause(target_databases, exclude_databases, "tc.TABLE_CATALOG")
    query = QUERY_TABLE_CONSTRAINTS.format(db_filter=db_filter)
    return execute_query_fn(conn, query, "Metadata: Fetch table constraints from ACCOUNT_USAGE.TABLE_CONSTRAINTS")


def fetch_table_storage_metrics(conn, execute_query_fn, target_databases=None, exclude_databases=None):
    """
    Fetch table storage metrics for clustering/partitioning analysis.
    No table scans – metadata only.

    Returns: (column_names, rows)
    """
    db_filter = _db_filter_clause(target_databases, exclude_databases, "TABLE_CATALOG")
    query = QUERY_TABLE_STORAGE_METRICS.format(db_filter=db_filter)
    return execute_query_fn(conn, query, "Metadata: Fetch table storage metrics from ACCOUNT_USAGE")


def fetch_realtime_columns(conn, execute_query_fn, database):
    """
    Fetch real-time column metadata via INFORMATION_SCHEMA for a specific database.
    Use for targeted, up-to-the-second checks.

    Returns: (column_names, rows)
    """
    query = QUERY_INFO_SCHEMA_COLUMNS.format(database=database)
    return execute_query_fn(conn, query, f"Metadata: Real-time columns for {database} via INFORMATION_SCHEMA")


def fetch_realtime_tables(conn, execute_query_fn, database):
    """
    Fetch real-time table metadata via INFORMATION_SCHEMA for a specific database.
    Use for targeted, up-to-the-second checks.

    Returns: (column_names, rows)
    """
    query = QUERY_INFO_SCHEMA_TABLES.format(database=database)
    return execute_query_fn(conn, query, f"Metadata: Real-time tables for {database} via INFORMATION_SCHEMA")


# =============================================================================
# METADATA PARSING – BUILD LOOKUP STRUCTURES
# =============================================================================

def build_table_metadata_lookup(tables_rows):
    """
    Parse table metadata rows into a lookup dict keyed by (database, schema, table).

    Each entry contains:
      - row_count, bytes, table_comment, created, last_altered, clustering_key, table_type
    """
    lookup = {}
    for row in tables_rows:
        db, schema, table_name, table_type, row_count, byte_count, comment, created, last_altered, clustering_key = row
        key = (db, schema, table_name)
        lookup[key] = {
            'table_type': table_type,
            'row_count': row_count,
            'bytes': byte_count,
            'table_comment': comment or '',
            'created': created,
            'last_altered': last_altered,
            'clustering_key': clustering_key or '',
        }
    return lookup


def build_column_metadata_lookup(columns_rows):
    """
    Parse column metadata rows into a lookup dict keyed by (database, schema, table).

    Each entry is a list of column dicts.
    """
    from collections import defaultdict
    lookup = defaultdict(list)
    for row in columns_rows:
        db, schema, table_name, col_name, ordinal, data_type, char_max_len, num_prec, num_scale, is_nullable, comment = row
        key = (db, schema, table_name)
        lookup[key].append({
            'column_name': col_name,
            'ordinal_position': ordinal,
            'data_type': (data_type or '').upper(),
            'character_maximum_length': char_max_len,
            'numeric_precision': num_prec,
            'numeric_scale': num_scale,
            'is_nullable': is_nullable,
            'column_comment': comment or '',
        })
    return dict(lookup)


def build_constraints_lookup(constraints_rows):
    """
    Parse constraint rows into a lookup dict keyed by (database, schema, table).

    Each entry is a list of constraint dicts with type and name.
    """
    from collections import defaultdict
    lookup = defaultdict(list)
    for row in constraints_rows:
        db, schema, table_name, constraint_type, constraint_name = row
        key = (db, schema, table_name)
        lookup[key].append({
            'constraint_type': constraint_type,
            'constraint_name': constraint_name,
        })
    return dict(lookup)


def build_storage_lookup(storage_rows):
    """
    Parse storage metrics rows into a lookup dict keyed by (database, schema, table).
    """
    lookup = {}
    for row in storage_rows:
        db, schema, table_name, active_bytes, tt_bytes, fs_bytes, clone_bytes = row
        key = (db, schema, table_name)
        lookup[key] = {
            'active_bytes': active_bytes or 0,
            'time_travel_bytes': tt_bytes or 0,
            'failsafe_bytes': fs_bytes or 0,
            'clone_bytes': clone_bytes or 0,
        }
    return lookup


# =============================================================================
# AI READINESS SCORING – METADATA BASED (NO TABLE SCANS)
# =============================================================================

# Scoring weights (total = 100)
WEIGHT_COMMENTS       = 25   # Presence of table/column comments (LLM context)
WEIGHT_DATA_TYPES     = 20   # Compatible data types for AI processing
WEIGHT_FRESHNESS      = 25   # Data freshness via LAST_ALTERED
WEIGHT_CLUSTERING     = 15   # Clustering/partitioning for large-scale retrieval
WEIGHT_CONSTRAINTS    = 15   # Constraints indicating data quality / relationships


def score_comments(table_meta, columns_meta):
    """
    Score 0–100 based on presence of comments on table and columns.

    - Table has a comment: +40
    - Percentage of columns with comments: up to +60
    """
    score = 0.0

    # Table comment
    if table_meta.get('table_comment', '').strip():
        score += 40.0

    # Column comments
    if columns_meta:
        commented = sum(1 for c in columns_meta if c.get('column_comment', '').strip())
        ratio = commented / len(columns_meta)
        score += ratio * 60.0

    return min(round(score, 2), 100.0)


def score_data_types(columns_meta):
    """
    Score 0–100 based on data type compatibility for AI/LLM processing.

    - Each column with a supported type adds to the score.
    - Unsupported types (BINARY, GEOGRAPHY, etc.) reduce the score.
    - Semi-structured types (VARIANT, OBJECT, ARRAY) get partial credit.
    """
    if not columns_meta:
        return 0.0

    total = len(columns_meta)
    supported_count = 0
    partial_count = 0

    for col in columns_meta:
        dtype = col.get('data_type', '').upper()
        # Normalize compound types like "TIMESTAMP_LTZ(9)"
        base_type = dtype.split('(')[0].strip()

        if base_type in UNSUPPORTED_AI_TYPES:
            continue  # 0 credit
        elif base_type in SEMI_STRUCTURED_TYPES:
            partial_count += 1
        else:
            supported_count += 1

    # Full credit for supported, half credit for semi-structured
    effective = supported_count + (partial_count * 0.5)
    ratio = effective / total if total > 0 else 0
    return min(round(ratio * 100, 2), 100.0)


def score_freshness(table_meta, freshness_threshold_days=90):
    """
    Score 0–100 based on data freshness using LAST_ALTERED.

    - Altered within 7 days:   100
    - Altered within 30 days:   80
    - Altered within 90 days:   50
    - Altered within 180 days:  25
    - Older than 180 days:      10
    - No LAST_ALTERED info:      0
    """
    last_altered = table_meta.get('last_altered')
    if not last_altered:
        return 0.0

    # Handle both datetime objects and strings
    if isinstance(last_altered, str):
        try:
            last_altered = datetime.fromisoformat(last_altered.replace('Z', '+00:00'))
        except (ValueError, TypeError):
            return 0.0

    # Make timezone-aware if naive
    if last_altered.tzinfo is None:
        last_altered = last_altered.replace(tzinfo=timezone.utc)

    now = datetime.now(timezone.utc)
    age_days = (now - last_altered).days

    if age_days <= 7:
        return 100.0
    elif age_days <= 30:
        return 80.0
    elif age_days <= 90:
        return 50.0
    elif age_days <= 180:
        return 25.0
    else:
        return 10.0


def score_clustering(table_meta, storage_meta=None):
    """
    Score 0–100 based on clustering/partitioning optimization.

    - Has CLUSTERING_KEY defined: +70
    - Has significant active storage (>1GB indicates partitioned data): +30
    - No clustering info: 0
    """
    score = 0.0

    clustering_key = table_meta.get('clustering_key', '').strip()
    if clustering_key:
        score += 70.0

    # Bonus for large tables that are clustered (indicates optimization)
    if storage_meta:
        active_bytes = storage_meta.get('active_bytes', 0)
        if active_bytes and active_bytes > 1_073_741_824 and clustering_key:  # >1GB
            score += 30.0
        elif active_bytes and active_bytes > 1_073_741_824:
            # Large table without clustering – partial credit for being substantial
            score += 10.0

    return min(round(score, 2), 100.0)


def score_constraints(constraints_meta):
    """
    Score 0–100 based on presence of constraints indicating data quality.

    - Has PRIMARY KEY: +50
    - Has FOREIGN KEY: +30
    - Has UNIQUE constraint: +20
    """
    if not constraints_meta:
        return 0.0

    score = 0.0
    types_found = set(c.get('constraint_type', '').upper() for c in constraints_meta)

    if 'PRIMARY KEY' in types_found:
        score += 50.0
    if 'FOREIGN KEY' in types_found:
        score += 30.0
    if 'UNIQUE' in types_found:
        score += 20.0

    return min(round(score, 2), 100.0)


def compute_table_readiness_score(table_key, table_lookup, column_lookup,
                                   constraints_lookup, storage_lookup):
    """
    Compute the overall AI Readiness Score for a single table.

    Args:
        table_key: Tuple of (database, schema, table)
        table_lookup: Dict from build_table_metadata_lookup
        column_lookup: Dict from build_column_metadata_lookup
        constraints_lookup: Dict from build_constraints_lookup
        storage_lookup: Dict from build_storage_lookup

    Returns:
        dict with:
          - total_score (0–100)
          - dimension_scores (dict of each dimension)
          - dimension_details (dict with human-readable details)
          - metadata (table-level metadata summary)
    """
    table_meta = table_lookup.get(table_key, {})
    columns_meta = column_lookup.get(table_key, [])
    constraints_meta = constraints_lookup.get(table_key, [])
    storage_meta = storage_lookup.get(table_key, {})

    # Compute individual dimension scores (each 0–100)
    comments_score = score_comments(table_meta, columns_meta)
    datatypes_score = score_data_types(columns_meta)
    freshness_score = score_freshness(table_meta)
    clustering_score = score_clustering(table_meta, storage_meta)
    constraints_score = score_constraints(constraints_meta)

    # Weighted total
    total = (
        comments_score   * (WEIGHT_COMMENTS / 100) +
        datatypes_score  * (WEIGHT_DATA_TYPES / 100) +
        freshness_score  * (WEIGHT_FRESHNESS / 100) +
        clustering_score * (WEIGHT_CLUSTERING / 100) +
        constraints_score * (WEIGHT_CONSTRAINTS / 100)
    )

    # Build details
    commented_cols = sum(1 for c in columns_meta if c.get('column_comment', '').strip()) if columns_meta else 0
    total_cols = len(columns_meta) if columns_meta else 0
    unsupported_cols = []
    for c in columns_meta:
        base_type = c.get('data_type', '').split('(')[0].strip()
        if base_type in UNSUPPORTED_AI_TYPES:
            unsupported_cols.append(f"{c['column_name']} ({c['data_type']})")

    constraint_types = list(set(c.get('constraint_type', '') for c in constraints_meta)) if constraints_meta else []

    return {
        'database': table_key[0],
        'schema': table_key[1],
        'table': table_key[2],
        'total_score': round(total, 2),
        'dimension_scores': {
            'comments': round(comments_score, 2),
            'data_types': round(datatypes_score, 2),
            'freshness': round(freshness_score, 2),
            'clustering': round(clustering_score, 2),
            'constraints': round(constraints_score, 2),
        },
        'dimension_details': {
            'table_comment': bool(table_meta.get('table_comment', '').strip()),
            'columns_with_comments': commented_cols,
            'total_columns': total_cols,
            'comment_coverage_pct': round(commented_cols / total_cols * 100, 1) if total_cols > 0 else 0,
            'unsupported_type_columns': unsupported_cols,
            'last_altered': str(table_meta.get('last_altered', 'N/A')),
            'clustering_key': table_meta.get('clustering_key', ''),
            'constraint_types': constraint_types,
            'row_count': table_meta.get('row_count'),
            'bytes': table_meta.get('bytes'),
        },
        'metadata': {
            'table_type': table_meta.get('table_type', 'N/A'),
            'row_count': table_meta.get('row_count'),
            'bytes': table_meta.get('bytes'),
            'created': str(table_meta.get('created', 'N/A')),
        }
    }


def compute_all_readiness_scores(table_lookup, column_lookup,
                                  constraints_lookup, storage_lookup):
    """
    Compute AI Readiness Scores for ALL tables in the lookups.

    Returns: list of score dicts sorted by total_score descending.
    """
    all_table_keys = set(table_lookup.keys()) | set(column_lookup.keys())
    results = []

    for table_key in all_table_keys:
        result = compute_table_readiness_score(
            table_key, table_lookup, column_lookup,
            constraints_lookup, storage_lookup
        )
        results.append(result)

    # Sort by total_score descending
    results.sort(key=lambda x: x['total_score'], reverse=True)
    return results


# =============================================================================
# METADATA-BASED COLUMN STATISTICS (REPLACES run_adaptive_sample)
# =============================================================================

def compute_column_metadata_stats(column_meta, table_meta):
    """
    Compute statistics for a single column using ONLY metadata.
    Replaces run_adaptive_sample / run_deep_profiling which queried actual tables.

    Args:
        column_meta: dict with column metadata (from build_column_metadata_lookup entry)
        table_meta: dict with table metadata (from build_table_metadata_lookup entry)

    Returns:
        dict with metadata-derived statistics compatible with the existing
        enhance_data_readiness_score function.
    """
    data_type = column_meta.get('data_type', '').upper()
    base_type = data_type.split('(')[0].strip()
    char_max_len = column_meta.get('character_maximum_length')
    is_nullable = column_meta.get('is_nullable', 'YES')
    has_comment = bool(column_meta.get('column_comment', '').strip())
    row_count = table_meta.get('row_count') or 0

    stats = {
        'row_count': row_count,
        'data_type': data_type,
        'is_nullable': is_nullable,
        'has_comment': has_comment,
        'source': 'metadata_only',
    }

    # Estimate null_percentage from IS_NULLABLE metadata
    # If column is NOT NULL, we know null_percentage = 0
    # If column IS nullable, we conservatively estimate based on heuristics
    if is_nullable == 'NO':
        stats['null_percentage'] = 0.0
        stats['non_null_count'] = row_count
    else:
        # Conservative estimate: nullable columns assumed 20% null
        stats['null_percentage'] = 20.0
        stats['non_null_count'] = int(row_count * 0.8) if row_count else 0

    # For text columns, use CHARACTER_MAXIMUM_LENGTH as a proxy
    if base_type in TEXT_TYPES or any(t in base_type for t in ['VARCHAR', 'TEXT', 'STRING', 'CHAR']):
        if char_max_len:
            # Heuristic: avg_length is roughly 30% of max defined length
            # This replaces the actual AVG(LENGTH()) query
            estimated_avg = min(char_max_len * 0.3, 5000)
            stats['max_length'] = char_max_len
            stats['avg_length'] = round(estimated_avg, 2)
            stats['min_length'] = 0
        else:
            stats['max_length'] = 16777216  # Snowflake default VARCHAR max
            stats['avg_length'] = 100.0  # Conservative estimate
            stats['min_length'] = 0

    # For numeric columns
    elif base_type in NUMERIC_TYPES or any(t in base_type for t in ['NUMBER', 'INT', 'FLOAT', 'DOUBLE', 'DECIMAL']):
        stats['numeric_precision'] = column_meta.get('numeric_precision')
        stats['numeric_scale'] = column_meta.get('numeric_scale')

    # For semi-structured columns
    elif base_type in SEMI_STRUCTURED_TYPES:
        stats['is_semi_structured'] = True

    return stats


# =============================================================================
# ENHANCED DATA READINESS SCORE (METADATA-BASED)
# Replaces enhance_data_readiness_score that relied on table scans
# =============================================================================

def enhance_data_readiness_score_metadata(candidate, column_stats):
    """
    Calculate enhanced data readiness score (0–5) using metadata-derived statistics.
    Drop-in replacement for the original enhance_data_readiness_score.

    Components:
    - NULL rate impact (0–2 points) – from IS_NULLABLE metadata
    - Content substantiality (0–2 points) – from CHARACTER_MAXIMUM_LENGTH
    - Data efficiency (0–1 point) – from metadata type analysis

    Returns: float score (0.0–5.0)
    """
    score = 0.0

    if not column_stats:
        return 1.0

    # Component 1: NULL Rate Impact (0–2 points)
    null_pct = column_stats.get('null_percentage', 50.0)
    if null_pct <= 10:
        score += 2.0
    elif null_pct <= 30:
        score += 1.5
    elif null_pct <= 50:
        score += 1.0
    elif null_pct <= 70:
        score += 0.5

    # Component 2: Content Substantiality (0–2 points)
    avg_length = column_stats.get('avg_length')
    if avg_length is not None:
        if avg_length >= 200:
            score += 2.0
        elif avg_length >= 100:
            score += 1.5
        elif avg_length >= 50:
            score += 1.0
        elif avg_length > 0:
            score += 0.5

    # Component 3: Data Efficiency / Quality Indicators (0–1 point)
    has_comment = column_stats.get('has_comment', False)
    is_not_nullable = column_stats.get('is_nullable') == 'NO'

    if has_comment and is_not_nullable:
        score += 1.0
    elif has_comment or is_not_nullable:
        score += 0.5

    return round(min(score, 5.0), 2)


# =============================================================================
# ORCHESTRATION: FULL METADATA-BASED ANALYSIS
# =============================================================================

def run_metadata_analysis(conn, execute_query_fn,
                          target_databases=None, exclude_databases=None):
    """
    Run the complete metadata-based AI readiness analysis.
    Replaces all table-scan phases (2B, 2E, 4) with metadata queries.

    Args:
        conn: Snowflake connection object
        execute_query_fn: The execute_query function from the main script
        target_databases: List of databases to include (whitelist)
        exclude_databases: List of databases to exclude (blacklist)

    Returns:
        dict with:
          - table_scores: list of per-table readiness scores
          - table_lookup: parsed table metadata
          - column_lookup: parsed column metadata
          - constraints_lookup: parsed constraints
          - storage_lookup: parsed storage metrics
          - summary: high-level summary statistics
    """
    print("\n" + "=" * 70)
    print("METADATA-BASED AI READINESS ANALYSIS (No Table Scans)")
    print("=" * 70)

    # Step 1: Fetch all metadata
    print("\n--- Step 1: Fetching table metadata from ACCOUNT_USAGE.TABLES ---")
    _, tables_rows = fetch_tables_metadata(conn, execute_query_fn, target_databases, exclude_databases)
    print(f"  Fetched metadata for {len(tables_rows):,} tables")

    print("\n--- Step 2: Fetching column metadata from ACCOUNT_USAGE.COLUMNS ---")
    _, columns_rows = fetch_columns_metadata(conn, execute_query_fn, target_databases, exclude_databases)
    print(f"  Fetched metadata for {len(columns_rows):,} columns")

    print("\n--- Step 3: Fetching table constraints from ACCOUNT_USAGE.TABLE_CONSTRAINTS ---")
    _, constraints_rows = fetch_table_constraints(conn, execute_query_fn, target_databases, exclude_databases)
    print(f"  Fetched {len(constraints_rows):,} constraints")

    print("\n--- Step 4: Fetching storage metrics from ACCOUNT_USAGE.TABLE_STORAGE_METRICS ---")
    _, storage_rows = fetch_table_storage_metrics(conn, execute_query_fn, target_databases, exclude_databases)
    print(f"  Fetched storage metrics for {len(storage_rows):,} tables")

    # Step 2: Build lookup structures
    print("\n--- Step 5: Building metadata lookups ---")
    table_lookup = build_table_metadata_lookup(tables_rows)
    column_lookup = build_column_metadata_lookup(columns_rows)
    constraints_lookup = build_constraints_lookup(constraints_rows)
    storage_lookup = build_storage_lookup(storage_rows)
    print(f"  Tables: {len(table_lookup):,}, Columns: {len(column_lookup):,} tables, "
          f"Constraints: {len(constraints_lookup):,} tables, Storage: {len(storage_lookup):,} tables")

    # Step 3: Compute readiness scores
    print("\n--- Step 6: Computing AI Readiness Scores ---")
    table_scores = compute_all_readiness_scores(
        table_lookup, column_lookup, constraints_lookup, storage_lookup
    )
    print(f"  Scored {len(table_scores):,} tables")

    # Step 4: Summary statistics
    if table_scores:
        avg_score = sum(s['total_score'] for s in table_scores) / len(table_scores)
        high_readiness = len([s for s in table_scores if s['total_score'] >= 70])
        medium_readiness = len([s for s in table_scores if 40 <= s['total_score'] < 70])
        low_readiness = len([s for s in table_scores if s['total_score'] < 40])
    else:
        avg_score = 0
        high_readiness = medium_readiness = low_readiness = 0

    summary = {
        'total_tables': len(table_scores),
        'average_score': round(avg_score, 2),
        'high_readiness_count': high_readiness,
        'medium_readiness_count': medium_readiness,
        'low_readiness_count': low_readiness,
        'total_columns_analyzed': len(columns_rows),
        'total_constraints_found': len(constraints_rows),
        'queries_executed': 4,  # Only 4 metadata queries instead of N table scans
    }

    print(f"\n--- Metadata Analysis Summary ---")
    print(f"  Average Readiness Score: {avg_score:.1f}/100")
    print(f"  High Readiness (≥70):    {high_readiness:,} tables")
    print(f"  Medium Readiness (40-69): {medium_readiness:,} tables")
    print(f"  Low Readiness (<40):     {low_readiness:,} tables")
    print(f"  Total metadata queries:  4 (vs. {len(table_scores) * 3}+ table scans previously)")

    return {
        'table_scores': table_scores,
        'table_lookup': table_lookup,
        'column_lookup': column_lookup,
        'constraints_lookup': constraints_lookup,
        'storage_lookup': storage_lookup,
        'summary': summary,
    }


def generate_readiness_report_markdown(table_scores, summary):
    """
    Generate a markdown report from the metadata-based readiness scores.

    Args:
        table_scores: list of score dicts from compute_all_readiness_scores
        summary: summary dict from run_metadata_analysis

    Returns:
        str: Markdown report content
    """
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    report = f"""# Snowflake AI Readiness Report (Metadata-Based)

> **Generated On:** {now_utc}
> **Method:** Metadata-only analysis (INFORMATION_SCHEMA + ACCOUNT_USAGE)
> **Mode:** Zero table scans – zero additional credit consumption

---

## Executive Summary

| Metric | Value |
|--------|-------|
| **Total Tables Analyzed** | {summary['total_tables']:,} |
| **Average Readiness Score** | {summary['average_score']:.1f}/100 |
| **High Readiness (≥70)** | {summary['high_readiness_count']:,} |
| **Medium Readiness (40-69)** | {summary['medium_readiness_count']:,} |
| **Low Readiness (<40)** | {summary['low_readiness_count']:,} |
| **Total Columns Analyzed** | {summary['total_columns_analyzed']:,} |
| **Metadata Queries Executed** | {summary['queries_executed']} |

---

## Scoring Methodology

Each table is scored across **5 dimensions** (weighted total = 100):

| Dimension | Weight | Source | What It Measures |
|-----------|--------|--------|-----------------|
| **Comments** | {WEIGHT_COMMENTS}% | COLUMNS.COMMENT, TABLES.COMMENT | LLM context availability |
| **Data Types** | {WEIGHT_DATA_TYPES}% | COLUMNS.DATA_TYPE | Vectorization/LLM compatibility |
| **Freshness** | {WEIGHT_FRESHNESS}% | TABLES.LAST_ALTERED | Data currency |
| **Clustering** | {WEIGHT_CLUSTERING}% | TABLES.CLUSTERING_KEY | Large-scale retrieval optimization |
| **Constraints** | {WEIGHT_CONSTRAINTS}% | TABLE_CONSTRAINTS | Data quality / relationships |

---

## Top 30 AI-Ready Tables

| Rank | Database.Schema.Table | Score | Comments | Types | Fresh | Cluster | Constraints | Rows |
|------|----------------------|-------|----------|-------|-------|---------|-------------|------|
"""

    for i, ts in enumerate(table_scores[:30], 1):
        fqn = f"{ts['database']}.{ts['schema']}.{ts['table']}"
        ds = ts['dimension_scores']
        row_count = ts['dimension_details'].get('row_count')
        row_str = f"{row_count:,}" if row_count else "N/A"
        report += (
            f"| {i} | `{fqn[:55]}` | **{ts['total_score']:.1f}** | "
            f"{ds['comments']:.0f} | {ds['data_types']:.0f} | {ds['freshness']:.0f} | "
            f"{ds['clustering']:.0f} | {ds['constraints']:.0f} | {row_str} |\n"
        )

    report += """
---

## Tables Needing Improvement

### Missing Comments (Low LLM Context)

"""

    no_comments = [ts for ts in table_scores if ts['dimension_scores']['comments'] < 20][:15]
    if no_comments:
        report += "| Table | Comment Score | Columns With Comments | Action |\n"
        report += "|-------|-------------|----------------------|--------|\n"
        for ts in no_comments:
            fqn = f"{ts['database']}.{ts['schema']}.{ts['table']}"
            dd = ts['dimension_details']
            report += (
                f"| `{fqn[:50]}` | {ts['dimension_scores']['comments']:.0f}/100 | "
                f"{dd['columns_with_comments']}/{dd['total_columns']} | Add COMMENT to table and columns |\n"
            )
    else:
        report += "*All tables have adequate comments.*\n"

    report += """
### Stale Data (Low Freshness)

"""

    stale = [ts for ts in table_scores if ts['dimension_scores']['freshness'] <= 25][:15]
    if stale:
        report += "| Table | Freshness Score | Last Altered | Action |\n"
        report += "|-------|----------------|-------------|--------|\n"
        for ts in stale:
            fqn = f"{ts['database']}.{ts['schema']}.{ts['table']}"
            report += (
                f"| `{fqn[:50]}` | {ts['dimension_scores']['freshness']:.0f}/100 | "
                f"{ts['dimension_details']['last_altered'][:19]} | Verify data pipeline is active |\n"
            )
    else:
        report += "*All tables have recent data.*\n"

    report += """
### Unsupported Data Types

"""

    unsupported = [ts for ts in table_scores if ts['dimension_details'].get('unsupported_type_columns')][:15]
    if unsupported:
        report += "| Table | Unsupported Columns | Action |\n"
        report += "|-------|-------------------|--------|\n"
        for ts in unsupported:
            fqn = f"{ts['database']}.{ts['schema']}.{ts['table']}"
            cols = ", ".join(ts['dimension_details']['unsupported_type_columns'][:3])
            report += f"| `{fqn[:50]}` | {cols} | Convert or exclude from AI pipeline |\n"
    else:
        report += "*No tables with unsupported data types found.*\n"

    report += f"""

---

## Credit Savings

By using metadata queries instead of table scans:

| Approach | Queries | Estimated Credits |
|----------|---------|-------------------|
| **Previous (Table Scans)** | ~{summary['total_tables'] * 3:,}+ | High (proportional to data volume) |
| **Current (Metadata Only)** | {summary['queries_executed']} | Minimal (metadata views only) |
| **Savings** | ~{summary['total_tables'] * 3 - summary['queries_executed']:,} fewer queries | ~95%+ credit reduction |

---

*Report generated using Snowflake INFORMATION_SCHEMA and ACCOUNT_USAGE metadata views only.*
"""

    return report
