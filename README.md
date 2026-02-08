# Snowflake AI Readiness Agent

> **Version:** 2.0 | **Architecture:** Autonomous Agent | **Timestamps:** UTC

An autonomous agent for analyzing Snowflake environments to identify AI/ML enablement opportunities using Snowflake Cortex AI services.

## Overview

This agent analyzes your Snowflake environment to identify opportunities for AI/ML enablement using Snowflake's native Cortex AI features:

- **Cortex LLM** - Text summarization, classification, sentiment analysis
- **Cortex Search/RAG** - Semantic search and retrieval-augmented generation
- **Cortex ML** - Time-series forecasting and anomaly detection
- **Cortex Extract** - Semi-structured data processing (JSON, XML, VARIANT)
- **Document AI** - Document processing from stages (PDF, DOCX, images)

### Key Features

| Feature | Description |
|---------|-------------|
| **Agent Architecture** | Structured application with CLI interface and autonomous execution |
| **YAML Configuration** | Simple `config.yaml` for all settings - no environment variables needed |
| **Database Filtering** | Analyze specific databases or exclude system databases |
| **Run Modes** | Fresh (overwrite) or Append (incremental) analysis modes |
| **Dry Run Mode** | Validate configuration and estimate scope before full analysis |
| **Deep Data Profiling** | Validate candidates with actual data (sparsity, cardinality, content type) |
| **Confirmed Candidates** | Distinguish metadata-based candidates from data-validated ones |
| **Progress Tracking** | Real-time progress bar with current object and running statistics |
| **Report Index** | Main README.md with analysis summary and links to all reports |
| **UTC Timestamps** | All output documentation uses standardized UTC format |

## Quick Start

```bash
# 1. Clone and setup
git clone <repository-url>
cd snowflake_ai_enablement_analysis
python -m venv venv && source venv/bin/activate
pip install -r config/requirements.txt

# 2. Configure
cp config.example.yaml config.yaml
# Edit config.yaml with your Snowflake credentials

# 3. Dry run (recommended first)
python scripts/snowflake_full_analysis.py --dry-run

# 4. Full analysis
python scripts/snowflake_full_analysis.py
```

### Command Line Options

```bash
python scripts/snowflake_full_analysis.py [OPTIONS]

Options:
  -c, --config PATH    Path to YAML configuration file (default: config.yaml)
  -d, --dry-run        Run in dry-run mode (validate only)
  -q, --quiet          Suppress non-essential output
  -v, --version        Show agent version and exit
  -h, --help           Show help message
```

> 📖 **For detailed instructions, see [docs/USER_GUIDE.md](docs/USER_GUIDE.md)**

## Repository Structure

```
snowflake_ai_enablement_analysis/
├── README.md                        # This file
├── LICENSE                          # MIT License
├── config.example.yaml              # Configuration template (copy to config.yaml)
├── .gitignore                       # Git ignore rules (includes config.yaml)
├── scripts/
│   ├── snowflake_full_analysis.py  # Main analysis script
│   └── create_presentation.py      # Presentation generator
├── config/
│   └── requirements.txt            # Python dependencies
├── docs/
│   ├── USER_GUIDE.md               # Comprehensive user guide
│   └── detailed_prompts.md         # Feature documentation
└── snowflake-ai-enablement-reports/   # Standard output destination
    ├── README.md                   # Main report index (START HERE)
    ├── reports/
    │   ├── executive_summary.md    # High-level findings
    │   ├── detailed_analysis_report.md  # Comprehensive analysis with reasoning
    │   ├── ai_strategy_roadmap.md  # Phased implementation plan
    │   ├── data_quality_dashboard.md
    │   └── scoring_comparison.md
    ├── profiles/
    │   ├── consolidated_profiles.md # Index of schema analyses
    │   └── *_analysis.md           # Per-schema analysis reports
    ├── metadata/
    │   ├── all_candidates.json     # All AI candidates
    │   ├── confirmed_candidates.json  # Data-validated candidates
    │   ├── enhanced_text_candidates.json
    │   ├── full_inventory.csv
    │   ├── stages_inventory.csv
    │   └── data_analysis_cache.json
    ├── logs/
    │   ├── audit_trail.sql         # Query audit log
    │   ├── analysis_errors.log
    │   └── analysis_summary.log
    └── presentation/               # Generated presentations
```

## Key Metrics

| Metric | Value |
|--------|-------|
| Databases Analyzed | 67 |
| Schemas Analyzed | 937 |
| Tables/Views | 13,927 |
| Total Columns | 424,279 |
| Data Stages | 269 |
| AI Candidates Identified | 149,342 |
| Schema Reports Generated | 921 |

## Data Analysis Features

The analyzer now includes comprehensive actual data analysis capabilities:

### What's Analyzed

- **NULL Rate Impact** - Measures data completeness (columns with >70% NULLs are flagged)
- **Content Substantiality** - Evaluates text length and richness for LLM/Search suitability
- **Data Efficiency** - Compares actual vs. defined column sizes to identify over-provisioned storage
- **Volume Assessment** - Calculates real data distributions for cost estimation

### How It Works

**Phase 1: Metadata Discovery**
- Inventory all databases, schemas, tables, columns, stages
- Identify ~149,342 AI candidates using metadata heuristics

**Phase 2: Actual Data Analysis (NEW)**
- Adaptive sampling (10K → 1K → 100 rows) with fallback strategy
- Incremental analysis with caching (skip already-analyzed candidates)
- Enhanced scoring based on real data quality
- Full scans on top 200 candidates for exact statistics

**Phase 3-6: Enhanced Reporting**
- All existing reports enhanced with actual data insights
- New data quality dashboard with actionable recommendations
- Before/after scoring comparison to show impact

### Key Benefits

- **Evidence-Based Decisions** - Recommendations backed by actual data analysis, not just metadata
- **Cost Optimization** - Identify over-provisioned columns wasting storage
- **Quality Filtering** - Avoid candidates with poor data quality (high NULLs, minimal content)
- **Incremental Analysis** - Cache enables re-running on new databases without re-analyzing everything

### Enhanced Scoring

Data Readiness scores (0-5 scale) now based on:

| Component | Weight | Criteria |
|-----------|--------|----------|
| NULL Rate Impact | 0-2 pts | ≤10% NULLs = 2.0, >70% NULLs = 0.0 |
| Content Substantiality | 0-2 pts | Avg ≥200 chars = 2.0, <50 chars = 0.5 |
| Data Efficiency | 0-1 pt | Actual >50% of defined = 1.0, <25% = 0.0 |

Result: Candidates with high NULL rates or minimal content drop in ranking; columns with rich, complete data rise to the top.

## AI Opportunities Summary

| Cortex Feature | Candidates | Use Cases |
|----------------|------------|-----------|
| Cortex LLM | 125,468 | Text summarization, classification, sentiment |
| Cortex Search/RAG | 9,925 | Semantic search, content retrieval |
| Cortex ML | 6,216 | Forecasting, anomaly detection |
| Cortex Extract | 2,566 | JSON/XML parsing, nested data |
| Document AI | 269+ | PDF/document processing from stages |

## Benefits of AI Enablement

### Cost Benefits

| Area | Benefit | Estimated Savings |
|------|---------|-------------------|
| **Infrastructure** | No separate ML platforms or GPU clusters | 40-60% reduction |
| **Data Movement** | Process data in place, no ETL to external tools | 70-80% reduction |
| **Development Time** | SQL-based AI vs. months of ML engineering | 60-80% faster |
| **Tool Licensing** | Consolidated in Snowflake credits | 30-50% reduction |
| **Operations** | Managed service, auto-scaling, no MLOps overhead | 50-70% reduction |

### Business Value

| Use Case | Impact |
|----------|--------|
| Content auto-classification | 90% reduction in manual tagging effort |
| Semantic search | 3x improvement in content discovery |
| Predictive analytics | 20-30% better forecasting accuracy |
| Document processing | 80% reduction in manual document review |
| Anomaly detection | 50% faster issue identification and resolution |

### Technical Advantages

- **Zero Data Movement** - AI processes data where it already lives in Snowflake
- **Unified Governance** - Leverage existing RBAC, row-level security, and data masking
- **SQL Interface** - Use familiar SQL syntax, no Python or ML expertise required
- **Auto-Scaling** - Handle any workload without manual provisioning
- **Built-in Audit** - Complete logging of all AI operations for compliance

### ROI Summary

| Metric | Typical Range |
|--------|---------------|
| Year 1 ROI | 150-400% |
| Payback Period | 3-6 months |
| Annual Cost Savings | $100K-500K (varies by scale) |

See the generated AI Strategy Roadmap in `snowflake-ai-enablement-reports/reports/ai_strategy_roadmap.md` for detailed ROI calculations and implementation costs.

## Prerequisites

| Requirement | Details |
|-------------|----------|
| **Python** | 3.8 or higher |
| **Snowflake Account** | With ACCOUNT_USAGE access |
| **Authentication** | Private key (recommended) or password |

### Required Snowflake Permissions

```sql
-- Minimum required: Access to ACCOUNT_USAGE for metadata discovery
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE <role_name>;
GRANT USAGE ON WAREHOUSE <warehouse_name> TO ROLE <role_name>;

-- For data profiling: SELECT access to target databases
GRANT USAGE ON DATABASE <database_name> TO ROLE <role_name>;
GRANT USAGE ON ALL SCHEMAS IN DATABASE <database_name> TO ROLE <role_name>;
GRANT SELECT ON ALL TABLES IN DATABASE <database_name> TO ROLE <role_name>;
```

## Installation

### Step 1: Clone Repository

```bash
git clone <repository-url>
cd snowflake_ai_enablement_analysis
```

### Step 2: Create Virtual Environment

```bash
# Create virtual environment
python -m venv venv

# Activate (macOS/Linux)
source venv/bin/activate

# Activate (Windows)
venv\Scripts\activate
```

### Step 3: Install Dependencies

```bash
pip install -r config/requirements.txt
```

Dependencies include:
- `snowflake-connector-python` - Snowflake connectivity
- `cryptography` - Private key authentication
- `pandas` - Data processing
- `PyYAML` - Configuration parsing

## Configuration

### Step 1: Create Configuration File

```bash
cp config.example.yaml config.yaml
```

> ⚠️ **Security Warning:** `config.yaml` contains sensitive credentials and is git-ignored. Never commit this file to version control.

### Step 2: Edit config.yaml

```yaml
# Snowflake Connection
snowflake:
  account: "your_account_identifier"    # e.g., "xy12345.us-east-1"
  user: "your_username"
  
  # Authentication: Choose ONE method
  # Option 1: Password (simple)
  password: "your_password"
  
  # Option 2: Private Key (recommended for production)
  # private_key_path: "~/.snowflake/keys/rsa_key.pem"
  # private_key_passphrase: "optional_passphrase"  # If key is encrypted
  
  warehouse: "COMPUTE_WH"
  role: "ACCOUNTADMIN"  # Or role with required permissions

# Database Filtering (Optional)
# Specify databases to analyze, or leave commented for all databases
# target_databases:
#   - "PRODUCTION_DB"
#   - "ANALYTICS_DB"

# Databases to exclude (system databases excluded by default)
exclude_databases:
  - "SNOWFLAKE"
  - "SNOWFLAKE_SAMPLE_DATA"

# Dry Run Mode - Start here!
dry_run:
  enabled: true  # Set to false for full analysis
```

### Step 3: Generate Private Key (If Using Key Authentication)

```bash
# Create directory
mkdir -p ~/.snowflake/keys

# Generate private key
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM \
  -out ~/.snowflake/keys/rsa_key.pem -nocrypt

# Generate public key
openssl rsa -in ~/.snowflake/keys/rsa_key.pem -pubout \
  -out ~/.snowflake/keys/rsa_key.pub

# Secure permissions
chmod 600 ~/.snowflake/keys/rsa_key.pem

# Register in Snowflake (run as SECURITYADMIN)
# ALTER USER your_user SET RSA_PUBLIC_KEY='<public_key_content>';
```

## Usage

### Dry Run (Recommended First)

Validate your configuration before running full analysis:

```bash
# Ensure dry_run.enabled: true in config.yaml
python scripts/snowflake_full_analysis.py
```

Dry run performs:
1. ✓ Connection validation
2. ✓ Database access verification
3. ✓ Scope estimation (tables, columns, candidates)
4. ✓ Runtime estimation
5. ✓ Sample query preview

### Full Analysis

```bash
# Set dry_run.enabled: false in config.yaml
python scripts/snowflake_full_analysis.py
```

Full analysis performs:
1. **Phase 1:** Metadata discovery (databases, schemas, tables, columns)
2. **Phase 2:** AI candidate identification
3. **Phase 3:** Deep data profiling (sparsity, cardinality, content validation)
4. **Phase 4:** Candidate confirmation and scoring
5. **Phase 5:** Report generation

### Run Modes: Fresh vs Append

Configure in `config.yaml`:

```yaml
run_mode:
  mode: "fresh"  # or "append"
```

| Mode | Behavior | Use Case |
|------|----------|----------|
| **fresh** | Overwrites existing reports | First run, complete re-analysis |
| **append** | Merges new results with existing | Incremental database analysis |

**Incremental Analysis Workflow:**

```bash
# Step 1: Analyze first set of databases
# config.yaml: target_databases: ["DB1", "DB2"], run_mode.mode: "fresh"
python scripts/snowflake_full_analysis.py

# Step 2: Add more databases incrementally
# config.yaml: target_databases: ["DB3", "DB4"], run_mode.mode: "append"
python scripts/snowflake_full_analysis.py

# Step 3: Continue adding until complete
# Results are merged into comprehensive report
```

### Output Files

After running, check these key outputs:

| File | Description |
|------|-------------|
| `snowflake-ai-enablement-reports/README.md` | **Start here** - Main report index |
| `snowflake-ai-enablement-reports/reports/executive_summary.md` | High-level findings for stakeholders |
| `snowflake-ai-enablement-reports/reports/detailed_analysis_report.md` | Comprehensive analysis with reasoning |
| `snowflake-ai-enablement-reports/reports/ai_strategy_roadmap.md` | Implementation plan with SQL examples |
| `snowflake-ai-enablement-reports/metadata/confirmed_candidates.json` | Data-validated AI candidates |
| `snowflake-ai-enablement-reports/metadata/all_candidates.json` | All candidates with confirmation status |

### Understanding Candidates

| Type | Description | Confidence |
|------|-------------|------------|
| **Confirmed Candidate** | Data profiling validates AI suitability | High |
| **Potential Candidate** | Metadata suggests AI potential, not yet validated | Medium |
| **Unconfirmed** | Data quality issues detected (high NULLs, short content) | Low |

## Safety Features

| Feature | Description |
|---------|-------------|
| **Read-only** | Only SELECT queries are executed |
| **Query validation** | All queries validated before execution |
| **Audit trail** | Complete log in `snowflake-ai-enablement-reports/logs/audit_trail.sql` |
| **No modifications** | Cannot INSERT, UPDATE, DELETE, or DROP |
| **Efficient sampling** | Uses SAMPLE, LIMIT, and HLL to minimize compute |

## Documentation

| Document | Description |
|----------|-------------|
| [User Guide](docs/USER_GUIDE.md) | Comprehensive usage guide |
| [Feature Documentation](docs/detailed_prompts.md) | Feature generation prompts and implementation details |

**Generated Reports** (after running analysis):
| Report | Location |
|--------|----------|
| Executive Summary | `snowflake-ai-enablement-reports/reports/executive_summary.md` |
| Detailed Analysis | `snowflake-ai-enablement-reports/reports/detailed_analysis_report.md` |
| AI Strategy Roadmap | `snowflake-ai-enablement-reports/reports/ai_strategy_roadmap.md` |
| Schema Profiles | `snowflake-ai-enablement-reports/profiles/consolidated_profiles.md` |

## Scoring Methodology

Each AI candidate is scored on four dimensions (0-5 scale each):

1. **Business Potential** - Expected value and impact
2. **Data Readiness** - Data quality and completeness
3. **Metadata Quality** - Documentation and discoverability
4. **Governance Risk** - PII/sensitivity considerations (inverted)

Total score ranges from 0-20, with higher scores indicating better candidates.

## Governance Considerations

The analysis identifies 7,784+ columns with potential PII or sensitive data. Review the governance sections in reports before enabling AI features on sensitive data.

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/improvement`)
3. Commit changes (`git commit -am 'Add improvement'`)
4. Push to branch (`git push origin feature/improvement`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

### Third-Party Licenses

This project uses the following open-source packages:

| Package | License |
|---------|---------|
| snowflake-connector-python | Apache 2.0 |
| cryptography | Apache 2.0 / BSD |
| pandas | BSD 3-Clause |

## Support

For issues and feature requests, please open an issue in the repository.

## Disclaimer

This tool performs read-only analysis of your Snowflake environment. It does not modify any data or configurations. Always review generated recommendations with your data governance team before implementation.
