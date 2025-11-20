# Instawork Feast Feature Store

A production-ready feature store built with [Feast](https://feast.dev/) for managing machine learning features from Redshift data sources.

## Overview

This project provides a complete feature store infrastructure for Instawork, featuring:
- **959 features** across **23 feature views**
- **3 entity types**: Business, Worker (Pro), and Shift
- **Offline store**: Amazon Redshift
- **Online store**: Redis
- **Auto-generated feature definitions** from Redshift metadata

## Project Structure

```
feast_project/
├── feature_repo/              # Feast feature repository
│   ├── all_features.py        # Auto-generated feature definitions
│   ├── feature_store.yaml     # Feast configuration
│   ├── example_usage.py       # Usage examples
│   ├── requirements.txt       # Python dependencies
│   └── data/                  # Local data files and registry
├── scripts/                   # Utility scripts
│   ├── generate_all_features.py    # Generate features from CSV metadata
│   ├── feature_store_columns.csv   # Redshift metadata
│   ├── test_online_features.py     # Test online feature retrieval
│   └── ...                    # Other utility scripts
└── README.md                  # This file
```

## Prerequisites

- Python 3.13+
- AWS credentials configured (for Redshift access)
- Redis server running (for online store)
- Access to Redshift cluster: `instawork-dw`

## Setup

### 1. Create Virtual Environment

```bash
python -m venv feast_env
source feast_env/bin/activate  # On Windows: feast_env\Scripts\activate
```

### 2. Install Dependencies

```bash
cd feature_repo
pip install -r requirements.txt
```

### 3. Configure Feature Store

Edit `feature_repo/feature_store.yaml` with your credentials:

```yaml
offline_store:
  type: redshift
  cluster_id: instawork-dw
  region: us-west-2
  database: instawork
  user: your_username
  s3_staging_location: s3://your-bucket/feast-staging
  iam_role: arn:aws:iam::ACCOUNT:role/YourRole

online_store:
  type: redis
  connection_string: "localhost:6379"  # Update if Redis is elsewhere
```

### 4. Apply Feature Definitions

```bash
cd feature_repo
feast apply
```

This will register all feature views and entities with Feast.

## Generating Features from Metadata

The feature definitions in `all_features.py` are auto-generated from Redshift metadata.

### Regenerate Features

If you update the Redshift schema or add new tables:

```bash
cd scripts
python generate_all_features.py feature_store_columns.csv ../feature_repo/all_features.py
```

This script:
- Reads table metadata from CSV
- Identifies entities (business, worker, shift) based on table prefixes
- Maps Redshift data types to Feast types
- Generates complete feature view definitions
- Creates organized, production-ready code

### CSV Format

The `feature_store_columns.csv` should contain:
- `table_schema`: Schema name (e.g., `dbt-cchia`)
- `table_name`: Table name (e.g., `business_features_inference`)
- `column_name`: Column name
- `data_type`: Redshift data type

## Using the Feature Store

### Initialize Feature Store

```python
from feast import FeatureStore

store = FeatureStore(repo_path="feature_repo")
```

### Get Online Features

```python
# Get features for a worker
entity_rows = [{"id_worker_id": 12345}]

features = store.get_online_features(
    features=[
        "pro_core_features:b_is_email_verified",
        "pro_core_features:rv_int_account_age_days",
        "pro_quiz_features:rv_float_avg_quiz_score",
    ],
    entity_rows=entity_rows,
).to_dict()

print(features)
```

### Get Historical Features (for Training)

```python
import pandas as pd
from datetime import datetime, timedelta

# Create entity dataframe
entity_df = pd.DataFrame({
    "id_worker_id": [12345, 67890],
    "event_timestamp": [datetime.now() - timedelta(days=1)] * 2
})

# Get historical features
training_df = store.get_historical_features(
    entity_df=entity_df,
    features=[
        "pro_core_features:b_is_email_verified",
        "pro_core_features:rv_int_account_age_days",
        "business_features:rv_float_fill_rate",
    ],
).to_df()

print(training_df)
```

### List Available Features

```python
# List all feature views
for fv in store.list_feature_views():
    print(f"{fv.name}: {len(fv.schema)} features")
    print(f"  Entity: {[e.name for e in fv.entities]}")
```

See `feature_repo/example_usage.py` for more examples.

## Feature Views

### Business Features (3 views)
- `business_features`: Core business metrics
- `business_hypertrack_features`: Hypertrack-related features
- `business_no_show_features`: No-show rate features

### Worker/Pro Features (18 views)
- `pro_core_features`: Core worker attributes
- `pro_quiz_features`: Quiz performance
- `pro_experience_features`: Experience metrics
- `pro_attire_features`: Attire-related features
- `pro_amplitude_features`: Amplitude analytics
- `pro_hypertrack_features`: Location tracking
- `pro_education_features`: Education background
- `pro_company_features`: Company associations
- `pro_ticket_features`: Ticket/support features
- `pro_referral_features`: Referral metrics
- `pro_shift_outcome_features`: Shift completion rates
- `pro_quality_ratings_features`: Quality ratings
- `pro_business_features`: Business interactions
- `pro_time_features`: Time-based metrics
- `pro_skill_vector_features`: Skill vectors
- `pro_shift_features`: Shift participation
- `pro_position_rating_features`: Position ratings
- `pro_resume_features`: Resume data

### Shift Features (2 views)
- `shift_core_features`: Core shift information
- `shift_benefits_features`: Benefits information

## Entities

- **business**: `id_business_id` (Int32)
- **worker**: `id_worker_id` (Int32)
- **shift**: `id_shift_id` (Int32)

## Scripts

### `scripts/generate_all_features.py`
Generates `all_features.py` from Redshift metadata CSV.

**Usage:**
```bash
python generate_all_features.py feature_store_columns.csv [output_file]
```

### `scripts/test_online_features.py`
Test script for retrieving online features.

### `scripts/check_redis_worker.py`
Utility to check Redis worker status.

### `scripts/feast_ui.py`
Launch Feast UI for exploring features.

## Common Commands

```bash
# Apply feature definitions
feast apply

# List feature views
feast feature-views list

# List entities
feast entities list

# Materialize features to online store
feast materialize-incremental $(date -u +"%Y-%m-%dT%H:%M:%S")

# Launch Feast UI
feast ui
```

## Data Flow

1. **Source**: Data in Redshift tables (schema: `dbt-cchia`)
2. **Offline Store**: Redshift (for historical features and training data)
3. **Online Store**: Redis (for low-latency feature serving)
4. **Materialization**: Features are materialized from Redshift to Redis

## Troubleshooting

### Table Not Found Error
If you see `DataSourceNotFoundException`, verify:
- Schema name is correct in `feature_store.yaml`
- Table exists in Redshift
- AWS credentials are configured
- IAM role has Redshift access

### Redis Connection Error
- Ensure Redis is running: `redis-cli ping`
- Check connection string in `feature_store.yaml`
- Verify Redis is accessible from your network

### Feature Generation Issues
- Verify CSV format matches expected schema
- Check that table names follow naming conventions:
  - `business_*` → business entity
  - `pro_*` → worker entity
  - `shift_*` → shift entity

## Development

### Adding New Features

1. Add table to Redshift
2. Update `scripts/feature_store_columns.csv` with new metadata
3. Regenerate features: `python scripts/generate_all_features.py ...`
4. Review generated code in `feature_repo/all_features.py`
5. Apply: `feast apply`

### Testing

```bash
# Test online feature retrieval
python scripts/test_online_features.py

# Test feature generation
python scripts/generate_all_features.py scripts/feature_store_columns.csv /tmp/test_features.py
```

## Resources

- [Feast Documentation](https://docs.feast.dev/)
- [Feast GitHub](https://github.com/feast-dev/feast)
- [Redshift Documentation](https://docs.aws.amazon.com/redshift/)

## License

Internal use only - Instawork

