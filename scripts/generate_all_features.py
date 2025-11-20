#!/usr/bin/env python
"""
Generate all_features.py from Redshift metadata CSV (FIXED VERSION)
Usage: python generate_all_features_fixed.py metadata.csv
"""

import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

# Redshift to Feast type mapping
TYPE_MAPPING = {
    "integer": "Int32",
    "bigint": "Int64",
    "smallint": "Int32",
    "double precision": "Float64",
    "numeric": "Float64",
    "character varying": "String",
    "date": "UnixTimestamp",
    "timestamp without time zone": "UnixTimestamp",
}


def identify_entity_info(table_name: str, columns: list) -> dict:
    """Identify entity information for a table"""

    entity_info = {
        "entity_name": None,
        "entity_column": None,
        "timestamp_column": "ts_ds",
    }

    # Determine primary entity based on table prefix
    if table_name.startswith("business_"):
        entity_info["entity_name"] = "business"
        entity_info["entity_column"] = "id_business_id"
    elif table_name.startswith("pro_"):
        entity_info["entity_name"] = "worker"
        entity_info["entity_column"] = "id_worker_id"
    elif table_name.startswith("shift_"):
        entity_info["entity_name"] = "shift"
        entity_info["entity_column"] = "id_shift_id"

    return entity_info


def generate_all_features_file(csv_path: str, output_path: str = "all_features.py"):
    """Generate complete all_features.py from CSV metadata"""

    print(f"📖 Reading metadata from {csv_path}")
    df = pd.read_csv(csv_path)

    print(f"📊 Found {len(df)} columns across {df['table_name'].nunique()} tables")

    schema = df["table_schema"].iloc[0]
    print(f"📋 Schema: {schema}")

    # Collect all unique entities
    entities = set()
    for table_name in df["table_name"].unique():
        table_cols = df[df["table_name"] == table_name]["column_name"].tolist()
        entity_info = identify_entity_info(table_name, table_cols)
        if entity_info["entity_name"]:
            entities.add((entity_info["entity_name"], entity_info["entity_column"]))

    # Collect all unique Feast types used
    all_types = set()
    for dtype in df["data_type"].unique():
        feast_type = TYPE_MAPPING.get(dtype, "String")
        all_types.add(feast_type)

    # Start building the file
    code = f'''"""
Feast Feature Definitions - All Features
Auto-generated from Redshift metadata on {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

This file contains all entity and feature view definitions for the Feast feature store.

Statistics:
- Tables: {df["table_name"].nunique()}
- Total Columns: {len(df)}
- Entities: {len(entities)}
- Schema: {schema}
"""

from feast import Entity, FeatureView, Field, RedshiftSource, ValueType
from feast.types import {", ".join(sorted(all_types))}
from datetime import timedelta

# ============================================================================
# ENTITIES
# ============================================================================

'''

    # Add entity definitions with value_type
    for entity_name, entity_col in sorted(entities):
        code += f'''{entity_name}_entity = Entity(
    name="{entity_name}",
    join_keys=["{entity_col}"],
    value_type=ValueType.INT32,
    description="{entity_name.capitalize()} entity"
)

'''

    # Group tables by entity type for organization
    business_tables = []
    worker_tables = []
    shift_tables = []

    for table_name in sorted(df["table_name"].unique()):
        if table_name.startswith("business_"):
            business_tables.append(table_name)
        elif table_name.startswith("pro_"):
            worker_tables.append(table_name)
        elif table_name.startswith("shift_"):
            shift_tables.append(table_name)

    # Generate Business Feature Views
    if business_tables:
        code += """# ============================================================================
# BUSINESS FEATURE VIEWS
# ============================================================================

"""
        for table_name in business_tables:
            code += generate_feature_view_code(df, table_name, schema)

    # Generate Worker/Pro Feature Views
    if worker_tables:
        code += """# ============================================================================
# WORKER/PRO FEATURE VIEWS
# ============================================================================

"""
        for table_name in worker_tables:
            code += generate_feature_view_code(df, table_name, schema)

    # Generate Shift Feature Views
    if shift_tables:
        code += """# ============================================================================
# SHIFT FEATURE VIEWS
# ============================================================================

"""
        for table_name in shift_tables:
            code += generate_feature_view_code(df, table_name, schema)

    # Add lists at the end
    all_fv_names = [
        table_name.replace("_inference", "") + "_fv"
        for table_name in df["table_name"].unique()
    ]

    code += """# ============================================================================
# ALL FEATURE VIEWS AND ENTITIES
# ============================================================================

all_feature_views = [
"""
    for fv_name in sorted(all_fv_names):
        code += f"    {fv_name},\n"

    code += """]

all_entities = [
"""
    for entity_name, _ in sorted(entities):
        code += f"    {entity_name}_entity,\n"

    code += """]

# Feature view count by entity
feature_view_counts = {
"""
    for entity_name, _ in sorted(entities):
        count = sum(1 for fv in all_fv_names if entity_name in fv)
        code += f'    "{entity_name}": {count},\n'

    code += """}
"""

    # Write to file
    output_file = Path(output_path)
    output_file.write_text(code)

    print(f"\n✅ Generated {output_path}")
    print(f"📊 Statistics:")
    print(f"   - Entities: {len(entities)}")
    print(f"   - Feature Views: {len(all_fv_names)}")
    print(f"   - Total Features: {len(df)}")
    print(f"   - Business Feature Views: {len(business_tables)}")
    print(f"   - Worker Feature Views: {len(worker_tables)}")
    print(f"   - Shift Feature Views: {len(shift_tables)}")


def generate_feature_view_code(df: pd.DataFrame, table_name: str, schema: str) -> str:
    """Generate code for a single feature view"""

    table_df = df[df["table_name"] == table_name]
    columns = table_df["column_name"].tolist()
    data_types = table_df["data_type"].tolist()

    entity_info = identify_entity_info(table_name, columns)

    if not entity_info["entity_name"]:
        return f"# Skipped {table_name} - no entity identified\n\n"

    # Separate entity/timestamp columns from feature columns
    exclude_cols = {entity_info["entity_column"], entity_info["timestamp_column"]}

    features = []
    for col, dtype in zip(columns, data_types):
        if col not in exclude_cols:
            feast_type = TYPE_MAPPING.get(dtype, "String")
            features.append((col, feast_type))

    if not features:
        return f"# Skipped {table_name} - no feature columns\n\n"

    feature_view_name = table_name.replace("_inference", "")

    code = f'''# {feature_view_name.replace("_", " ").title()}
{feature_view_name}_source = RedshiftSource(
    name="{feature_view_name}_source",
    schema="{schema}",
    table="{table_name}",
    timestamp_field="{entity_info["timestamp_column"]}",
)

{feature_view_name}_fv = FeatureView(
    name="{feature_view_name}",
    entities=[{entity_info["entity_name"]}_entity],
    ttl=timedelta(days=365),
    schema=[
'''

    # Add all feature fields
    for col, feast_type in features:
        code += f'        Field(name="{col}", dtype={feast_type}),\n'

    code += f'''    ],
    source={feature_view_name}_source,
    tags={{
        "source": "redshift",
        "table": "{table_name}",
        "entity": "{entity_info["entity_name"]}"
    }},
)

'''

    return code


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(
            "Usage: python generate_all_features_fixed.py <metadata.csv> [output_file]"
        )
        print("\nExample:")
        print("  python generate_all_features_fixed.py metadata.csv")
        print("  python generate_all_features_fixed.py metadata.csv all_features.py")
        sys.exit(1)

    csv_path = sys.argv[1]
    output_path = sys.argv[2] if len(sys.argv) > 2 else "all_features.py"

    if not Path(csv_path).exists():
        print(f"❌ Error: File not found: {csv_path}")
        sys.exit(1)

    # Generate the all_features.py file
    generate_all_features_file(csv_path, output_path)

    print("\n" + "=" * 60)
    print("🎉 Generation complete!")
    print("=" * 60)
    print(f"\n📝 File created: {output_path}")
    print("\n💡 Next steps:")
    print("   1. Review the generated all_features.py")
    print("   2. Update feature_store.yaml with your credentials")
    print("   3. Run: feast apply")
    print("   4. Check: feast feature-views list")
