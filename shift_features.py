"""
Shift Feature Views
Auto-generated from Redshift metadata on 2025-11-20 14:27:02

This file contains all shift-related feature view definitions.
"""

from feast import FeatureView, Field, RedshiftSource
from feast.types import Float64, Int32, Int64, String, UnixTimestamp
from datetime import timedelta
from entities import shift_entity

# SHIFT FEATURE VIEWS
# ============================================================================

# Shift Benefits Features
shift_benefits_features_source = RedshiftSource(
    name="shift_benefits_features_source",
    schema="dbt-cchia",
    table="shift_benefits_features_inference",
    timestamp_field="ts_ds",
)

shift_benefits_features_fv = FeatureView(
    name="shift_benefits_features",
    entities=[shift_entity],
    ttl=timedelta(days=365),
    schema=[
        Field(name="id_business_id", dtype=Int32),
        Field(name="id_company_id", dtype=Int32),
        Field(name="b_has_free_meals", dtype=Int32),
        Field(name="b_has_parking", dtype=Int32),
        Field(name="b_is_flexible_time_task", dtype=Int32),
    ],
    source=shift_benefits_features_source,
    tags={
        "source": "redshift",
        "table": "shift_benefits_features_inference",
        "entity": "shift"
    },
)

# Shift Core Features
shift_core_features_source = RedshiftSource(
    name="shift_core_features_source",
    schema="dbt-cchia",
    table="shift_core_features_inference",
    timestamp_field="ts_ds",
)

shift_core_features_fv = FeatureView(
    name="shift_core_features",
    entities=[shift_entity],
    ttl=timedelta(days=365),
    schema=[
        Field(name="id_shift_group_id", dtype=Int32),
        Field(name="id_worker_id", dtype=Int32),
        Field(name="id_business_id", dtype=Int32),
        Field(name="id_company_id", dtype=Int32),
        Field(name="id_position_id", dtype=Int32),
        Field(name="id_regionmapping_id", dtype=Int32),
        Field(name="ts_shift_created_at", dtype=UnixTimestamp),
        Field(name="ts_shift_starts_at", dtype=UnixTimestamp),
        Field(name="ts_shift_ends_at", dtype=UnixTimestamp),
        Field(name="ts_shift_group_created_at", dtype=UnixTimestamp),
        Field(name="b_w2_employees_only", dtype=Int32),
        Field(name="b_is_filled", dtype=Int32),
        Field(name="b_worker_no_show", dtype=Int32),
        Field(name="rv_float_rating_by_worker", dtype=Int32),
        Field(name="rv_float_rating_by_business", dtype=Int32),
        Field(name="b_has_rating_by_worker", dtype=Int32),
        Field(name="b_has_rating_by_business", dtype=Int32),
        Field(name="rv_float_business_rate_usd", dtype=Float64),
        Field(name="rv_float_applicant_rate_usd", dtype=Float64),
        Field(name="rv_int_created_to_start_hours", dtype=Int64),
        Field(name="rv_int_group_created_to_start_hours", dtype=Int64),
        Field(name="rv_int_shift_duration_hours", dtype=Int64),
        Field(name="rv_int_day_of_week", dtype=Int32),
        Field(name="rv_int_hour_of_day", dtype=Int32),
        Field(name="b_is_weekend", dtype=Int32),
        Field(name="b_is_daytime", dtype=Int32),
        Field(name="rv_int_user_shift_sequence", dtype=Int64),
        Field(name="rv_int_user_business_shift_sequence", dtype=Int64),
        Field(name="rv_int_user_position_shift_sequence", dtype=Int64),
        Field(name="rv_int_hours_since_previous_shift", dtype=Int64),
        Field(name="rv_int_hours_to_next_shift", dtype=Int64),
        Field(name="b_is_bad_quality_outcome", dtype=Int32),
        Field(name="mc_str_shift_style", dtype=String),
        Field(name="b_b_is_partial", dtype=Int32),
    ],
    source=shift_core_features_source,
    tags={
        "source": "redshift",
        "table": "shift_core_features_inference",
        "entity": "shift"
    },
)
