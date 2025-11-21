"""
Business Feature Views
Auto-generated from Redshift metadata on 2025-11-20 14:27:02

This file contains all business-related feature view definitions.
"""

from feast import FeatureView, Field, RedshiftSource
from feast.types import Float64, Int32, Int64, String, UnixTimestamp
from datetime import timedelta
from entities import business_entity

# ============================================================================
# BUSINESS FEATURE VIEWS
# ============================================================================

# Business Features
business_features_source = RedshiftSource(
    name="business_features_source",
    schema="dbt-cchia",
    table="business_features_inference",
    timestamp_field="ts_ds",
)

business_features_fv = FeatureView(
    name="business_features",
    entities=[business_entity],
    ttl=timedelta(days=365),
    schema=[
        Field(name="id_company_id", dtype=Int32),
        Field(name="mc_int_business_region", dtype=Int32),
        Field(name="mc_str_business_region_name", dtype=String),
        Field(name="mc_str_business_name", dtype=String),
        Field(name="mc_str_business_display_name", dtype=String),
        Field(name="mc_str_business_type", dtype=String),
        Field(name="rv_int_total_shifts", dtype=Int64),
        Field(name="rv_int_total_filled_shifts", dtype=Int64),
        Field(name="rv_int_total_completed_shifts", dtype=Int64),
        Field(name="rv_float_fill_rate", dtype=Float64),
        Field(name="rv_float_avg_business_rating_by_worker", dtype=Int64),
        Field(name="rv_float_avg_worker_rating_by_business", dtype=Int64),
        Field(name="mc_str_partner_status", dtype=String),
        Field(name="mc_str_partner_period", dtype=String),
        Field(name="mc_str_partner_type", dtype=String),
        Field(name="mc_str_primary_industry", dtype=String),
        Field(name="mc_str_secondary_industry", dtype=String),
        Field(name="rv_int_unique_workers", dtype=Int64),
        Field(name="rv_int_total_unique_workers", dtype=Int64),
        Field(name="ts_first_shift_date", dtype=UnixTimestamp),
        Field(name="ts_last_shift_date", dtype=UnixTimestamp),
        Field(name="rv_int_days_since_first_shift", dtype=Int64),
        Field(name="rv_int_days_since_last_shift", dtype=Int64),
        Field(name="rv_int_shifts_l90d", dtype=Int64),
        Field(name="rv_int_w2_employees_only", dtype=Int64),
        Field(name="rv_int_total_ratings_by_workers", dtype=Int64),
        Field(name="rv_int_total_ratings_by_business", dtype=Int64),
        Field(name="rv_float_avg_filled_shift_business_rate", dtype=Float64),
        Field(name="rv_float_avg_unfilled_shift_business_rate", dtype=Float64),
        Field(name="rv_float_avg_filled_shift_applicant_rate", dtype=Float64),
        Field(name="rv_float_avg_unfilled_shift_applicant_rate", dtype=Float64),
        Field(name="rv_int_num_gigs", dtype=Int64),
        Field(name="rv_int_num_gigs_posted", dtype=Int64),
        Field(name="rv_int_num_gigs_filled", dtype=Int64),
        Field(name="rv_float_gig_fill_rate", dtype=Float64),
        Field(name="rv_int_unique_companies", dtype=Int64),
        Field(name="rv_int_unique_shift_days", dtype=Int64),
        Field(name="rv_int_business_cumulative_filled_shifts", dtype=Int64),
        Field(name="mc_str_business_timezone", dtype=String),
        Field(name="rv_int_daily_shifts", dtype=Int64),
        Field(name="rv_int_days_since_last_ud", dtype=Int64),
        Field(name="ts_gig_date", dtype=UnixTimestamp),
        Field(name="rv_int_cancelled_filled_shifts", dtype=Int64),
        Field(name="rv_int_total_cancelled_shifts", dtype=Int64),
        Field(name="rv_int_n_booked_shifts", dtype=Int64),
        Field(name="rv_int_n_completed_shifts", dtype=Int64),
        Field(name="rv_int_active_pros", dtype=Int64),
        Field(name="rv_int_deactivated_pros", dtype=Int64),
        Field(name="rv_int_inferred_active_pros", dtype=Int64),
        Field(name="rv_int_rejected_pros", dtype=Int64),
        Field(name="rv_int_removed_pros", dtype=Int64),
    ],
    source=business_features_source,
    tags={
        "source": "redshift",
        "table": "business_features_inference",
        "entity": "business"
    },
)

# Business Hypertrack Features
business_hypertrack_features_source = RedshiftSource(
    name="business_hypertrack_features_source",
    schema="dbt-cchia",
    table="business_hypertrack_features_inference",
    timestamp_field="ts_ds",
)

business_hypertrack_features_fv = FeatureView(
    name="business_hypertrack_features",
    entities=[business_entity],
    ttl=timedelta(days=365),
    schema=[
        Field(name="rv_int_n_business_shifts", dtype=Int64),
        Field(name="rv_float_avg_business_tracking_rate", dtype=Float64),
        Field(name="rv_float_avg_business_time_in_fence", dtype=Float64),
        Field(name="rv_float_avg_business_active_time", dtype=Float64),
        Field(name="rv_float_avg_business_total_duration", dtype=Float64),
        Field(name="rv_float_avg_business_in_fence_rate", dtype=Float64),
    ],
    source=business_hypertrack_features_source,
    tags={
        "source": "redshift",
        "table": "business_hypertrack_features_inference",
        "entity": "business"
    },
)

# Business No Show Features
business_no_show_features_source = RedshiftSource(
    name="business_no_show_features_source",
    schema="dbt-cchia",
    table="business_no_show_features_inference",
    timestamp_field="ts_ds",
)

business_no_show_features_fv = FeatureView(
    name="business_no_show_features",
    entities=[business_entity],
    ttl=timedelta(days=365),
    schema=[
        Field(name="rv_int_cumulative_auto_no_shows", dtype=Int64),
        Field(name="rv_int_business_cumulative_filled_shifts", dtype=Int64),
        Field(name="rv_int_cumulative_corrected_auto_no_shows", dtype=Int64),
        Field(name="rv_int_cumulative_corrected_manual_no_shows", dtype=Int64),
        Field(name="rv_int_cumulative_manual_no_shows", dtype=Int64),
        Field(name="rv_float_correction_rate", dtype=Float64),
        Field(name="rv_float_manual_no_show_rate", dtype=Float64),
        Field(name="rv_float_auto_no_show_rate", dtype=Float64),
    ],
    source=business_no_show_features_source,
    tags={
        "source": "redshift",
        "table": "business_no_show_features_inference",
        "entity": "business"
    },
)

