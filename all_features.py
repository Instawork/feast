# all_features.py
from datetime import timedelta

from feast import Entity, FeatureView, Field, RedshiftSource, ValueType
from feast.types import Float32, Float64, Int32, Int64, String

# ============================================================================
# ENTITIES
# ============================================================================

worker = Entity(
    name="id_worker_id",
    description="Instawork Professional identifier",
    value_type=ValueType.INT64,
)

business = Entity(
    name="id_business_id",
    description="Instawork Business identifier",
    value_type=ValueType.INT64,
)

shift = Entity(
    name="id_shift_id",
    description="Instawork Shift identifier",
    value_type=ValueType.INT64,
)

# ============================================================================
# PRO EDUCATION FEATURES
# ============================================================================

# pro_education_source = RedshiftSource(
#     name="pro_education_source",
#     database="instawork",
#     schema="dbt-cchia",
#     table="pro_education_features_inference",
#     timestamp_field="ts_ds",
# )

pro_education_source = RedshiftSource(
    name="pro_education_source",
    query="""
        SELECT *
        FROM "dbt-cchia"."pro_education_features_inference"
        WHERE id_worker_id IN (
            SELECT DISTINCT id_worker_id
            FROM "dbt-cchia"."pro_education_features_inference"
            LIMIT 10
        )
        AND ts_ds = (SELECT MAX(ts_ds) FROM "dbt-cchia"."pro_education_features_inference")
    """,
    timestamp_field="ts_ds",
)

pro_education_features = FeatureView(
    name="pro_education_features",
    entities=[worker],
    ttl=timedelta(days=365),
    schema=[
        Field(name="b_has_degree_associates", dtype=Int32),
        Field(name="b_has_degree_bachelors", dtype=Int32),
        Field(name="b_has_degree_masters", dtype=Int32),
        Field(name="b_has_degree_phd", dtype=Int32),
        Field(name="b_has_degree_culinary_school", dtype=Int32),
        Field(name="b_has_degree_hospitality_school", dtype=Int32),
        Field(name="b_has_degree_hotel_school", dtype=Int32),
        Field(name="b_has_degree_high_school", dtype=Int32),
        Field(name="b_has_degree_some_college", dtype=Int32),
        Field(name="b_has_offshift_degree_culinary_school", dtype=Int32),
        Field(name="b_has_offshift_degree_hospitality_school", dtype=Int32),
        Field(name="b_has_offshift_degree_hotel_school", dtype=Int32),
        Field(name="b_has_offshift_degree_some_college", dtype=Int32),
        Field(name="rv_int_education_level", dtype=Int32),
        Field(name="rv_int_total_education_entries", dtype=Int32),
        Field(name="rv_int_unique_schools_attended", dtype=Int32),
        Field(name="rv_int_unique_degrees_earned", dtype=Int32),
        Field(name="rv_float_avg_education_duration_years", dtype=Float32),
        Field(name="rv_int_earliest_education_start_year", dtype=Int32),
        Field(name="rv_int_latest_education_end_year", dtype=Int32),
        Field(name="rv_int_years_since_last_education", dtype=Int32),
        Field(name="b_has_completed_degree", dtype=Int32),
        Field(name="b_has_relevant_industry_education", dtype=Int32),
        Field(name="b_has_higher_education", dtype=Int32),
        Field(name="mc_str_education_level_category", dtype=String),
    ],
    online=True,
    source=pro_education_source,
    tags={"owner": "data_team", "domain": "pro_profile", "category": "education"},
)

# ============================================================================
# BUSINESS FEATURES
# ============================================================================

# business_features_source = RedshiftSource(
#     name="business_features_source",
#     database="instawork",
#     schema="dbt-cchia",
#     table="business_features_inference",
#     timestamp_field="ts_ds",
# )


business_features_source = RedshiftSource(
    name="pro_education_source",
    query="""
        SELECT *
        FROM "dbt-cchia"."business_features_inference"
        WHERE id_business_id IN (
            SELECT DISTINCT id_business_id
            FROM "dbt-cchia"."business_features_inference"
            LIMIT 10
        )
        AND ts_ds = (SELECT MAX(ts_ds) FROM "dbt-cchia"."business_features_inference")
    """,
    timestamp_field="ts_ds",
)

business_features = FeatureView(
    name="business_features",
    entities=[business],
    ttl=timedelta(days=365),
    schema=[
        # IDs
        Field(name="id_company_id", dtype=Int64),
        # Region and location
        Field(name="mc_int_business_region", dtype=Int32),
        Field(name="mc_str_business_region_name", dtype=String),
        Field(name="mc_str_business_name", dtype=String),
        Field(name="mc_str_business_display_name", dtype=String),
        Field(name="mc_str_business_type", dtype=String),
        Field(name="mc_str_business_timezone", dtype=String),
        # Shift metrics
        Field(name="rv_int_total_shifts", dtype=Float64),
        Field(name="rv_int_total_filled_shifts", dtype=Float64),
        Field(name="rv_int_total_completed_shifts", dtype=Float64),
        Field(name="rv_float_fill_rate", dtype=Float64),
        Field(name="rv_int_shifts_l90d", dtype=Float64),
        Field(name="rv_int_daily_shifts", dtype=Float64),
        Field(name="rv_int_cancelled_filled_shifts", dtype=Float64),
        Field(name="rv_int_total_cancelled_shifts", dtype=Float64),
        Field(name="rv_int_n_booked_shifts", dtype=Float64),
        Field(name="rv_int_n_completed_shifts", dtype=Float64),
        # Ratings
        Field(name="rv_float_avg_business_rating_by_worker", dtype=Float64),
        Field(name="rv_float_avg_worker_rating_by_business", dtype=Float64),
        Field(name="rv_int_total_ratings_by_workers", dtype=Float64),
        Field(name="rv_int_total_ratings_by_business", dtype=Float64),
        # Partner information
        Field(name="mc_str_partner_status", dtype=String),
        Field(name="mc_str_partner_period", dtype=String),
        Field(name="mc_str_partner_type", dtype=String),
        Field(name="mc_str_primary_industry", dtype=String),
        Field(name="mc_str_secondary_industry", dtype=String),
        # Worker metrics
        Field(name="rv_int_unique_workers", dtype=Float64),
        Field(name="rv_int_total_unique_workers", dtype=Float64),
        Field(name="rv_int_w2_employees_only", dtype=Float64),
        Field(name="rv_int_active_pros", dtype=Float64),
        Field(name="rv_int_deactivated_pros", dtype=Float64),
        Field(name="rv_int_inferred_active_pros", dtype=Float64),
        Field(name="rv_int_rejected_pros", dtype=Float64),
        Field(name="rv_int_removed_pros", dtype=Float64),
        # Dates and timing
        Field(name="ts_first_shift_date", dtype=String),
        Field(name="ts_last_shift_date", dtype=String),
        Field(name="rv_int_days_since_first_shift", dtype=Float64),
        Field(name="rv_int_days_since_last_shift", dtype=Float64),
        Field(name="rv_int_days_since_last_ud", dtype=Float64),
        Field(name="ts_gig_date", dtype=String),
        # Rate metrics
        Field(name="rv_float_avg_filled_shift_business_rate", dtype=Float64),
        Field(name="rv_float_avg_unfilled_shift_business_rate", dtype=Float64),
        Field(name="rv_float_avg_filled_shift_applicant_rate", dtype=Float64),
        Field(name="rv_float_avg_unfilled_shift_applicant_rate", dtype=Float64),
        # Gig metrics
        Field(name="rv_int_num_gigs", dtype=Float64),
        Field(name="rv_int_num_gigs_posted", dtype=Float64),
        Field(name="rv_int_num_gigs_filled", dtype=Float64),
        Field(name="rv_float_gig_fill_rate", dtype=Float64),
        # Other metrics
        Field(name="rv_int_unique_companies", dtype=Float64),
        Field(name="rv_int_unique_shift_days", dtype=Float64),
        Field(name="rv_int_business_cumulative_filled_shifts", dtype=Float64),
    ],
    online=True,
    source=business_features_source,
    tags={"owner": "data_team", "domain": "business_profile"},
)

# ============================================================================
# SHIFT CORE FEATURES
# ============================================================================

shift_core_features_source = RedshiftSource(
    name="shift_core_features_source",
    database="instawork",
    schema="dbt-cchia",
    table="shift_core_features_inference",
    timestamp_field="ts_ds",
)

shift_core_features = FeatureView(
    name="shift_core_features",
    entities=[shift],
    ttl=timedelta(days=90),  # Shorter TTL for shift data
    schema=[
        # IDs
        Field(name="id_shift_group_id", dtype=Int64),
        Field(name="id_worker_id", dtype=Int64),
        Field(name="id_business_id", dtype=Int64),
        Field(name="id_company_id", dtype=Int64),
        Field(name="id_position_id", dtype=Int64),
        Field(name="id_regionmapping_id", dtype=Int64),
        # Timestamps
        Field(name="ts_shift_created_at", dtype=String),
        Field(name="ts_shift_starts_at", dtype=String),
        Field(name="ts_shift_ends_at", dtype=String),
        Field(name="ts_shift_group_created_at", dtype=String),
        # Boolean flags
        Field(name="b_w2_employees_only", dtype=Int32),
        Field(name="b_is_filled", dtype=Int32),
        Field(name="b_worker_no_show", dtype=Int32),
        Field(name="b_has_rating_by_worker", dtype=Int32),
        Field(name="b_has_rating_by_business", dtype=Int32),
        Field(name="b_is_weekend", dtype=Int32),
        Field(name="b_is_daytime", dtype=Int32),
        Field(name="b_is_bad_quality_outcome", dtype=Int32),
        Field(name="b_b_is_partial", dtype=Int32),
        # Ratings
        Field(name="rv_float_rating_by_worker", dtype=Float64),
        Field(name="rv_float_rating_by_business", dtype=Float64),
        # Rates
        Field(name="rv_float_business_rate_usd", dtype=Float64),
        Field(name="rv_float_applicant_rate_usd", dtype=Float64),
        # Timing metrics
        Field(name="rv_int_created_to_start_hours", dtype=Int64),
        Field(name="rv_int_group_created_to_start_hours", dtype=Int64),
        Field(name="rv_int_shift_duration_hours", dtype=Int64),
        Field(name="rv_int_day_of_week", dtype=Int32),
        Field(name="rv_int_hour_of_day", dtype=Int32),
        # Sequence metrics
        Field(name="rv_int_user_shift_sequence", dtype=Int64),
        Field(name="rv_int_user_business_shift_sequence", dtype=Int64),
        Field(name="rv_int_user_position_shift_sequence", dtype=Int64),
        Field(name="rv_int_hours_since_previous_shift", dtype=Float64),
        Field(name="rv_int_hours_to_next_shift", dtype=Float64),
        # Categorical
        Field(name="mc_str_shift_style", dtype=String),
    ],
    online=False,
    source=shift_core_features_source,
    tags={"owner": "data_team", "domain": "shift", "category": "core"},
)
