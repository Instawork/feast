"""
Example usage of all_features.py

This shows how to use the generated feature definitions.
"""

from feast import FeatureStore
from all_features import all_feature_views, all_entities
from datetime import datetime, timedelta

# Initialize feature store
store = FeatureStore(repo_path=".")

# Example 1: Get online features for a worker
def get_worker_features(worker_id: int):
    """Get features for a specific worker"""
    
    entity_rows = [{"id_worker_id": worker_id}]
    
    features = store.get_online_features(
        features=[
            "pro_core_features:b_is_email_verified",
            "pro_core_features:mc_str_worker_level",
            "pro_core_features:rv_int_account_age_days",
            "pro_core_features:b_active_last_7_days",
            "pro_quiz_features:rv_int_num_quiz_passed",
            "pro_quiz_features:rv_float_avg_quiz_score",
        ],
        entity_rows=entity_rows,
    ).to_dict()
    
    return features

# Example 2: Get features for a business
def get_business_features(business_id: int):
    """Get features for a specific business"""
    
    entity_rows = [{"id_business_id": business_id}]
    
    features = store.get_online_features(
        features=[
            "business_features:rv_float_fill_rate",
            "business_features:rv_int_total_shifts",
            "business_features:mc_str_partner_status",
            "business_no_show_features:rv_float_auto_no_show_rate",
        ],
        entity_rows=entity_rows,
    ).to_dict()
    
    return features

# Example 3: Get historical features for training
def get_training_data(entity_df):
    """Get historical features for model training"""
    
    training_df = store.get_historical_features(
        entity_df=entity_df,
        features=[
            "pro_core_features:b_is_email_verified",
            "pro_core_features:rv_int_account_age_days",
            "pro_experience_features:rv_int_current_exp_months",
            "pro_quiz_features:rv_float_avg_quiz_score",
            "business_features:rv_float_fill_rate",
        ],
    ).to_df()
    
    return training_df

# Example 4: List all available features
def list_all_features():
    """List all feature views and their features"""
    
    print("Available Feature Views:")
    print("=" * 60)
    
    for fv in store.list_feature_views():
        print(f"\n{fv.name}")
        print(f"  Entity: {[e.name for e in fv.entities]}")
        print(f"  Features: {len(fv.schema)}")
        print(f"  Tags: {fv.tags}")

# Example 5: Get features for multiple entities
def get_batch_features(worker_ids: list):
    """Get features for multiple workers"""
    
    entity_rows = [{"id_worker_id": wid} for wid in worker_ids]
    
    features = store.get_online_features(
        features=[
            "pro_core_features:mc_str_worker_level",
            "pro_core_features:rv_int_account_age_days",
            "pro_shift_outcome_features:rv_int_n_filled_shifts",
        ],
        entity_rows=entity_rows,
    ).to_dict()
    
    return features

if __name__ == "__main__":
    # Example usage
    print("Getting features for worker 12345...")
    worker_features = get_worker_features(12345)
    print(worker_features)
    
    print("\nGetting features for business 67890...")
    business_features = get_business_features(67890)
    print(business_features)
    
    print("\nListing all available features...")
    list_all_features()
