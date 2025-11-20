# test_online_features.py
import pandas as pd
from feast import FeatureStore

store = FeatureStore(repo_path=".")

print("=" * 70)
print("Testing Online Features from Redis")
print("=" * 70)

# Test with the actual worker IDs in Redis
worker_ids = [11, 20, 32, 34, 36, 49, 51, 53, 55, 58]

print(f"\n1. Worker Education Features:")
worker_features = store.get_online_features(
    features=[
        "pro_education_features:b_has_degree_bachelors",
        "pro_education_features:b_has_degree_masters",
        "pro_education_features:mc_str_education_level_category",
    ],
    entity_rows=[{"id_worker_id": wid} for wid in worker_ids],
).to_dict()

df = pd.DataFrame(worker_features)
print(df)

# Test with actual business IDs in Redis
business_ids = [1250, 2499, 7013, 8770, 9119]

print(f"\n2. Business Features:")
business_features = store.get_online_features(
    features=[
        "business_features:mc_str_business_name",
        "business_features:rv_float_fill_rate",
        "business_features:rv_float_avg_business_rating_by_worker",
    ],
    entity_rows=[{"id_business_id": bid} for bid in business_ids],
).to_dict()

df = pd.DataFrame(business_features)
print(df)

print("\n✅ Online features working!")
