from datetime import datetime

import pandas as pd
from feast import FeatureStore

print("Testing FEAST → Redshift connection...")

store = FeatureStore(repo_path=".")

# Create entity dataframe
entity_df = pd.DataFrame(
    {
        "id_worker_id": [1, 2, 3],  # Use actual worker IDs
        "event_timestamp": [datetime.now()] * 3,
    }
)

# Query Redshift
result = store.get_historical_features(
    entity_df=entity_df,
    features=["pro_education_features:b_has_degree_bachelors"],
).to_df()

print("✅ Success!")
print(result)
