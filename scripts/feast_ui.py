import streamlit as st
from feast import FeatureStore
import pandas as pd

st.set_page_config(page_title="FEAST Feature Store UI", layout="wide")

st.title("🍽️ FEAST Feature Store Explorer")

# Initialize feature store
@st.cache_resource
def get_feature_store():
    return FeatureStore(repo_path=".")

store = get_feature_store()

# Sidebar
st.sidebar.header("Navigation")
page = st.sidebar.radio("Go to", ["Overview", "Feature Views", "Entities", "Query Features"])

# Overview Page
if page == "Overview":
    st.header("📊 Overview")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        fvs = store.list_feature_views()
        st.metric("Feature Views", len(fvs))
    
    with col2:
        entities = store.list_entities()
        st.metric("Entities", len(entities))
    
    with col3:
        # Count features
        total_features = sum(len(fv.schema) for fv in fvs)
        st.metric("Total Features", total_features)
    
    st.subheader("Project Info")
    st.write(f"**Project:** {store.project}")
    st.write(f"**Registry:** {store.config.registry}")
    st.write(f"**Provider:** {store.config.provider}")

# Feature Views Page
elif page == "Feature Views":
    st.header("📋 Feature Views")
    
    fvs = store.list_feature_views()
    
    for fv in fvs:
        with st.expander(f"🔹 {fv.name}", expanded=True):
            # Fix: entities might be strings or objects
            entity_names = []
            for e in fv.entities:
                if isinstance(e, str):
                    entity_names.append(e)
                else:
                    entity_names.append(e.name)
            
            st.write(f"**Entities:** {entity_names}")
            st.write(f"**TTL:** {fv.ttl}")
            st.write(f"**Online:** {fv.online}")
            st.write(f"**Tags:** {fv.tags}")
            
            st.subheader("Features")
            features_df = pd.DataFrame([
                {"Feature": f.name, "Type": str(f.dtype)}
                for f in fv.schema
            ])
            st.dataframe(features_df, use_container_width=True)

# Entities Page
elif page == "Entities":
    st.header("🏷️ Entities")
    
    entities = store.list_entities()
    
    for entity in entities:
        with st.expander(f"🔹 {entity.name}", expanded=True):
            st.write(f"**Description:** {entity.description}")
            st.write(f"**Value Type:** {entity.value_type}")
            if hasattr(entity, 'tags') and entity.tags:
                st.write(f"**Tags:** {entity.tags}")

# Query Features Page
elif page == "Query Features":
    st.header("🔍 Query Features")
    
    # Load data to get worker IDs
    try:
        df = pd.read_parquet("data/pro_education_features.parquet")
        all_worker_ids = df['id_worker_id'].unique()
        
        st.subheader("Select Workers")
        
        # Input method
        input_method = st.radio("Input method:", ["Select from list", "Enter manually"])
        
        if input_method == "Select from list":
            selected_workers = st.multiselect(
                "Select worker IDs:",
                options=all_worker_ids[:100].tolist(),  # Show first 100
                default=all_worker_ids[:5].tolist()
            )
        else:
            worker_input = st.text_input("Enter worker IDs (comma-separated):", "3,8,12,13,21")
            selected_workers = [int(x.strip()) for x in worker_input.split(",") if x.strip()]
        
        # Feature selection
        st.subheader("Select Features")
        all_features = [
            "worker_education_features:b_has_degree_bachelors",
            "worker_education_features:b_has_degree_masters",
            "worker_education_features:b_has_degree_phd",
            "worker_education_features:b_has_degree_culinary_school",
            "worker_education_features:b_has_degree_hospitality_school",
            "worker_education_features:rv_int_education_level",
            "worker_education_features:rv_int_total_education_entries",
            "worker_education_features:b_has_completed_degree",
            "worker_education_features:mc_str_education_level_category",
        ]
        
        selected_features = st.multiselect(
            "Select features:",
            options=all_features,
            default=all_features[:5]
        )
        
        # Query button
        if st.button("🚀 Get Features", type="primary"):
            if not selected_workers:
                st.error("Please select at least one worker")
            elif not selected_features:
                st.error("Please select at least one feature")
            else:
                with st.spinner("Fetching features from Redis..."):
                    try:
                        features = store.get_online_features(
                            features=selected_features,
                            entity_rows=[{"id_worker_id": wid} for wid in selected_workers],
                        ).to_dict()
                        
                        result_df = pd.DataFrame(features)
                        
                        st.success(f"✅ Retrieved features for {len(selected_workers)} workers")
                        st.dataframe(result_df, use_container_width=True)
                        
                        # Statistics
                        st.subheader("📊 Statistics")
                        col1, col2, col3 = st.columns(3)
                        
                        with col1:
                            if 'b_has_degree_bachelors' in result_df.columns:
                                st.metric("With Bachelor's", result_df['b_has_degree_bachelors'].sum())
                        
                        with col2:
                            if 'b_has_degree_masters' in result_df.columns:
                                st.metric("With Master's", result_df['b_has_degree_masters'].sum())
                        
                        with col3:
                            if 'b_has_degree_phd' in result_df.columns:
                                st.metric("With PhD", result_df['b_has_degree_phd'].sum())
                        
                        # Download button
                        csv = result_df.to_csv(index=False)
                        st.download_button(
                            label="📥 Download CSV",
                            data=csv,
                            file_name="features.csv",
                            mime="text/csv"
                        )
                        
                    except Exception as e:
                        st.error(f"Error: {e}")
                        import traceback
                        st.code(traceback.format_exc())
    
    except FileNotFoundError:
        st.error("Data file not found. Please ensure 'data/pro_education_features.parquet' exists.")
    except Exception as e:
        st.error(f"Error loading data: {e}")

# Footer
st.sidebar.markdown("---")
st.sidebar.info("FEAST Feature Store UI\nBuilt with Streamlit")
