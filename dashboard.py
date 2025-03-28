import streamlit as st
import pandas as pd

# Load the Parquet file
try:
    df = pd.read_parquet("output/processed_data")
except Exception as e:
    st.error(f"Error loading Parquet file: {e}")
    st.stop()

st.title("📊 YouTube Trending Analytics")

# Show available columns for debugging
st.subheader("📋 Available Columns")
st.write(df.columns.tolist())

# Show top rows of the DataFrame
st.subheader("🔍 Sample Data")
st.dataframe(df.head())

# Visualize Top 10 Channels by Total Views
if 'channel_title' in df.columns and 'total_views' in df.columns:
    st.subheader("🔥 Top 10 Channels by Total Views")
    top10 = df.sort_values("total_views", ascending=False).head(10)
    st.bar_chart(top10.set_index("channel_title")["total_views"])
else:
    st.warning("Missing 'channel_title' or 'total_views' column in the data.")
 