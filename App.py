import streamlit as st
import pandas as pd
import plotly.express as px
from google.cloud import bigquery
import os

# Set Google Cloud credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\sande\Downloads\lustrous-router-454110-h9-0a05cb5bdaef.json"
client = bigquery.Client()

# Title of the Streamlit app
st.title("📊 Stock Market Visualization with Filters")

# Query data from BigQuery
def fetch_data_from_bigquery():
    query = """
    SELECT * FROM lustrous-router-454110-h9.Sandeep01.stock_details
    """
    df = client.query(query).to_dataframe()
    return df

df = fetch_data_from_bigquery()

# Sidebar filters
st.sidebar.header('Filter Options')

# Multi-select for companies
selected_companies = st.sidebar.multiselect('Select Companies', df['company'].unique(), default=df['company'].unique()[:5])

# Date range filter with fixed range from 2018 to 2023
start_date, end_date = st.sidebar.date_input('Select Date Range', 
                                             [pd.to_datetime('2018-01-01').date(), 
                                              pd.to_datetime('2023-12-31').date()],
                                             min_value=pd.to_datetime('2018-01-01').date(),
                                             max_value=pd.to_datetime('2023-12-31').date())

# Metric selection
metric_options = ['open', 'close', 'high', 'low', 'volume']
selected_metric = st.sidebar.selectbox('Select Metric', metric_options)

# Aggregation filter
aggregation_method = st.sidebar.radio('Select Aggregation Method', ['Mean', 'Median', 'Sum'])

# Moving average window filter (optional)
window_size = st.sidebar.slider('Moving Average Window Size (Days)', 1, 30, 7)

# Filter data based on selections
filtered_data = df[(df['company'].isin(selected_companies)) & (df['date'].dt.date >= start_date) & (df['date'].dt.date <= end_date)]

# Apply aggregation
if aggregation_method == 'Mean':
    filtered_data = filtered_data.groupby('company')[selected_metric].mean().reset_index()
elif aggregation_method == 'Median':
    filtered_data = filtered_data.groupby('company')[selected_metric].median().reset_index()
elif aggregation_method == 'Sum':
    filtered_data = filtered_data.groupby('company')[selected_metric].sum().reset_index()

# Visualization
st.subheader(f'{aggregation_method} of {selected_metric} by Company')
fig = px.bar(filtered_data, x='company', y=selected_metric, color='company')
st.plotly_chart(fig)

# Summary Statistics
st.subheader('Summary Statistics')
st.write(filtered_data.describe())

# Download Data
st.subheader('Download Data')
st.download_button(label='Download Filtered Data as CSV', data=filtered_data.to_csv(index=False), file_name='filtered_data.csv', mime='text/csv')

st.write('Visualization complete!')
