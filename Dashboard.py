import streamlit as st
from google.cloud import bigquery

# Set up BigQuery client
client = bigquery.Client()

# Streamlit app
st.title("Top 10 Companies by Trading Volume")

# Date range input
start_date = st.date_input("Start Date")
end_date = st.date_input("End Date")

if st.button("Get Data"):
    query = f"""
    SELECT company, SUM(volume) AS total_volume
    FROM `lustrous-router-454110-h9.Sandeep01.stock_details`
    WHERE date BETWEEN TIMESTAMP('{start_date}') AND TIMESTAMP('{end_date}')
    GROUP BY company
    ORDER BY total_volume DESC
    LIMIT 10;
    """
    query_job = client.query(query)
    results = query_job.to_dataframe()

    # Display results
    st.write(results)
