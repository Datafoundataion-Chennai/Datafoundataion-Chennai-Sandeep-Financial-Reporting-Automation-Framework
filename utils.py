import streamlit as st
import pandas as pd
from google.cloud import bigquery
from config import client

def display_pagination(dataframe, rows_per_page=10):
    """
    Implement pagination for displaying large datasets.
    
    Args:
        dataframe (pd.DataFrame): Data to paginate
        rows_per_page (int): Number of rows to display per page
    
    Returns:
        pd.DataFrame: Paginated subset of the dataframe
    """
    if 'page' not in st.session_state:
        st.session_state.page = 1
    
    total_rows = len(dataframe)
    total_pages = (total_rows + rows_per_page - 1) // rows_per_page
    
    start_idx = (st.session_state.page - 1) * rows_per_page
    end_idx = min(start_idx + rows_per_page, total_rows)
    
    display_start = start_idx + 1
    display_end = end_idx
    
    st.write(f"Showing rows {display_start} to {display_end} of {total_rows}")
    
    paginated_df = dataframe.iloc[start_idx:end_idx].reset_index(drop=True)
    paginated_df.index = pd.RangeIndex(start=display_start, stop=display_end + 1, step=1)
    
    return paginated_df

def fetch_stats_data(selected_companies, start_date, end_date):
    """
    Fetch aggregate statistics for selected companies.
    
    Args:
        selected_companies (list): List of companies to fetch stats for
        start_date (date): Start date for data
        end_date (date): End date for data
    
    Returns:
        pd.DataFrame: Aggregate statistics
    """
    stats_query = """
    SELECT 
        company,
        AVG(open) AS avg_open,
        AVG(close) AS avg_close,
        AVG(high) AS avg_high,
        AVG(low) AS avg_low,
        AVG(volume) AS avg_volume
    FROM `lustrous-router-454110-h9.Sandeep01.stock_details`
    WHERE company IN UNNEST(@companies)
    AND DATE(date) BETWEEN @start_date AND @end_date
    GROUP BY company
    ORDER BY company
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("companies", "STRING", selected_companies),
            bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
            bigquery.ScalarQueryParameter("end_date", "DATE", end_date)
        ]
    )
    
    return client.query(stats_query, job_config=job_config).to_dataframe()