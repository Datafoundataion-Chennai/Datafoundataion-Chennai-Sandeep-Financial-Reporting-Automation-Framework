import pandas as pd
from google.cloud import bigquery
from config import client, logger

def get_companies():
    """
    Fetch list of distinct companies from BigQuery.
    
    Returns:
        list: List of company names
    """
    logger.info("Fetching company list")
    query = """
    SELECT DISTINCT company 
    FROM `lustrous-router-454110-h9.Sandeep01.stock_details` 
    ORDER BY company
    """
    return client.query(query).to_dataframe()['company'].tolist()

def fetch_data_from_bigquery(
    selected_companies, 
    start_date, 
    end_date, 
    metric, 
    aggregation_method, 
    chart_type, 
    use_smoothing=False, 
    window_size=7
):
    """
    Fetch stock data from BigQuery with flexible querying options.
    
    Args:
        selected_companies (list): List of companies to fetch
        start_date (date): Start date for data
        end_date (date): End date for data
        metric (str): Stock metric to query (open/close/high/low/volume)
        aggregation_method (str): SQL aggregation method
        chart_type (str): Type of chart to generate
        use_smoothing (bool): Apply moving average smoothing
        window_size (int): Size of smoothing window
    
    Returns:
        pd.DataFrame: Queried stock data
    """
    logger.info(f"Fetching data: companies={selected_companies}, metric={metric}, agg={aggregation_method}")
    
    try:
        if chart_type == 'Bar':
            # Bar chart query logic (similar to original code)
            agg_expr = f"{aggregation_method}(CAST({metric} AS FLOAT64))" if aggregation_method != 'APPROX_QUANTILES' else f"APPROX_QUANTILES(CAST({metric} AS FLOAT64), 2)[OFFSET(1)]"
            query = f"""
                SELECT 
                    company,
                    {agg_expr} AS {metric}
                FROM `lustrous-router-454110-h9.Sandeep01.stock_details`
                WHERE company IN UNNEST(@companies)
                AND DATE(date) BETWEEN @start_date AND @end_date
                GROUP BY company
                ORDER BY company
                """
        elif chart_type == 'Candlestick':
            # Candlestick chart query logic (similar to original code)
            query = f"""
                SELECT 
                    company,
                    DATE(date) AS date,
                    CAST(open AS FLOAT64) AS open,
                    CAST(high AS FLOAT64) AS high,
                    CAST(low AS FLOAT64) AS low,
                    CAST(close AS FLOAT64) AS close
                FROM `lustrous-router-454110-h9.Sandeep01.stock_details`
                WHERE company IN UNNEST(@companies)
                AND DATE(date) BETWEEN @start_date AND @end_date
                ORDER BY company, DATE(date)
                """
        else:  # Line or Area chart
            # Line/Area chart query logic with optional smoothing
            agg_clause = f"""
                AVG(CAST({metric} AS FLOAT64)) OVER (PARTITION BY company ORDER BY DATE(date) ROWS BETWEEN {window_size - 1} PRECEDING AND CURRENT ROW)
                """ if use_smoothing and window_size > 1 else f"CAST({metric} AS FLOAT64)"
            query = f"""
                SELECT 
                    company,
                    DATE(date) AS date,
                    {agg_clause} AS {metric}
                FROM `lustrous-router-454110-h9.Sandeep01.stock_details`
                WHERE company IN UNNEST(@companies)
                AND DATE(date) BETWEEN @start_date AND @end_date
                ORDER BY company, DATE(date)
                """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("companies", "STRING", selected_companies),
                bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
                bigquery.ScalarQueryParameter("end_date", "DATE", end_date)
            ]
        )
        df = client.query(query, job_config=job_config).to_dataframe()
        
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], utc=True)
        
        return df
    
    except Exception as e:
        logger.error(f"Data fetch failed: {e}")
        return pd.DataFrame()

def fetch_avg_metrics(selected_companies, start_date, end_date, chart_type):
    """
    Fetch average stock metrics from BigQuery.
    
    Args:
        selected_companies (list): List of companies
        start_date (date): Start date for data
        end_date (date): End date for data
        chart_type (str): Type of chart to generate
    
    Returns:
        pd.DataFrame: Average stock metrics
    """
    logger.info(f"Fetching avg metrics: companies={selected_companies}, chart_type={chart_type}")
    
    try:
        if chart_type == 'Bar':
            query = """
                SELECT 
                    company,
                    AVG(CAST(open AS FLOAT64)) AS avg_open,
                    AVG(CAST(high AS FLOAT64)) AS avg_high,
                    AVG(CAST(close AS FLOAT64)) AS avg_close,
                    AVG(CAST(low AS FLOAT64)) AS avg_low,
                    AVG(CAST(volume AS FLOAT64)) AS avg_volume
                FROM `lustrous-router-454110-h9.Sandeep01.stock_details`
                WHERE company IN UNNEST(@companies)
                AND DATE(date) BETWEEN @start_date AND @end_date
                GROUP BY company
                ORDER BY company
                """
        else:  # Line or Area chart
            query = """
                SELECT 
                    company,
                    DATE(date) AS date,
                    AVG(CAST(open AS FLOAT64)) AS avg_open,
                    AVG(CAST(high AS FLOAT64)) AS avg_high,
                    AVG(CAST(close AS FLOAT64)) AS avg_close,
                    AVG(CAST(low AS FLOAT64)) AS avg_low,
                    AVG(CAST(volume AS FLOAT64)) AS avg_volume
                FROM `lustrous-router-454110-h9.Sandeep01.stock_details`
                WHERE company IN UNNEST(@companies)
                AND DATE(date) BETWEEN @start_date AND @end_date
                GROUP BY company, DATE(date)
                ORDER BY company, DATE(date)
                """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("companies", "STRING", selected_companies),
                bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
                bigquery.ScalarQueryParameter("end_date", "DATE", end_date)
            ]
        )
        df = client.query(query, job_config=job_config).to_dataframe()
        
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], utc=True)
        
        return df
    
    except Exception as e:
        logger.error(f"Avg metrics fetch failed: {e}")
        return pd.DataFrame()