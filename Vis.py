import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from google.cloud import bigquery
import os
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    filename=r'C:\Users\sande\Vir Env\stock_explorer.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('StockExplorer')

# Google Cloud setup
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\sande\Downloads\lustrous-router-454110-h9-0a05cb5bdaef.json"
client = bigquery.Client()

# App layout
st.title("Stock Market Explorer")
st.markdown("Welcome to your stock adventure! Explore data from 2018-2023")

# Optimized data fetch with caching for main chart
@st.cache_data(ttl=3600)
def fetch_data_from_bigquery(selected_companies, start_date, end_date, metric, aggregation_method, chart_type, use_smoothing=False, window_size=7):
    logger.info(f"Fetching data: companies={selected_companies}, metric={metric}, agg={aggregation_method}, chart_type={chart_type}, smoothing={use_smoothing}, window={window_size}")
    with st.spinner("Fetching stock data... 📡"):
        try:
            if chart_type == 'Bar':
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
            st.error(f"Couldn’t fetch data: {e}")
            return pd.DataFrame()

# Fetch average metrics for additional charts
@st.cache_data(ttl=3600)
def fetch_avg_metrics(selected_companies, start_date, end_date, chart_type):
    logger.info(f"Fetching avg metrics: companies={selected_companies}, chart_type={chart_type}")
    with st.spinner("Fetching average metrics data... 📡"):
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
            else:  # Line or Area chart.  Candlestick does not use this.
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
            st.error(f"Couldn’t fetch average metrics data: {e}")
            return pd.DataFrame()

# Cached company list fetch
@st.cache_data
def get_companies():
    logger.info("Fetching company list")
    query = """
    SELECT DISTINCT company 
    FROM `lustrous-router-454110-h9.Sandeep01.stock_details` 
    ORDER BY company
    """
    return client.query(query).to_dataframe()['company'].tolist()

# Sidebar controls
st.sidebar.header(" Tune Your Experience")

# Company selection
all_companies = get_companies()
selected_companies = st.sidebar.multiselect(
    "Companies",
    all_companies,
    default=None,
    key="company_select",
    help="Choose companies or leave empty for all!",
    on_change=lambda: logger.info(f"Companies selected: {st.session_state.company_select}")
)
if selected_companies:
    st.sidebar.write(f"Selected: {len(selected_companies)} companies")
else:
    st.sidebar.info(f"Showing all {len(all_companies)} companies")

# Date range
date_range = st.sidebar.date_input(
    "Date Range",
    value=[pd.to_datetime('2018-01-01').date(), pd.to_datetime('2023-12-31').date()],
    min_value=pd.to_datetime('2018-01-01').date(),
    max_value=pd.to_datetime('2023-12-31').date(),
    key="date_range",
    on_change=lambda: logger.info(f"Date range changed: {st.session_state.date_range}")
)
start_date, end_date = date_range if len(date_range) == 2 else (pd.to_datetime('2018-01-01').date(), pd.to_datetime('2023-12-31').date())

# Metric and chart selection
metric_options = {
    'open': 'Opening Price',
    'close': 'Closing Price',
    'high': 'Highest Price',
    'low': 'Lowest Price',
    'volume': 'Trading Volume'
}
col1, col2 = st.sidebar.columns(2)
selected_metric = col1.selectbox(
    "Main Chart Metric",
    metric_options.keys(),
    format_func=lambda x: metric_options[x],
    key="metric_select",
    disabled=False # Removed the conditional disabling
    #on_change=lambda: logger.info(f"Metric selected: {st.session_state.metric_select}") # Removed this line to fix error.
)
chart_type = col2.selectbox(
    "Chart Style",
    ['Bar', 'Line', 'Area', 'Candlestick'],
    key="chart_type_select",
    on_change=lambda: logger.info(f"Chart type changed: {st.session_state.chart_type_select}")
)

# Aggregation selection
aggregation_options = {
    'AVG': 'Average',
    'APPROX_QUANTILES': 'Median',
    'SUM': 'Total'
}
aggregation_method = st.sidebar.selectbox(
    "Aggregation (Main Chart)",
    aggregation_options.keys(),
    format_func=lambda x: aggregation_options[x],
    key="agg_select",
    on_change=lambda: logger.info(f"Aggregation changed: {st.session_state.agg_select}")
)

# Smoothing controls (for Line/Area charts only)
if chart_type in ['Line', 'Area']:
    use_smoothing = st.sidebar.checkbox(
        "Smooth Data (Main Chart)",
        key="smooth_checkbox",
        help="Toggle to smooth trends",
        on_change=lambda: logger.info(f"Smoothing toggled: {st.session_state.smooth_checkbox}")
    )
    window_size = st.sidebar.slider(
        "Smoothing Days",
        1, 30, 7,
        key="smooth_slider",
        help="More days = smoother trends",
        on_change=lambda: logger.info(f"Smoothing window changed: {st.session_state.smooth_slider}")
    ) if use_smoothing else 7
else:
    use_smoothing, window_size = False, 7

# Fetch data
selected_companies = selected_companies or all_companies
df = fetch_data_from_bigquery(
    selected_companies,
    start_date,
    end_date,
    selected_metric,
    aggregation_method,
    chart_type,
    use_smoothing,
    window_size
)

# Fetch average metrics for additional charts
avg_metrics_df = fetch_avg_metrics(
    selected_companies,
    start_date,
    end_date,
    chart_type
)

# Main content
if not df.empty and not avg_metrics_df.empty:
    # Main Chart (selected metric)
    st.subheader(f"{aggregation_options[aggregation_method]} {metric_options[selected_metric]} (Main Chart)")
    if chart_type == 'Bar':
        fig = px.bar(
            df,
            x='company',
            y=selected_metric,
            color='company',
            title=f"{chart_type} Chart: {aggregation_options[aggregation_method]} {metric_options[selected_metric]}",
            labels={selected_metric: metric_options[selected_metric]},
            template='simple_white',
            hover_data={selected_metric: ':.2f'}
        )
        fig.update_layout(showlegend=True)
        st.plotly_chart(fig, use_container_width=True)
    elif chart_type == 'Candlestick':
        fig = go.Figure(data=[go.Candlestick(
            x=df['date'],
            open=df['open'],
            high=df['high'],
            low=df['low'],
            close=df['close'],
            increasing_line_color='green',  # You can customize these colors
            decreasing_line_color='red',
            name = 'Stock Price'
        )])

        fig.update_layout(
            title=f"Candlestick Chart",
            yaxis_title='Price',
            template='simple_white'
        )
        st.plotly_chart(fig, use_container_width=True)

    elif chart_type in ['Line', 'Area']:
        chart_func = {'Line': px.line, 'Area': px.area}[chart_type]
        fig = chart_func(
            df,
            x='date',
            y=selected_metric,
            color='company',
            title=f"{chart_type} Chart: {aggregation_options[aggregation_method]} {metric_options[selected_metric]}",
            labels={selected_metric: metric_options[selected_metric]},
            template='simple_white',
            hover_data={selected_metric: ':.2f'}
        )
        fig.update_layout(showlegend=True)
        st.plotly_chart(fig, use_container_width=True)

    # Additional Charts for average metrics
    st.subheader("Average Metrics Overview")
    avg_metrics = {
        'avg_open': 'Average Opening Price',
        'avg_high': 'Average Highest Price',
        'avg_close': 'Average Closing Price',
        'avg_low': 'Average Lowest Price',
        'avg_volume': 'Average Trading Volume'
    }
    for metric, title in avg_metrics.items():
        if chart_type == 'Bar':
            fig = px.bar(
                avg_metrics_df,
                x='company',
                y=metric,
                color='company',
                title=f"{chart_type} Chart: {title}",
                labels={metric: title},
                template='simple_white',
                hover_data={metric: ':.2f'}
            )
            fig.update_layout(showlegend=True)
            st.plotly_chart(fig, use_container_width=True)
        elif chart_type in ['Line', 'Area']:
            chart_func = {'Line': px.line, 'Area': px.area}[chart_type]
            fig = chart_func(
                avg_metrics_df,
                x='date',
                y=metric,
                color='company',
                title=f"{chart_type} Chart: {title}",
                labels={metric: title},
                template='simple_white',
                hover_data={metric: ':.2f'}
            )
            fig.update_layout(showlegend=True)
            st.plotly_chart(fig, use_container_width=True)

    # Stats section with pagination
    with st.expander("Quick Stats", expanded=True):
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
        stats_df = client.query(stats_query, job_config=job_config).to_dataframe()

        # Pagination controls
        if 'page' not in st.session_state:
            st.session_state.page = 1
        
        rows_per_page = st.selectbox(
            "Rows per page",
            options=[5, 10, 20, 50],
            index=1,
            key="rows_per_page",
            on_change=lambda: logger.info(f"Rows per page changed: {st.session_state.rows_per_page}")
        )
        
        total_rows = len(stats_df)
        total_pages = (total_rows + rows_per_page - 1) // rows_per_page
        
        # Calculate page boundaries (internal 0-based, display 1-based)
        start_idx = (st.session_state.page - 1) * rows_per_page
        end_idx = min(start_idx + rows_per_page, total_rows)
        
        # Display paginated data with 1-based indexing
        display_start = start_idx + 1
        display_end = end_idx
        st.write(f"Showing rows {display_start} to {display_end} of {total_rows}")
        
        paginated_df = stats_df.iloc[start_idx:end_idx].reset_index(drop=True)
        paginated_df.index = pd.RangeIndex(start=display_start, stop=display_end + 1, step=1)
        
        st.dataframe(
            paginated_df.style.format("{:.2f}", subset=[col for col in paginated_df.columns if col != 'company']),
            use_container_width=True
        )
        
        # Pagination navigation
        if total_pages > 1:
            col1, col2, col3 = st.columns([1, 1, 2])
            with col1:
                if st.button("Previous", key="prev_button", disabled=(st.session_state.page <= 1)):
                    st.session_state.page -= 1
                    logger.info(f"Page changed to: {st.session_state.page}")
                    st.rerun()
            with col2:
                if st.button("Next", key="next_button", disabled=(st.session_state.page >= total_pages)):
                    st.session_state.page += 1
                    logger.info(f"Page changed to: {st.session_state.page}")
                    st.rerun()
            with col3:
                st.write(f"Page {st.session_state.page} of {total_pages}")
        
        # Download button for full dataset
        st.download_button(
            label="Download Full Stats (CSV)",
            data=stats_df.to_csv(index=False),
            file_name='stock_stats.csv',
            mime='text/csv'
        )

    # Display log history
    with st.expander("Change History", expanded=False):
        if os.path.exists('stock_explorer.log'):
            with open('stock_explorer.log', 'r') as log_file:
                log_content = log_file.read()
            st.text_area("Log History", log_content, height=300)
        else:
            st.write("No log history available yet.")
else:
    st.warning("No data available! Check your filters or connection.")

# Footer
st.markdown("---")
