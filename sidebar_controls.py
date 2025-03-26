import streamlit as st
import pandas as pd
from database import get_companies
from config import logger

def create_company_selector(all_companies):
    """
    Create company selection sidebar control.
    
    Args:
        all_companies (list): List of all available companies
    
    Returns:
        list: Selected companies
    """
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
    
    return selected_companies or all_companies

def create_date_range_selector():
    """
    Create date range sidebar control.
    
    Returns:
        tuple: Start and end dates
    """
    date_range = st.sidebar.date_input(
        "Date Range",
        value=[pd.to_datetime('2018-01-01').date(), pd.to_datetime('2023-12-31').date()],
        min_value=pd.to_datetime('2018-01-01').date(),
        max_value=pd.to_datetime('2023-12-31').date(),
        key="date_range",
        on_change=lambda: logger.info(f"Date range changed: {st.session_state.date_range}")
    )
    return date_range if len(date_range) == 2 else (pd.to_datetime('2018-01-01').date(), pd.to_datetime('2023-12-31').date())

def create_metric_controls():
    """
    Create sidebar controls for metric and chart type selection.
    
    Returns:
        tuple: Selected metric, chart type, and aggregation method
    """
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
        key="metric_select"
    )
    
    chart_type = col2.selectbox(
        "Chart Style",
        ['Bar', 'Line', 'Area', 'Candlestick'],
        key="chart_type_select",
        on_change=lambda: logger.info(f"Chart type changed: {st.session_state.chart_type_select}")
    )
    
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
    
    # Optional smoothing for Line/Area charts
    use_smoothing, window_size = False, 7
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
    
    return selected_metric, chart_type, aggregation_method, use_smoothing, window_size