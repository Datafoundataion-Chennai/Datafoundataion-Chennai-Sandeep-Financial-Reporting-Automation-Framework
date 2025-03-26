import os
import streamlit as st
import pandas as pd

from config import logger
from database import get_companies, fetch_data_from_bigquery, fetch_avg_metrics
from sidebar_controls import (
    create_company_selector, 
    create_date_range_selector, 
    create_metric_controls
)
from visualization import plot_main_chart, plot_average_metrics_chart
from utils import display_pagination, fetch_stats_data

def main():
    # App title and description
    st.title("Stock Market Explorer")
    st.markdown("Welcome to your stock adventure! Explore data from 2018-2023")

    # Sidebar controls
    all_companies = get_companies()
    
    # Company selection
    selected_companies = create_company_selector(all_companies)
    
    # Date range selection
    start_date, end_date = create_date_range_selector()
    
    # Metric and chart controls
    selected_metric, chart_type, aggregation_method, use_smoothing, window_size = create_metric_controls()

    # Metric display options
    metric_options = {
        'open': 'Opening Price',
        'close': 'Closing Price',
        'high': 'Highest Price',
        'low': 'Lowest Price',
        'volume': 'Trading Volume'
    }

    # Fetch data
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

    # Fetch average metrics
    avg_metrics_df = fetch_avg_metrics(
        selected_companies,
        start_date,
        end_date,
        chart_type
    )

    # Main content
    if not df.empty and not avg_metrics_df.empty:
        # Main Chart Section
        st.subheader(f"Average {metric_options[selected_metric]} (Main Chart)")
        main_chart = plot_main_chart(
            df, 
            chart_type, 
            selected_metric, 
            aggregation_method, 
            metric_options
        )
        main_chart.update_layout(showlegend=True)
        st.plotly_chart(main_chart, use_container_width=True)

        # Average Metrics Section
        st.subheader("Average Metrics Overview")
        avg_metrics = {
            'avg_open': 'Average Opening Price',
            'avg_high': 'Average Highest Price',
            'avg_close': 'Average Closing Price',
            'avg_low': 'Average Lowest Price',
            'avg_volume': 'Average Trading Volume'
        }

        for metric, title in avg_metrics.items():
            avg_chart = plot_average_metrics_chart(
                avg_metrics_df, 
                chart_type, 
                metric, 
                title
            )
            avg_chart.update_layout(showlegend=True)
            st.plotly_chart(avg_chart, use_container_width=True)

        # Stats Section with Pagination
        with st.expander("Quick Stats", expanded=True):
            # Fetch stats data
            stats_df = fetch_stats_data(selected_companies, start_date, end_date)
            
            # Pagination controls
            rows_per_page = st.selectbox(
                "Rows per page",
                options=[5, 10, 20, 50],
                index=1,
                key="rows_per_page"
            )
            
            # Display paginated data
            paginated_df = display_pagination(stats_df, rows_per_page)
            st.dataframe(
                paginated_df.style.format("{:.2f}", subset=[col for col in paginated_df.columns if col != 'company']),
                use_container_width=True
            )
            
            # Download button
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

if __name__ == "__main__":
    main()