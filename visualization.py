import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

def plot_main_chart(df, chart_type, selected_metric, aggregation_method, metric_options):
    """
    Generate main chart based on selected parameters.
    
    Args:
        df (pd.DataFrame): Data to plot
        chart_type (str): Type of chart (Bar, Line, Area, Candlestick)
        selected_metric (str): Metric to display
        aggregation_method (str): Aggregation method used
        metric_options (dict): Mapping of metric keys to display names
    
    Returns:
        plotly chart object
    """
    if chart_type == 'Bar':
        return px.bar(
            df,
            x='company',
            y=selected_metric,
            color='company',
            title=f"{chart_type} Chart: {aggregation_method} {metric_options[selected_metric]}",
            labels={selected_metric: metric_options[selected_metric]},
            template='simple_white',
            hover_data={selected_metric: ':.2f'}
        )
    
    elif chart_type == 'Candlestick':
        return go.Figure(data=[go.Candlestick(
            x=df['date'],
            open=df['open'],
            high=df['high'],
            low=df['low'],
            close=df['close'],
            increasing_line_color='green',
            decreasing_line_color='red',
            name='Stock Price'
        )])
    
    elif chart_type in ['Line', 'Area']:
        chart_func = {'Line': px.line, 'Area': px.area}[chart_type]
        return chart_func(
            df,
            x='date',
            y=selected_metric,
            color='company',
            title=f"{chart_type} Chart: {aggregation_method} {metric_options[selected_metric]}",
            labels={selected_metric: metric_options[selected_metric]},
            template='simple_white',
            hover_data={selected_metric: ':.2f'}
        )

def plot_average_metrics_chart(avg_metrics_df, chart_type, metric, title):
    """
    Generate chart for average metrics.
    
    Args:
        avg_metrics_df (pd.DataFrame): Average metrics data
        chart_type (str): Type of chart (Bar, Line, Area)
        metric (str): Metric to plot
        title (str): Chart title
    
    Returns:
        plotly chart object
    """
    chart_func = {
        'Bar': px.bar,
        'Line': px.line,
        'Area': px.area
    }[chart_type]
    
    x_col = 'company' if chart_type == 'Bar' else 'date'
    
    return chart_func(
        avg_metrics_df,
        x=x_col,
        y=metric,
        color='company',
        title=f"{chart_type} Chart: {title}",
        labels={metric: title},
        template='simple_white',
        hover_data={metric: ':.2f'}
    )