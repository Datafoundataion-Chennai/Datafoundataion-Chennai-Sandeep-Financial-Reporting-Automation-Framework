import os
import logging
from google.cloud import bigquery

def setup_logging(log_path=r'C:\Users\sande\Vir Env\stock_explorer.log'):
    """
    Configure logging for the application.
    
    Args:
        log_path (str): Path to the log file
    
    Returns:
        logging.Logger: Configured logger
    """
    logging.basicConfig(
        filename=log_path,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger('StockExplorer')

def setup_bigquery_client(credentials_path=r"C:\Users\sande\Downloads\lustrous-router-454110-h9-0a05cb5bdaef.json"):
    """
    Set up BigQuery client with specified credentials.
    
    Args:
        credentials_path (str): Path to Google Cloud credentials JSON file
    
    Returns:
        bigquery.Client: Configured BigQuery client
    """
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
    return bigquery.Client()

# Initialize logger and BigQuery client
logger = setup_logging()
client = setup_bigquery_client()