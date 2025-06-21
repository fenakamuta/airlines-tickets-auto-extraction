import os
import sys
import logging
from google.cloud import bigquery
from google.cloud import storage
from google.oauth2 import service_account

# Add src directory to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from config import GOOGLE_GCS_BUCKET_NAME, GCS_CREDENTIALS_PATH, LOG_LEVEL, LOG_FORMAT

logging.basicConfig(level=getattr(logging, LOG_LEVEL), format=LOG_FORMAT)
logger = logging.getLogger(__name__)

def init_clients():
    """Initialize BigQuery and Storage clients."""
    try:
        if GCS_CREDENTIALS_PATH and os.path.exists(GCS_CREDENTIALS_PATH):
            credentials = service_account.Credentials.from_service_account_file(GCS_CREDENTIALS_PATH)
            bq_client = bigquery.Client(credentials=credentials)
            storage_client = storage.Client(credentials=credentials)
        else:
            bq_client = bigquery.Client()
            storage_client = storage.Client()
        
        return bq_client, storage_client
    except Exception as e:
        logger.error(f"Failed to initialize clients: {e}")
        return None, None

def list_latam_files(storage_client, bucket_name):
    """List all LATAM CSV files in GCS."""
    try:
        bucket = storage_client.bucket(bucket_name)
        blobs = bucket.list_blobs(prefix='latam/')
        
        files = []
        for blob in blobs:
            if blob.name.endswith('.csv') and 'latam/' in blob.name:
                files.append(blob.name)
        
        logger.info(f"Found {len(files)} LATAM CSV files")
        return files
    except Exception as e:
        logger.error(f"Failed to list files: {e}")
        return []

def create_latam_table(bq_client, dataset_id, table_id):
    """Create BigQuery table for LATAM flight data."""
    try:
        dataset_ref = bq_client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)
        
        # LATAM flight data schema (basic fields only)
        schema = [
            bigquery.SchemaField("depart_time", "DATETIME"),
            bigquery.SchemaField("origin", "STRING"),
            bigquery.SchemaField("arrive_time", "DATETIME"),
            bigquery.SchemaField("destination", "STRING"),
            bigquery.SchemaField("duration", "FLOAT64"),
            bigquery.SchemaField("price", "FLOAT64"),
            bigquery.SchemaField("operator", "STRING"),
            bigquery.SchemaField("query_time", "DATETIME"),
        ]
        
        table = bigquery.Table(table_ref, schema=schema)
        table = bq_client.create_table(table, exists_ok=True)
        
        logger.info(f"Created/updated LATAM table: {dataset_id}.{table_id}")
        return table
    except Exception as e:
        logger.error(f"Failed to create table: {e}")
        return None

def load_latam_data():
    """Load all LATAM flight data files to BigQuery."""
    # Configuration
    dataset_id = os.getenv('BIGQUERY_DATASET_ID', 'ticket_airlines')
    table_id = os.getenv('BIGQUERY_LATAM_TABLE_ID', 'latam_flights')
    
    if GOOGLE_GCS_BUCKET_NAME == 'your-anac-data-bucket':
        logger.error("Set GOOGLE_GCS_BUCKET_NAME in config.py or environment")
        return
    
    # Initialize clients
    bq_client, storage_client = init_clients()
    if not bq_client or not storage_client:
        return
    
    # Create dataset if it doesn't exist
    try:
        dataset_ref = bq_client.dataset(dataset_id)
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"  # Set your preferred location
        bq_client.create_dataset(dataset, exists_ok=True)
        logger.info(f"Dataset {dataset_id} ready")
    except Exception as e:
        logger.error(f"Failed to create dataset: {e}")
        return
    
    # Create LATAM table
    table = create_latam_table(bq_client, dataset_id, table_id)
    if not table:
        return
    
    # List LATAM files
    files = list_latam_files(storage_client, GOOGLE_GCS_BUCKET_NAME)
    if not files:
        logger.warning("No LATAM files found in GCS")
        return
    
    # Load each file individually (LATAM files are smaller and have different structures)
    successful_loads = 0
    total_rows = 0
    
    for file_path in files:
        logger.info(f"Processing: {file_path}")
        
        try:
            # GCS source URI for this file
            source_uri = f"gs://{GOOGLE_GCS_BUCKET_NAME}/{file_path}"
            
            # Configure the load job for CSV
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,  # Skip header
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # Append to existing data
                autodetect=False,  # Use our defined schema
                max_bad_records=1000,  # Allow more bad records
                ignore_unknown_values=True,  # Ignore extra columns
                allow_quoted_newlines=True,  # Handle quoted fields with newlines
                allow_jagged_rows=True,  # Allow rows with different column counts
            )
            
            # Start the load job
            load_job = bq_client.load_table_from_uri(
                source_uri, table, job_config=job_config
            )
            
            logger.info(f"Loading {file_path}...")
            load_job.result()  # Wait for the job to complete
            
            logger.info(f"Loaded {load_job.output_rows} rows from {file_path}")
            successful_loads += 1
            total_rows += load_job.output_rows or 0
            
            if load_job.errors:
                logger.warning(f"Job completed with {len(load_job.errors)} errors")
                for error in load_job.errors[:3]:  # Show first 3 errors
                    logger.warning(f"Error: {error}")
                    
        except Exception as e:
            logger.error(f"Failed to load {file_path}: {e}")
            continue
    
    logger.info(f"Successfully loaded {successful_loads}/{len(files)} files to {dataset_id}.{table_id}")
    logger.info(f"Total rows loaded: {total_rows}")
    
    return True

if __name__ == "__main__":
    try:
        load_latam_data()
    except KeyboardInterrupt:
        logger.info("Interrupted by user") 