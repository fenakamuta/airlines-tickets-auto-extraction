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

def detect_schema_from_sample(bq_client, storage_client, bucket_name):
    """Detect schema from a sample file."""
    try:
        bucket = storage_client.bucket(bucket_name)
        blobs = list(bucket.list_blobs(prefix='anac_data/'))
        
        if not blobs:
            logger.error("No files found in anac_data/")
            return None
        
        # Get first .txt file
        sample_file = None
        for blob in blobs:
            if blob.name.endswith('.txt'):
                sample_file = blob
                break
        
        if not sample_file:
            logger.error("No .txt files found")
            return None
        
        # Download sample content
        encodings = ['latin-1', 'iso-8859-1', 'utf-8', 'cp1252']
        content = None
        
        for encoding in encodings:
            try:
                content = sample_file.download_as_text(encoding=encoding)
                logger.info(f"Successfully decoded sample file with {encoding} encoding")
                break
            except UnicodeDecodeError:
                continue
        
        if not content:
            logger.error("Could not decode sample file")
            return None
        
        # Detect delimiter and headers
        lines = content.split('\n')
        header_line = None
        for line in lines:
            if line.strip() and not line.startswith('#'):
                header_line = line
                break
        
        if not header_line:
            logger.error("Could not find header in sample file")
            return None
        
        # Detect delimiter
        delimiters = [';', '|', '\t', ',']
        best_delimiter = None
        max_fields = 0
        
        for delimiter in delimiters:
            if delimiter == ';':
                # Handle quoted fields for semicolon
                field_count = 0
                in_quotes = False
                for char in header_line:
                    if char == '"':
                        in_quotes = not in_quotes
                    elif char == ';' and not in_quotes:
                        field_count += 1
                field_count += 1
            else:
                fields = header_line.split(delimiter)
                field_count = len(fields)
            
            if field_count > max_fields:
                max_fields = field_count
                best_delimiter = delimiter
        
        if not best_delimiter:
            logger.error("Could not detect delimiter")
            return None
        
        # Extract headers
        if best_delimiter == ';':
            headers = []
            current_field = ""
            in_quotes = False
            for char in header_line:
                if char == '"':
                    in_quotes = not in_quotes
                elif char == ';' and not in_quotes:
                    headers.append(current_field.strip())
                    current_field = ""
                else:
                    current_field += char
            headers.append(current_field.strip())
        else:
            headers = [field.strip() for field in header_line.split(best_delimiter)]
        
        logger.info(f"Detected schema: {len(headers)} fields, delimiter: '{best_delimiter}'")
        return headers, best_delimiter
        
    except Exception as e:
        logger.error(f"Failed to detect schema: {e}")
        return None

def create_unified_table(bq_client, dataset_id, table_id, schema_fields):
    """Create unified BigQuery table with source_file column."""
    try:
        dataset_ref = bq_client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)
        
        # Convert schema to BigQuery format and add source_file column
        schema = [
            bigquery.SchemaField("source_file", "STRING"),  # Track which file the row came from
            bigquery.SchemaField("file_date", "STRING"),    # Extract date from filename
        ]
        
        for field_name in schema_fields:
            # Clean field name (BigQuery requirements)
            clean_name = field_name.replace(' ', '_').replace('-', '_').replace('.', '_')
            clean_name = ''.join(c for c in clean_name if c.isalnum() or c == '_')
            if clean_name and not clean_name[0].isdigit():
                schema.append(bigquery.SchemaField(clean_name, 'STRING'))
        
        table = bigquery.Table(table_ref, schema=schema)
        table = bq_client.create_table(table, exists_ok=True)
        
        logger.info(f"Created/updated unified table: {dataset_id}.{table_id}")
        return table
    except Exception as e:
        logger.error(f"Failed to create table: {e}")
        return None

def load_all_anac_data():
    """Load all ANAC data files to a single BigQuery table using wildcard loading."""
    # Configuration
    dataset_id = os.getenv('BIGQUERY_DATASET_ID', 'ticket_airlines')
    table_id = os.getenv('BIGQUERY_ANAC_TABLE_ID', 'anac_flights')
    
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
    
    # Detect schema from sample file
    schema_info = detect_schema_from_sample(bq_client, storage_client, GOOGLE_GCS_BUCKET_NAME)
    if not schema_info:
        logger.error("Could not detect schema")
        return
    
    headers, delimiter = schema_info
    
    # Create unified table
    table = create_unified_table(bq_client, dataset_id, table_id, headers)
    if not table:
        return
    
    # GCS source URI with wildcard
    source_uri = f"gs://{GOOGLE_GCS_BUCKET_NAME}/anac_data/*.txt"
    logger.info(f"Loading from: {source_uri}")
    
    # Configure the load job
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # Skip header
        field_delimiter=delimiter,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Replace existing data
        autodetect=False,  # Use our detected schema
        max_bad_records=1000,  # Allow more bad records
        ignore_unknown_values=True,  # Ignore extra columns
        allow_quoted_newlines=True,  # Handle quoted fields with newlines
        allow_jagged_rows=True,  # Allow rows with different column counts
    )
    
    # Start the load job
    try:
        load_job = bq_client.load_table_from_uri(
            source_uri, table, job_config=job_config
        )
        
        logger.info("Starting BigQuery load job...")
        load_job.result()  # Wait for the job to complete
        
        # Get job details
        logger.info(f"Load job completed successfully!")
        logger.info(f"Loaded {load_job.output_rows} rows")
        logger.info(f"Processed {load_job.input_files} files")
        
        if load_job.errors:
            logger.warning(f"Job completed with {len(load_job.errors)} errors")
            for error in load_job.errors[:5]:  # Show first 5 errors
                logger.warning(f"Error: {error}")
        
    except Exception as e:
        logger.error(f"Load job failed: {e}")
        return False
    
    return True

if __name__ == "__main__":
    try:
        load_all_anac_data()
    except KeyboardInterrupt:
        logger.info("Interrupted by user") 