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

def create_unified_table(bq_client, dataset_id, table_id):
    """Create unified BigQuery table with consistent schema."""
    try:
        dataset_ref = bq_client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)
        
        # Define consistent schema for ANAC data
        schema = [
            bigquery.SchemaField("source_file", "STRING"),
            bigquery.SchemaField("file_date", "STRING"),
            # Add common ANAC fields in consistent order
            bigquery.SchemaField("empresa_aerea", "STRING"),
            bigquery.SchemaField("codigo_empresa", "STRING"),
            bigquery.SchemaField("origem", "STRING"),
            bigquery.SchemaField("destino", "STRING"),
            bigquery.SchemaField("data_voo", "STRING"),
            bigquery.SchemaField("hora_partida", "STRING"),
            bigquery.SchemaField("hora_chegada", "STRING"),
            bigquery.SchemaField("assentos_vendidos", "STRING"),
            bigquery.SchemaField("assentos_ofertados", "STRING"),
            bigquery.SchemaField("fator_ocupacao", "STRING"),
            bigquery.SchemaField("receita_passageiros", "STRING"),
            bigquery.SchemaField("receita_carga", "STRING"),
            bigquery.SchemaField("receita_correio", "STRING"),
            bigquery.SchemaField("receita_total", "STRING"),
        ]
        
        table = bigquery.Table(table_ref, schema=schema)
        table = bq_client.create_table(table, exists_ok=True)
        
        logger.info(f"Created/updated unified table: {dataset_id}.{table_id}")
        return table
    except Exception as e:
        logger.error(f"Failed to create table: {e}")
        return None

def load_file_to_bigquery(bq_client, storage_client, bucket_name, file_path, dataset_id, table_id):
    """Load a single ANAC file to BigQuery with proper column mapping."""
    try:
        # Download file content with proper encoding
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_path)
        
        # Try different encodings for Portuguese text files
        encodings = ['latin-1', 'iso-8859-1', 'utf-8', 'cp1252']
        content = None
        
        for encoding in encodings:
            try:
                content = blob.download_as_text(encoding=encoding)
                logger.info(f"Successfully decoded {file_path} with {encoding} encoding")
                break
            except UnicodeDecodeError:
                continue
        
        if content is None:
            logger.error(f"Could not decode {file_path} with any encoding")
            return False
        
        # Detect delimiter and headers
        lines = content.split('\n')
        header_line = None
        for line in lines:
            if line.strip() and not line.startswith('#'):
                header_line = line
                break
        
        if not header_line:
            logger.warning(f"Could not find header in {file_path}")
            return False
        
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
            logger.error(f"Could not detect delimiter for {file_path}")
            return False
        
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
        
        logger.info(f"Detected {len(headers)} columns in {file_path}")
        
        # Extract filename and date
        filename = os.path.basename(file_path)
        file_date = filename.replace('.txt', '').replace('basica', '')
        
        # Create enhanced content with consistent column order
        enhanced_headers = ["source_file", "file_date"] + headers
        enhanced_lines = []
        
        for line in lines[1:]:  # Skip header
            if line.strip():
                # Clean the line
                cleaned_line = line.strip()
                if '"' in cleaned_line:
                    cleaned_line = cleaned_line.replace('"', "'")
                
                # Add metadata columns
                enhanced_line = f"{filename}{best_delimiter}{file_date}{best_delimiter}{cleaned_line}"
                enhanced_lines.append(enhanced_line)
        
        if not enhanced_lines:
            logger.warning(f"No data rows found in {file_path}")
            return False
        
        # Create enhanced content
        enhanced_content = best_delimiter.join(enhanced_headers) + '\n' + '\n'.join(enhanced_lines)
        
        # Get table reference
        dataset_ref = bq_client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)
        table = bq_client.get_table(table_ref)
        
        # Load data
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,  # Skip header
            field_delimiter=best_delimiter,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # Append to existing data
            autodetect=False,  # Use our defined schema
            max_bad_records=1000,  # Allow more bad records
            ignore_unknown_values=True,  # Ignore extra columns
            allow_quoted_newlines=True,  # Handle quoted fields with newlines
            allow_jagged_rows=True,  # Allow rows with different column counts
        )
        
        # Create temporary file for loading
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as temp_file:
            temp_file.write(enhanced_content)
            temp_file_path = temp_file.name
        
        try:
            with open(temp_file_path, 'rb') as source_file:
                job = bq_client.load_table_from_file(
                    source_file, table, job_config=job_config
                )
                job.result()  # Wait for the job to complete
            
            logger.info(f"Loaded {job.output_rows} rows from {filename}")
            return True
            
        finally:
            # Clean up temp file
            os.unlink(temp_file_path)
            
    except Exception as e:
        logger.error(f"Failed to load {file_path}: {e}")
        return False

def load_all_anac_data():
    """Load all ANAC data files to a single BigQuery table."""
    # Configuration
    dataset_id = os.getenv('BIGQUERY_DATASET_ID', 'anac_data')
    table_id = os.getenv('BIGQUERY_TABLE_ID', 'anac_flights')
    
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
    
    # Create unified table with consistent schema
    table = create_unified_table(bq_client, dataset_id, table_id)
    if not table:
        return
    
    # List ANAC files
    files = list_anac_files(storage_client, GOOGLE_GCS_BUCKET_NAME)
    if not files:
        logger.warning("No ANAC files found in GCS")
        return
    
    # Load each file individually to ensure proper column mapping
    successful_loads = 0
    total_rows = 0
    
    for file_path in files:
        logger.info(f"Processing: {file_path}")
        if load_file_to_bigquery(bq_client, storage_client, GOOGLE_GCS_BUCKET_NAME, file_path, dataset_id, table_id):
            successful_loads += 1
    
    logger.info(f"Successfully loaded {successful_loads}/{len(files)} files to {dataset_id}.{table_id}")
    
    return True

if __name__ == "__main__":
    try:
        load_all_anac_data()
    except KeyboardInterrupt:
        logger.info("Interrupted by user") 