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

def list_anac_files(storage_client, bucket_name):
    """List all ANAC text files in the bucket."""
    try:
        bucket = storage_client.bucket(bucket_name)
        blobs = list(bucket.list_blobs(prefix='anac_data/'))
        
        # Filter for .txt files
        txt_files = [blob.name for blob in blobs if blob.name.endswith('.txt')]
        
        logger.info(f"Found {len(txt_files)} ANAC text files")
        return txt_files
        
    except Exception as e:
        logger.error(f"Failed to list ANAC files: {e}")
        return []

def detect_unified_schema(bq_client, storage_client, bucket_name):
    """Detect unified schema from all ANAC files."""
    try:
        files = list_anac_files(storage_client, bucket_name)
        if not files:
            logger.error("No ANAC files found to detect schema")
            return None
        
        all_headers = set()
        
        # Sample a few files to detect all possible headers
        sample_files = files[:min(5, len(files))]  # Check first 5 files
        
        for file_path in sample_files:
            logger.info(f"Analyzing schema from: {file_path}")
            
            # Download file content
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(file_path)
            
            # Try different encodings
            encodings = ['latin-1', 'iso-8859-1', 'utf-8', 'cp1252']
            content = None
            
            for encoding in encodings:
                try:
                    content = blob.download_as_text(encoding=encoding)
                    break
                except UnicodeDecodeError:
                    continue
            
            if not content:
                logger.warning(f"Could not decode {file_path}")
                continue
            
            # Detect delimiter and headers
            lines = content.split('\n')
            header_line = None
            for line in lines:
                if line.strip() and not line.startswith('#'):
                    header_line = line
                    break
            
            if not header_line:
                logger.warning(f"Could not find header in {file_path}")
                continue
            
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
                logger.warning(f"Could not detect delimiter for {file_path}")
                continue
            
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
            
            # Clean and add headers
            for header in headers:
                if header.strip():
                    # Clean header name for BigQuery
                    clean_header = header.strip().replace(' ', '_').replace('-', '_').replace('.', '_')
                    clean_header = ''.join(c for c in clean_header if c.isalnum() or c == '_')
                    if clean_header and not clean_header[0].isdigit():
                        all_headers.add(clean_header)
        
        # Convert to sorted list for consistent schema
        unified_headers = sorted(list(all_headers))
        logger.info(f"Detected {len(unified_headers)} unique columns across all files")
        logger.info(f"Columns: {unified_headers}")
        
        return unified_headers
        
    except Exception as e:
        logger.error(f"Failed to detect unified schema: {e}")
        return None

def create_unified_table(bq_client, dataset_id, table_id, schema_fields):
    """Create unified BigQuery table with detected schema."""
    try:
        dataset_ref = bq_client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)
        
        # Create schema with metadata columns first, then all detected fields
        schema = [
            bigquery.SchemaField("source_file", "STRING"),
            bigquery.SchemaField("file_date", "STRING"),
        ]
        
        # Add all detected fields
        for field_name in schema_fields:
            schema.append(bigquery.SchemaField(field_name, "STRING"))
        
        table = bigquery.Table(table_ref, schema=schema)
        table = bq_client.create_table(table, exists_ok=True)
        
        logger.info(f"Created/updated unified table: {dataset_id}.{table_id} with {len(schema)} columns")
        return table
    except Exception as e:
        logger.error(f"Failed to create table: {e}")
        return None

def load_file_to_bigquery(bq_client, storage_client, bucket_name, file_path, dataset_id, table_id, unified_schema):
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
        
        # Clean headers to match unified schema
        clean_headers = []
        for header in headers:
            clean_header = header.strip().replace(' ', '_').replace('-', '_').replace('.', '_')
            clean_header = ''.join(c for c in clean_header if c.isalnum() or c == '_')
            if clean_header and not clean_header[0].isdigit():
                clean_headers.append(clean_header)
            else:
                clean_headers.append(f"field_{len(clean_headers)}")
        
        logger.info(f"Detected {len(clean_headers)} columns in {file_path}")
        
        # Extract filename and date
        filename = os.path.basename(file_path)
        file_date = filename.replace('.txt', '').replace('basica', '')
        
        # Create enhanced content with unified schema order
        enhanced_headers = ["source_file", "file_date"] + unified_schema
        enhanced_lines = []
        
        for line in lines[1:]:  # Skip header
            if line.strip():
                # Parse the original line
                if best_delimiter == ';':
                    # Handle quoted fields
                    fields = []
                    current_field = ""
                    in_quotes = False
                    for char in line:
                        if char == '"':
                            in_quotes = not in_quotes
                        elif char == ';' and not in_quotes:
                            fields.append(current_field.strip())
                            current_field = ""
                        else:
                            current_field += char
                    fields.append(current_field.strip())
                else:
                    fields = [field.strip() for field in line.split(best_delimiter)]
                
                # Create row with unified schema
                row_data = [filename, file_date]  # Metadata columns
                
                # Map fields to unified schema
                for schema_field in unified_schema:
                    field_value = ""
                    if schema_field in clean_headers:
                        field_index = clean_headers.index(schema_field)
                        if field_index < len(fields):
                            field_value = fields[field_index].replace('"', "'")
                    row_data.append(field_value)
                
                enhanced_line = best_delimiter.join(row_data)
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

def clear_existing_table(bq_client, dataset_id, table_id):
    """Clear existing data from the table."""
    try:
        dataset_ref = bq_client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)
        
        # Delete table if it exists
        bq_client.delete_table(table_ref, not_found_ok=True)
        logger.info(f"Cleared existing table: {dataset_id}.{table_id}")
        return True
    except Exception as e:
        logger.error(f"Failed to clear table: {e}")
        return False

def load_all_anac_data():
    """Load all ANAC data files to a single BigQuery table."""
    # Configuration
    dataset_id = os.getenv('GOOGLE_BIGQUERY_DATASET_ID', 'ticket_airlines')
    table_id = os.getenv('GOOGLE_BIGQUERY_ANAC_TABLE_ID', 'anac_flights')
    
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
    
    # Detect unified schema from all files
    unified_schema = detect_unified_schema(bq_client, storage_client, GOOGLE_GCS_BUCKET_NAME)
    if not unified_schema:
        logger.error("Could not detect unified schema")
        return
    
    # Clear existing table to start fresh
    clear_existing_table(bq_client, dataset_id, table_id)
    
    # Create unified table with detected schema
    table = create_unified_table(bq_client, dataset_id, table_id, unified_schema)
    if not table:
        return
    
    # List ANAC files
    files = list_anac_files(storage_client, GOOGLE_GCS_BUCKET_NAME)
    if not files:
        logger.warning("No ANAC files found in GCS")
        return
    
    # Load each file individually to ensure proper column mapping
    successful_loads = 0
    
    for file_path in files:
        logger.info(f"Processing: {file_path}")
        if load_file_to_bigquery(bq_client, storage_client, GOOGLE_GCS_BUCKET_NAME, file_path, dataset_id, table_id, unified_schema):
            successful_loads += 1
    
    logger.info(f"Successfully loaded {successful_loads}/{len(files)} files to {dataset_id}.{table_id}")
    
    return True

if __name__ == "__main__":
    try:
        load_all_anac_data()
    except KeyboardInterrupt:
        logger.info("Interrupted by user") 