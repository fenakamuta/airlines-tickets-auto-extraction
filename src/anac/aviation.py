import os
import zipfile
import tempfile
import requests
from pathlib import Path
from playwright.sync_api import sync_playwright
import time
from google.cloud import storage
from google.oauth2 import service_account
import logging
import sys
from urllib.parse import urljoin, urlparse

# Add src directory to path for imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from config import GOOGLE_GCS_BUCKET_NAME, GCS_CREDENTIALS_PATH, ANAC_BASE_URL, LOG_LEVEL, LOG_FORMAT, TEMP_DIR

logging.basicConfig(level=getattr(logging, LOG_LEVEL), format=LOG_FORMAT)
logger = logging.getLogger(__name__)

def init_gcs_client():
    """Initialize Google Cloud Storage client."""
    try:
        if GCS_CREDENTIALS_PATH and os.path.exists(GCS_CREDENTIALS_PATH):
            credentials = service_account.Credentials.from_service_account_file(GCS_CREDENTIALS_PATH)
            return storage.Client(credentials=credentials)
        return storage.Client()
    except Exception as e:
        logger.error(f"GCS client init failed: {e}")
        return None

def download_file(url, filename, temp_dir, max_retries=3):
    """Download a file with retries."""
    for attempt in range(max_retries):
        try:
            logger.info(f"Downloading {filename} from {url} (attempt {attempt + 1})")
            
            # Disable SSL verification for government websites (common issue)
            response = requests.get(url, stream=True, timeout=30, verify=False)
            response.raise_for_status()
            
            file_path = os.path.join(temp_dir, filename)
            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            logger.info(f"Downloaded {filename}")
            return file_path
        except Exception as e:
            logger.warning(f"Download attempt {attempt + 1} failed: {e}")
            if attempt == max_retries - 1:
                logger.error(f"Download failed: {filename}")
                return None
            time.sleep(2 ** attempt)
    return None

def extract_zip(zip_path, extract_dir):
    """Extract ZIP file."""
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)
        
        extracted_files = []
        for root, _, files in os.walk(extract_dir):
            extracted_files.extend([os.path.join(root, f) for f in files])
        
        logger.info(f"Extracted {len(extracted_files)} files")
        return extracted_files
    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        return []

def upload_to_gcs(local_path, gcs_path, gcs_client, bucket_name, max_retries=3):
    """Upload file to GCS with retries."""
    if not gcs_client:
        return False
    
    for attempt in range(max_retries):
        try:
            bucket = gcs_client.bucket(bucket_name)
            blob = bucket.blob(gcs_path)
            blob.upload_from_filename(local_path)
            logger.info(f"Uploaded: {gcs_path}")
            return True
        except Exception as e:
            if attempt == max_retries - 1:
                logger.error(f"Upload failed: {local_path}")
                return False
            time.sleep(2 ** attempt)
    return False

def process_anac_data():
    """Main function to process ANAC data."""
    if GOOGLE_GCS_BUCKET_NAME == 'your-anac-data-bucket':
        logger.error("Set GOOGLE_GCS_BUCKET_NAME in config.py or environment")
        return
    
    logger.warning("SSL verification disabled for government website compatibility")
    
    gcs_client = init_gcs_client()
    
    # Create temp directory in a known location
    temp_base = Path(TEMP_DIR)
    temp_base.mkdir(parents=True, exist_ok=True)
    temp_dir = temp_base / f"anac_processing_{int(time.time())}"
    temp_dir.mkdir(exist_ok=True)
    
    logger.info(f"Temp directory: {temp_dir.absolute()}")
    
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            
            logger.info("Loading ANAC website...")
            page.goto(ANAC_BASE_URL, wait_until="networkidle", timeout=30000)
            
            # Handle cookies
            try:
                page.wait_for_selector('button.br-button.secondary.small.btn-accept[aria-label="Aceitar cookies"]', timeout=10000)
                page.click('button.br-button.secondary.small.btn-accept[aria-label="Aceitar cookies"]')
            except:
                pass
            
            # Find ZIP links
            page.wait_for_selector('#content-core', timeout=15000)
            links = page.query_selector_all('#content-core a[href*=".zip"]')
            
            if not links:
                logger.warning("No ZIP files found")
                return
            
            logger.info(f"Found {len(links)} ZIP files")
            
            # Process each ZIP file
            for i, link in enumerate(links, 1):
                href = link.get_attribute('href')
                text = link.inner_text().strip()
                
                if not href:
                    continue
                
                # Convert relative URL to absolute URL
                if not urlparse(href).netloc:
                    href = urljoin(ANAC_BASE_URL, href)
                
                # Remove /view suffix if present (it's a web page, not the actual file)
                if href.endswith('/view'):
                    href = href[:-5]  # Remove '/view'
                
                # Clean filename
                safe_text = "".join(c for c in text if c.isalnum() or c in (' ', '-', '_')).rstrip()
                filename = f"anac_data_{i}_{safe_text.replace(' ', '_').replace('/', '_')}.zip"
                logger.info(f"Processing {i}/{len(links)}: {text}")
                logger.info(f"URL: {href}")
                
                # Download
                zip_path = download_file(href, filename, temp_dir)
                if not zip_path:
                    continue
                
                # Extract
                extract_dir = temp_dir / f"extracted_{i}"
                extract_dir.mkdir(exist_ok=True)
                extracted_files = extract_zip(zip_path, extract_dir)
                
                # Upload extracted files with simple names
                for file_path in extracted_files:
                    # Get the original filename from the ZIP
                    original_filename = os.path.basename(file_path)
                    
                    # Create simple GCS path: anac_data/basica2024-01.txt
                    gcs_path = f"anac_data/{original_filename}"
                    upload_to_gcs(file_path, gcs_path, gcs_client, GOOGLE_GCS_BUCKET_NAME)
            
            logger.info("Processing completed successfully")
            browser.close()
    
    except Exception as e:
        logger.error(f"Processing failed: {e}")
    
    finally:
        # Cleanup
        try:
            import shutil
            shutil.rmtree(temp_dir)
            logger.info(f"Cleaned up: {temp_dir}")
        except Exception as e:
            logger.error(f"Cleanup failed: {e}")
            logger.info(f"Manual cleanup needed: {temp_dir}")

if __name__ == "__main__":
    try:
        process_anac_data()
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
