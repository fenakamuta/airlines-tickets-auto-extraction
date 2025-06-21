import os
from dotenv import load_dotenv
load_dotenv()

# Google Cloud Storage
GOOGLE_GCS_BUCKET_NAME = os.getenv('GOOGLE_GCS_BUCKET_NAME', 'your-anac-data-bucket')
GCS_CREDENTIALS_PATH = os.getenv('GCS_CREDENTIALS_PATH')

# ANAC Website
ANAC_YEAR = os.getenv('ANAC_YEAR', '2024')
ANAC_BASE_URL = f"https://www.gov.br/anac/pt-br/assuntos/regulados/empresas-aereas/Instrucoes-para-a-elaboracao-e-apresentacao-das-demonstracoes-contabeis/envio-de-informacoes/basica/{ANAC_YEAR}"

# Processing - Temp directory in current working directory for visibility
TEMP_DIR = os.getenv('TEMP_DIR', './anac_temp')
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'