import logging
from dotenv import load_dotenv
from extract import run_extract
from transform import run_transform
from load import run_load

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def main():
    logger.info("Iniciando pipeline ETL...")
    raw_data = run_extract()
    transformed_data = run_transform(raw_data)
    run_load(transformed_data)
    logger.info("Pipeline ETL finalizado.")


if __name__ == "__main__":
    main()


"""

**`requirements.txt`**

pandas==2.2.0
sqlalchemy==2.0.25
psycopg2-binary==2.9.9
python-dotenv==1.0.0
openpyxl==3.1.2
requests==2.31.0


---

**`.env.example`**

DB_HOST=localhost
DB_PORT=55432
DB_NAME=project_analytics
DB_USER=analytics_user
DB_PASSWORD=


---

**`.gitignore`**

venv/
.venv/
.env
_pycache_/
*.pyc
*.log
.idea/
data/
*.csv
*.xlsx
"""