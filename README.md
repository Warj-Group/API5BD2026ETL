# API5BD2026ETL
Repository for the implementation of an academic project in partnership with a real company. API5 2026.1 Fatec SJC/SP.
WARJ-10: Adicionado Sprint-1 para inicialiação do projeto


### Checks for syntax errors and best practices (breaks CI if it fails)
> ruff check .
### Checks if the formatting is standard (optional, but recommended)
> ruff format --check
### Fixes errors in files
> ruff check --fix .

### Create a virtual environment (Run the backend)
> .venv\Scripts\activate

> uvicorn app.main:app --reload --port 8000

### Install dependencies
> pip install -r requirements.txt

### Run the ETL
> python main.py