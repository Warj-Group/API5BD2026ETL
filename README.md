<div align="center">
   <img src="public/warj_banner2.png" width="850" alt="API5 Banner">

# API5BD2026 - ETL

[![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=flat&logo=python&logoColor=white)](https://www.python.org/)
[![Node.js](https://img.shields.io/badge/Node.js-22.22.0-339933?style=flat&logo=nodedotjs&logoColor=white)](https://nodejs.org/)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![Checked with mypy](https://www.mypy-lang.org/static/mypy_badge.svg)](https://mypy-lang.org/)
[![Husky](https://img.shields.io/badge/Husky-64b5f6?style=flat&logo=dog&logoColor=white)](https://typicode.github.io/husky/)
[![SonarCloud Quality Gate](https://img.shields.io/sonar/quality_gate/Warj-Group_API5BD2026ETL?server=https%3A%2F%2Fsonarcloud.io&logo=sonarcloud&style=flat)](https://sonarcloud.io/summary/new_code?id=Warj-Group_API5BD2026ETL)
</div>

<br>

## Initial Configuration

It is required to use the following tools in your local environment:

* **Python 3.11**, download: [Official Python Website](https://www.python.org/downloads/release/python-3110/).
* **Node.js 22.22.0**, download: [Official Node.js Website](https://nodejs.org/en/blog/release/v22.22.0).

<br>

## Environment Setup

Clone the repository to your local environment and open it in your preferred IDE:

```bash
git clone https://github.com/Warj-Group/API5BD2026ETL.git
cd API5BD2026ETL
```

### Virtual Environment (venv)
Create and activate the virtual environment **before** installing any dependencies.

* **Windows:**
```bash
python -m venv venv
venv\Scripts\activate
```

* **Linux/Mac/Git Bash:**
```bash
python3 -m venv venv
source venv/bin/activate
```

### Setup Script
With the virtual environment activated and Node.js installed, run the automation script to download dependencies (via `pip` and `npm`) and configure the Git hooks (Husky):

* **Windows:** `setup_etl.bat`
* **Linux/Mac/Git Bash:** `bash setup_etl.sh`

### Execution
Since the ETL is a data extraction and loading process, execution is done by running the main pipeline script:

```bash
# Run the main script (adjust the path according to the final structure)
python main.py
```

<br> 

## Development and Quality

During development, to maintain code quality and standards, we use modern tools for linting, formatting, and static type checking.

### 1. Ruff: Linter and Imports
Searches for unused variables, duplicated imports, syntax errors, etc.
* **Check only:** `ruff check .`
* **Fix automatically (Recommended):** `ruff check --fix .`

### 2. Ruff: Formatter
Adjusts spaces, line breaks, and quotes to match the official standard.
* **Check only (used in CI):** `ruff format --check .`
* **Format automatically (Recommended):** `ruff format .`

### 3. Mypy: Type Checking
Reads the code looking for static typing errors (e.g., passing a `string` to a function that requires an `int`).
* **Check:** `mypy .`

<br>

## Contribution Guidelines

To ensure traceability between YouTrack tasks and GitHub commits, strictly follow the standards below:

### 1. Commit Messages
Messages must contain the task ID for automatic integration with YouTrack:
* **Format:** `{type}/{yt_id}: Description`
* **Example:** `feat/WARJ-1: implemented oracle database extractor`

### 2. Branch Naming Convention
Create working branches linked to the Sprint cards:
* **Format:** `{type}/{yt_id}-brief-description`
* **Example:** `feature/WARJ-1-oracle-extractor`

### 3. Automatic Validation
The project uses **Husky** and **Commitlint**. If the commit standard is not followed or if Ruff/Mypy detects structural flaws, the submission (push/commit) will be blocked by the terminal with the appropriate correction instructions.

<br>

## Additional Documentation
For details on the group's architecture, CI/CD, and design patterns, access our Wiki: [WARJ-GROUP - Wiki Documentation](https://github.com/Warj-Group/API5BD2026Main/wiki)