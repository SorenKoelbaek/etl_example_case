# PySpark ETL Demo

This is a minimal demo repo to illustrate an ETL pipeline in PySpark.
The example contains a working example of reading data from a REST API, reading pokemon and then appending the result to a table.



## Structure
- `dependencies/api.py` – simple API client
- `notebooks/ingest.py` – PySpark ETL pipeline notebook
- `main.py` – runs the notebook as a test
- `requirements.txt` – dependencies

### Setup
Create a virtual environment and install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```
### Run Locally
```bash 
python3 run_local.py