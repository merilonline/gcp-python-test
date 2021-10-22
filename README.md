
# GCP-Python-Test

GCP Python test project to upload a file to gcs, reusable function to upload to bigquery, unittest by quering the final table row count

### Details for Cloud Function
* Trigger function `load_file_to_bigquery` in `main.py` 
* Dependencies in `requirements.txt`

### Unit test

* set env variable `export GOOGLE_APPLICATION_CREDENTIALS=/path/auth_key`
* run unittest using `pytest -s test_cloud_function.py`

```shell
pip install -r requirements.txt 
pip install -r requirements_dev.txt

export GOOGLE_APPLICATION_CREDENTIALS=<PATH_TO_GCP_AUTH_CREDENTIAL>
pytest -s test_cloud_function.py
```