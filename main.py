"""
    Trigger function for Google Cloud Function
    @auther Meril K Abraham
"""

import uuid
from google.cloud import bigquery
import pandas as pd

client = bigquery.Client()

dataset_name = "gcp_cloudfunction_test"
project_name = "mylearning-329506"

job_config = bigquery.LoadJobConfig(
    autodetect=True,
    create_disposition="CREATE_IF_NEEDED",
    write_disposition="WRITE_TRUNCATE",  # WRITE_APPEND / WRITE_TRUNCATE
    ignore_unknown_values=True,
    allow_jagged_rows=True,
)


def load_file_to_bigquery(event, context):
    """Triggered when a file is created in Cloud Storage bucket and uplaods to Google Big Query.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    file = event
    table_id = f"{project_name}.{dataset_name}.{file['name'].split('/')[-1].split('.')[0]}"
    trace_id = uuid.uuid4()
    print(f"[STARTED][{trace_id}] Loading {file['name']} to {table_id} big query table")
    try:
        file_name = f"gs://{file['bucket']}/{file['name']}"
        df = pd.read_csv(file_name, delimiter=';')  # using pandas for advanced transformations/processing
        df['created'] = file['timeCreated']  # Adding created time

        load_job = client.load_table_from_dataframe(df, table_id, job_config=job_config, location="europe-north1")
        load_job.result()  # Wait for the job to complete.
        print(f"[COMPLETED][{trace_id}]{file['name']} loaded successfully.")
    except Exception as ex:
        print(f"[ERROR][{trace_id}] Error loading {file['name']} to BigQuery.")
        print(ex.message)

