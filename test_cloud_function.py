from google.cloud import storage
from google.cloud import bigquery
from main import load_file_to_bigquery
import time


class TestCloudFunction:
    project_name = "mylearning-329506"

    def upload_file_to_gcs(self, bucket_name, source, destination, project_name):
        storage_client = storage.Client(project=project_name)
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(destination)
        blob.upload_from_filename(source)
        print('File {} uploaded to {}'.format(
            source,
            destination
        ))

    def test_case1(self):
        event = {
            'bucket': 'meril-testbucket-1',
            'contentType': 'text/csv',
            'name': 'test/client.csv',
            'timeCreated': '2021-10-22T14:22:53.036Z',
            'updated': '2021-10-22T14:22:53.036Z'
        }
        self.upload_file_to_gcs('meril-testbucket-1', 'sample/client.csv', 'test/client.csv', self.project_name)
        load_file_to_bigquery(event, {})
        client = bigquery.Client()
        QUERY = (
            'SELECT count(*) as `count` FROM `mylearning-329506.gcp_cloudfunction_test.client` LIMIT 1')
        query_job = client.query(QUERY)  # API request
        rows = query_job.result()
        query_success = False
        for row in rows:
            query_success = True
        assert query_success

    def test_case2(self):
        self.upload_file_to_gcs('meril-testbucket-1', 'sample/products.csv', 'test/products.csv', self.project_name)
        time.sleep(10)
        client = bigquery.Client()
        QUERY = (
            'SELECT count(*) as `count` FROM `mylearning-329506.gcp_cloudfunction_test.products` LIMIT 1')
        query_job = client.query(QUERY)  # API request
        rows = query_job.result()
        query_success = False
        for row in rows:
            query_success = True
        assert query_success

