from google.cloud import bigquery

from google.oauth2 import service_account
credentials = service_account.Credentials.from_service_account_file('/home/andre/beam/cred/vfpt-edis-d3596adc75e7.json')

bucket_name = 'beamdmaap/output'
project_id = 'vfpt-edis'
dataset_id = 'datafusion'
table_id = 'emp'

client = bigquery.Client(credentials=credentials,project=project_id)

destination_uri = "gs://{}/{}".format(bucket_name, "shakespeare.csv")
dataset_ref = bigquery.DatasetReference(project_id, dataset_id)
table_ref = dataset_ref.table(table_id)

extract_job = client.extract_table(
    table_ref,
    destination_uri,
    # Location must match that of the source table.
    location="EU",
)  # API request
extract_job.result()  # Waits for job to complete.

print(
    "Exported {}:{}.{} to {}".format(project_id, dataset_id, table_id, destination_uri)
)