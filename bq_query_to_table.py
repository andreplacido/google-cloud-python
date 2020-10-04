
from google.cloud import bigquery

from google.oauth2 import service_account
credentials = service_account.Credentials.from_service_account_file('/home/andre/beam/cred/vfpt-edis-d3596adc75e7.json')

project_id = 'vfpt-edis'
dataset_id = 'datafusion'
table_id = 'emp'

client = bigquery.Client(credentials=credentials,project=project_id)

job_config = bigquery.QueryJobConfig()
# Set the destination table
table_ref = client.dataset(dataset_id).table("target_table")
job_config.destination = table_ref
sql = """
    SELECT *
    FROM vfpt-edis.datafusion.emp
"""

# Start the query, passing in the extra configuration.
query_job = client.query(
    sql,
    # Location must match that of the dataset(s) referenced in the query
    # and of the destination table.
    location="EU",
    job_config=job_config,
)  # API request - starts the query

query_job.result()  # Waits for the query to finish
print("Query results loaded to table {}".format(table_ref.path))