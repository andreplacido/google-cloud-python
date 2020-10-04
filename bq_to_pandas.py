
import pandas_gbq
import subprocess


from google.cloud import bigquery

from google.oauth2 import service_account
credentials = service_account.Credentials.from_service_account_file('/home/andre/beam/cred/vfpt-edis-d3596adc75e7.json')

#===ARGUMENTS===
import argparse

parser = argparse.ArgumentParser()
"""""
parser.add_argument('--input',
                    dest='input',
                    required=True,
                    help='Input file to process.')
"""""

parser.add_argument('--feedname',
                    dest='feedname',
                    required=True,
                    help='file or feedname.')

parser.add_argument('--table',
                    dest='table',
                    required=True,
                    help='bq table. Enter dataset.table!!')

#==== bash arguments====
parser.add_argument('--output',
                    dest='output',
                    required=True,
                    help='Output file to write results to.')

parser.add_argument('--timestamp',
                    dest='timestamp',
                    required=True,
                    help='Ex.: 20200925120000.')

parser.add_argument('--source',
                    dest='source',
                    required=True,
                    help='feed source system')

path_args, pipeline_args = parser.parse_known_args()

#sql_pattern = path_args.sql
outputs_prefix = path_args.output
feedname_prefix = path_args.feedname
table_prefix = path_args.table
timestamp_prefix = path_args.timestamp
source_prefix = path_args.source

#bucket_name = 'beamdmaap/output'
project_id = 'vfpt-edis'
dataset_id = 'datafusion'
table_id = table_prefix
sql_st = "select * from "+ dataset_id +"."+ table_prefix
output_folder = '/home/andre/google-cloud-python/output/'

#===FIM======

client = bigquery.Client(credentials=credentials,project=project_id)

sql = sql_st
#sql = """
#SELECT *
#FROM `datafusion.emp`
#"""

df = pandas_gbq.read_gbq(sql, project_id=project_id)

print(df)

#df.to_csv (r'/home/andre/google-cloud-python/output/dataframe.csv',sep='š', index = None, header=True)
df.to_csv (output_folder+feedname_prefix,sep='š', index = None, header=True)

#replace he delimiter
#read input file
#fin = open("/home/andre/google-cloud-python/output/dataframe.csv", "rt")
fin = open(output_folder+feedname_prefix, "rt")
#read file contents to string
data = fin.read()
#replace all occurrences of the required string
data = data.replace('š', '|;|')
#close the input file
fin.close()
#open the input file in write mode
#fin = open("/home/andre/google-cloud-python/output/dataframe.csv", "wt")
fin = open(output_folder+feedname_prefix, "wt")
#overrite the input file with the resulting data
fin.write(data)
#close the file
fin.close()


#Call Script
#"/home/andre/script/hello.sh"

subprocess.Popen(['bash', '/home/andre/script/hello.sh'])
subprocess.Popen(['bash', '/home/andre/script/arguments.sh','ahps', '43','Andre Placido'])
subprocess.Popen(['bash', '/home/andre/script/mariana.sh','-f','Andre Placido'])

#check Argumentos
print("Argumentos: ")
print("table Name: "+ str(table_prefix))
print("feedname: "+ str(feedname_prefix))
print("timestamp: "+ str(timestamp_prefix))
print("source_system: "+ str(source_prefix))
print("output_bucket: "+ str(outputs_prefix))
