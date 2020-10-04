#===========================
#Andre Placido
#Create Date - 2020/09/25
#Last Update - 2020/09/28
#===========================

import pandas_gbq
import subprocess


from google.cloud import bigquery

#===ARGUMENTS===
import argparse

parser = argparse.ArgumentParser()

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

outputs_prefix = path_args.output
feedname_prefix = path_args.feedname
table_prefix = path_args.table
timestamp_prefix = path_args.timestamp
source_prefix = path_args.source

#bucket_name = 'beamdmaap/output'
project_id = 'vf-pt-datahub-prd-mgmt'
dataset_id = 'feedmonitor_ops'
table_id = table_prefix
sql_st = "select * from "+ dataset_id +"."+ table_prefix
output_folder = '/home/dmaap/cherry_picking/input_folder/PT_RAW-'

#===FIM======

client = bigquery.Client(project=project_id)

sql = sql_st

df = pandas_gbq.read_gbq(sql, project_id=project_id)

output_file = (output_folder+source_prefix+'-'+feedname_prefix+'-'+timestamp_prefix+'-1_1.csv')
#print(df)

df.to_csv (output_file,sep='š', index = None, header=True)

#replace he delimiter
fin = open(output_file, "rt")
data = fin.read()
data = data.replace('š', '|;|')
fin.close()
fin = open(output_file, "wt")
#overrite the input file with the resulting data
fin.write(data)
#close the file
fin.close()

#Call Script
subprocess.Popen(['bash', '/home/dmaap/cherry_picking/move_file-cp.sh','-f', feedname_prefix,'-t',timestamp_prefix,'-s', source_prefix, '-b', outputs_prefix ])

#check Argumentos
print("Argumentos: ")
print("table Name: "+ str(table_prefix))
print("feedname: "+ str(feedname_prefix))
print("timestamp: "+ str(timestamp_prefix))
print("source_system: "+ str(source_prefix))
print("output_bucket: "+ str(outputs_prefix))

