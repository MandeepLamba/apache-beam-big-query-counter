import sys

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from ext import ReadFromBigQuery

options = {
    'project': "<project_id>",
    'runner': 'Direct',
    'region': "asia-south1",
    'temp_location': "gs://dataflow-temp-storage/tmp",
}
# Define the pipeline options
options = PipelineOptions(sys.argv, **options)
pipline = beam.Pipeline(options=options)
pipline | 'Read from BigQuery' >> ReadFromBigQuery(
    query='SELECT * FROM `bigquery-public-data.samples.shakespeare` LIMIT 10',
    use_standard_sql=True) | "PrintData" >> beam.Map(print)

result = pipline.run()
result.wait_until_finish()

query_result = result.metrics().query()

for counter in query_result['counters']:
    print(f"Counter: {counter.key.metric.name}, Value: {counter.committed}")
