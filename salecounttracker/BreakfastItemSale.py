from __future__ import absolute_import
import argparse
import csv
import logging

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import parse_table_schema_from_json

class PipelineBuilder:
	gcs_bucket = 'saledata-deba'
	input_folder_gs = 'input'
	schema_folder_gs = 'schema'

	def get_saleDate(self, record):
		saleDate = (record.split(','))[0]
		return saleDate

	def get_id_item_count(self, record):
		field_array = record.split(',')
		id_item_count = (field_array[4] + '_' + field_array[3], int(field_array[2]))
		return id_item_count

	def get_total(self, record):
                return [int(str(record[0]).split('_')[0]), str(record[0]).split('_')[1], sum(record[1])]

	def get_bq_schema(self, env, schema_file):

		if env == 'dev':
			with open(schema_file) as f:
				data = f.read()
				schema_str = '{"fields": ' + data + '}'
				#print(schema_str)
		elif env == 'test':
			from google.cloud import storage
			client = storage.Client()
			bucket = client.get_bucket(self.gcs_bucket) # ('saledata-deba')
			blob = bucket.get_blob(self.schema_folder_gs + '/' + schema_file) #('schema/BreadBasketSchema.json')
			schema_str = '{"fields": ' + blob.download_as_string().decode("utf-8") + '}'
			# print(schema_str)

		schema = parse_table_schema_from_json(schema_str)
		return schema

	# Here the input would be a "list", e.g. ['10/30/2016', 3]
	def form_json(self, record, env, schema_file):
		schema = self.get_bq_schema(env, schema_file)

		field_map = [f for f in schema.fields]
		# print schema_str

		json_record = {}
		values = [x for x in record]
		i = 0
		for value in values:
			if i == 0:
				json_record['item_id'] = value
			if i == 1:
				json_record['item'] = value
			if i == 2:
				json_record['sale_count'] = value
			# json_record[field_map[i].name] = value
			i += 1
		return json_record

class Printer(beam.DoFn):
        # process and self are keywords, element is not
        def process(self, element):
                print (element)

class TypeOf(beam.DoFn):
        def process(self, element):
                print (type(element))

def run(argv=None):
	parser = argparse.ArgumentParser()

	parser.add_argument(
		'--env',
		default='dev' # dev/test/prod
	)

	parser.add_argument(
		'--input',
		default='BreadBasket_DMS_Updated_Test.csv'
	)

	parser.add_argument(
        	'--schema_file',
        	default='BreadBasketSchema_Updated.json'
	)

	parser.add_argument(
                '--output',
                default='horizontal-time-237307:BreakfastDataset.BreakfastDatatable'
        )

	known_args, pipeline_args = parser.parse_known_args(argv)

	pipelinebuilder = PipelineBuilder()

	schema_bq = pipelinebuilder.get_bq_schema(known_args.env, known_args.schema_file)

	if known_args.env == 'dev':
		input_file_with_path = known_args.input
	else:
		input_file_with_path = 'gs://' + PipelineBuilder.gcs_bucket + '/' + PipelineBuilder.input_folder_gs + '/' + known_args.input


	p = beam.Pipeline(options=PipelineOptions(pipeline_args))

	(p
		| 'Read from a File' >> beam.io.ReadFromText(input_file_with_path, skip_header_lines=1)
		| 'Get item ID, item desc and count' >> beam.Map(lambda s: pipelinebuilder.get_id_item_count(s))
		#| 'Set count to 1 per date' >> beam.Map(lambda s: pipelinebuilder.set_count_1_perdate(s))
		| 'Group by date' >> beam.GroupByKey()
		| 'Sum group of 1s against each date' >> beam.Map(lambda s:pipelinebuilder.get_total(s))
		| 'Form JSON' >> beam.Map(lambda s:pipelinebuilder.form_json(s, known_args.env, known_args.schema_file))
		#| 'Print the data' >> beam.ParDo(Printer())
		| 'Write to BQ' >> beam.io.Write(
			beam.io.BigQuerySink(
				known_args.output,
				schema=schema_bq, # 'sale_date:STRING, sale_count:INTEGER', # schema_bq,
				create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
				write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
			)
		)
	)

	p.run().wait_until_finish()

if __name__ == '__main__':
	# logging.getLogger().setLevel(logging.INFO)
	run()
