from google.cloud import bigquery
import os
import logging

logging.basicConfig(level=logging.INFO)

class BigQueryClient:
    def __init__(self):

        self.project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")
        if not self.project_id:
            logging.critical("GOOGLE_CLOUD_PROJECT env var not set.")
            raise ValueError("Missing required GOOGLE_CLOUD_PROJECT env var")
        
        self.client = bigquery.Client(project=self.project_id)
        self.dataset_id = os.environ.get("BIGQUERY_DATASET_ID")
        self.table_id = os.environ.get("BIGQUERY_TABLE_ID")

        if not self.dataset_id or not self.table_id:
            logging.critical("BIGQUERY_DATASET_ID or BIGQUERY_TABLE_ID env vars not set.")
            # You could raise an Exception here to stop execution
            raise ValueError("Missing required environment variables")

    def load_table_to_bigquery(self, data: list):



        dataset_ref = self.client.dataset(self.dataset_id)
        table_ref = dataset_ref.table(self.table_id)

        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("TimeDK", "TIMESTAMP"),
                bigquery.SchemaField("TimeUTC", "TIMESTAMP"),
                bigquery.SchemaField("PriceArea", "STRING"),
                bigquery.SchemaField("DayAheadPriceEUR", "FLOAT"),
                bigquery.SchemaField("DayAheadPriceDKK", "FLOAT"),
            ]
        )
        try:
            load_job = self.client.load_table_from_json(
                data, table_ref, job_config=job_config
            )

            load_job.result()  # Waits for the job to complete.

            logging.info(f"Loaded {load_job.output_rows} rows into {self.dataset_id}:{self.table_id}.")
        except Exception as e:
            logging.error(f"Error loading data into BigQuery: {e}")
        