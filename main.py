import functions_framework
from dotenv import load_dotenv

from client.bigquery_client import BigQueryClient
from data_api.el_data_api_call import EnerginetDataClient

load_dotenv()


@functions_framework.http
def main(request):
    # Initialize clients
    data_client = EnerginetDataClient()
    bq_client = BigQueryClient()

    # Load data from Energinet API
    records = data_client.load_data()

    # 2. Check if the list is not empty
    if records:
        # Load data into BigQuery
        bq_client.load_table_to_bigquery(records)

        return "Data loaded successfully.", 200
    else:
        # This will catch both API errors (empty dict)
        # or successful calls with no data (empty list)
        return "No data to load.", 204
