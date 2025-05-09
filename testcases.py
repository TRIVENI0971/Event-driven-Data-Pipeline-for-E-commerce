from google.cloud import bigquery
import pytest
import os

BQ_PROJECT = "black-seer-454106-e1"
BQ_DATASET = "DataPipeline"
BQ_ORDERS_TABLE="Orders"

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\Tammana Triveni\Downloads\black-seer-454106-e1-4bafb35faab6.json"
client = bigquery.Client()

def test_bigquery_connection():
    datasets = list(client.list_datasets())
    assert len(datasets) > 0 
    assert isinstance(datasets, list)
    assert client is not None, "BigQuery client is not initialized."

def test_bigquery_table_schema():
    table_ref = client.dataset(BQ_DATASET).table(BQ_ORDERS_TABLE)
    table = client.get_table(table_ref)

    expected_columns = {"order_id","customer_id","order_status","purchase_timestamp","approved_at","delivered_carrier_date","estimated_delivery_date"}
    actual_columns = {schema.name for schema in table.schema}

    assert expected_columns.issubset(actual_columns)
    assert "order_id" in expected_columns




