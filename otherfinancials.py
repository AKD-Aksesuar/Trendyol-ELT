import requests
from dotenv import load_dotenv
import os
import polars as pl
import json
import time
import io
from google.cloud import bigquery

# Load Environment Variables
load_dotenv()

# --- API Credentials ---
TRENDYOL_API_TOKEN = os.getenv('TRENDYOL_API_TOKEN')
TRENDYOL_SUPPLIER_ID = os.getenv('TRENDYOL_SUPPLIER_ID')
API_URL = f"https://apigw.trendyol.com/integration/finance/che/sellers/{TRENDYOL_SUPPLIER_ID}/otherfinancials"

# --- Google Cloud BigQuery Credentials and Project ---
GOOGLE_APPLICATION_CREDENTIALS = os.getenv('GOOGLE_APPLICATION_CREDENTIALS') # Path to your service account key
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID') # Your Google Cloud Project ID
BIGQUERY_DATASET_ID = os.getenv('BIGQUERY_DATASET_ID') # Name of your BigQuery dataset
BIGQUERY_TEMP_TABLE_ID = "otherfinancials_temp" # Temporary table for otherfinancials
BIGQUERY_MAIN_TABLE_ID = "otherfinancials" # Main table for otherfinancials
BIGQUERY_LOCATION = os.getenv('BIGQUERY_LOCATION') # BigQuery dataset location

# --- API Headers ---
headers ={
    'Authorization': f'Basic {TRENDYOL_API_TOKEN}',
    'User-Agent': f'{TRENDYOL_SUPPLIER_ID} - SelfIntegration'
}

# --- Transaction Types to iterate through ---
TRANSACTION_TYPES = ['CashAdvance', 'WireTransfer', 'IncomingTransfer', 'ReturnInvoice', 'CommissionAgreementInvoice', 'PaymentOrder', 'DeductionInvoices', 'FinancialItem', 'Stoppage']


# --- Date Range Configuration ---
N_DAYS_TO_FETCH = 30 # Define the total number of days back you want to fetch data for
MAX_DAYS_PER_CHUNK = 15 # Max days per API call chunk (safety margin under 90)

OVERALL_END_DATE_MS = int(time.time() * 1000)
OVERALL_START_DATE_MS = OVERALL_END_DATE_MS - (N_DAYS_TO_FETCH * 24 * 60 * 60 * 1000)

# Base API Parameters (startDate, endDate, transactionType will be set per chunk/type)
base_params = {
    'supplierId': TRENDYOL_SUPPLIER_ID,
    'page': 0,
    'size': 500 # otherfinancials API allows up to 500
}

# --- Pagination and Chunking Function for otherfinancials ---
def fetch_otherfinancials_in_chunks(transaction_type: str):
    """
    Fetches otherfinancials for a given transaction_type from the Trendyol API 
    in chunks to respect date range limitations, with pagination within each chunk.
    Returns a list of raw settlement dictionaries.
    """
    all_otherfinancials_for_type = []
    current_chunk_start_ms = OVERALL_START_DATE_MS
    max_chunk_duration_ms = MAX_DAYS_PER_CHUNK * 24 * 60 * 60 * 1000

    print(f"\nFetching otherfinancials for Transaction Type: {transaction_type}")
    print(f"Overall fetch period: From {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(OVERALL_START_DATE_MS/1000))} to {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(OVERALL_END_DATE_MS/1000))}")

    while current_chunk_start_ms < OVERALL_END_DATE_MS:
        current_chunk_end_ms = min(current_chunk_start_ms + max_chunk_duration_ms, OVERALL_END_DATE_MS)
        
        print(f"  Fetching for date chunk: "
              f"From {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(current_chunk_start_ms/1000))} "
              f"to {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(current_chunk_end_ms/1000))}")

        current_page_for_chunk = 0
        while True:
            call_params = base_params.copy()
            call_params['startDate'] = current_chunk_start_ms
            call_params['endDate'] = current_chunk_end_ms
            call_params['transactionType'] = transaction_type
            call_params['page'] = current_page_for_chunk

            print(f"    Fetching page: {current_page_for_chunk} for current chunk...")

            try:
                response = requests.get(API_URL, headers=headers, params=call_params)
                response.raise_for_status()
                data = response.json()
                otherfinancials_on_page = data.get('content', [])

                if not otherfinancials_on_page:
                    print("    No more otherfinancials found on this page or content is empty.")
                    break 
                all_otherfinancials_for_type.extend(otherfinancials_on_page)
                print(f"    Fetched {len(otherfinancials_on_page)} otherfinancials on this page.")

                if current_page_for_chunk >= data.get('totalPages', 1) - 1:
                    print("    Fetched all pages for this date chunk.")
                    break 
                current_page_for_chunk += 1
            except requests.exceptions.RequestException as e:
                print(f"    Error fetching otherfinancials: {e}")
                if response is not None:
                    print(f"    Response status: {response.status_code}, text: {response.text}")
                break 
            except json.JSONDecodeError:
                print(f"    Error decoding JSON from response: {response.text if response else 'No response object'}")
                break 
        
        if current_chunk_end_ms == OVERALL_END_DATE_MS:
             break 
        current_chunk_start_ms = current_chunk_end_ms + 1 
        
    return all_otherfinancials_for_type

# --- BigQuery Merge and Drop Function for otherfinancials ---
def merge_and_drop_otherfinancials_temp_table(client: bigquery.Client):
    """
    Merges data from the temporary otherfinancials table to the main otherfinancials table
    and then drops the temporary table.
    """
    temp_table_full_id = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_TEMP_TABLE_ID}"
    main_table_full_id = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_MAIN_TABLE_ID}"

    # Assumes 'id' is the primary key for otherfinancials. Adjust if different.
    # Also includes 'transactionType' in the merge condition if it's part of the composite key,
    # or in the update/insert fields. For now, using 'id' as the main join key.
    merge_sql = f"""
    MERGE `{main_table_full_id}` T
    USING `{temp_table_full_id}` S
    ON T.id = S.id AND T.transactionType = S.transactionType -- Assuming id + transactionType makes a unique record
    WHEN MATCHED THEN
        UPDATE SET
            T.rawJSON = PARSE_JSON(S.rawJSON) -- Update rawJSON
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (id, transactionType, rawJSON)
        VALUES (S.id, S.transactionType, PARSE_JSON(S.rawJSON)); 
    """
    # If 'id' is globally unique across transaction types, 'AND T.transactionType = S.transactionType' might not be needed in ON.
    # However, including it can be safer if an 'id' could repeat for different transaction types but represent distinct events.

    print(f"\nAttempting to MERGE data from `{temp_table_full_id}` into `{main_table_full_id}`...")
    try:
        merge_job = client.query(merge_sql, location=BIGQUERY_LOCATION)
        job_result = merge_job.result()
        if merge_job.errors:
            print(f"MERGE job failed with errors: {merge_job.errors}")
            return False
        print(f"MERGE operation completed. Statement ID: {merge_job.statement_type}. "
              f"Rows affected: {merge_job.num_dml_affected_rows if merge_job.num_dml_affected_rows is not None else 'N/A'}")
    except Exception as e:
        print(f"Error during MERGE operation: {e}")
        return False

    drop_sql = f"DROP TABLE IF EXISTS `{temp_table_full_id}`;"
    print(f"\nAttempting to DROP temporary table `{temp_table_full_id}`...")
    try:
        drop_job = client.query(drop_sql, location=BIGQUERY_LOCATION)
        drop_job.result()
        if drop_job.errors:
            print(f"DROP TABLE job failed with errors: {drop_job.errors}")
            return False
        print(f"Temporary table `{temp_table_full_id}` dropped successfully.")
        return True
    except Exception as e:
        print(f"Error dropping temporary table: {e}")
        return False

# --- Main Execution ---
if __name__ == "__main__":
    print("Starting settlement fetch process...")
    
    all_fetched_otherfinancials = []
    for tx_type in TRANSACTION_TYPES:
        otherfinancials_for_current_type = fetch_otherfinancials_in_chunks(tx_type)
        all_fetched_otherfinancials.extend(otherfinancials_for_current_type)
        print(f"Fetched {len(otherfinancials_for_current_type)} records for transaction type: {tx_type}")

    if all_fetched_otherfinancials:
        print(f"\nSuccessfully fetched a total of {len(all_fetched_otherfinancials)} otherfinancials across all types. Preparing DataFrame...")
        data_for_df = []
        for settlement_detail in all_fetched_otherfinancials:
            # IMPORTANT: Determine the correct unique ID field for otherfinancials.
            # Trying 'id', then 'transactionId'. Adjust if necessary.
            unique_id = settlement_detail.get('id', settlement_detail.get('transactionId'))
            if unique_id is None:
                # Fallback or error handling if no ID is found.
                # For now, skipping records without a clear ID to avoid MERGE issues.
                # Consider logging this or using a hash of the record as an ID.
                print(f"Warning: Settlement record missing 'id' or 'transactionId': {json.dumps(settlement_detail)[:100]}...")
                continue 
            
            data_for_df.append({
                'id': str(unique_id), # Ensure ID is string for consistency if it can be numeric/string
                'transactionType': settlement_detail.get('transactionType', 'UNKNOWN'), # Get from data if available, else use the loop's tx_type
                'rawJSON': json.dumps(settlement_detail)
            })

        if not data_for_df:
            print("No settlement data with valid IDs to process into DataFrame.")
        else:
            df = pl.DataFrame(data_for_df)
            df = df.with_columns([
                pl.col('id').cast(pl.String), 
                pl.col('transactionType').cast(pl.String),
                pl.col('rawJSON').cast(pl.String)
            ])

            print("\nPolars DataFrame created:")
            print(df.head())

            if not GCP_PROJECT_ID:
                print("GCP_PROJECT_ID not set in .env. Skipping BigQuery operations.")
            else:
                load_to_temp_successful = False
                client = bigquery.Client(project=GCP_PROJECT_ID, location=BIGQUERY_LOCATION)
                try:
                    temp_table_ref = client.dataset(BIGQUERY_DATASET_ID).table(BIGQUERY_TEMP_TABLE_ID)
                    print(f"\nAttempting to load DataFrame to BigQuery temp table: {GCP_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_TEMP_TABLE_ID}...")
                    
                    schema = [
                        bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
                        bigquery.SchemaField("transactionType", "STRING", mode="NULLABLE"),
                        bigquery.SchemaField("rawJSON", "STRING", mode="NULLABLE")
                    ]

                    with io.BytesIO() as stream:
                        df.write_parquet(stream) 
                        stream.seek(0)

                        parquet_options = bigquery.ParquetOptions()
                        parquet_options.enable_list_inference = True

                        job_config = bigquery.LoadJobConfig(
                            source_format=bigquery.SourceFormat.PARQUET,
                            parquet_options=parquet_options,
                            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                            schema=schema, 
                        )

                        load_job = client.load_table_from_file(
                            stream,
                            temp_table_ref,
                            job_config=job_config,
                        )
                        load_job.result() 
                        if load_job.errors:
                             print(f"Load to temp table job failed with errors: {load_job.errors}")
                        else:
                            print(f"Loaded {load_job.output_rows} rows into temp table {temp_table_ref}.")
                            load_to_temp_successful = True
                except Exception as e:
                    print(f"Error loading data to BigQuery temp table: {e}")

                if load_to_temp_successful:
                    merge_and_drop_otherfinancials_temp_table(client)
                else:
                    print("Skipping MERGE and DROP operations due to failure in loading to temp table.")
    else:
        print("No otherfinancials found after processing all transaction types and chunks.")