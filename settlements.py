import requests
from dotenv import load_dotenv
import os
import polars as pl
import json
import time
import io
from google.cloud import bigquery
import asyncio
import aiohttp
from typing import List, Dict, Any

# Load Environment Variables
load_dotenv()

# --- API Credentials ---
TRENDYOL_API_TOKEN = os.getenv('TRENDYOL_API_TOKEN')
TRENDYOL_SUPPLIER_ID = os.getenv('TRENDYOL_SUPPLIER_ID')
API_URL = f"https://apigw.trendyol.com/integration/finance/che/sellers/{TRENDYOL_SUPPLIER_ID}/settlements"

# --- Google Cloud BigQuery Credentials and Project ---
GOOGLE_APPLICATION_CREDENTIALS = os.getenv('GOOGLE_APPLICATION_CREDENTIALS') # Path to your GCP service account key file
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID') # Your Google Cloud Project ID
BIGQUERY_DATASET_ID = os.getenv('BIGQUERY_DATASET_ID') # Name of your BigQuery dataset
BIGQUERY_TEMP_TABLE_ID = "settlements_temp" # Temporary table for settlements
BIGQUERY_MAIN_TABLE_ID = "settlements" # Main table for settlements
BIGQUERY_LOCATION = os.getenv('BIGQUERY_LOCATION') # BigQuery dataset location

# --- API Headers ---
headers ={
    'Authorization': f'Basic {TRENDYOL_API_TOKEN}',
    'User-Agent': f'{TRENDYOL_SUPPLIER_ID} - SelfIntegration'
}

# --- Transaction Types to iterate through ---
TRANSACTION_TYPES = ['Sale', 'Return', 'Discount', 'DiscountCancel', 'Coupon', 'CouponCancel', 'ProvisionPositive', 'ProvisionNegative', 'TyDiscount', 'TyDiscountCancel', 'TyCoupon', 'TyCouponCancel', 'SellerRevenuePositive', 'SellerRevenueNegative', 'CommissionPositive', 'CommissionNegative', 'SellerRevenuePositiveCancel', 'SellerRevenueNegativeCancel', 'CommissionPositiveCancel', 'CommissionNegativeCancel', 'ManualRefund', 'ManualRefundCancel', 'DeliveryFee', 'DeliveryFeeCancel']



# --- Date Range Configuration ---
N_DAYS_TO_FETCH = 30 # Define the total number of days back you want to fetch data for
MAX_DAYS_PER_CHUNK = 15 # Max days per API call chunk (safety margin under 90)

# Rate Limiting: 2000 requests per minute
REQUESTS_PER_MINUTE = 2000
REQUESTS_PER_SECOND = REQUESTS_PER_MINUTE / 60

OVERALL_END_DATE_MS = int(time.time() * 1000)
OVERALL_START_DATE_MS = OVERALL_END_DATE_MS - (N_DAYS_TO_FETCH * 24 * 60 * 60 * 1000)

# Base API Parameters (startDate, endDate, transactionType will be set per chunk/type)
base_params = {
    'supplierId': TRENDYOL_SUPPLIER_ID,
    'page': 0,
    'size': 500 # Settlements API allows up to 500
}

# --- Parameter Generation Function ---
def generate_api_call_params_settlements():
    """
    Generates a list of parameter dictionaries for each API call required,
    considering transaction types, date chunking, and pagination.
    """
    all_call_params = []
    current_chunk_start_ms = OVERALL_START_DATE_MS
    max_chunk_duration_ms = MAX_DAYS_PER_CHUNK * 24 * 60 * 60 * 1000

    print(f"Overall fetch period: From {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(OVERALL_START_DATE_MS/1000))} to {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(OVERALL_END_DATE_MS/1000))}")

    while current_chunk_start_ms < OVERALL_END_DATE_MS:
        current_chunk_end_ms = min(current_chunk_start_ms + max_chunk_duration_ms, OVERALL_END_DATE_MS)
        
        print(f"\nGenerating parameters for date chunk: "
              f"From {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(current_chunk_start_ms/1000))} "
              f"to {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(current_chunk_end_ms/1000))}")

        for transaction_type in TRANSACTION_TYPES:
            print(f"  Generating parameters for Transaction Type: {transaction_type}")
            # Initial sequential call to determine totalPages for this type and chunk
            initial_params = base_params.copy()
            initial_params['startDate'] = current_chunk_start_ms
            initial_params['endDate'] = current_chunk_end_ms
            initial_params['transactionType'] = transaction_type
            initial_params['page'] = 0 
            
            try:
                response = requests.get(API_URL, headers=headers, params=initial_params)
                response.raise_for_status()
                data = response.json()
                total_pages = data.get('totalPages', 1)
                print(f"    Found {total_pages} pages for {transaction_type} in this chunk.")

                for page_num in range(total_pages):
                    call_params = base_params.copy()
                    call_params['startDate'] = current_chunk_start_ms
                    call_params['endDate'] = current_chunk_end_ms
                    call_params['transactionType'] = transaction_type
                    call_params['page'] = page_num
                    all_call_params.append(call_params)

            except requests.exceptions.RequestException as e:
                print(f"    Error fetching initial page to determine pagination for {transaction_type}: {e}")
                if response is not None:
                    print(f"    Response status: {response.status_code}, text: {response.text}")
                # Continue to the next transaction type/chunk even if one fails
            except json.JSONDecodeError:
                print(f"    Error decoding JSON from initial response for {transaction_type}: {response.text if response else 'No response object'}")
                # Continue to the next transaction type/chunk even if one fails

        # Move to the start of the next chunk
        if current_chunk_end_ms == OVERALL_END_DATE_MS:
             break 
        current_chunk_start_ms = current_chunk_end_ms + 1 
    
    print(f"\nGenerated parameters for a total of {len(all_call_params)} API calls.")
    return all_call_params

# --- Asynchronous Fetching Function ---
async def fetch_single_page_async_settlements(session: aiohttp.ClientSession, params: Dict[str, Any], limiter: asyncio.Semaphore):
    """
    Fetches a single page of settlements asynchronously.
    """
    await asyncio.sleep(1.0 / REQUESTS_PER_SECOND)
    async with limiter: 
        tx_type = params.get('transactionType', 'UNKNOWN')
        page = params.get('page', 'UNKNOWN')
        start_date = time.strftime('%Y-%m-%d', time.localtime(params.get('startDate', 0)/1000))
        end_date = time.strftime('%Y-%m-%d', time.localtime(params.get('endDate', 0)/1000))
        print(f"  Fetching {tx_type} page {page} for chunk {start_date} to {end_date}...")
        try:
            async with session.get(API_URL, headers=headers, params=params) as response:
                response.raise_for_status()
                data = await response.json()
                settlements_on_page = data.get('content', [])
                print(f"  Fetched {len(settlements_on_page)} records for {tx_type} page {page}.")
                return settlements_on_page
        except aiohttp.ClientResponseError as e:
            print(f"  Error fetching {tx_type} page {page}: {e.status} - {e.message}")
            return [] 
        except aiohttp.ClientConnectionError as e:
            print(f"  Connection error fetching {tx_type} page {page}: {e}")
            return [] 
        except json.JSONDecodeError:
             response_text = await response.text() if response else 'No response object'
             print(f"  Error decoding JSON from response for {tx_type} page {page}: {response_text}")
             return [] 


async def fetch_all_settlements_parallel(call_params_list: List[Dict[str, Any]], concurrency_limit: int = 10):
    """
    Fetches all settlements in parallel using the generated parameter list, with rate limiting.
    """
    all_settlements_raw = []
    limiter = asyncio.Semaphore(concurrency_limit) 

    async with aiohttp.ClientSession() as session:
        tasks = [fetch_single_page_async_settlements(session, params, limiter) for params in call_params_list]
        results = await asyncio.gather(*tasks)
        
        for page_settlements in results:
            all_settlements_raw.extend(page_settlements)
    
    return all_settlements_raw


# --- BigQuery Merge and Drop Function for Settlements ---
def merge_and_drop_settlements_temp_table(client: bigquery.Client):
    """
    Merges data from the temporary settlements table to the main settlements table
    and then drops the temporary table.
    """
    temp_table_full_id = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_TEMP_TABLE_ID}"
    main_table_full_id = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_MAIN_TABLE_ID}"

    # Assumes 'id' is the primary key for settlements. Adjust if different.
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
    
    # Step 1: Generate all API call parameters
    api_call_parameters = generate_api_call_params_settlements()

    if api_call_parameters:
        # Step 2: Fetch settlements in parallel using async
        print("\nStarting parallel settlement fetching...")
        fetched_settlements_list = asyncio.run(fetch_all_settlements_parallel(api_call_parameters))

        if fetched_settlements_list:
            print(f"\nSuccessfully fetched a total of {len(fetched_settlements_list)} settlements after parallel processing. Preparing DataFrame...")
            data_for_df = []
            for settlement_detail in fetched_settlements_list:
                # IMPORTANT: Determine the correct unique ID field for settlements.
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
                        merge_and_drop_settlements_temp_table(client)
                    else:
                        print("Skipping MERGE and DROP operations due to failure in loading to temp table.")
        else:
            print("No settlements fetched after parallel processing. Nothing to load to BigQuery.")
    else:
        print("No API call parameters generated. Nothing to fetch.")