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
API_URL = f"https://apigw.trendyol.com/integration/order/sellers/{TRENDYOL_SUPPLIER_ID}/orders"

# --- Google Cloud BigQuery Credentials and Project ---
GOOGLE_APPLICATION_CREDENTIALS = os.getenv('GOOGLE_APPLICATION_CREDENTIALS') # Path to your service account key
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID') # Your Google Cloud Project ID
BIGQUERY_DATASET_ID = os.getenv('BIGQUERY_DATASET_ID') # Name of your BigQuery dataset
BIGQUERY_TEMP_TABLE_ID = "orders_temp" # Name of your temporary BigQuery table
BIGQUERY_ORDERS_TABLE_ID = "orders" # Name of your main orders BigQuery table
BIGQUERY_LOCATION = os.getenv('BIGQUERY_LOCATION') # BigQuery dataset location

# --- API Headers ---
headers ={
    'Authorization': f'Basic {TRENDYOL_API_TOKEN}',
    'User-Agent': f'{TRENDYOL_SUPPLIER_ID} - SelfIntegration'
}

# --- Date Range Configuration ---
N_DAYS_TO_FETCH = 45 # Define the total number of days back you want to fetch orders for
MAX_DAYS_PER_CHUNK = 15 # Max days per API call chunk
# 2000 requests per minute = ~16.6 requests per second
REQUESTS_PER_MINUTE = 2000
REQUESTS_PER_SECOND = REQUESTS_PER_MINUTE / 60

OVERALL_END_DATE_MS = int(time.time() * 1000)
OVERALL_START_DATE_MS = OVERALL_END_DATE_MS - (N_DAYS_TO_FETCH * 24 * 60 * 60 * 1000)

# Base API Parameters (startDate and endDate will be set per chunk)
base_params = {
    'supplierId': TRENDYOL_SUPPLIER_ID,
    'page': 0,
    'size': 50 # Max 200 per Trendyol API
}

# --- Pagination and Chunking Function (Parameter Generation) ---
def generate_api_call_params():
    """
    Generates a list of parameter dictionaries for each API call required,
    considering date chunking and pagination.
    """
    all_call_params = []
    current_chunk_start_ms = OVERALL_START_DATE_MS
    max_chunk_duration_ms = MAX_DAYS_PER_CHUNK * 24 * 60 * 60 * 1000

    print(f"Overall fetch period: From {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(OVERALL_START_DATE_MS/1000))} to {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(OVERALL_END_DATE_MS/1000))}")

    while current_chunk_start_ms < OVERALL_END_DATE_MS:
        current_chunk_end_ms = min(current_chunk_start_ms + max_chunk_duration_ms, OVERALL_END_DATE_MS)
        
        print(f"Generating parameters for date chunk: "
              f"From {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(current_chunk_start_ms/1000))} "
              f"to {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(current_chunk_end_ms/1000))}")

        # For parameter generation, we need to make an initial call to find totalPages
        # This initial call will be sequential for simplicity in determining pagination.
        # The actual data fetching for all pages will be parallelized later.
        initial_params = base_params.copy()
        initial_params['startDate'] = current_chunk_start_ms
        initial_params['endDate'] = current_chunk_end_ms
        initial_params['page'] = 0 # Start with the first page to get totalPages
        
        try:
            response = requests.get(API_URL, headers=headers, params=initial_params)
            response.raise_for_status()
            data = response.json()
            total_pages = data.get('totalPages', 1)
            print(f"  Found {total_pages} pages for this chunk.")

            for page_num in range(total_pages):
                call_params = base_params.copy()
                call_params['startDate'] = current_chunk_start_ms
                call_params['endDate'] = current_chunk_end_ms
                call_params['page'] = page_num
                all_call_params.append(call_params)

        except requests.exceptions.RequestException as e:
            print(f"  Error fetching initial page to determine pagination: {e}")
            if response is not None:
                print(f"  Response status: {response.status_code}, text: {response.text}")
            # Continue to the next chunk even if one fails
        except json.JSONDecodeError:
            print(f"  Error decoding JSON from initial response: {response.text if response else 'No response object'}")
            # Continue to the next chunk even if one fails

        # Move to the start of the next chunk
        if current_chunk_end_ms == OVERALL_END_DATE_MS:
             break # Reached the overall end date
        current_chunk_start_ms = current_chunk_end_ms + 1 
    
    print(f"Generated parameters for a total of {len(all_call_params)} API calls.")
    return all_call_params

# --- Asynchronous Fetching Function ---
async def fetch_single_page_async(session: aiohttp.ClientSession, params: Dict[str, Any], limiter: asyncio.Semaphore):
    """
    Fetches a single page of orders asynchronously.
    """
    # Ensure minimum time delay between requests for rate limiting
    await asyncio.sleep(1.0 / REQUESTS_PER_SECOND)
    async with limiter: # Acquire a semaphore slot for concurrency control
        print(f"  Fetching page {params['page']} for chunk {time.strftime('%Y-%m-%d', time.localtime(params['startDate']/1000))} to {time.strftime('%Y-%m-%d', time.localtime(params['endDate']/1000))}...")
        try:
            async with session.get(API_URL, headers=headers, params=params) as response:
                response.raise_for_status()
                data = await response.json()
                orders_on_page = data.get('content', [])
                print(f"  Fetched {len(orders_on_page)} orders on page {params['page']}.")
                return orders_on_page
        except aiohttp.ClientResponseError as e:
            print(f"  Error fetching orders for page {params['page']}: {e.status} - {e.message}")
            return [] # Return empty list on error
        except aiohttp.ClientConnectionError as e:
            print(f"  Connection error fetching orders for page {params['page']}: {e}")
            return [] # Return empty list on error
        except json.JSONDecodeError:
             response_text = await response.text() if response else 'No response object'
             print(f"  Error decoding JSON from response for page {params['page']}: {response_text}")
             return [] # Return empty list on error


async def fetch_all_orders_parallel(call_params_list: List[Dict[str, Any]], concurrency_limit: int = 10):
    """
    Fetches all orders in parallel using the generated parameter list, with rate limiting.
    """
    all_orders_raw = []
    # Use a semaphore to limit concurrent connections/requests
    limiter = asyncio.Semaphore(concurrency_limit) 

    async with aiohttp.ClientSession() as session:
        tasks = [fetch_single_page_async(session, params, limiter) for params in call_params_list]
        # Gather results from all tasks as they complete
        results = await asyncio.gather(*tasks) 
        
        for page_orders in results:
            all_orders_raw.extend(page_orders)
    
    return all_orders_raw


# --- BigQuery Merge and Drop Function ---
def merge_and_drop_temp_table(client: bigquery.Client):
    """
    Merges data from the temporary table to the main orders table
    and then drops the temporary table.
    """
    temp_table_full_id = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_TEMP_TABLE_ID}"
    orders_table_full_id = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_ORDERS_TABLE_ID}"

    # Using rawJSON as per your latest script version
    merge_sql = f"""
    MERGE `{orders_table_full_id}` T
    USING `{temp_table_full_id}` S
    ON T.id = S.id
    WHEN MATCHED THEN
        UPDATE SET
            T.orderNumber = S.orderNumber,
            T.rawJSON = PARSE_JSON(S.rawJSON) -- Convert STRING to JSON
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (id, orderNumber, rawJSON)
        VALUES (S.id, S.orderNumber, PARSE_JSON(S.rawJSON)); -- Convert STRING to JSON
    """

    print(f"Attempting to MERGE data from `{temp_table_full_id}` into `{orders_table_full_id}`...")
    try:
        merge_job = client.query(merge_sql, location=BIGQUERY_LOCATION)
        job_result = merge_job.result() # Wait for the job to complete.
        if merge_job.errors:
            print(f"MERGE job failed with errors: {merge_job.errors}")
            return False
        print(f"MERGE operation completed. Statement ID: {merge_job.statement_type}. "
              f"Rows affected: {merge_job.num_dml_affected_rows if merge_job.num_dml_affected_rows is not None else 'N/A'}")
    except Exception as e:
        print(f"Error during MERGE operation: {e}")
        return False

    drop_sql = f"DROP TABLE IF EXISTS `{temp_table_full_id}`;"
    print(f"Attempting to DROP temporary table `{temp_table_full_id}`...")
    try:
        drop_job = client.query(drop_sql, location=BIGQUERY_LOCATION)
        drop_job.result() # Wait for the job to complete.
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
    print("Starting order fetch process...")
    
    # Step 1: Generate all API call parameters
    api_call_parameters = generate_api_call_params()

    if api_call_parameters:
        # Step 2: Fetch orders in parallel using async
        print("Starting parallel order fetching...")
        fetched_orders_list = asyncio.run(fetch_all_orders_parallel(api_call_parameters))

        if fetched_orders_list:
            print(f"Successfully fetched a total of {len(fetched_orders_list)} orders after parallel processing. Preparing DataFrame...")
            data_for_df = []
            for order_detail in fetched_orders_list:
                data_for_df.append({
                    'id': order_detail.get('id'),
                    'orderNumber': order_detail.get('orderNumber'),
                    'rawJSON': json.dumps(order_detail) # Keep as JSON string for Polars
                })

            df = pl.DataFrame(data_for_df)
            # Using String for id as per your script
            df = df.with_columns([
                pl.col('id').cast(pl.String), 
                pl.col('orderNumber').cast(pl.String),
                pl.col('rawJSON').cast(pl.String)
            ])

            print("Polars DataFrame created:")

            if not GCP_PROJECT_ID:
                print("GCP_PROJECT_ID not set in .env. Skipping BigQuery operations.")
            else:
                load_to_temp_successful = False
                client = bigquery.Client(project=GCP_PROJECT_ID, location=BIGQUERY_LOCATION)
                try:
                    temp_table_ref = client.dataset(BIGQUERY_DATASET_ID).table(BIGQUERY_TEMP_TABLE_ID)

                    print(f"Attempting to load DataFrame to BigQuery temp table: {GCP_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_TEMP_TABLE_ID}...")
                    
                    # Define the schema for the BigQuery temp table (rawJSON as STRING)
                    schema = [
                        bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
                        bigquery.SchemaField("orderNumber", "STRING", mode="NULLABLE"), # Changed to NULLABLE as orderNumber might not always be required based on previous schema discussions
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
                            # Optional: Verify schema of temp table
                            # loaded_table_check = client.get_table(temp_table_ref)
                            # print("Schema of temporary table after load:")
                            # for field_check in loaded_table_check.schema:
                            #     print(f"- {field_check.name}: {field_check.field_type}")
                            load_to_temp_successful = True
                except Exception as e:
                    print(f"Error loading data to BigQuery temp table: {e}")

                if load_to_temp_successful:
                    merge_and_drop_temp_table(client)
                else:
                    print("Skipping MERGE and DROP operations due to failure in loading to temp table.")
        else:
            print("No orders fetched after parallel processing. Nothing to load to BigQuery.")
    else:
        print("No API call parameters generated. Nothing to fetch.")