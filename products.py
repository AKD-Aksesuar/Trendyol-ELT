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
API_URL = f"https://apigw.trendyol.com/integration/product/sellers/{TRENDYOL_SUPPLIER_ID}/products"

# --- Google Cloud BigQuery Credentials and Project ---
GOOGLE_APPLICATION_CREDENTIALS = os.getenv('GOOGLE_APPLICATION_CREDENTIALS') # Path to your service account key
GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID') # Your Google Cloud Project ID
BIGQUERY_DATASET_ID = os.getenv('BIGQUERY_DATASET_ID') # Name of your BigQuery dataset
BIGQUERY_TEMP_TABLE_ID = "products_temp" # Name of your temporary BigQuery table for products
BIGQUERY_PRODUCTS_TABLE_ID = "products" # Name of your main products BigQuery table
BIGQUERY_LOCATION = os.getenv('BIGQUERY_LOCATION') # BigQuery dataset location

# --- API Headers ---
headers ={
    'Authorization': f'Basic {TRENDYOL_API_TOKEN}',
    'User-Agent': f'{TRENDYOL_SUPPLIER_ID} - SelfIntegration'
}

# --- Rate Limiting: 2000 requests per minute ---
REQUESTS_PER_MINUTE = 2000
REQUESTS_PER_SECOND = REQUESTS_PER_MINUTE / 60

# --- Base API Parameters ---
# Parameters for products API are simpler
base_params = {
    'supplierId': TRENDYOL_SUPPLIER_ID,
    'page': 0,
    'size': 50 # Max 200 per Trendyol API
}

# --- Parameter Generation Function for Products ---
def generate_api_call_params_products():
    """
    Generates a list of parameter dictionaries for each API call required,
    considering pagination.
    """
    all_call_params = []
    
    print("Generating parameters for product pages...")

    # Initial sequential call to determine totalPages
    initial_params = base_params.copy()
    initial_params['page'] = 0 
    
    try:
        response = requests.get(API_URL, headers=headers, params=initial_params)
        response.raise_for_status()
        data = response.json()
        total_pages = data.get('totalPages', 1)
        print(f"  Found {total_pages} pages for products.")

        for page_num in range(total_pages):
            call_params = base_params.copy()
            call_params['page'] = page_num
            all_call_params.append(call_params)

    except requests.exceptions.RequestException as e:
        print(f"  Error fetching initial page to determine pagination: {e}")
        if response is not None:
            print(f"  Response status: {response.status_code}, text: {response.text}")
        # Continue with no parameters if initial fetch fails
    except json.JSONDecodeError:
        print(f"  Error decoding JSON from initial response: {response.text if response else 'No response object'}")
        # Continue with no parameters if initial fetch fails
    
    print(f"\nGenerated parameters for a total of {len(all_call_params)} API calls.")
    return all_call_params

# --- Asynchronous Fetching Function for Products ---
async def fetch_single_page_async_products(session: aiohttp.ClientSession, params: Dict[str, Any], limiter: asyncio.Semaphore):
    """
    Fetches a single page of products asynchronously.
    """
    await asyncio.sleep(1.0 / REQUESTS_PER_SECOND)
    async with limiter: 
        page = params.get('page', 'UNKNOWN')
        print(f"  Fetching products page {page}...")
        try:
            async with session.get(API_URL, headers=headers, params=params) as response:
                response.raise_for_status()
                data = await response.json()
                products_on_page = data.get('content', [])
                print(f"  Fetched {len(products_on_page)} products on page {page}.")
                return products_on_page
        except aiohttp.ClientResponseError as e:
            print(f"  Error fetching products page {page}: {e.status} - {e.message}")
            return [] 
        except aiohttp.ClientConnectionError as e:
            print(f"  Connection error fetching products page {page}: {e}")
            return [] 
        except json.JSONDecodeError:
             response_text = await response.text() if response else 'No response object'
             print(f"  Error decoding JSON from response for products page {page}: {response_text}")
             return [] 


async def fetch_all_products_parallel(call_params_list: List[Dict[str, Any]], concurrency_limit: int = 10):
    """
    Fetches all products in parallel using the generated parameter list, with rate limiting.
    """
    all_products_raw = []
    limiter = asyncio.Semaphore(concurrency_limit) 

    async with aiohttp.ClientSession() as session:
        tasks = [fetch_single_page_async_products(session, params, limiter) for params in call_params_list]
        results = await asyncio.gather(*tasks)
        
        for page_products in results:
            all_products_raw.extend(page_products)
    
    return all_products_raw


# --- BigQuery Merge and Drop Function for Products ---
def merge_and_drop_products_temp_table(client: bigquery.Client):
    """
    Merges data from the temporary products table to the main products table
    (parsing rawJSON string to JSON type) and then drops the temporary table.
    """
    temp_table_full_id = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_TEMP_TABLE_ID}"
    products_table_full_id = f"{GCP_PROJECT_ID}.{BIGQUERY_DATASET_ID}.{BIGQUERY_PRODUCTS_TABLE_ID}"

    # MERGE statement for products
    # rawJSON from source (S) is STRING, target (T) is JSON. Use PARSE_JSON().
    merge_sql = f"""
    MERGE `{products_table_full_id}` T
    USING `{temp_table_full_id}` S
    ON T.id = S.id
    WHEN MATCHED THEN
        UPDATE SET
            T.rawJSON = PARSE_JSON(S.rawJSON) -- Convert STRING to JSON
    WHEN NOT MATCHED BY TARGET THEN
        INSERT (id, rawJSON)
        VALUES (S.id, PARSE_JSON(S.rawJSON)); -- Convert STRING to JSON
    """

    print(f"\nAttempting to MERGE data from `{temp_table_full_id}` into `{products_table_full_id}`...")
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

    # Drop the temporary table
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
    print("Starting product fetch process...")

    # Step 1: Generate all API call parameters
    api_call_parameters = generate_api_call_params_products()

    if api_call_parameters:
        # Step 2: Fetch products in parallel using async
        print("\nStarting parallel product fetching...")
        fetched_products_list = asyncio.run(fetch_all_products_parallel(api_call_parameters))

        if fetched_products_list:
            print(f"Successfully fetched {len(fetched_products_list)} products. Preparing DataFrame...")
            data_for_df = []
            for product_detail in fetched_products_list:
                data_for_df.append({
                    'id': product_detail.get('id'),
                    'rawJSON': json.dumps(product_detail) # Keep as JSON string for Polars
                })

            df = pl.DataFrame(data_for_df)
            df = df.with_columns([
                pl.col('id').cast(pl.String),
                pl.col('rawJSON').cast(pl.String) # Keep rawJSON as String in Polars/Parquet
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

                    # Schema for products_temp: rawJSON will be loaded as STRING
                    schema = [
                        bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
                        bigquery.SchemaField("rawJSON", "STRING", mode="NULLABLE"),
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
                            schema=schema, # Provide the explicit schema here
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
                            # Verify the schema of the temporary table
                            loaded_table = client.get_table(temp_table_ref)
                            print("Schema of temporary table after load:")
                            for schema_field in loaded_table.schema:
                                print(f"- {schema_field.name}: {schema_field.field_type}")
                            load_to_temp_successful = True

                except Exception as e:
                    print(f"Error loading data to BigQuery temp table: {e}")

                if load_to_temp_successful:
                    merge_and_drop_products_temp_table(client)
                else:
                    print("Skipping MERGE and DROP operations due to failure in loading to temp table.")
        else:
            print("No products fetched after parallel processing. Nothing to load to BigQuery.")
    else:
        print("No API call parameters generated. Nothing to fetch.")