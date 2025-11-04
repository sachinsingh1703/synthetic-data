import pandas as pd
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from datetime import datetime
import importlib

# --- Configuration ---
ROWS_PER_BATCH = 10_000
TOTAL_BATCHES = 100
OUTPUT_PATH = "/opt/airflow/data/generated_users"
GENERATOR_MODULE_PATH = "utils.generator"

@dag(
    dag_id="ai_data_generator_1M",
    start_date=days_ago(1),
    schedule_interval=None,
    tags=["gemini", "data-generation", "batch"],
)
def generate_1m_users_dag():
    """
    DAG to generate 1 million user records in parallel batches.
    It uses a generator function (created by Gemini and saved
    in Streamlit) to define the schema.
    """

    @task
    def define_batches() -> list[int]:
        return list(range(TOTAL_BATCHES))

    @task
    def generate_and_save_batch(batch_id: int):
        """
        A single mapped task that imports the LATEST generator
        code, runs it 10k times, and saves to Parquet.
        """
        
        # --- Reload the module ---
        # This is critical. It forces Airflow to re-import the
        # generator.py file that you saved from Streamlit,
        # rather than using a cached version.
        try:
            # Dynamically import the module
            generator_module = importlib.import_module(GENERATOR_MODULE_PATH)
            # Reload it to get the latest changes
            importlib.reload(generator_module)
            
            # Get the generation function
            generate_data = getattr(generator_module, "generate_data")
            
        except Exception as e:
            print(f"Error importing generator function: {e}")
            raise

        print(f"--- Starting batch {batch_id} ---")
        data = [generate_data() for _ in range(ROWS_PER_BATCH)]
        
        df = pd.DataFrame(data)
        file_path = f"{OUTPUT_PATH}/user_batch_{batch_id:03d}.parquet"
        df.to_parquet(file_path, index=False)
        
        print(f"--- Finished batch {batch_id}, saved to {file_path} ---")
        return file_path

    @task
    def consolidate_results(file_paths: list[str]):
        print(f"Successfully generated {len(file_paths)} batches.")

    # --- Define the DAG structure ---
    batch_list = define_batches()
    generated_files = generate_and_save_batch.expand(batch_id=batch_list)
    consolidate_results(generated_files)

generate_1m_users_dag()