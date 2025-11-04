from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
import importlib
import os

GENERATOR_MODULE_NAME = "utils.database_generator"
OUTPUT_DIR = "/opt/airflow/data/generated_users"

@dag(
    dag_id="ai_database_generator",
    start_date=days_ago(1),
    schedule_interval=None,
    tags=["gemini", "database", "multi-table"],
)
def generate_database_dag():
    """
    DAG to generate a full multi-table database.
    
    It imports and runs the `main()` function from the
    AI-generated database_generator.py script.
    """

    @task
    def clear_previous_data():
        """Deletes all .parquet files from the output directory."""
        print(f"Clearing old data from {OUTPUT_DIR}...")
        try:
            files = [f for f in os.listdir(OUTPUT_DIR) if f.endswith(".parquet")]
            for f in files:
                os.remove(os.path.join(OUTPUT_DIR, f))
                print(f"Removed {f}")
            print(f"Cleared {len(files)} files.")
        except Exception as e:
            print(f"Error clearing directory: {e}")
            raise

    @task
    def run_database_generation_script():
        """
        Imports the AI-generated script and runs its main() function.
        """
        print("Importing AI-generated script...")
        try:
            # Dynamically import the module
            generator_module = importlib.import_module(GENERATOR_MODULE_NAME)
            # Reload it to get the latest changes
            importlib.reload(generator_module)
            
            # Get the main generation function
            main_func = getattr(generator_module, "main")
            
        except Exception as e:
            print(f"Error importing generator function: {e}")
            print("Did the AI generate the code correctly? Does 'main()' exist?")
            raise

        print("--- Starting Database Generation ---")
        main_func()
        print("--- Database Generation Complete ---")

    # Define DAG structure: Clear data, then run the script
    clear_data_task = clear_previous_data()
    run_script_task = run_database_generation_script()
    
    clear_data_task >> run_script_task

# Instantiate the DAG
generate_database_dag()