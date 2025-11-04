# /dags/gemini_data_generation_dag.py

import os
import json
from datetime import datetime
from typing import Dict, List
from pathlib import Path
import re

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.models.xcom_arg import XComArg

# Import our worker script as a module
import sys
sys.path.append('/opt/airflow/scripts')
from gemini_worker import GeminiWorker

# Constants
OUTPUT_DIR = "/opt/airflow/data/pipeline_runs"
TEMP_DIR = "/opt/airflow/data/temp"
DEFAULT_BATCH_SIZE = 100  # Reduced batch size for better parallelization
MAX_ACTIVE_TASKS = 5  # Control parallel execution

@dag(
    dag_id="synthetic_data_generator",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_tasks=MAX_ACTIVE_TASKS,  # Control concurrent tasks
    concurrency=MAX_ACTIVE_TASKS,       # Limit concurrent task instances
    params={
        "user_prompt": Param(
            default="Generate user data with columns: user_id (UUID), first_name, last_name, email, country",
            type="string",
            description="Describe the data structure you want to generate"
        ),
        "output_format": Param(
            default="csv",
            type="string",
            enum=["csv", "json"],
            description="Output format for the generated data"
        ),
        "total_rows": Param(
            default=1000000,
            type="integer",
            description="Total number of rows to generate"
        ),
        "batch_size": Param(
            default=DEFAULT_BATCH_SIZE,
            type="integer",
            description="Number of rows per batch"
        )
    }
)
def synthetic_data_generator():
    """
    DAG to generate large-scale synthetic datasets using the Gemini API.
    Uses dynamic task mapping for parallel generation of data batches.
    """

    # Start node
    start = EmptyOperator(task_id="start")

    @task
    def prepare_batches(**context) -> List[Dict]:
        """
        Calculate the number of batches needed and prepare batch configurations
        """
        params = context["params"]
        total_rows = params["total_rows"]
        batch_size = params["batch_size"]
        run_id = context["run_id"]

        # Calculate number of batches
        num_batches = (total_rows + batch_size - 1) // batch_size
        
        # Create output directory
        run_output_dir = os.path.join(OUTPUT_DIR, run_id)
        temp_dir = os.path.join(TEMP_DIR, run_id)
        os.makedirs(run_output_dir, exist_ok=True)
        os.makedirs(temp_dir, exist_ok=True)

        print(f"Starting Run: {run_id}")
        print(f"Total Rows: {total_rows}")
        print(f"Batch Size: {batch_size}")
        print(f"Number of Batches: {num_batches}")

        # Prepare batch configurations
        batch_configs = []
        remaining_rows = total_rows

        # Try to extract explicit column names from the user prompt so we can enforce the same schema across batches
        def _parse_columns_from_prompt(p: str) -> List[str]:
            m = re.search(r"columns?\s*:\s*(.+)", p, flags=re.IGNORECASE)
            if not m:
                return []
            cols_raw = m.group(1)
            cols = []
            for part in cols_raw.split(','):
                name = re.sub(r"\(.*?\)", "", part).strip()
                name = re.sub(r"\s+", "_", name)
                name = name.strip(' "\'')
                if name:
                    cols.append(name)
            return cols

        parsed_columns = _parse_columns_from_prompt(params.get("user_prompt", ""))

        for i in range(num_batches):
            # Handle the last batch which might be smaller
            current_batch_size = min(batch_size, remaining_rows)
            
            batch_configs.append({
                "batch_id": i,
                "rows": current_batch_size,
                "prompt": params["user_prompt"],
                "format": params["output_format"],
                "output_path": os.path.join(temp_dir, f"batch_{i:04d}.{params['output_format']}"),
                "columns": parsed_columns,
                "run_id": run_id
            })
            
            remaining_rows -= current_batch_size

        return batch_configs

    @task(max_active_tis_per_dag=MAX_ACTIVE_TASKS)
    def generate_batch(batch_config: dict) -> str:
        """
        Generate a batch of synthetic data using the Gemini worker
        """
        worker = GeminiWorker()
        
        # Optimize prompt for batch generation
        batch_prompt = f"""
        Generate exactly {batch_config['rows']} rows of data.
        Structure: {batch_config['prompt']}
        Format: Keep it concise, focus on essential data only.
        """
        
        try:
            # Use parsed columns (if any) to ensure consistent schema
            columns = batch_config.get("columns", None)
            worker.generate_and_save(
                user_prompt=batch_config["prompt"],
                output_format=batch_config["format"],
                row_count=batch_config["rows"],
                output_path=batch_config["output_path"],
                columns=columns
            )
            
            return batch_config["output_path"]
            
        except Exception as e:
            print(f"Error in batch {batch_config['batch_id']}: {e}")
            raise

    @task
    def combine_files(batch_files: List[str], **context) -> str:
        """
        Combine all generated batch files into a single output file
        """
        params = context["params"]
        run_id = context["run_id"]
        output_format = params["output_format"].lower()
        
        # Define output paths
        run_output_dir = os.path.join(OUTPUT_DIR, run_id)
        final_path = os.path.join(run_output_dir, f"final_output.{output_format}")
        
        print(f"Combining {len(batch_files)} batch files into {final_path}")
        
        try:
            if output_format == 'csv':
                # For CSV, we need to handle headers and concatenation carefully
                import pandas as pd
                
                # Determine expected columns from the user prompt
                def _parse_columns_from_prompt(p: str) -> List[str]:
                    m = re.search(r"columns?\s*:\s*(.+)", p, flags=re.IGNORECASE)
                    if not m:
                        return []
                    cols_raw = m.group(1)
                    cols = []
                    for part in cols_raw.split(','):
                        name = re.sub(r"\(.*?\)", "", part).strip()
                        name = re.sub(r"\s+", "_", name)
                        name = name.strip(' "\'')
                        if name:
                            cols.append(name)
                    return cols

                expected_columns = _parse_columns_from_prompt(params.get("user_prompt", ""))

                # Read and combine all CSV files (worker produces data rows only, no header)
                dfs = []
                for file in sorted(batch_files):
                    if os.path.exists(file):
                        if expected_columns:
                            # read rows without header, assign expected column names
                            df = pd.read_csv(file, header=None, names=expected_columns)
                        else:
                            # Fallback: try to read and let pandas infer columns (risky)
                            df = pd.read_csv(file)
                        dfs.append(df)
                        os.remove(file)  # Clean up batch file
                
                if dfs:
                    combined_df = pd.concat(dfs, ignore_index=True)
                    combined_df.to_csv(final_path, index=False)
                else:
                    raise ValueError("No valid batch files found to combine")
                    
            else:  # JSON format
                combined_data = []
                
                # Read and combine all JSON files
                for file in sorted(batch_files):
                    if os.path.exists(file):
                        with open(file, 'r', encoding='utf-8') as f:
                            batch_data = json.load(f)
                            if isinstance(batch_data, list):
                                combined_data.extend(batch_data)
                            else:
                                combined_data.append(batch_data)
                        os.remove(file)  # Clean up batch file
                
                if combined_data:
                    with open(final_path, 'w', encoding='utf-8') as f:
                        json.dump(combined_data, f, indent=2)
                else:
                    raise ValueError("No valid batch files found to combine")
            
            # Clean up temp directory
            temp_dir = os.path.join(TEMP_DIR, run_id)
            if os.path.exists(temp_dir):
                import shutil
                shutil.rmtree(temp_dir)
            
            print(f"Successfully created final output file: {final_path}")
            
            # Store the output path in XCom for the UI
            context['task_instance'].xcom_push(key='final_output_path', value=final_path)
            return final_path
            
        except Exception as e:
            print(f"Error combining files: {e}")
            raise

    # End node
    end = EmptyOperator(task_id="end")

    # Define the DAG flow
    batch_configs = prepare_batches()
    generated_files = generate_batch.expand(batch_config=batch_configs)
    final_output = combine_files(generated_files)

    # Set up the task dependencies
    start >> batch_configs >> generated_files >> final_output >> end

# Instantiate the DAG
synthetic_data_generator()