# /streamlit/app.py

import os
import json
import time
import pandas as pd
import requests
from typing import Optional, Dict, Any
import streamlit as st
from datetime import datetime

# Import our Gemini worker
import sys
sys.path.append('/app/scripts')  # Add scripts directory to Python path
from gemini_worker import GeminiWorker

def get_gemini_csv_sample(prompt: str, num_rows: int = 5) -> Optional[pd.DataFrame]:
    """
    Generate a sample dataset using the Gemini worker with proper header handling.
    Returns a pandas DataFrame with the sample data or None if generation fails.
    """
    import io

    try:
        # Initialize worker
        worker = GeminiWorker()
        
        # Enhanced prompt to ensure proper header and type information
        enhanced_prompt = f"""
        Based on this request: {prompt}
        
        Generate a CSV dataset with these REQUIREMENTS:
        1. Use clear, descriptive column names (not generic like 'data' or 'column')
        2. Include data type for each column (e.g., UUID, email, name, date, number)
        3. Generate exactly {num_rows} rows of consistent, well-formatted data
        4. Maintain data type consistency in each column
        
        Format columns like this: column_name (data_type), e.g.:
        user_id (UUID), first_name (name), email (email), signup_date (date)
        """
        
        # Get column information
        columns, column_types = worker.parse_columns_from_user_prompt(enhanced_prompt)
        
        if not columns:
            st.error("Could not determine column structure from prompt")
            st.error("Please include column definitions in your prompt, for example:")
            st.code("Generate user data with columns: id (UUID), name (text), email (email)")
            return None
        
        # Generate sample data
        sample_data = worker.generate_data(
            user_prompt=enhanced_prompt,
            output_format="csv",
            row_count=num_rows,
            temperature=0.1
        )
        
        if not sample_data:
            st.error("No data received from the generator")
            return None
            
        try:
            # Handle both string and list responses
            if isinstance(sample_data, list):
                # If it's a list, convert to CSV string
                import csv
                from io import StringIO
                output = StringIO()
                writer = csv.writer(output)
                writer.writerows(sample_data)
                sample_data = output.getvalue()
                output.close()
            
            # Parse CSV data into DataFrame
            df = pd.read_csv(
                io.StringIO(sample_data),
                names=columns,
                skipinitialspace=True
            )
            
            if df.empty:
                st.error("Generated DataFrame is empty")
                return None
                
            # Show column type information
            st.info("📊 Detected Column Types:")
            for col, type_info in column_types.items():
                st.write(f"- `{col}`: {type_info}")
                
            return df
            
        except pd.errors.EmptyDataError:
            st.error("Generated CSV data was empty or invalid")
            return None
        except Exception as e:
            st.error(f"Error parsing CSV data: {str(e)}")
            st.error("Please check if your prompt clearly defines the expected columns and their types")
            return None

    except Exception as e:
        # Check specifically for the list vs str type error
        if "sequence item 0: expected str instance, list found" in str(e):
            st.error("❌ Data format error: Received a list instead of text data")
            st.info("This usually happens when the API returns structured data instead of CSV text")
            st.info("💡 Try regenerating the sample or adjusting the prompt to be more specific about CSV format")
        else:
            st.error(f"❌ Sample generation failed: {str(e)}")
            st.info("💡 Try rephrasing your prompt to include clear column definitions")
            st.info("Example: Generate user data with columns: id (UUID), name (text), email (email)")
        return None

# --- Airflow Configuration ---
AIRFLOW_BASE_URL = os.environ.get("AIRFLOW_API_URL", "http://airflow-webserver:8080/api/v1")
AIRFLOW_DAG_ID = "synthetic_data_generator"  # Match the DAG ID in your DAG file
AIRFLOW_USER = os.environ.get("AIRFLOW_USER", "airflow")
AIRFLOW_PASS = os.environ.get("AIRFLOW_PASS", "airflow")

# --- Helper Functions ---

def trigger_airflow_dag(prompt: str, total_rows: int) -> str | None:
    """Triggers the Airflow DAG and returns the run_id if successful."""
    dag_run_url = f"{AIRFLOW_BASE_URL}/dags/{AIRFLOW_DAG_ID}/dagRuns"
    try:
        response = requests.post(
            dag_run_url,
            auth=(AIRFLOW_USER, AIRFLOW_PASS),
            json={
                "conf": {
                    "user_prompt": prompt,
                    "output_format": "csv",  # Default to CSV for now
                    "total_rows": total_rows,
                    "batch_size": 500  # Use default batch size
                }
            }
        )
        response.raise_for_status()
        run_id = response.json().get("dag_run_id")
        st.success(f"Successfully triggered pipeline! Run ID: {run_id}")
        st.info("Your data is being generated. You can check status below.")
        st.balloons()
        return run_id
    except Exception as e:
        st.error(f"Failed to trigger Airflow DAG: {e}")
        # Try to get more detailed error from response if available
        try:
             error_detail = response.json()
             st.error(f"Airflow API Response: {error_detail}")
        except:
             st.error(f"Raw Response: {response.text}")
        return None

def get_airflow_dag_run_status(run_id: str) -> str | None:
    """Gets the status of a specific DAG run from the Airflow API."""
    if not run_id: return None
    status_url = f"{AIRFLOW_BASE_URL}/dags/{AIRFLOW_DAG_ID}/dagRuns/{run_id}"
    try:
        response = requests.get(status_url, auth=(AIRFLOW_USER, AIRFLOW_PASS))
        response.raise_for_status()
        return response.json().get("state")
    except Exception as e:
        print(f"Error checking DAG run status for {run_id}: {e}")
        return None

# def extract_text_from_pdf(pdf_file):
    # ... (same as before) ...
    #reader = pypdf2.PdfReader(BytesIO(pdf_file.read()))
    #text = ""
    #for page in reader.pages:
    #    extracted = page.extract_text()
    #    if extracted:
    #         text += extracted
   # return text

# --- Streamlit UI ---
st.set_page_config(layout="wide")
st.title("🤖 Gemini Data Generation Pipeline (CSV Output)")

# --- Session State Initialization ---
if 'prompt' not in st.session_state:
    st.session_state.prompt = ""
if 'sample_df' not in st.session_state: # Changed from sample_data to sample_df
    st.session_state.sample_df = None
if 'run_id' not in st.session_state:
    st.session_state.run_id = None
if 'dag_status' not in st.session_state:
     st.session_state.dag_status = None
if 'final_csv_path' not in st.session_state:
     st.session_state.final_csv_path = None # Store path once finished

# --- 1. User Input ---
st.header("1. Enter Your Prompt")
col1, col2 = st.columns(2)
with col1:
    prompt_text = st.text_area("Enter prompt:", height=200, key="prompt_input")
with col2:
    prompt_pdf = st.file_uploader("Or upload PDF:", type="pdf")

# Update prompt state
#if prompt_pdf:
    #st.session_state.prompt = extract_text_from_pdf(prompt_pdf)
    #st.text_area("Extracted PDF Text:", value=st.session_state.prompt, height=200, key="prompt_display", disabled=True)
if prompt_text:
    st.session_state.prompt = prompt_text
    
total_rows = st.number_input("Total rows to generate:", min_value=100, max_value=5_000_000, value=500, step=100) # Smaller default

# --- 2. Generate Sample ---
st.header("2. Review Sample (CSV Format)")

# Check environment variables first
if not os.environ.get("GEMINI_API_KEY"):
    st.error("❌ GEMINI_API_KEY environment variable is not set. Please configure it first.")
else:
    if st.button("Generate Sample"):
        if st.session_state.prompt:
            with st.spinner("Calling Gemini for a CSV sample..."):
                try:
                    st.session_state.sample_df = get_gemini_csv_sample(st.session_state.prompt, 5)
                    if st.session_state.sample_df is None:
                        st.error("❌ Failed to generate or parse the sample CSV. Check the details below.")
                        st.error("This could be due to invalid API key or API response format issues.")
                        st.info("💡 Try running the app with DEBUG=true environment variable for more detailed logs.")
                    # Clear previous run status if generating new sample
                    st.session_state.run_id = None
                    st.session_state.dag_status = None
                    st.session_state.final_csv_path = None
                except Exception as e:
                    st.error(f"❌ An error occurred: {str(e)}")
                    st.error("Please check if your API key is valid and has sufficient permissions.")
        else:
            st.warning("⚠️ Please enter a prompt first.")

# Display Sample if available
if st.session_state.sample_df is not None:
    # Display column names first
    st.write("Generated Column Names:")
    st.write(", ".join([f"`{col}`" for col in st.session_state.sample_df.columns]))
    
    # Display the full DataFrame with headers
    st.write("Sample Data:")
    st.dataframe(
        st.session_state.sample_df,
        use_container_width=True,
        column_config={col: st.column_config.Column(label=col) for col in st.session_state.sample_df.columns}
    )
    st.markdown("---")

    # --- 3. Start Full Pipeline ---
    st.header("3. Start Full Pipeline")
    if st.button("✅ Continue & Start Full Generation", type="primary"):
        st.session_state.sample_df = None # Clear sample display
        st.session_state.run_id = None # Reset run ID
        st.session_state.dag_status = None # Reset status
        st.session_state.final_csv_path = None # Reset path
        with st.spinner("Triggering Airflow batch job..."):
            st.session_state.run_id = trigger_airflow_dag(st.session_state.prompt, total_rows)
            if st.session_state.run_id:
                 st.session_state.dag_status = "queued" # Initial status
    st.markdown("---")


# --- 4. Monitor, Preview, and Download ---
if st.session_state.run_id:
    st.header("4. Pipeline Status & Output")
    st.write(f"Monitoring DAG Run ID: **{st.session_state.run_id}**")

    # Button to manually check status
    if st.button("🔄 Check Status"):
        with st.spinner("Checking Airflow DAG status..."):
            st.session_state.dag_status = get_airflow_dag_run_status(st.session_state.run_id)
            if st.session_state.dag_status == 'success' and not st.session_state.final_csv_path:
                 # Attempt to retrieve final path from XCom if run succeeded
                 # NOTE: This requires Airflow API access to XComs, which might need setup.
                 # Simplified approach: Construct path based on run_id.
                 st.session_state.final_csv_path = f"/app/data/pipeline_runs/{st.session_state.run_id}/final_output.csv"

    # Display current status
    if st.session_state.dag_status:
        if st.session_state.dag_status == 'success':
            st.success(f"✅ Pipeline finished successfully!")
        elif st.session_state.dag_status == 'failed':
            st.error(f"❌ Pipeline failed. Check Airflow logs for details.")
        elif st.session_state.dag_status in ['queued', 'running']:
            st.info(f"⏳ Pipeline status: {st.session_state.dag_status}")
        else:
            st.warning(f"Unknown pipeline status: {st.session_state.dag_status}")

    # Show Preview and Download button ONLY if status is success
    if st.session_state.dag_status == 'success' and st.session_state.final_csv_path:
        st.subheader("Preview Final Data (First 10 Rows)")
        
        # Construct the path relative to THIS container's mount
        preview_path = st.session_state.final_csv_path 
        
        try:
            # Check if file exists before reading
            if os.path.exists(preview_path):
                 preview_df = pd.read_csv(preview_path, nrows=10)
                 st.dataframe(preview_df, use_container_width=True)

                 # Add download button for the *full* file
                 with open(preview_path, "rb") as fp:
                      st.download_button(
                           label="⬇️ Download Full CSV",
                           data=fp,
                           file_name="generated_data.csv",
                           mime="text/csv",
                      )
            else:
                 st.error(f"Could not find the final CSV file at expected path: {preview_path}. There might be an issue with the volume mount or file saving.")
        
        except Exception as e:
            st.error(f"Error reading or displaying preview file: {e}")
            st.error(f"Attempted to read from: {preview_path}")