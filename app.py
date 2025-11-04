import streamlit as st
import pandas as pd
import glob
import os
import importlib.util
import google.generativeai as genai
import re
import requests
from requests.auth import HTTPBasicAuth
import time

# --- Configuration ---
GENERATOR_FILE_PATH = "dags/utils/generator.py"
DATA_DIR = "data/generated_users"
CONSOLIDATED_FILE = "data/full_dataset.csv.gz"
DAG_ID = "ai_data_generator_1M"

# --- Airflow API Configuration ---
AIRFLOW_API_URL = os.environ.get("AIRFLOW_API_URL", "http://airflow-webserver:8080/api/v1")
AIRFLOW_USER = os.environ.get("AIRFLOW_USER", "airflow")
AIRFLOW_PASS = os.environ.get("AIRFLOW_PASS", "airflow")
AIRFLOW_AUTH = HTTPBasicAuth(AIRFLOW_USER, AIRFLOW_PASS)

st.set_page_config(layout="wide", page_title="AI Data Manager")
st.title("🤖 AI Data Generator & Manager")

# --- Helper Functions (Loading/Saving/Testing) ---

def load_generator_code():
    if not os.path.exists(GENERATOR_FILE_PATH):
        # Return a default placeholder if no file exists
        return """from faker import Faker
from uuid import uuid4
from datetime import datetime
import random

fake = Faker()

def generate_data():
    # Default placeholder
    return {"message": "Please generate code first"}
"""
    with open(GENERATOR_FILE_PATH, "r") as f:
        return f.read()

def save_generator_code(code_text):
    try:
        with open(GENERATOR_FILE_PATH, "w") as f:
            f.write(code_text)
        return True
    except Exception as e:
        st.error(f"Failed to save file: {e}")
        return False

def test_generator_function():
    if not os.path.exists(GENERATOR_FILE_PATH):
        st.error("Please save the generated code before testing.")
        return None
    try:
        spec = importlib.util.spec_from_file_location("generator", GENERATOR_FILE_PATH)
        generator_module = importlib.util.module_from_spec(spec)
        importlib.invalidate_caches()
        spec.loader.exec_module(generator_module)
        generate_func = getattr(generator_module, "generate_data")
        data = [generate_func() for _ in range(10)]
        return pd.DataFrame(data)
    except Exception as e:
        st.error(f"Error testing saved code: {e}")
        st.error("Did you save valid Python code? Does the function 'generate_data' exist?")
        return None

def consolidate_data(parquet_files):
    try:
        df_list = [pd.read_parquet(f) for f in parquet_files]
        full_df = pd.concat(df_list, ignore_index=True)
        full_df.to_csv(CONSOLIDATED_FILE, index=False, compression="gzip")
        return full_df.shape
    except Exception as e:
        st.error(f"Failed to consolidate files: {e}")
        return None

def clean_gemini_response(text):
    text = text.replace("```python", "").replace("```", "")
    return text.strip()

# --- Airflow API Functions ---

def trigger_airflow_dag():
    url = f"{AIRFLOW_API_URL}/dags/{DAG_ID}/dagRuns"
    headers = {"Content-Type": "application/json"}
    body = {"conf": {}}
    
    try:
        response = requests.post(url, auth=AIRFLOW_AUTH, headers=headers, json=body)
        response.raise_for_status() 
        data = response.json()
        dag_run_id = data.get("dag_run_id")
        st.session_state.dag_run_id = dag_run_id
        return dag_run_id
    except requests.exceptions.RequestException as e:
        st.error(f"Failed to trigger DAG. Is Airflow running? Error: {e}")
        return None

def get_dag_run_status():
    if "dag_run_id" not in st.session_state:
        return None
        
    dag_run_id = st.session_state.dag_run_id
    url = f"{AIRFLOW_API_URL}/dags/{DAG_ID}/dagRuns/{dag_run_id}"
    
    try:
        response = requests.get(url, auth=AIRFLOW_AUTH)
        response.raise_for_status()
        data = response.json()
        return data.get("state") 
    except requests.exceptions.RequestException as e:
        st.error(f"Failed to get DAG status: {e}")
        return "failed" 

# --- Gemini Code Generation Function ---

def call_gemini_api(user_prompt, api_key):
    try:
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel('gemini-2.5-pro') 
        
        full_prompt = f"""
        You are an expert Python code generator. Your task is to write a single Python script that uses the Faker library to generate synthetic data.

        RULES:
        1.  The script MUST include all necessary imports: `from faker import Faker`, `from uuid import uuid4`, `from datetime import datetime`, `import random`.
        2.  It MUST initialize Faker: `fake = Faker()`.
        3.  It MUST define one function named `generate_data()`.
        4.  The `generate_data()` function MUST take no arguments and return a single Python dictionary.
        5.  **CRITICAL RULE:** You MUST use only valid Faker provider methods. Do NOT make up method names.
            * For a **product name**, use `fake.catch_phrase()` or `fake.bs()`.
            * For a company name, use `fake.company()`.
            * For a job title, use `fake.job()`.
            * For an address, use `fake.address()`.
            * For text, use `fake.sentence()`.
            * For a full name, use `fake.name()`.
            * For an email, use `fake.email()`.
            * For a UUID, use `str(uuid4())`.
            * For a random integer, use `random.randint(a, b)`.
            * For a random float, use `random.uniform(a, b)`.
            * For a date, use `fake.date_time_between()`.
        6.  The dictionary keys and value types should be based on the user's request, using ONLY the valid methods listed above.
        7.  Respond ONLY with the complete, runnable Python code. Do not include markdown (```python) or any other explanation or text.

        USER REQUEST:
        "Generate data for: {user_prompt}"
        """
        
        response = model.generate_content(full_prompt)
        cleaned_response = response.text.replace("```python", "").replace("```", "").strip()
        return cleaned_response
    
    except Exception as e:
        st.error(f"Error calling Gemini API: {e}")
        return None

# --- Streamlit UI ---

API_KEY = os.environ.get("GEMINI_API_KEY")

# --- STATE INITIALIZATION ---
# This is the fix. We use a key_counter to force the editor to reset.
if 'key_counter' not in st.session_state:
    st.session_state.key_counter = 0
if 'current_code' not in st.session_state:
    st.session_state.current_code = load_generator_code()
if 'code_is_saved' not in st.session_state:
    st.session_state.code_is_saved = os.path.exists(GENERATOR_FILE_PATH)
# ---

st.header("1. 🤖 Generate Data Schema with AI")
st.info("Describe the data you want in plain English. The AI will write the Python code for you to review, edit, and save.")

if not API_KEY:
    st.error("`GEMINI_API_KEY` not found. Please set it in your `.env` file and restart your containers.")
else:
    user_prompt = st.text_area("Enter your prompt:", 
                               height=100,
                               placeholder="e.g., 'A customer order with an order_id, product_name, quantity, price, and order_date.'")
    
    if st.button("Generate Code", use_container_width=True):
        if user_prompt:
            with st.spinner("Calling Gemini API..."):
                generated_code = call_gemini_api(user_prompt, API_KEY)
            if generated_code:
                # --- THIS IS THE FIX ---
                # 1. Update the code in the state
                st.session_state.current_code = generated_code
                # 2. Increment the key to force re-render
                st.session_state.key_counter += 1
                st.session_state.code_is_saved = False # New code is not saved yet
                st.success("Code generated! Review and save below.")
            else:
                st.error("AI failed to generate code.")
        else:
            st.warning("Please enter a prompt.")

# --- Editor and Save/Test Section ---
st.subheader("Generated Code Editor")
st.info("You must **Save Code** before you can **Test** or **Start Generation**.")

# The editor's key is now dynamic. When 'key_counter' changes,
# this widget is destroyed and re-created, forcing it to read
# the new 'value' from st.session_state.current_code.
editor_key = f"code_editor_{st.session_state.key_counter}"
code_in_editor = st.text_area("Edit the code below:", 
                              value=st.session_state.current_code, 
                              height=350, 
                              key=editor_key) # Dynamic key

col1, col2 = st.columns(2)
with col1:
    if st.button("💾 Save Code", use_container_width=True, type="primary"):
        # We save the content of the *current* editor widget
        code_to_save = st.session_state[editor_key]
        save_generator_code(code_to_save)
        # We also update the session state in case the user edited it manually
        st.session_state.current_code = code_to_save
        st.session_state.code_is_saved = True # Flag that we are ready to run
        st.success("Code saved to file!")

with col2:
    if st.button("🧪 Test Saved Code (10 Rows)", use_container_width=True):
        # Test function reads from the file that was just saved
        st.session_state.test_data = test_generator_function()

# Show test data if it's in session state
if "test_data" in st.session_state and st.session_state.test_data is not None and not st.session_state.test_data.empty:
    st.subheader("Test Output (10 Rows)")
    st.dataframe(st.session_state.test_data, hide_index=True)


# --- Start Generation Section ---
st.markdown("---")
# Only show the "Start" button if the code has been saved
if st.session_state.code_is_saved:
    if st.button("🚀 Start Data Generation (1 Million Rows)", type="primary", use_container_width=True):
        # Final check: make sure the code in the editor is what's saved
        if st.session_state[editor_key] != load_generator_code():
             st.warning("Your latest edits are not saved. Please click 'Save Code' first.")
        else:
            dag_run_id = trigger_airflow_dag()
            if dag_run_id:
                st.session_state.monitoring_dag = True
                st.info(f"Successfully triggered Airflow DAG run: `{dag_run_id}`")
            else:
                st.error("Failed to trigger DAG. Check the Airflow Webserver logs.")
else:
    # Show a warning if the user hasn't saved the code yet
    st.warning("Please **Save Code** in the editor above before starting data generation.")

# Polling logic
if st.session_state.get("monitoring_dag", False):
    with st.spinner("Data generation in progress... This may take several minutes. Polling Airflow every 10 seconds."):
        status = "running"
        while status == "running":
            time.sleep(10) 
            status = get_dag_run_status()
        
        if status == "success":
            st.success("Data generation complete! ✅")
            st.balloons()
            st.session_state.monitoring_dag = False
            st.rerun() # Rerun the page to make the download section appear
            
        elif status == "failed":
            st.error(f"DAG run {st.session_state.dag_run_id} failed. Please check the Airflow UI for logs.")
            st.session_state.monitoring_dag = False
        else:
            st.info(f"DAG run status: {status}")

# --- Validate & Download Section ---

st.markdown("---")
st.header("2. 📊 Validate & Download Full Dataset")
st.info(f"This section scans the `{DATA_DIR}` directory for data generated by your Airflow DAG.")

parquet_files = sorted(glob.glob(f"{DATA_DIR}/*.parquet"))

if not parquet_files:
    st.warning("No data files found. Please run your Airflow DAG first.")
else:
    st.success(f"Found {len(parquet_files)} data files (batches).")
    st.subheader("Data Validation (Sample from first batch)")
    try:
        sample_df = pd.read_parquet( parquet_files[0] )
        st.dataframe(sample_df.head(100), hide_index=True)
        
        total_rows_estimate = len(parquet_files) * (sample_df.shape[0] if not sample_df.empty else 10000)
        col1, col2 = st.columns(2)
        col1.metric("Total Files Found", f"{len(parquet_files)}")
        col2.metric("Estimated Total Rows", f"~{total_rows_estimate:,}")

    except Exception as e:
        st.error(f"Failed to read sample file {parquet_files[0]}: {e}")

    st.subheader("Download Full 1M Row Dataset")
    
    if st.button("Combine all files into a single CSV.gz", type="primary"):
        with st.spinner(f"Consolidating {len(parquet_files)} files... This may take a moment."):
            shape = consolidate_data(parquet_files)
        if shape:
            st.session_state.consolidation_complete = True
            st.session_state.consolidated_shape = shape
            st.success(f"Successfully consolidated {shape[0]:,} rows and {shape[1]} columns.")
    
    if st.session_state.get("consolidation_complete", False):
        try:
            with open(CONSOLIDATED_FILE, "rb") as f:
                st.download_button(
                    label="⬇️ Download full_dataset.csv.gz",
                    data=f,
                    file_name="full_dataset.csv.gz",
                    mime="application/gzip",
                    use_container_width=True
                )
        except FileNotFoundError:
            st.error("Consolidated file not found. Please click 'Combine' again.")