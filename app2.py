import streamlit as st
import pandas as pd
import glob
import os
import google.generativeai as genai
import re
import requests
from requests.auth import HTTPBasicAuth
import time
import json
import zipfile
import io

# --- Configuration ---
GENERATOR_FILE_PATH = "dags/utils/database_generator.py"
DATA_DIR = "data/generated_users"
DAG_ID = "ai_database_generator"

# --- NEW: Default placeholder code ---
DEFAULT_GENERATOR_CODE = """# Please define a schema and generate code.
import os
print("This is a placeholder. Generate a schema in the UI.")
os.makedirs("/opt/airflow/data/generated_users", exist_ok=True)
"""

# --- Airflow API Configuration ---
AIRFLOW_API_URL = os.environ.get("AIRFLOW_API_URL", "http://airflow-webserver:8080/api/v1")
AIRFLOW_USER = os.environ.get("AIRFLOW_USER", "airflow")
AIRFLOW_PASS = os.environ.get("AIRFLOW_PASS", "airflow")
AIRFLOW_AUTH = HTTPBasicAuth(AIRFLOW_USER, AIRFLOW_PASS)

st.set_page_config(layout="wide", page_title="AI Database Generator")
st.title("🤖 AI Multi-Table Database Generator")

# --- Helper Functions ---

def load_generator_code(default=False):
    if default or not os.path.exists(GENERATOR_FILE_PATH):
        return DEFAULT_GENERATOR_CODE
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

def clean_json_response(text):
    text = text.replace("```python", "").replace("```json", "").replace("```", "")
    text = text.strip()
    match = re.search(r'\{.*\}', text, re.DOTALL)
    if match:
        return match.group(0) 
    match = re.search(r'\[.*\]', text, re.DOTALL)
    if match:
        return match.group(0)
    return text 

def clean_python_response(text):
    text = text.replace("```python", "").replace("```", "")
    return text.strip()

def create_zip_archive(parquet_files):
    try:
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_f:
            total_files = len(parquet_files)
            progress_bar = st.progress(0, text="Zipping files...")
            
            for i, file_path in enumerate(parquet_files):
                df = pd.read_parquet(file_path)
                csv_data = df.to_csv(index=False)
                base_name = os.path.basename(file_path)
                csv_file_name = base_name.replace('.parquet', '.csv')
                zip_f.writestr(csv_file_name, csv_data)
                
                progress_text = f"Zipping {csv_file_name}... ({i+1}/{total_files})"
                progress_bar.progress((i + 1) / total_files, text=progress_text)
        
        progress_bar.empty()
        zip_buffer.seek(0)
        return zip_buffer
    except Exception as e:
        st.error(f"Failed to create zip file: {e}")
        progress_bar.empty()
        return None

def cleanup_data_and_reset():
    # 1. Delete data files
    try:
        parquet_files = sorted(glob.glob(f"{DATA_DIR}/*.parquet"))
        if not parquet_files:
            st.toast("No data files to clean up.")
        else:
            for f in parquet_files:
                os.remove(f)
            st.toast(f"Cleaned up {len(parquet_files)} data files.")
    except Exception as e:
        st.error(f"Error cleaning data files: {e}")

    # 2. Reset the generator.py code file
    save_generator_code(DEFAULT_GENERATOR_CODE)

    # 3. Reset all Streamlit state variables by DELETING them
    # We must not SET the 'generation_mode' key, as the widget already exists.
    # We delete it, and the init block will reset it on the rerun.
    
    # Get all keys to delete
    keys_to_delete = [
        'key_counter', 'current_code', 'code_is_saved', 'schema_defined',
        'num_tables', 'tables', 'dag_run_id', 'monitoring_dag',
        'zip_data_ready', 'zip_data', 'generation_mode' # Delete this key too
    ]
    
    for key in keys_to_delete:
        if key in st.session_state:
            del st.session_state[key]
        
    # 4. Rerun the app. The STATE INITIALIZATION block will now
    #    re-create all the keys with their default values.
    st.rerun()

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

# --- Gemini API Functions ---

def call_gemini_for_schema(user_prompt, api_key):
    try:
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel('gemini-2.5-pro') 
        
        full_prompt = f"""
        You are an expert database architect. Your task is to read a user's natural language prompt and design a normalized, relational database schema. You must return this schema as a structured JSON object.

        **Your Thought Process:**
        1.  **Identify Entities:** First, identify the core entities (e.g., 'Customers', 'Products', 'Sales'). These will be your tables.
        2.  **Define Columns & PKs:** For each table, define its columns based on the prompt. Assign a clear Primary Key (PK). If the user gives a pattern (like 'CUST-XXXX'), include that in the prompt.
        3.  **Assign Relationships (FKs):** Identify the 'Fact' table (e.g., 'Sales') and the 'Dimension' tables (e.g., 'Customers', 'Products'). Link them using Foreign Keys (FKs) in the format `TableName.column_name`.
        4.  **Create Table-Specific Prompts:** This is critical. For each table's `"prompt"` field in the JSON, you must write a *new, small prompt* that describes *only that table's columns*. For example, the `Customers` table prompt should be 'A customer with a customer_id (CUST-XXXX), name, and email,' not the user's full request.

        **JSON Output Rules:**
        * The output MUST be a dictionary where each key is a table ID (e.g., "table_0", "table_1").
        * Each value must be a dictionary with these keys: "name", "rows", "prompt", "pk", "fk".
        * "rows": Small "dimension" tables should be ~1000. Large "fact" tables should be 1,000,000.
        * "fk": A list of foreign keys (e.g., `["Customers.customer_id"]`).
        
        **Example:**
        *User Prompt:* "I need a retail database with 1000 customers (ID: CUST-XXXX), 500 products (ID: P-XXXX), and 1M sales."
        *Your JSON Response:*
        {{
          "table_0": {{
            "name": "Customers",
            "rows": 1000,
            "prompt": "A customer with a customer_id (like CUST-XXXX) and a name",
            "pk": "customer_id",
            "fk": []
          }},
          "table_1": {{
            "name": "Products",
            "rows": 500,
            "prompt": "A product with a product_id (like P-XXXX) and a product_name",
            "pk": "product_id",
            "fk": []
          }},
          "table_2": {{
            "name": "Sales",
            "rows": 1000000,
            "prompt": "A sale with a sale_id (SALE-XXXX), customer_id, product_id, and amount",
            "pk": "sale_id",
            "fk": ["Customers.customer_id", "Products.product_id"]
          }}
        }}

        Respond ONLY with the raw JSON object and nothing else.

        USER PROMPT:
        "{user_prompt}"
        """
        
        response = model.generate_content(full_prompt)
        cleaned_response = clean_json_response(response.text)
        return cleaned_response
    
    except Exception as e:
        st.error(f"Error calling Gemini API for schema: {e}")
        return None

def call_gemini_for_code(schema, api_key):
    try:
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel('gemini-2.5-pro') 
        schema_str = json.dumps(schema, indent=2)
        
        full_prompt = f"""
        You are an expert Python data engineer. Your task is to write a single Python script to generate a multi-table dataset with specific Primary Key (PK) and Foreign Key (FK) requirements.
        You will be given a JSON object describing the tables, their relationships, and the number of rows for each.
        
        YOUR GOAL is to write a Python script with a single `main()` function. This script must:
        1.  Import necessary libraries: `pandas as pd`, `faker`, `datetime`, `random`, `os`.
        2.  Define the output directory: `OUTPUT_DIR = "/opt/airflow/data/generated_users"` and ensure it exists.
        3.  Initialize Faker: `fake = Faker()`.
        4.  **Generation Order:** Generate tables with fewer rows first ("Dimension").
        5.  **PATTERNED PRIMARY KEYS:** For columns specified as a Primary Key (PK) (e.g., 'customer_id'), check the user's prompt for that table. 
            * If the prompt mentions a specific pattern (like 'CUST-XXXX'), generate IDs matching that pattern using Python f-strings (e.g., `f"CUST-{{i:04d}}"` where `i` is the sequence number starting from 1).
            * If no pattern is mentioned, generate **sequential integers starting from 1**.
        6.  Store these generated PKs in a list in memory (e.g., `customer_id_list`).
        7.  Generate tables with many rows last ("Fact").
        8.  **FOREIGN KEYS:** When generating a Fact table's Foreign Key (FK) column (e.g., 'customer_id' in Sales), you MUST use `random.choice(customer_id_list)` to select a valid PK from the corresponding PK list.
        9.  **Row Counts:** Generate the exact number of rows specified for each table.
        10. **Batching:** For large tables (> 100,000 rows), generate data in batches.
        11. Save each table as a separate `.parquet` file in the `OUTPUT_DIR`.
        12. Include print statements for progress.

        **CRITICAL FAKER RULES:**
        * For a **product name**: use `fake.catch_phrase()` or `fake.bs()`.
        * For **IDs NOT specified as PK/FK**: use `random.randint(1000, 9999)`.
        
        Respond ONLY with the complete, runnable Python code.

        ---
        HERE IS THE DATABASE SCHEMA:
        {schema_str}
        ---
        """
        
        response = model.generate_content(full_prompt)
        cleaned_response = clean_python_response(response.text)
        return cleaned_response
    
    except Exception as e:
        st.error(f"Error calling Gemini API for code: {e}")
        return None

# --- Streamlit UI ---

API_KEY = os.environ.get("GEMINI_API_KEY")
if not API_KEY:
    st.error("`GEMINI_API_KEY` not found. Please set it in your `.env` file and restart your containers.")
    st.stop()

# --- STATE INITIALIZATION ---
if 'key_counter' not in st.session_state:
    st.session_state.key_counter = 0
if 'current_code' not in st.session_state:
    st.session_state.current_code = load_generator_code()
if 'code_is_saved' not in st.session_state:
    st.session_state.code_is_saved = os.path.exists(GENERATOR_FILE_PATH) and st.session_state.current_code != DEFAULT_GENERATOR_CODE
if 'generation_mode' not in st.session_state:
    st.session_state.generation_mode = "Single Prompt (AI-Assisted)"
if 'schema_defined' not in st.session_state:
    st.session_state.schema_defined = False
if 'num_tables' not in st.session_state:
    st.session_state.num_tables = 1
if 'tables' not in st.session_state:
    st.session_state.tables = {}
# ---

# --- Mode Selection ---
st.header("1. 🤖 Select Generation Mode")
mode = st.radio(
    "How do you want to define your schema?",
    ["Single Prompt (AI-Assisted)", "Multi-Prompt (Manual)"],
    key='generation_mode',
    horizontal=True,
    label_visibility="collapsed"
)

# --- Single Prompt UI ---
if st.session_state.generation_mode == "Single Prompt (AI-Assisted)":
    st.info("Describe your entire database in one prompt. The AI will generate the schema for you to review.")
    single_prompt = st.text_area(
        "Describe your database:",
        height=150,
        placeholder="e.g., 'A 4-table retail database. I need 1000 Customers (ID: CUST-XXXX), 500 Products (ID: P-XXXX), 50 Stores (ID: S-XXX), and 1 million Sales. Sales should link to all other tables.'"
    )
    if st.button("Generate Schema from Prompt", use_container_width=True, type="primary"):
        if single_prompt:
            with st.spinner("Calling AI to design schema..."):
                schema_json = call_gemini_for_schema(single_prompt, API_KEY)
            try:
                parsed_schema = json.loads(schema_json)
                st.session_state.tables = parsed_schema
                st.session_state.num_tables = len(parsed_schema)
                st.session_state.schema_defined = True
                st.success("Schema generated! Review and edit it below.")
                st.rerun() 
            except Exception as e:
                st.error(f"AI returned invalid JSON. Please try again. Error: {e}")
                st.code(f"---AI Response---\n{schema_json}", language="json")
        else:
            st.warning("Please enter a prompt.")

# --- Multi Prompt UI ---
elif st.session_state.generation_mode == "Multi-Prompt (Manual)":
    st.info("Manually define how many tables you want and fill in the details for each.")
    num_tables_input = st.number_input("How many tables do you want to generate?", min_value=1, max_value=10, value=st.session_state.num_tables)
    
    if st.button("Define Table Schema", use_container_width=True):
        st.session_state.num_tables = num_tables_input
        st.session_state.schema_defined = True
        st.session_state.tables = {} 

# --- Shared UI: Step 2 (Define Tables) ---
if st.session_state.schema_defined:
    st.markdown("---")
    st.header("2. 📝 Review & Edit Schema")
    st.info("Review the schema below. The AI's guesses (in Single-Prompt mode) or your manual setup (in Multi-Prompt mode) are shown. You can edit anything.")

    pk_options = []
    
    # --- UPDATED: 2x2 Grid for Table Definitions ---
    st.subheader("Define Tables")
    cols = st.columns(2)
    
    # First pass to get names and PKs
    for i in range(st.session_state.num_tables):
        table_key = f"table_{i}"
        if table_key not in st.session_state.tables:
            st.session_state.tables[table_key] = {
                "name": f"Table{i+1}", "rows": 1000, "prompt": "", "pk": "", "fk": []
            }
        
        table_name = st.session_state.tables[table_key].get('name', f'Table {i+1}')
        col_index = i % 2
        
        with cols[col_index]:
            with st.expander(f"Table {i+1}: {table_name}", expanded=True):
                t_def = st.session_state.tables[table_key]
                t_def['name'] = st.text_input(f"Table Name", value=t_def.get('name', f"Table{i+1}"), key=f"name_{i}")
                
                t_def['rows'] = st.number_input(
                    f"Number of Rows for {t_def['name']}",
                    min_value=1, max_value=1_000_000,
                    value=t_def.get('rows', 1000),
                    key=f"rows_{i}"
                )
                st.caption("Max: 1,000,000. Use small counts for dimension tables, large for fact tables.")
                
                t_def['prompt'] = st.text_area(f"Prompt for {t_def['name']}", value=t_def.get('prompt', ''), key=f"prompt_{i}", placeholder=f"e.g., A student with a student_id (like STUD-XXXX), name, and email")
                t_def['pk'] = st.text_input(f"Primary Key Column Name", value=t_def.get('pk', ''), key=f"pk_{i}", placeholder=f"e.g., student_id")
                
                if t_def['name'] and t_def['pk']:
                    pk_options.append(f"{t_def['name']}.{t_def['pk']}")

    # --- UPDATED: Separate section for Foreign Keys ---
    st.subheader("Define Table Links (Foreign Keys)")
    st.info("Link your tables together by selecting the Primary Keys from other tables.")
    
    for i in range(st.session_state.num_tables):
        table_key = f"table_{i}"
        t_def = st.session_state.tables[table_key]
        table_name = t_def.get('name', f'Table {i+1}')
        
        with st.expander(f"Links for Table {i+1}: {table_name}"):
            available_links = [opt for opt in pk_options if not opt.startswith(f"{t_def['name']}.")]
            if available_links:
                t_def['fk'] = st.multiselect(
                    f"Link {t_def['name']} to other tables:",
                    options=available_links,
                    default=t_def.get('fk', []),
                    key=f"fk_{i}"
                )
            else:
                st.caption("No other Primary Keys are defined yet. Define PKs in other tables to link them here.")


    # --- Shared UI: Step 3 (Generate Code) ---
    st.markdown("---")
    st.header("3. 🤖 Generate & Save Code")

    if st.button("Generate Database Code", use_container_width=True, type="primary"):
        with st.spinner("Calling Gemini API to write Python code..."):
            generated_code = call_gemini_for_code(st.session_state.tables, API_KEY)
        if generated_code:
            st.session_state.current_code = generated_code
            st.session_state.key_counter += 1
            st.session_state.code_is_saved = False
            st.success("Code generated! Review and save below.")
        else:
            st.error("AI failed to generate code.")

    editor_key = f"code_editor_{st.session_state.key_counter}"
    code_in_editor = st.text_area(
        "AI-Generated Code (Edit as needed):", 
        value=st.session_state.current_code, 
        height=400, 
        key=editor_key
    )

    if st.button("💾 Save Code to Airflow", use_container_width=True):
        code_to_save = st.session_state[editor_key]
        save_generator_code(code_to_save)
        st.session_state.current_code = code_to_save
        st.session_state.code_is_saved = True
        st.success("Code saved to file!")

    # --- Shared UI: Step 4 (Run Pipeline) ---
    st.markdown("---")
    st.header("4. 🚀 Run Pipeline & Download Data")

    if not st.session_state.get("code_is_saved", False):
        st.warning("Please **Save Code** before starting data generation.")
    else:
        if st.button("🚀 Start Database Generation", type="primary", use_container_width=True):
            if st.session_state.get(editor_key, "") != load_generator_code():
                 st.warning("Your latest edits are not saved. Please click 'Save Code' first.")
            else:
                dag_run_id = trigger_airflow_dag()
                if dag_run_id:
                    st.session_state.monitoring_dag = True
                    st.info(f"Successfully triggered Airflow DAG run: `{dag_run_id}`")
                else:
                    st.error("Failed to trigger DAG. Check the Airflow Webserver logs.")

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
                st.rerun() 
                
            elif status == "failed":
                st.error(f"DAG run {st.session_state.dag_run_id} failed. Please check the Airflow UI for logs.")
                st.session_state.monitoring_dag = False

    # --- Shared UI: Step 5 (Download) ---
    st.subheader("Generated Database Files")
    st.info(f"Files are saved as Parquet in your project's `{DATA_DIR}` folder.")

    parquet_files = sorted(glob.glob(f"{DATA_DIR}/*.parquet"))

    if not parquet_files:
        st.warning("No data files found. Please run your Airflow DAG first.")
    else:
        st.success(f"Found {len(parquet_files)} database tables!")
        
        file_names = [os.path.basename(f) for f in parquet_files]
        selected_file_name = st.selectbox("Select a table to preview:", file_names)
        
        if selected_file_name:
            try:
                selected_file_path = os.path.join(DATA_DIR, selected_file_name)
                sample_df = pd.read_parquet(selected_file_path)
                st.dataframe(sample_df.head(100), hide_index=True)
                
                total_rows = len(sample_df)
                st.metric(label=f"Total Rows in {selected_file_name}", value=f"{total_rows:,}")
                
            except Exception as e:
                st.error(f"Failed to read sample file {selected_file_name}: {e}")

        st.subheader("Download All Tables (.zip)")
        if st.button("📦 Prepare All Tables as .zip", type="primary", use_container_width=True):
            zip_data = create_zip_archive(parquet_files)
            if zip_data:
                st.session_state.zip_data_ready = True
                st.session_state.zip_data = zip_data
                st.success("Zip file is ready to download!")
        
        if st.session_state.get("zip_data_ready", False):
            col1, col2 = st.columns([3, 1])
            with col1:
                st.download_button(
                    label="⬇️ Download database.zip",
                    data=st.session_state.zip_data,
                    file_name="generated_database.zip",
                    mime="application/zip",
                    use_container_width=True
                )
            with col2:
                if st.button("🧹 Clean Up & Reset", use_container_width=True):
                    cleanup_data_and_reset()