import streamlit as st
import pandas as pd
import sys 
import glob
import os
import google.generativeai as genai
import re
import time
import json
import zipfile
import io
import importlib  # <-- Added for local execution
from dotenv import load_dotenv  # <-- ADD THIS LINE

load_dotenv()

# --- Configuration ---
GENERATOR_FILE_PATH = "dags/utils/database_generator.py"
DATA_DIR = "data/generated_users"
# DAG_ID = "ai_database_generator" # <-- No longer needed

# --- NEW: Updated placeholder code for local execution ---
DEFAULT_GENERATOR_CODE = """# Please define a schema and generate code.
import os
print("This is a placeholder. Generate a schema in the UI.")

# Define and create the output directory relative to this script
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "..", "..", "data", "generated_users")
os.makedirs(OUTPUT_DIR, exist_ok=True)

def main():
    print("Placeholder main() function. Generate new code in the UI.")
    # Create a dummy file to show it works
    with open(os.path.join(OUTPUT_DIR, "placeholder.txt"), "w") as f:
        f.write("This is a placeholder.")

if __name__ == "__main__":
    main()
"""

# --- Airflow API Configuration ---
# --- All Airflow constants have been removed ---

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
        # Ensure the directory exists
        os.makedirs(os.path.dirname(GENERATOR_FILE_PATH), exist_ok=True)
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

def create_zip_archive(dataframes_dict):
    progress_bar = None # Define progress_bar outside the try block
    try:
        zip_buffer = io.BytesIO()  # <-- FIX 1: Was io.BytesB
        
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_f:
            total_files = len(dataframes_dict)
            # Now progress_bar is defined
            progress_bar = st.progress(0, text="Zipping files...") 
            
            for i, (table_name, df) in enumerate(dataframes_dict.items()):
                csv_data = df.to_csv(index=False)
                csv_file_name = f"{table_name}.csv"
                zip_f.writestr(csv_file_name, csv_data)
                
                progress_text = f"Zipping {csv_file_name}... ({i+1}/{total_files})"
                progress_bar.progress((i + 1) / total_files, text=progress_text)
            
        progress_bar.empty()
        zip_buffer.seek(0)
        return zip_buffer
        
    except Exception as e:
        st.error(f"Failed to create zip file: {e}")
        # FIX 2: Check if progress_bar exists before trying to empty it
        if progress_bar: 
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
    keys_to_delete = [
        'key_counter', 'current_code', 'code_is_saved', 'schema_defined',
        'num_tables', 'tables', 'zip_data_ready', 'zip_data', 'generation_mode'
        # 'dag_run_id', 'monitoring_dag' <-- Removed Airflow keys
    ]
    
    for key in keys_to_delete:
        if key in st.session_state:
            del st.session_state[key]
            
    # 4. Rerun the app.
    st.rerun()

# --- Airflow API Functions ---
# --- All Airflow API functions (trigger_airflow_dag, get_dag_run_status) have been removed ---

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
        # Use the model you prefer (gemini-2.5-pro, gemini-1.5-pro, etc.)
        model = genai.GenerativeModel('gemini-2.5-pro') 
        schema_str = json.dumps(schema, indent=2)
        
        # --- PROMPT UPDATED FOR ROBUSTNESS ---
        full_prompt = f"""
        You are an expert Python data engineer. Your task is to write a single, robust Python script to generate a multi-table dataset.

        You will be given a JSON object describing the tables, their relationships, and the number of rows for each.
        
        YOUR GOAL is to write a Python script with a single `main()` function. This script must:

        1.  **IMPORTS:** Import all necessary libraries: `pandas as pd`, `numpy as np`, `faker`, `datetime`, `random`, `os`.
        2.  **SETUP:** * Define `OUTPUT_DIR = "{DATA_DIR}"`.
            * Ensure the directory exists: `os.makedirs(OUTPUT_DIR, exist_ok=True)`.
            * Initialize Faker: `fake = Faker()`.
        
        3.  **DEPENDENCY MANAGEMENT (Topological Sort):**
            * You MUST generate "Parent" tables (those with no Foreign Keys) first.
            * You MUST generate "Child" tables (those with Foreign Keys) *after* their parents are created.
            * **Store IDs:** As you create a parent table, store its Primary Key values in a Python list in memory (e.g., `customer_ids = df['customer_id'].tolist()`). You will need these for the child tables.

        4.  **DATA GENERATION RULES:**
            * **Patterned IDs:** If the prompt asks for a pattern (e.g., 'CUST-XXXX'), use f-strings (e.g., `f"CUST-{{i:04d}}"`). Otherwise, use sequential integers.
            * **Foreign Keys:** When generating a child table, fill the Foreign Key column by using `random.choice(parent_id_list)` to ensure every FK exists in the parent.
            * **Row Counts:** Generate the exact number of rows specified. Since this is a demo, generate all rows in memory (no batching needed for <50k rows).

        5.  **CRITICAL: PREVENT DATA TYPE ERRORS (Strict Types):**
            * **NEVER mix data types** in a single column construction.
            * **`numpy.select` / `np.where` Safety:** If you use conditional logic to create a column (e.g., assigning 'Gold' or 'Silver' status), ensure the `default` value matches the data type of the choices.
                * ❌ *WRONG:* `choices=['A', 'B']; default=0` (Mixes String and Int)
                * ✅ *CORRECT:* `choices=['A', 'B']; default='Unknown'` (All Strings)
            * **Dates:** Ensure dates are generated as datetime objects or consistent ISO strings.

        6.  **CRITICAL: PREVENT FAKER ERRORS:**
            * Use ONLY standard Faker providers.
            * For "Product Name": Use `fake.catch_phrase()` or `fake.bs()`. Do NOT use `fake.product_name()`.
            * For "Company": Use `fake.company()`.
            * For general text: Use `fake.word()` or `fake.sentence()`.

        7.  **OUTPUT:**
            * Save each table as a `.parquet` file in `OUTPUT_DIR`.
            * Print status messages (e.g., "Generated Customers...").
            * The `main()` function should return nothing.

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
    st.error("`GEMINI_API_KEY` not found. Please set it as an environment variable.")
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

    if st.button("✅ Lock in Code", use_container_width=True):
        code_to_save = st.session_state[editor_key]
        st.session_state.current_code = code_to_save
        st.session_state.code_is_saved = True
        st.success("Code is locked in and ready to run!")

    # --- Shared UI: Step 4 (Run Pipeline) ---
    st.markdown("---")
    st.header("4. 🚀 Run Pipeline & Download Data")

    if not st.session_state.get("code_is_saved", False):
        st.warning("Please **Lock in Code** before starting data generation.")
    else:
        if st.button("🚀 Start Database Generation", type="primary", use_container_width=True):
            
            with st.spinner("Data generation in progress... This may take several minutes."):
                try:
                    # 1. Get the code string from session state
                    generated_code = st.session_state.current_code
                    
                    # 2. Create a scope for the exec function
                    local_scope = {}
                    
                    # 3. Execute the code, which defines the main() function
                    exec(generated_code, globals(), local_scope)
                    
                    # 4. Extract the main() function
                    main_func = local_scope['main']
                    
                    st.toast("--- Starting Database Generation ---")
                    # 5. Run main() and get the in-memory data
                    data_dict = main_func() 
                    st.toast("--- Database Generation Complete ---")
                    
                    # 6. Store the resulting data in session state
                    st.session_state.generated_data = data_dict
                    
                    st.success("Data generation complete! ✅")
                    st.balloons()
                    st.rerun()

                except Exception as e:
                    st.error(f"Data generation failed. See details below.")
                    st.exception(e) # Show full traceback    # --- Polling logic has been removed ---

    # --- Shared UI: Step 5 (Download) ---
    # --- Shared UI: Step 5 (Download) ---
    st.subheader("Generated Database Tables")

    if not st.session_state.get("generated_data"):
        st.warning("No data found. Run the generation pipeline first.")
    else:
        data_dict = st.session_state.generated_data
        st.success(f"Found {len(data_dict)} database tables in memory!")

        file_names = list(data_dict.keys())
        selected_table_name = st.selectbox("Select a table to preview:", file_names)

        if selected_table_name:
            try:
                sample_df = data_dict[selected_table_name]
                st.dataframe(sample_df.head(100), hide_index=True)

                total_rows = len(sample_df)
                st.metric(label=f"Total Rows in {selected_table_name}", value=f"{total_rows:,}")

            except Exception as e:
                st.error(f"Failed to read sample data: {e}")

        st.subheader("Download All Tables (.zip)")
        if st.button("📦 Prepare All Tables as .zip", type="primary", use_container_width=True):
            zip_data = create_zip_archive(data_dict)
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
                    cleanup_data_and_reset() # This function remains the same