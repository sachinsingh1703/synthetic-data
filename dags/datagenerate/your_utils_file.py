import google.generativeai as genai
import os
import pandas as pd
import io 
import csv 
import re
import logging
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# Configure logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# --- Hardened API Configuration ---
API_KEY = os.environ.get("GEMINI_API_KEY")
model = None
RETRYABLE_ERRORS = ()

if not API_KEY:
    log.warning("GEMINI_API_KEY environment variable not set. API calls will fail.")
else:
    try:
        genai.configure(api_key=API_KEY)
        model = genai.GenerativeModel('gemini-1.5-flash-latest')
        # Define retryable errors (must be done after genai is configured)
        RETRYABLE_ERRORS = (
            genai.types.generation_types.InternalServerError,
            genai.types.generation_types.ResourceExhausted,
        )
    except Exception as e:
        log.error(f"Failed to configure Gemini API: {e}")
# --- End Configuration ---


# --- MODIFIED FUNCTION FOR RAW TEXT ---
@retry(
    # Only retry on specific, intermittent API errors
    retry=retry_if_exception_type(RETRYABLE_ERRORS), 
    wait=wait_exponential(multiplier=1, min=2, max=60), # Exponential backoff
    stop=stop_after_attempt(5), # Max 5 attempts
    reraise=True # Reraise the last exception if all retries fail
)
def call_gemini_text(prompt_text: str) -> str:
    """
    Calls the Gemini API with retries and returns the raw text response.
    Includes robust error checking and markdown cleaning.
    """
    if not model:
        log.error("Gemini model is not initialized. Check GEMINI_API_KEY.")
        raise ValueError("Gemini model not initialized.")

    # --- CRITICAL FIX ---
    # Set high token limit to prevent truncation (the EOF error)
    # Set low temperature for consistent, non-creative formatting
    generation_config = genai.types.GenerationConfig(
        max_output_tokens=8192,
        temperature=0.1
    )
    
    response = None
    try:
        response = model.generate_content(
            prompt_text,
            generation_config=generation_config
        )

        if not response or not hasattr(response, 'text') or not response.text:
            log.error("Error: No valid text received from Gemini API.")
            log.error(f"Response object received: {response}")
            raise ValueError("Gemini API did not return valid content text.")

        # --- More robust markdown cleaning ---
        cleaned_text = response.text.strip()
        # Use regex to find content inside ```...```, optional 'csv'
        match = re.search(r"```(?:csv)?\n(.*?)\n```", cleaned_text, re.DOTALL | re.IGNORECASE)
        if match:
            cleaned_text = match.group(1).strip()
        # Fallback for simple ``` wrapper
        elif cleaned_text.startswith("```") and cleaned_text.endswith("```"):
            cleaned_text = cleaned_text[3:-3].strip()
        
        return cleaned_text

    except Exception as e:
        log.error(f"Error calling Gemini or processing response: {e}")
        if response:
            log.error(f"Full Gemini response (if available): {response}")
        # Tenacity will handle the retry/raise
        raise
# --- END MODIFIED FUNCTION ---


# --- UPDATED VALIDATION FOR CSV (with Logging) ---
def verify_csv_data(csv_string: str) -> tuple[bool, int]:
    """
    Validates if the string is valid CSV and has consistent columns.
    Returns (is_valid, number_of_columns)
    """
    if not csv_string or not csv_string.strip():
        log.warning("Validation Error: Received empty string.")
        return False, 0
        
    try:
        # Use StringIO to treat the string as a file
        f = io.StringIO(csv_string.strip())
        reader = csv.reader(f, delimiter=',', quotechar='"', skipinitialspace=True)
        
        first_row = next(reader)
        num_columns = len(first_row)
        if num_columns == 0:
            log.warning("Validation Error: First row has 0 columns.")
            return False, 0

        # Check subsequent rows for consistent column count
        for i, row in enumerate(reader):
            if len(row) != num_columns:
                log.warning(f"Validation Error: Row {i+2} has {len(row)} columns, expected {num_columns}.")
                log.warning(f"Problematic row: {row}")
                return False, num_columns
        
        # If all rows passed
        return True, num_columns
        
    except csv.Error as csv_err:
        log.warning(f"CSV Parsing Error during validation: {csv_err}")
        return False, 0
    except StopIteration: # Handle case with only a header row
        if num_columns > 0:
            log.info("CSV Validation: File contains only a header row. Valid.")
            return True, num_columns # Valid, just a header
        else:
            log.warning("Validation Error: CSV contains no data or header.")
            return False, 0
    except Exception as e:
        log.error(f"Unexpected Error during CSV validation: {e}")
        return False, 0
# --- END UPDATED VALIDATION ---


# --- UPDATED SAMPLE FUNCTION (with Logging) ---
def get_gemini_csv_sample(prompt: str, num_rows: int = 5) -> pd.DataFrame | None:
    """
    Gets a small CSV sample from Gemini and returns it as a Pandas DataFrame.
    Returns None if generation or parsing fails.
    """
    # First check if API key is configured
    if not API_KEY:
        log.error("ERROR: GEMINI_API_KEY environment variable is not set")
        return None

    # (Your excellent prompt template is unchanged)
    sample_prompt = f"""
    Based on the user's request: "{prompt}"

    Generate a CSV dataset with EXACTLY {num_rows} data rows PLUS one header row.

    ---
    ### HEADER ROW RULES (MUST Follow)
    1.  The first row MUST be the header.
    2.  Column names MUST be descriptive and relevant to the request (e.g., "user_id", "full_name", "birth_date").
    3.  All column names MUST be enclosed in double quotes (").
    4.  Use underscores for multi-word column names (e.g., "date_created", "total_amount").

    ---
    ### DATA ROW RULES (MUST Follow)
    1.  Generate EXACTLY {num_rows} data rows immediately after the header.
    2.  Use a comma (,) as the ONLY delimiter between fields.
    3.  Enclose all **non-numeric** fields (text, dates, IDs with letters, etc.) in double quotes (").
    4.  **Numeric** fields (integers, decimals) should NOT be quoted.
    5.  If a text field must contain a comma, it MUST be enclosed in double quotes (e.g., "123 Main St, Anytown").
    6.  If a text field must contain a double quote, it MUST be escaped by doubling it (e.g., "John ""The Boss"" Doe").
    7.  The data MUST be realistic, varied, and directly relevant to the user's request. Do not use placeholder text.
    8.  Every row MUST have the exact same number of columns as the header.

    ---
    ### EXAMPLE (for a request about "employees")
    "employee_id","full_name","department","office_location","salary"
    "E1001","John Smith","Engineering","123 Main St, New York",120000
    "E1002","Jane ""JD"" Doe","Sales","456 Oak Ave, Chicago",85000.50
    "E1003","Robert Brown","Marketing","789 Pine Ln",72000

    ---
    ### FINAL OUTPUT INSTRUCTIONS
    -   Total rows generated must be exactly {num_rows + 1}.
    -   **CRITICAL: Return ONLY the raw CSV text. Do NOT include any other text, explanation, or markdown formatting like ```csv.**
    -   The response must start *immediately* with the header row.
    """
    try:
        log.info(f"Attempting to generate CSV sample with prompt: {prompt}")
        csv_text = call_gemini_text(sample_prompt)
        log.info(f"Received response from Gemini. Length: {len(csv_text) if csv_text else 0}")
        
        if not csv_text:
            log.error("ERROR: Received empty response from Gemini API")
            return None
            
        # Validate the sample
        is_valid, num_cols = verify_csv_data(csv_text)
        if not is_valid:
            log.error("ERROR: Generated sample failed CSV validation")
            log.error(f"Sample received (first 500 chars):\n {csv_text[:500]}")
            return None

        # Use StringIO and pandas to parse the CSV string
        csv_io = io.StringIO(csv_text)
        df = pd.read_csv(csv_io, quotechar='"', skipinitialspace=True)
        
        # Validate row count (relaxed: warn instead of fail for samples)
        actual_rows = len(df)
        if actual_rows != num_rows:
            log.warning(f"WARNING: Generated {actual_rows} rows, but {num_rows} were requested. Proceeding with sample.")
            if actual_rows == 0:
                log.error("ERROR: Sample contains 0 data rows.")
                return None
            
        # Validate header names
        if any(not isinstance(col, str) or not col.strip() for col in df.columns):
            log.error("ERROR: Invalid or empty column names in header")
            return None
            
        log.info(f"Successfully generated and parsed sample with {len(df.columns)} columns.")
        return df

    except Exception as e:
        # This will catch errors from call_gemini_text (like auth or retry failures)
        log.error(f"Error getting or parsing CSV sample: {e}")
        return None
# --- END UPDATED SAMPLE FUNCTION ---