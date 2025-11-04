#!/usr/bin/env python3
"""
Gemini Worker Script - Handles individual batch data generation
"""

import os
import json
import time
import argparse
import google.generativeai as genai
from typing import Optional, Union, Dict, List
import pandas as pd
import io
import logging
import re
import csv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class GeminiWorker:
    def __init__(self, api_key: Optional[str] = None):
        """Initialize the Gemini worker with API key"""
        self.api_key = api_key or os.environ.get("GEMINI_API_KEY")
        if not self.api_key:
            raise ValueError("GEMINI_API_KEY must be provided either as an argument or environment variable")
        
        # Initialize Gemini API
        genai.configure(api_key=self.api_key)
        self.model = genai.GenerativeModel('gemini-2.5-flash')
        
    def parse_columns_from_user_prompt(self, user_prompt: str) -> tuple[List[str], Dict[str, str]]:
        """Try to extract a list of column names and their types from the user prompt.

        Looks for patterns like 'columns: a (type), b (type), c'. Returns (names, types).
        """
        if not user_prompt:
            return [], {}

        m = re.search(r"columns?\s*:\s*(.+?)(?:\.|$)", user_prompt, flags=re.IGNORECASE)
        if not m:
            return [], {}

        cols_raw = m.group(1)
        cols = []
        types = {}
        
        for part in cols_raw.split(','):
            part = part.strip()
            # Try to extract type information in parentheses
            type_match = re.search(r"(.*?)(?:\s*\((.*?)\))?$", part)
            if type_match:
                name = type_match.group(1).strip()
                col_type = type_match.group(2).strip() if type_match.group(2) else "string"
                
                # Clean the name
                name = re.sub(r"\s+", "_", name)
                name = name.strip(' "\'')
                
                if name:
                    cols.append(name)
                    types[name] = col_type.lower()
        
        return cols, types

    def generate_system_prompt(self, user_prompt: str, row_count: int, output_format: str, columns: List[str] | None = None, column_types: Dict[str, str] | None = None) -> str:
        """Construct a deterministic system prompt which enforces exact columns and their data types.

        The worker will generate only raw data rows (no header). The DAG will write the header once when combining.
        """
        # Build detailed column specifications with types and examples
        column_specs = []
        if columns and column_types:
            for col in columns:
                col_type = column_types.get(col, "string").lower()
                example = ""
                if "uuid" in col_type:
                    example = '"550e8400-e29b-41d4-a716-446655440000"'
                elif "email" in col_type:
                    example = '"user@example.com"'
                elif "name" in col_type:
                    example = '"John"'
                elif "number" in col_type or "int" in col_type:
                    example = "42"
                elif "date" in col_type:
                    example = '"2025-10-28"'
                else:
                    example = '"text"'
                    
                column_specs.append(f"- {col} ({col_type}): {example}")

        col_instruction = ""
        if column_specs:
            col_instruction = "COLUMN SPECIFICATIONS (maintain this exact order and format):\n" + "\n".join(column_specs) + "\n"
        elif columns:
            col_instruction = f"Use these columns in this exact order: {', '.join(columns)}.\n"

        return f"""You are a deterministic CSV data generator. Generate exactly {row_count} rows of consistent, realistic data following these specifications exactly.

USER REQUEST:
{user_prompt}

{col_instruction}
STRICT REQUIREMENTS:
1. Generate EXACTLY {row_count} rows of data
2. Each row MUST have exactly {len(columns) if columns else 'N'} columns in the specified order
3. Keep data consistent and realistic across ALL rows
4. Maintain the same data type and format for each column across all rows
5. Return ONLY raw CSV rows (no headers, no markdown, no explanations)
6. Quote text fields with double quotes, leave numbers unquoted

FORMAT RULES:
- Separate columns with commas
- Text/string values: Use double quotes (e.g., "John Doe")
- Numbers: No quotes, just the number (e.g., 42, 12.5)
- Dates: Use consistent ISO format with quotes (e.g., "2025-10-28")
- UUIDs: Use proper UUID format with quotes
- Email: Use realistic email format with quotes

CRITICAL: Data across rows must be consistent - maintain same column order, types, and formats for ALL rows.

Example format (3 columns):
"abc123",42,"john@email.com"
"def456",17,"mary@email.com"

RESPONSE FORMAT: Raw CSV rows only, one per line."""

    def validate_csv_data(self, data: str, expected_num_columns: int | None = None, column_types: Dict[str, str] | None = None) -> tuple[bool, str]:
        """Validate CSV data rows using csv.reader to ensure consistent columns and data types.

        Returns (True, "") on success or (False, reason).
        """
        try:
            if not data or not data.strip():
                return False, "Generated CSV is empty"

            f = io.StringIO(data.strip())
            reader = csv.reader(f, delimiter=',', quotechar='"')
            rows = list(reader)
            if not rows:
                return False, "No rows parsed from CSV"

            # Determine column count from first row
            first_count = len(rows[0])
            if expected_num_columns and first_count != expected_num_columns:
                return False, f"Expected {expected_num_columns} columns but first row has {first_count}"

            # Validate each row's structure and content
            for i, row in enumerate(rows):
                # Check column count
                if len(row) != first_count:
                    return False, f"Row {i+1} has {len(row)} columns, expected {first_count}"
                
                # If we have type information, validate content
                if column_types and len(column_types) == first_count:
                    for col_idx, (value, (col_name, col_type)) in enumerate(zip(row, column_types.items())):
                        value = value.strip('"')  # Remove quotes for checking
                        
                        try:
                            # Type-specific validation
                            if "uuid" in col_type.lower():
                                if not re.match(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', value.lower()):
                                    return False, f"Row {i+1}, column {col_name}: Invalid UUID format: {value}"
                            
                            elif "email" in col_type.lower():
                                if not re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', value):
                                    return False, f"Row {i+1}, column {col_name}: Invalid email format: {value}"
                            
                            elif "number" in col_type.lower() or "int" in col_type.lower():
                                try:
                                    float(value)  # This handles both integers and floats
                                except ValueError:
                                    return False, f"Row {i+1}, column {col_name}: Expected number, got: {value}"
                            
                            elif "date" in col_type.lower():
                                if not re.match(r'^\d{4}-\d{2}-\d{2}$', value):
                                    return False, f"Row {i+1}, column {col_name}: Invalid date format (expected YYYY-MM-DD): {value}"

                        except Exception as e:
                            return False, f"Row {i+1}, column {col_name}: Validation error: {str(e)}"

            return True, ""
            
        except csv.Error as e:
            return False, f"CSV parsing error: {str(e)}"
        except Exception as e:
            return False, f"Unexpected CSV validation error: {str(e)}"

    def validate_json_data(self, data: str) -> tuple[bool, str]:
        """Validate JSON data format and consistency"""
        try:
            parsed = json.loads(data)
            if not isinstance(parsed, list):
                return False, "JSON data must be a list of objects"
            if not parsed:
                return False, "Generated JSON is empty"
            # Check consistency of keys across all objects
            keys = set(parsed[0].keys())
            for item in parsed[1:]:
                if set(item.keys()) != keys:
                    return False, "Inconsistent keys across JSON objects"
            return True, ""
        except json.JSONDecodeError as e:
            return False, f"JSON validation failed: {str(e)}"

    def generate_data(
        self,
        user_prompt: str,
        output_format: str,
        row_count: int,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        temperature: float = 0.1  # Low temperature for more consistent output
    ) -> Optional[str]:
        """
        Generate synthetic data using Gemini API with retry logic
        """
        # Try to derive columns from the user prompt and include them in the system prompt
        columns, column_types = self.parse_columns_from_user_prompt(user_prompt)
        prompt = self.generate_system_prompt(user_prompt, row_count, output_format, columns, column_types)

        for attempt in range(max_retries):
            try:
                # Set generation parameters for consistency
                response = self.model.generate_content(
                    prompt,
                    generation_config={
                        'temperature': temperature,
                        'top_p': 0.95,
                        'top_k': 40,
                    }
                )

                try:
                      generated_data = response.text.strip()
                except Exception as e:
                      logger.error(f"Failed to access response.text: {e}. Response may be empty or blocked.")
                      logger.error(f"Full response: {response}")
                      raise ValueError(f"Failed to get text from Gemini API: {e}")

                if not generated_data:
                       raise ValueError("Empty response from Gemini API")
                       
                  # Clean the response
                if generated_data.startswith('```'):
                      # strip markdown fences if model returned them
                      generated_data = '\n'.join(generated_data.split('\n')[1:-1])

                # Validate based on format (for CSV ensure expected column count if known)
                expected_cols = len(columns) if columns else None
                is_valid, error_msg = (
                    self.validate_csv_data(generated_data, expected_num_columns=expected_cols, column_types=column_types)
                    if output_format.lower() == 'csv'
                    else self.validate_json_data(generated_data)
                )

                if not is_valid:
                    raise ValueError(f"Validation failed: {error_msg}")

                return generated_data

            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed: {str(e)}")
                if attempt < max_retries - 1:
                    sleep_time = retry_delay * (2 ** attempt)  # Exponential backoff
                    logger.info(f"Retrying in {sleep_time} seconds...")
                    time.sleep(sleep_time)
                else:
                    logger.error(f"All attempts failed: {str(e)}")
                    raise

    def generate_and_save(
        self,
        user_prompt: str,
        output_format: str,
        row_count: int,
        output_path: str,
        columns: List[str] | None = None
    ) -> None:
        """Generate data and save to file"""
        data = self.generate_data(user_prompt, output_format, row_count)
        if data:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            with open(output_path, 'w', encoding='utf-8') as f:
                # Write data rows exactly as returned (no header). Ensure newline at end.
                f.write(data.rstrip() + "\n")
            logger.info(f"Successfully generated and saved data to {output_path}")
        else:
            raise RuntimeError("Failed to generate data")

def main():
    """Command-line interface for the worker script"""
    parser = argparse.ArgumentParser(description='Generate synthetic data using Gemini API')
    parser.add_argument('--prompt', required=True, help='User prompt describing the data to generate')
    parser.add_argument('--format', choices=['csv', 'json'], default='csv', help='Output format')
    parser.add_argument('--rows', type=int, required=True, help='Number of rows to generate')
    parser.add_argument('--output', required=True, help='Output file path')
    parser.add_argument('--api-key', help='Gemini API key (optional, can use GEMINI_API_KEY env var)')
    
    args = parser.parse_args()
    
    try:
        worker = GeminiWorker(api_key=args.api_key)
        worker.generate_and_save(args.prompt, args.format, args.rows, args.output)
        logger.info("Data generation completed successfully")
    except Exception as e:
        logger.error(f"Data generation failed: {str(e)}")
        raise

if __name__ == '__main__':
    main()