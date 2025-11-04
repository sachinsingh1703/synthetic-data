# AI-Powered Synthetic Database Generator

This is a full-stack data engineering project that uses a Streamlit web UI and the Google Gemini AI API to generate large, multi-table, and relationally-intact synthetic databases. The data generation is orchestrated by Apache Airflow, allowing you to create millions of realistic, linkable data rows from a simple text prompt.



[Image of AI data generator project architecture]


### Features

* **AI-Assisted Schema Design:** Use the "Single Prompt" mode to describe a complex database (e.g., "a 4-table bank database..."), and the AI will design the tables, columns, and relationships for you.
* **Manual Schema Control:** Use the "Multi-Prompt" mode to manually define each table's name, row count, column prompt, and primary key.
* **Guaranteed Referential Integrity:** The AI generates Python code that automatically creates foreign keys (FKs) that match the primary keys (PKs) of other tables, making the data 100% joinable.
* **Custom Primary Keys:** Define custom ID patterns in your prompts (e.g., `CUST-XXXX`, `PROD-10001`), and the generator will create them.
* **Scalable to 1M+ Rows:** By using Apache Airflow as the backend, the system can generate massive "fact" tables in batches, preventing memory errors and ensuring the process is robust.
* **Simple Web UI:** A clean Streamlit interface manages the entire workflow, from prompting to downloading.
* **One-Click Download:** All generated tables are converted to CSV and bundled into a single `.zip` file for easy download.

---

### Tech Stack

* **Frontend:** Streamlit
* **AI / LLM:** Google Gemini API (using `gemini-2.5-pro`)
* **Orchestration:** Apache Airflow (running via Docker)
* **Data Generation:** Python, `Faker`, `pandas`
* **Environment:** Docker & Docker Compose

---

### How to Run This Project

Follow these steps to get the application running on your local machine.

#### Prerequisites

* **Docker** and **Docker Compose:** Must be installed on your system.
* **Google Gemini API Key:** You must have a valid API key. You can get one from Google AI Studio.

#### Step 1: Create Your `.env` File

In the root of the project, create a file named `.env`. This file holds your secret keys and user ID.

1.  Find your local User ID by running this command in your terminal:
    ```bash
    id -u
    ```
    (This will likely return `1000` or `1001`).

2.  Add this content to your `.env` file, replacing `YOUR_ID_HERE` and `YOUR_KEY_HERE`:

    ```env
    # Your local user ID, e.g., 1000
    AIRFLOW_UID=YOUR_ID_HERE
    
    # Your Google Gemini API Key
    GEMINI_API_KEY=YOUR_KEY_HERE
    ```

#### Step 2: Build and Run the Containers

From the project's root directory, run the following command:

```bash
docker compose up -d --build