# Start from the official Airflow image
FROM apache/airflow:2.9.2

# Copy our new requirements file into the container
COPY requirements.txt /requirements.txt

# Install the packages as the default 'airflow' user
# This is the fix for the "pip as root" error
RUN pip install --no-cache-dir -r /requirements.txt