# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file first and install dependencies
COPY ./streamlit/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY ./streamlit/app.py .
COPY ./dags /app/dags

# Make port 8501 available to the outside world
EXPOSE 8501

# Define environment variable to find the utility file
ENV PYTHONPATH=/app/dags

# Run streamlit when the container launches
CMD ["streamlit", "run", "app.py"]