# Synthetic Data Generation Platform Setup Guide

This guide will help you set up and run the Synthetic Data Generation Platform, which uses Google's Gemini API, Apache Airflow, and Streamlit to generate large-scale synthetic datasets.

## Prerequisites

- Docker and Docker Compose
- Python 3.9+
- Google Cloud Account with Gemini API access

## Environment Setup

1. Clone the repository:
```bash
git clone <repository-url>
cd airflow-gemini-project
```

2. Create a `.env` file in the project root:
```bash
# Required Environment Variables
GEMINI_API_KEY=your_gemini_api_key_here
AIRFLOW_UID=50000  # This should match your host system's UID

# Airflow Configuration
AIRFLOW_USER=airflow
AIRFLOW_PASS=your_secure_password_here
```

3. Build and start the containers:
```bash
docker-compose up -d --build
```

This will start:
- Airflow webserver at http://localhost:8080
- Streamlit UI at http://localhost:8501
- PostgreSQL database for Airflow

## First-Time Setup

1. Wait for all containers to start (check with `docker-compose ps`)

2. Access the Airflow web UI:
   - URL: http://localhost:8080
   - Username: airflow
   - Password: (the one you set in .env)

3. Access the Streamlit UI:
   - URL: http://localhost:8501

## Using the Platform

1. Through the Streamlit UI:
   - Enter your data description prompt
   - Choose output format (CSV/JSON)
   - Set total rows and batch size
   - Click "Generate Data"

2. Monitor progress:
   - Watch the progress bar in Streamlit
   - Check detailed task status in Airflow UI
   - Download data when generation completes

## Project Structure

```
airflow-gemini-project/
├── airflow/
│   ├── Dockerfile
│   └── requirements.txt
├── dags/
│   ├── gemini_data_generation_dag.py
│   └── scripts/
│       └── gemini_worker.py
├── streamlit/
│   ├── app.py
│   ├── Dockerfile
│   └── requirements.txt
├── docker-compose.yml
└── requirements.txt
```

## Troubleshooting

1. If containers fail to start:
   - Check Docker logs: `docker-compose logs -f`
   - Ensure all required environment variables are set
   - Verify port availability (8080 and 8501)

2. If data generation fails:
   - Check Airflow task logs in the UI
   - Verify Gemini API key is valid
   - Check network connectivity to Gemini API

3. Common issues:
   - Permission errors: Ensure AIRFLOW_UID matches your system
   - Memory issues: Adjust Docker resource limits
   - API rate limits: Adjust batch size and concurrency

## Scaling Considerations

- Adjust `batch_size` based on your Gemini API quotas
- Modify Airflow's parallelism settings in docker-compose.yml
- Consider implementing a queue system for large requests
- Monitor memory usage when processing large datasets

## Security Notes

- Never commit the `.env` file
- Regularly rotate API keys and passwords
- Use secure passwords for Airflow access
- Monitor API usage and costs

## Contributing

1. Fork the repository
2. Create a feature branch
3. Submit a pull request

## Support

For issues and questions:
- Create a GitHub issue
- Check existing documentation
- Review Airflow/Streamlit logs

## License

[Your License Here]