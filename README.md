
# UK Legislation ETL Pipeline

This project is an ETL pipeline to extract, transform, and load UK legislation data from legislation.gov.uk. It leverages web scraping, text cleaning, embedding generation, and stores data in both SQL and vector databases for efficient querying and retrieval.

## Project Structure

```
uk-legislation-pipeline/
├── Dockerfile                  # Single Dockerfile for the entire solution
├── docker-compose.yml          # Optional: For local development/testing
├── src/
│   ├── extractors/
│   │   ├── __init__.py
│   │   └── legislation_scraper.py  # Web scraper for legislation.gov.uk
│   ├── text_transformers/
│   │   ├── __init__.py
│   │   ├── cleaner.py          # Data cleaning logic
│   │   └── embeddings.py       # Generate embeddings with MiniLM-L6-v2
│   ├── loaders/
│   │   ├── __init__.py
│   │   ├── sql_loader.py       # SQL database operations
│   │   └── vector_loader.py    # Vector database operations
│   ├── utils/
│   │   ├── __init__.py
│   │   ├── config.py           # Configuration management
│   │   ├── logging.py          # Logging setup
│   │   └── checkpoint.py       # Checkpointing for recovery
│   ├── databases/
│   │   ├── __init__.py
│   │   ├── sql_init.py         # SQL database initialization
│   │   └── vector_init.py      # Vector database initialization
│   ├── main.py                 # Main ETL pipeline orchestration
│   └── query.py                # CLI for querying the data
├── scripts/
│   ├── entrypoint.sh           # Docker entrypoint script
│   └── wait-for-it.sh          # Script to wait for services to be ready
├── requirements.txt            # Python dependencies
└── README.md                   # Documentation
```

## Prerequisites

- Docker and Docker Compose installed (for containerized deployment)
- Python 3.10+ (if running locally without Docker)
- Access to a SQL database (PostgreSQL recommended)
- Access to a vector database (Qdrant recommended)

## Setup and Installation

### Using Docker (Recommended)

1. Build the Docker image:

```bash
docker build -t uk-legislation-etl .
```

2. Run the containers:

```bash
docker-compose up
```

This will start all services including the ETL pipeline, SQL database, and vector database.

### Running Locally Without Docker

1. Create a virtual environment and activate it:

```bash
python3 -m venv venv
source venv/bin/activate
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Configure environment variables or edit `src/utils/config.py` with your database connection details.

4. Initialize databases (run the initialization scripts):

```bash
python src/databases/sql_init.py
python src/databases/vector_init.py
```

5. Run the ETL pipeline:

```bash
python src/main.py
```

## Configuration

- Database credentials and API keys can be set in environment variables or inside `src/utils/config.py`.
- Adjust scraping parameters or embedding model settings inside respective modules under `src/`.

## Usage

### Running the ETL Pipeline

```bash
python src/main.py
```

The pipeline will:
- Extract legislation data via web scraping
- Clean and transform the text data
- Generate vector embeddings
- Load data into the SQL and vector databases

### Querying the Data

Use the CLI tool to search and retrieve legislation info:

```bash
python src/query.py "your search query"
```

## Logging and Checkpointing

- Logs are configured in `src/utils/logging.py`.
- Pipeline checkpoints for recovery are managed in `src/utils/checkpoint.py`.

## Troubleshooting

- Ensure all dependent services (SQL and vector DB) are running before pipeline execution.
- Check logs for errors and warnings.
- Validate your configuration parameters carefully.

## Contributing

Feel free to fork the project, open issues, or submit pull requests.

## License

This project is licensed under the MIT License.

---

*Created by the tycoach*
