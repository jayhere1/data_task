

# JSON Data Processor

![CI/CD Pipeline](https://github.com/jayhere1/data_task/actions/workflows/ci-cd-pipeline.yml/badge.svg?branch=main)


## Overview
This project processes JSON files containing medical data about patients and encounters, extracts relevant information, and stores it in a SQLite database. The script is implemented in Python and runs within a Docker environment, leveraging asynchronous operations for enhanced performance.

## Prerequisites
- Docker
- Docker Compose
- Git (optional, for cloning the repository)

## Project Structure
- `emis_task/`: Main directory for the Python scripts and JSON data.
  - `main.py`: Main script for processing JSON files.
  - `tests/`: Contains pytest test files.
  - `exa-data-eng-assessment/data/`: Directory holding JSON data files.
- `pyproject.toml` & `poetry.lock`: Python project and dependency management files.
- `Dockerfile`: Configuration for building the Docker image.
- `docker-compose.yml`: Configuration for orchestrating the Docker container.

## Setup and Installation

1. **Clone the Repository** (optional if you have the files locally):
   ```bash
   git clone https://github.com/jayhere1/data_task.git
   cd data_task
   ```

2. **Build the Docker Image**:
   Navigate to the project directory and build the Docker image:
   ```bash
   docker-compose build
   ```

## Running the Application
To start the application using Docker Compose:
```bash
docker-compose up json_processor
```

This command starts the Docker container `json_processor`, which processes the JSON files located in the `./emis_task/exa-data-eng-assessment/data/` directory and writes the output to the SQLite database.

## Running Tests
To run the tests within the Docker environment:
```bash
docker-compose run --rm test_service
```
This command runs the test suite specified in the `tests/` directory, ensuring your processing logic is functioning as expected.

## Accessing Data
The SQLite database is stored in a volume that persists data even after the container is stopped. To access or manage the SQLite database:
- Use any SQLite client to connect to the database at `./sqlite_db/processed_data.db`.
- example: alexcvzz.vscode-sqlite on VS code
## Logs
To view the logs generated by the Docker container, execute:
```bash
docker-compose logs
```

## Stopping the Application
To stop and remove the containers, networks, and volumes created by `docker-compose up`, use:
```bash
docker-compose down -v
```

## Additional Notes
- Adjust the volume mounts in the `docker-compose.yml` if you need different directories for JSON data and the database.
- Ensure the Docker environment is appropriately configured for your system in terms of memory and disk space, especially if running for extended periods.
