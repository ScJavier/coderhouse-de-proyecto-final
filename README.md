# Coderhouse Data Engineering Final Project

This repository contains the code for the final project of the "Data Engineering" course at Coderhouse.com.

The project implements a simple ETL (Extract, Transform, Load) pipeline orchestrated with Airflow, running with docker-compose.

## Overview

The ETL pipeline retrieves exchange rates from the Frankfurter API for a given date and a specified target currency (by default, MXN - Mexican pesos). The data is then cleaned, validated, and loaded into an AWS Redshift database.

If the ETL process succeeds, an email notification is sent using the SendGrid API.

## Content

- `.env_template`: This file defines environment variables for the credentials required to connect to the AWS Redshift database and the SendGrid API. To run the project, you must fill in this file with your own credentials and rename it to `.env`.

- `Dockerfile`: This file contains instructions to build the Docker image from the `apache/airflow:2.9.0` base image. It installs necessary packages and requirements, and sets Python environment variables.

- `docker-compose.yaml`: Based on the official template to run `airflow` with `docker-compose`, with minor modifications to accommodate the custom Dockerfile and set environment variables from `.env`.

- `requirements.txt`: This file lists the Python packages required for the project. These packages are necessary for the correct functioning of the project.

- `dags/`: Contains the code to define the Directed Acyclic Graph (DAG) to be executed with Airflow, including:

    - `dag_etl_exchange_rates.py`: Defines the DAG.

    - `tasks_etl_exchange_rates.py`: Contains the functions to be executed in the DAG.

    - `utils/`: Contains auxiliary functions used in the pipeline tasks, divided into submodules based on the ETL steps (`extraction.py`, `load_to_db.py`, `validation.py`). `config.py` imports environment variables and makes them available for importing into the pipeline code as necessary.

## Instructions

To run this project, clone this repository to your local machine. Ensure that Docker Desktop is running on your computer and that you have installed the latest version of docker-compose.

For the initial setup, execute the following command in the terminal:

```bash
docker-compose up --build
```
For subsequent runs, omit the `--build` flag.

Once the containers are up and running, you can access the Airflow UI in your browser by navigating to `localhost:8080`.
