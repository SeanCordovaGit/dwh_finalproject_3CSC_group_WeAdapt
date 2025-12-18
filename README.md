# WeAdapt Data Warehouse Project

This is a capstone data engineering project for the WeAdapt group, focusing on building an ETL pipeline for a data warehouse.

## Overview

The project implements a comprehensive data warehouse solution using:
- Apache Airflow for orchestration
- PostgreSQL for data storage
- Docker for containerization
- Python for data processing

## Architecture

The system processes data from various departments including business, customer management, enterprise, marketing, and operations.

## Setup

1. Ensure Docker is installed
2. Run `docker-compose up` from the infra/ directory
3. Access Airflow UI at localhost:8080

## Project Structure

- `data_sources/`: Raw data files from various departments
- `etl_scripts/`: Python scripts for data processing and utilities
- `infra/`: Infrastructure configuration (Docker, requirements)
- `workflows/`: Airflow DAGs and workflows
- `presentation/`: Presentation materials and reports
- `sql/`: SQL scripts for database operations
- `test/`: Unit tests

## Usage

Trigger the main pipeline DAG `weadapt_dwh_etl_pipeline_v3_fixed` to process data end-to-end.
