# LinkedIn Job Scraper & ETL Pipeline

A Python-based ETL pipeline that scrapes LinkedIn job postings using Bright Data's API, cleans the data with Pandas, and loads it into a local SQLite database.

## Overview

This project demonstrates an end-to-end Extract, Transform, Load (ETL) process:
- **Extract:** Fetches job market data (e.g., Data Analyst, Business Analyst) for specific cities using the Bright Data scraper API and the `requests` library.
- **Transform:** Cleans and formats the raw data into a structured format using `pandas`.
- **Load:** Injects the cleaned data into a local `SQLite` database using Python's database cursor methods.

Building this pipeline emphasized core software engineering principles such as separation of concerns, robust API error handling, and Pythonic database interactions.

The project also has a small SQLite database that has over 1000 jobs as an example of what the project can do.

## Prerequisites

- Python 3.12+ (or preferred version)
- [uv](https://github.com/astral-sh/uv) - An extremely fast Python package and project manager.

## Installation

This project uses `uv` for fast, reproducible dependency resolution and virtual environment synchronization.

1. Clone the repository:
   ```bash
   git clone https://github.com/VelocityREST/linkedin-etl-pipeline.git
   cd linkedin-etl-pipeline
   ```

2. Sync the dependencies and set up the virtual environment:
   ```bash
   uv sync
   ```
   *This command will automatically create a virtual environment (if one doesn't exist) and install all required packages from the project lockfile.*

## Usage

Once your environment is synchronized, you can run the pipeline using `uv run`:

```bash
uv run marimo edit scraping_logic.py
```
*(Note: You will need to configure your Bright Data API credentials in the environment variables before running.)*

## Technologies Used
- Python
- Marimo
- Pandas
- SQLite
- Bright Data API
- requests
