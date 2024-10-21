# Simple ETL with Airflow
---

## NYC Taxi Trips ETL Pipeline

This project implements a simple ETL (Extract, Transform, Load) process using Apache Airflow to process NYC Taxi Trips data. The data is sourced from the NYC Open Data API and covers trips from January 2021 to December 2021.

## Project Overview

The ETL pipeline performs the following tasks:
1. **Extract**: Fetches raw trip data from the NYC Open Data API.
2. **Transform**: Aggregates the data to create daily and monthly statistics.
3. **Load**: Stores the transformed data into three tables:
   - `daily_trips`: Contains daily aggregated statistics of all taxi trips.
   - `monthly_trips`: Contains monthly aggregated statistics of all taxi trips.
   - `raw_trip_data`: Contains the raw data as fetched from the API.

## Data Source

The data is sourced from the NYC Open Data API:
- NYC Taxi Trips Data

## Pipeline Details

### DAG Configuration

- **Schedule**: The DAG is configured to run daily.
- **Tasks**:
  1. **Fetch Data**: Downloads the raw trip data from the API.
  2. **Transform Data**: Processes the raw data to generate daily and monthly statistics.
  3. **Load Data**: Loads the transformed data into the respective tables.

### Data Transformation

- **Daily Aggregation**: Calculates daily statistics such as total trips, total distance, and total fare.
- **Monthly Aggregation**: Calculates monthly statistics similar to the daily aggregation but on a monthly basis.
