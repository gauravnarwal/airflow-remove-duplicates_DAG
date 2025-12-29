## This repository contains an Apache Airflow DAG that:
- Reads a CSV file from an input directory
- Normalizes text data
- Removes duplicate values
- Writes the cleaned data to a processed directory

## Workflow Steps
1. Read CSV file from `/opt/airflow/data/input`
2. Normalize text (lowercase & trim spaces)
3. Remove duplicate values
4. Save cleaned data to `/opt/airflow/data/processed`
5. Remove original file
