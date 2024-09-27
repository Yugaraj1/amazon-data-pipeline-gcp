
# End-to-End Data Pipeline for Amazon Data: Leveraging Airflow for Daily Quality Checks

This project automates an ETL pipeline for Amazon data using GCP services. The pipeline uploads CSV files to Google Cloud Storage, cleans and transforms data with Dataprep, performs dimensional modeling and data quality checks in a BigQuery staging table, and moves validated data to a production table. Airflow automates daily tasks, ensuring ongoing data quality.



## Architecture Diagram


## Data Quality Log Tabe


## Workflow

1. Upload Data: Amazon CSV is stored in Google Cloud Storage.
2. Data Cleaning: Dataprep is used to clean and transform the data.
3. Staging in BigQuery: Data is loaded into a staging area where dimensional modeling and SQL quality checks are applied.
4. Production:  Clean, modeled data is moved to the production table in BigQuery.
5. Automation: Airflow schedules daily runs for ingestion, validation, and pushing data to production.
## Tech

1. Google Cloud Storage (GCS): Data storage.
2. Dataprep: Data cleaning and transformation.
3. BigQuery: Staging (with dimensional modeling) and production data.
4. Airflow: Orchestrating the pipeline.
## Future

1. Build Insight Dashboard: Create a dashboard to visualize key insights from the processed Amazon data.
2. Data Quality Dashboard: Develop a dashboard to monitor data quality metrics and trends over time.
3. Expand Quality Checks: Implement additional data quality checks for deeper validation.
4. Automate the Entire Pipeline: Fully automate data ingestion, transformation, validation, and reporting using Airflow.

