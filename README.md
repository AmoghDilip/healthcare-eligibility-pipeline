# Healthcare Eligibility Data Pipeline (PySpark)

## Overview
This repository contains a configuration-driven PySpark ETL pipeline designed to ingest, standardize, and unify member eligibility data from multiple healthcare partners. The solution is built to be scalable, allowing new partners to be onboarded with zero code changes.

## Architecture & Design Decisions
- **Cloud-Native Ingestion**: Developed and tested using **AWS Glue (Spark 3.3)** to ensure scalability for large datasets.
- **Configuration-Driven Design**: Partner-specific details (delimiters, column mappings, date formats) are stored in a central `config.json`. This decouples business logic from data ingestion.
- **Robust Data Cleaning**:
    - Standardizes names to Title Case.
    - Normalizes phone numbers to `XXX-XXX-XXXX` format.
    - Converts various partner date formats into a unified ISO-8601 (`YYYY-MM-DD`) format.
- **Defensive Engineering**: Implemented whitespace handling for CSV headers and values to prevent common data-loading errors.

## Project Structure
- `glue_job.py`: The core PySpark ETL logic.
- `config.json`: The mapping and metadata configuration for partners.
- `sample_data/`: (Optional) Local copies of `acme.txt` and `bettercare.csv`.

## How to Run
### 1. Requirements
- AWS Account with S3 and Glue access.
- An IAM Role with `AWSGlueServiceRole` and `AmazonS3ReadOnlyAccess` policies.

### 2. Deployment
1. Upload your data files and `config.json` to an S3 bucket.
2. Create a new AWS Glue Spark job.
3. Paste the contents of `glue_job.py` into the script editor.
4. Update the `BUCKET` variable in the script to point to your S3 bucket.
5. Run the job.

### 3. Adding a New Partner
To add a new partner, simply append a new entry to the `config.json` with their specific file path, delimiter, and column mappings. The pipeline will automatically process the new source during the next run.
