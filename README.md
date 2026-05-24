# US Airline Reliability Pipeline

An end-to-end batch data pipeline that processes 6 years of US airline 
on-time performance data to identify and rank the most reliable airlines 
annually using a composite scoring model.

## Architecture

```
GCS (Raw CSV)
      ↓
Dataproc (PySpark)
— Data quality checks
— Aggregations and KPI calculations
— Min-max normalization
— Composite reliability scoring
— Window-based annual ranking
      ↓
GCS (Processed Parquet, partitioned by year)
      ↓
BigQuery (Top 3 airlines per year)
```

## Dataset

- **Source:** US Bureau of Transportation Statistics (BTS)
- **Size:** 135,000+ records
- **Period:** 2020–2025
- **Coverage:** 26 airlines, 424 airports

## Composite Reliability Score

Each airline is scored annually combining three carrier-specific metrics:

- **Carrier Reliability Rate (50%)** — percentage of flights with no carrier-caused delay
- **Carrier Caused Delay (30%)** — proportion of delays attributed to the airline
- **Average Delay Severity (20%)** — average carrier delay minutes per delayed flight, normalized using min-max scaling

## Tech Stack

- **Processing:** Apache Spark (PySpark) on Google Cloud Dataproc
- **Storage:** Google Cloud Storage (Parquet, partitioned by year)
- **Serving:** Google BigQuery
- **Language:** Python

## Setup

1. Upload dataset to GCS
2. Upload `aviation_pipeline.py` to GCS
3. Create a Dataproc single-node cluster
4. Submit PySpark job pointing to the script in GCS
5. Output writes to GCS (Parquet) and BigQuery automatically

## Output

**GCS:** Processed metrics in Parquet format partitioned by year

**BigQuery:** Top 3 most reliable airlines per year with composite scores
