# End-to-End Databricks Lakehouse on AWS

## Overview
This project demonstrates a production-style Databricks Lakehouse architecture on AWS using Delta Lake, Auto Loader, and idempotent MERGE patterns.

## Architecture
S3 → Auto Loader → Bronze → Silver → Gold

## Bronze Layer
- Raw ingestion using Databricks Auto Loader
- Schema inference
- Ingest timestamp added
- Append-only Delta table

## Silver Layer
- Row-level deduplication using ROW_NUMBER()
- Latest record wins based on ingest_ts
- Idempotent MERGE into Silver Delta table
- Handles late-arriving data

## Gold Layer
- Business-level aggregations
- Multiple grains (city, category, region)
- Generic MERGE framework for idempotency
- No duplicates on rerun

## Incremental Processing
- Silver uses ingest_ts for deduplication
- Gold uses MERGE on business keys
- Pipelines are safe to rerun

## Cost Considerations
- Auto Loader avoids reprocessing files
- Delta MERGE minimizes full rewrites
- Gold tables reduce scan cost for BI