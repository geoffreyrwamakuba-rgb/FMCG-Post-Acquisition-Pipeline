# FMCG - Post-Acquisition Pipeline

## Executive Summary

This project builds an end-to-end data pipeline in Databricks to solve a post-acquisition data integration problem. It combines structured enterprise data with messy startup data into a single, reliable analytics layer using a Medallion Architecture (Bronze, Silver, Gold).

The pipeline supports both historical backfill and incremental updates, allowing consistent reporting across both companies through one unified dataset.

## Project Overview

![ ](https://github.com/geoffreyrwamakuba-rgb/FMCG-Post-Acquisition-Pipeline/blob/f9250d78b0fb23643e6cdae20ecbf7f1eaa54efb/FMCG_Data_Flow.png)

### 🏢 Business Scenario
A global FMCG enterprise (**Atlikon**) acquired a nutrition startup (**SportsBar**).

- **Atlikon** operates on a modern **Databricks Lakehouse**
- **SportsBar** delivers raw **CSV files in AWS S3**
- No unified or trusted view of **global revenue**

### ❌ The Problem
- Disparate data platforms  
- Dirty, inconsistent schemas  
- No incremental processing  
- No enterprise-level governance  

### ✅ The Solution
Designed and implemented a **scalable, auditable, incremental ELT pipeline** that:
- Ingests startup data from S3
- Cleans and standardizes it
- Merges it into the enterprise **Gold layer**
- Produces a **single source of truth** for analytics


## Key Features / Skills Demonstrated

### Incremental Loading
- Implemented **Staging Table Pattern**
- Processes **only new daily files**
- Recomputes monthly aggregates to handle **late-arriving data**

### Data Quality & Schema Handling
- Schema mismatches handled
- City typos corrected
- Implemented **Dictionary Mapping + Regex Cleaning**

### Orchestration
- Automated Databricks **DAGs**
- Dependency-driven execution:
  - Dimensions → Facts
- Email/Slack alerting on failures

### Auditability & Lineage
- Metadata columns added:
  - `ingestion_timestamp`
  - `file source + name`
- Enables full **data lineage & debugging**

---

## 📂 Repository Structure

```bash
├── 01_setup/
│   ├── 01_setup_catalog.sql              # Unity Catalog schemas & tables
│   ├── 01_dim_date.py                    # Programmatic Date Dimension
│   └── 03_utilities.py                   # Centralized configuration
│
├── 02_dimension_processing/
│   ├── 1_customer_data_processing.py     # Customer cleaning & ID mapping
│   ├── 2_product_data_processing.py      # Regex cleanup & hashing
│   └── 3_pricing_data_processing.py      # Dirty dates & price versioning
│
├── 03_fact_processing/
│   ├── 01_full_load_fact.py              # Historical load
│   └── 02_incremental_load_fact.py       # Daily incremental upsert logic
│
├── 04_denormalized_view/
│   ├── 01_denormalized_table_query.txt   # Combined columns
│   └── 02_parent_incremental_load.txt    # Incremental load of parent data

└── README.md
