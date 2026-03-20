# 📊 Stock Data Pipeline (Python + Snowflake)

## 🔹 Overview
This project implements an end-to-end data engineering pipeline for processing large-scale stock market data. The pipeline ingests raw CSV files, consolidates them into a unified dataset, and loads the data into Snowflake for cleaning, validation, and analytics.

The architecture follows a layered approach: **RAW → CLEAN → ANALYTICS**, similar to real-world data warehouse design.

---

## 🔹 Dataset

The dataset consists of historical stock price data for multiple companies.

- Source: Publicly available stock market datasets  
- Format: Multiple CSV files (one per stock ticker)  
- Key columns:
  - `date`
  - `open`
  - `high`
  - `low`
  - `close`
  - `volume`

Additionally, a metadata file (`symbols_valid_meta.csv`) is used to filter valid stock symbols and exclude ETFs.

> ⚠️ **Note:** The dataset is too large to upload to GitHub.  
> This repository includes only the pipeline code, which is designed to process externally available stock market data.

---

## 🔹 Pipeline Workflow

1. **Data Ingestion**
   - Reads metadata and multiple stock CSV files  
   - Filters valid stock symbols (non-ETF)  
   - Combines data into a single large dataset  

2. **Data Loading (Snowflake)**
   - Creates database and schemas  
   - Uploads data to Snowflake stage  
   - Loads into RAW table  

3. **Data Transformation**
   - Removes duplicates using window functions  
   - Calculates daily returns  
   - Cleans and standardizes dataset  

4. **Data Validation**
   - Row count checks (source vs target)  
   - Null value checks  
   - Duplicate detection  

5. **Analytics Layer**
   - Moving averages (MA20, MA50)  
   - Volatility calculation  
   - KPI table for analysis  

---

## 🔹 Tech Stack

- **Python** (Pandas, dotenv)  
- **Snowflake**  
- **SQL** (Window Functions, Aggregations)  
- **ETL Pipeline Design**  

---

## 🔹 Project Structure

```
stock-data-pipeline/
│
├── scripts/
│   ├── 01_build_big_csv.py
│   ├── 02_load_to_snowflake.py
│   ├── 03_transform_validate_analytics.py
│
├── README.md
```

---

## 🔹 How to Run

1. Clone the repository  
2. Set up environment variables in a `.env` file:

```
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_WAREHOUSE=your_warehouse
```

3. Run the pipeline:

```
python scripts/01_build_big_csv.py
python scripts/02_load_to_snowflake.py
python scripts/03_transform_validate_analytics.py
```

---

## 🔹 Key Features

- End-to-end ETL pipeline  
- Multi-layer data architecture (RAW, CLEAN, ANALYTICS)  
- Automated data validation framework  
- Scalable design for large datasets  
- Advanced SQL analytics using window functions  

---

## 🔹 Sample Transformations

- Deduplication using `ROW_NUMBER()`  
- Daily return calculation using `LAG()`  
- Moving averages using window functions  
- Rolling volatility using `STDDEV`  

---

## 🔹 Future Improvements

- Add orchestration using Airflow  
- Integrate with cloud storage (AWS S3)  
- Build dashboard (Streamlit / Tableau)  
- Automate pipeline scheduling  

---

## 🔹 Author

**Leena Rajesh Patil**  
MS Data Science, DePaul University
