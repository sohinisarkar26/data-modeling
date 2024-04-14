# Modeling Kaggel crypto dataset

## Project Overview

This project focuses on data modeling with the Kaggle Crypto Dataset, utilizing complex data types like arrays and structs within PostgreSQL hosted on AWS RDS. The goal is to build a cumulative table that incrementally stores cryptocurrency data over time such that it can scale with increasing data volume. Apache Airflow is used to automate and manage the backfill process, ensuring the database is incrementally updated with historical and new data.

## Dataset
The dataset is sourced from Kaggle's Crypto Dataset, which includes historical price data for various cryptocurrencies. It covers daily open, high, low, close prices, market capitalization, and trading volume

## Prerequisites
- Python 3.x
- AWS account (for RDS, S3 and IAM)

## Installing Dependencies

```pip install -r requirements.txt```

## Database Setup
### PostgreSQL on AWS RDS
- Instance Creation: Set up a PostgreSQL instance on AWS RDS, ensuring it's configured with adequate storage and performance settings for the project's needs.
- Security: Configure security groups and IAM roles to securely access the RDS instance from your environment and Airflow.

## Schema Design
The database schema utilizes PostgreSQL's support for complex data types, specifically arrays and structs (composite types in PostgreSQL), to efficiently model the cryptocurrency data.

```
create type crypto_stats as (
  date date,
  open double precision,
  high double precision,
  low double precision,
  close double precision,
	marketCap double precision,
  daily_price_range double precision,
  daily_price_change double precision,
  daily_price_change_pct numeric,
  price_direction price_direction_enum
);

CREATE TYPE price_direction_enum AS ENUM ('+ve','-ve');

create table crypto_struct (
crypto_name varchar(100) NOT NULL,
date date NOT NULL,
stats crypto_stats[]
);
```
Source Kaggel data -
![](images/kaggel_crypto_source.png)

Cumulative model table design -
![](images/kaggel_crypto_cumulative_design.png)

More details on backfill script in `postgres_backfill_dag.py` and sql queries executed on postgres AWS RDS is in `kaggel_crypto_postgres_queries.sql`


## Usage
- **Starting the Backfill:** Trigger the postgres_backfill_1 DAG from the Airflow UI or CLI.

![](images/airflow_dag.png)

- **Monitoring:** Use Airflow's UI to monitor the DAG's execution and troubleshoot any issues.
