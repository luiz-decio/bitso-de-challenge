# Bitso Data Engineer Challenges

# Challenge 1

## Project Overview

This project is designed to monitor the bid-ask spread from the Bitso order books `MXN_BTC` and `USD_MXN`. It retrieves order book data every second, calculates the spread, and stores observations in a data lake partitioned by time.

## Rationale

### Data Collection

The Bitso API is used to fetch order book data every second. The bid and ask prices are extracted to calculate the spread using the formula:

$`Spread = (BestAsk - BestBid) * 100 / BestAsk`$

The data is stored with a timestamp and relevant details in a partitioned structure, making it easier to query and analyze.

### Orchestration

Apache Airflow is used to orchestrate the data collection, processing, and alerting tasks. The tasks are scheduled to run every 10 minutes, collecting data every second and storing it in the data lake.

## Project Structure

```
Challenge_1
├── dags
│ └── etl_spread.py
├── data # Following a partiotioned s3 bucket strategie
│ └── book
├── └── year
├────── └── month
├──────── └── day
├────────── └── hour
├── logs
├── src
│ └── etl_btc_mxn.py # Script to calculate spread from order book data btc_msx
│ └── etl_usd_mxn.py # Script to calculate spread from order book data usd_mxn
│ └── utils.py # Collection of functions used in the etl process
├── Dockerfile # Dockerfile to set up the Airflow environment
├── docker-compose.override.yml # File to add the local paths to airflow environment
├── requirements.txt # Python dependencies
└── README.md # Project documentation
```

## Getting Started

### Prerequisites

- Docker
- Docker Compose
- Astronomer CLI (for local development)

### Setup Instructions

1. **Clone the Repository**

   ```bash
   git clone https://github.com/luiz-decio/bitso-de-challenge.git
   cd Challenge_1
   ```
2. **Set Up Environment Variables**
   
   Create a .env file in the root directory with your Bitso API credentials:
   ```bash
    API_KEY = your_api_key
    API_SECRET = your_api_secret
   ```
3. **Install Dependencies**    
   
   ```bash
    pip install requirements.txt
   ```
4. **Initialize the airflow environment**

   Use the Astronomer CLI to build the airflow dependencies and create the directories:
   ```bash
    astro dev init
   ```
5. **Build and Start the Docker Containers**

   Use the Astronomer CLI to start the Docker containers:
   ```bash
    astro dev start
   ```
6. **Access the Airflow UI**

   Open your browser and go to http://localhost:8080 to access the Airflow UI. The default login credentials are:
   - Username: `admin`
   - Password: `admin`
7. **Trigger the DAG**

   In the Airflow UI, you should see a DAG named bitso_order_book. Turn it on and trigger it manually to start the data collection process.

## Additional Information

### Directory Structure in S3

The output files are stored in a directory structure that simulates partitions in S3. The partitions are organized by year, month, day, and hour, making it easy to query specific time ranges.


# Challenge 2

It was used a star schema for the data model, which includes two fact tables and two dimension tables:

1. **Fact Table: `fact_transactions`**
2. **Fact Table: `fact_events`**
3. **Dimension Table: `dim_users`**
4. **Dimension Table: `dim_dates`**

## ETL Process

1. **Extract**: Loaded data from source CSV files.
2. **Transform**: Cleaned and normalized data, created fact and dimension tables, and generated queries for the use cases.
3. **Load**: Saved the transformed data to target CSV files.

## Data Staging:

- ```landing``` Data in its original format (CSV files provided).
- ```raw``` Simple transformation and validation applied.
- ```analytics``` Fact and Dimension tables read to be queried.

## Queries

The Queries and it's results can be found in this [jupyter notebook](/Challenge_2/notebooks/sql_queries.ipynb).

The SQL queries stored answer the following use cases:
1. Active users on a given day.
2. Users who haven't made a deposit.
3. Users with more than 5 deposits historically.
4. Last login time of users.
5. Login count between two dates.
6. Unique currencies deposited on a given day.
7. Unique currencies withdrew on a given day.
8. Total amount deposited of a given currency on a given day.

### Setup Instructions

1. **Clone the Repository**

   ```bash
   git clone https://github.com/luiz-decio/bitso-de-challenge.git
   cd Challenge_2
   ```
2. **Copy the RAW data files**
   
   Copy the original sample files into the path:
   ```
    Challenge_2/data/raw
   ```
3. **Install Dependencies**    
   
   ```bash
    pip install requirements.txt
   ```
4. **Initialize the airflow environment**

   Use the Astronomer CLI to build the airflow dependencies and create the directories:
   ```bash
    astro dev init
   ```
5. **Build and Start the Docker Containers**

   Use the Astronomer CLI to start the Docker containers:
   ```bash
    astro dev start
   ```
6. **Access the Airflow UI**

   Open your browser and go to http://localhost:8080 to access the Airflow UI. The default login credentials are:
   - Username: `admin`
   - Password: `admin`
7. **Trigger the DAG**

   In the Airflow UI, you should see a DAG named etl. Turn it on and trigger it manually to start the data collection process.




