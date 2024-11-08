import luigi
import pandas as pd
import snowflake.connector
import os

class ExtractData(luigi.Task):
    def output(self):
        return [luigi.LocalTarget(f"data_{month}.csv") for month in ["jan", "june", "july", "august"]]

    def run(self):
        # Load and save data for each month as CSV
        data_paths = [
            '/Users/pbritwum/Documents/Learning/ETL-Python/data/yellow_tripdata_2024-01.parquet',
            '/Users/pbritwum/Documents/Learning/ETL-Python/data/yellow_tripdata_2024-06.parquet',
            '/Users/pbritwum/Documents/Learning/ETL-Python/data/yellow_tripdata_2024-07.parquet',
            '/Users/pbritwum/Documents/Learning/ETL-Python/data/yellow_tripdata_2024-08.parquet'
        ]
        
        for i, path in enumerate(data_paths):
            data = pd.read_parquet(path)
            data.to_csv(self.output()[i].path, index=False)

class TransformData(luigi.Task):
    def requires(self):
        return ExtractData()

    def output(self):
        return luigi.LocalTarget("cleaned_data.csv")

    def run(self):
        # Combine the extracted CSVs
        dfs = [pd.read_csv(target.path) for target in self.input()]
        combined_data = pd.concat(dfs).drop_duplicates()

        # Apply transformations
        combined_data = combined_data.dropna(thresh=3)
        combined_data = combined_data.fillna(value=1)
        combined_data['tpep_pickup_datetime'] = pd.to_datetime(combined_data['tpep_pickup_datetime'], format='%Y-%m-%d %H:%M:%S')
        combined_data['tpep_dropoff_datetime'] = pd.to_datetime(combined_data['tpep_dropoff_datetime'], format='%Y-%m-%d %H:%M:%S')

        combined_data.to_csv(self.output().path, index=False)

class LoadDataToSnowflake(luigi.Task):
    table_name = luigi.Parameter(default="Trips_Combined")

    def requires(self):
        return TransformData()

    def output(self):
        return luigi.LocalTarget("load_complete.txt")

    def run(self):
        # Snowflake connection setup
        conn = snowflake.connector.connect(
            user='PBRIT',
            password='Quagmire_123@',
            account='gu84912.south-central-us.azure',
            warehouse='COMPUTE_WH',
            database='MYPASEL',
            schema='DBO',
            role='ACCOUNTADMIN'
        )
        cur = conn.cursor()
        
        # Create table if not exists
        create_table_query = '''
        CREATE OR REPLACE TABLE Trips_Combined (
            VendorID                INTEGER,          
            tpep_pickup_datetime    TIMESTAMP,         
            tpep_dropoff_datetime   TIMESTAMP,         
            passenger_count         FLOAT,        
            trip_distance           FLOAT,        
            RatecodeID              FLOAT,        
            store_and_fwd_flag      STRING,        
            PULocationID            INTEGER,          
            DOLocationID            INTEGER,          
            payment_type            INTEGER,          
            fare_amount             FLOAT,        
            extra                   FLOAT,        
            mta_tax                 FLOAT,        
            tip_amount              FLOAT,        
            tolls_amount            FLOAT,        
            improvement_surcharge   FLOAT,        
            total_amount            FLOAT,        
            congestion_surcharge    FLOAT,        
            Airport_fee             FLOAT,        
            pickup_month            INTEGER,          
            time_diff               FLOAT
        )
        '''
        cur.execute(create_table_query)

        # Load data in batches
        data = pd.read_csv(self.input().path)
        insert_query = f"INSERT INTO {self.table_name} VALUES ({', '.join(['%s'] * len(data.columns))})"
        rows = [tuple(row) for row in data.itertuples(index=False, name=None)]
        
        batch_size = 1000
        for i in range(0, len(rows), batch_size):
            cur.executemany(insert_query, rows[i:i+batch_size])
        
        conn.commit()
        cur.close()
        conn.close()

        # Mark task completion
        with self.output().open('w') as f:
            f.write("Data loaded successfully")


