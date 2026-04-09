from bs4 import BeautifulSoup
from datetime import datetime
import requests
import logging
import pandas as pd 
import numpy as np
import sqlite3
import requests

"""
Bank Market Capitalization ETL Pipeline

Description:
This project implements an end-to-end ETL (Extract, Transform, Load) pipeline
that retrieves financial data about the world's largest banks from a Wikipedia page.

The pipeline performs the following steps:
1. Extracts tabular data (Rank, Bank Name, Market Capitalization) using web scraping (BeautifulSoup).
2. Cleans and transforms the data using pandas and NumPy.
3. Converts market capitalization values into multiple currencies (GBP, EUR, INR).
4. Stores the processed data into:
   - A CSV file for external usage
   - A SQLite database for structured querying
5. Executes SQL queries to retrieve insights for different regional offices.

Technologies Used:
- Python
- BeautifulSoup (Web Scraping)
- Pandas (Data Processing)
- NumPy (Numerical Operations)
- SQLite (Database Management)

Purpose:
This project demonstrates practical skills in data engineering, including
data extraction, transformation, storage, and querying in a real-world scenario.
"""

# ---------------------- LOGGING SETUP ----------------------
logging.basicConfig(
    filename='etl.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


class BankETL:

    def __init__(self, url, db_name, table_name, csv_path):
        self.url = url
        self.db_name = db_name
        self.table_name = table_name
        self.csv_path = csv_path
        self.conn = sqlite3.connect(db_name)

    def extract(self):
        """
        Extract bank data from the web page and return as DataFrame.
        """
        logging.info("Starting data extraction")
        df = pd.DataFrame(columns=['Rank', 'Bank Name', 'Market cap (us$ billion)'])
        try:
            response = requests.get(self.url)
            response.raise_for_status()
            data = BeautifulSoup(response.text, 'html.parser')
            tables = data.find_all('table')
            rows = tables[1].find_all('tr')

            for row in rows:
                col = row.find_all('td')
                if len(col) != 0:
                    data_dict = {'Rank': col[0].contents[0].split(),
                                'Bank Name': col[1].contents[2],
                                'Market cap (us$ billion)': col[2].contents[0].split()}
                    df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True)
            logging.info("Extraction completed successfully")
            return df
        except Exception as e:
            logging.error("Error fetching data:", e)
            raise

    def transform(self, df):
        """
        Clean and transform data, including currency conversions.
        """
        logging.info("Starting data transformation")
        try:
            df['Market cap (us$ billion)'] = pd.to_numeric(
                    df['Market cap (us$ billion)'].replace(',', '', regex=True))
            df['GBP'] = np.round(np.multiply(df['Market cap (us$ billion)'], 0.74), 2)
            df['EUR'] = np.round(np.multiply(df['Market cap (us$ billion)'], 0.85), 2)
            df['INR'] = np.round(np.multiply(df['Market cap (us$ billion)'], 93.4), 2)
            logging.info("Transformation completed successfully")
            return df
        except Exception as e:
            logging.error(f"Transformation failed: {e}")
            raise

    def load_to_csv(self, df):
        """
        Save DataFrame to CSV file.
        """
        logging.info("Saving data to CSV")
        try:
            df.to_csv(self.csv_path, index=False)
            logging.info("CSV file saved successfully")
        except Exception as e:
            logging.error(f"CSV saving failed: {e}")
            raise

    def load_to_db(self, df):
        """
        Store DataFrame into SQLite database.
        """
        logging.info("Saving data to database")
        try:
            df.to_sql(self.table_name, self.conn, if_exists='replace', index=False)
            logging.info("Database updated successfully")
        except Exception as e:
            logging.error(f"Database load failed: {e}")
            raise

    def run_query(self, query_statement):
        """
        Execute SQL query and print results.
        """
        logging.info(f"Running query: {query_statement}")
        try:
            results = pd.read_sql(query_statement, self.conn)
            print (results)
        except Exception as e:
            logging.error(f"Query failed: {e}")
            raise

    def run_pipeline(self):
        """
        Execute full ETL pipeline.
        """
        logging.info("Pipeline started")
        df = self.extract()
        df = self.transform(df)

        self.load_to_csv(df)
        self.load_to_db(df)

        print("=== GBP ===")
        self.run_query(f"SELECT [Bank name], GBP FROM {self.table_name}")

        print("=== EUR ===")
        self.run_query(f"SELECT [Bank name], EUR FROM {self.table_name}")

        print("=== INR ===")
        self.run_query(f"SELECT [Bank name], INR FROM {self.table_name}")


if __name__ == "__main__":
    url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
    db_name = "BANKS.db"
    table_name = "BANKS"
    csv_path = "banks.csv"
    etl = BankETL(url, db_name, table_name, csv_path)
    etl.run_pipeline()
