# batch_jobs/ingestion.py

from pyspark.sql import SparkSession
import pandas as pd


def load_data():
    spark = SparkSession.builder \
            .appName('AML Batch Ingestion') \
            .getOrCreate()
    
    df_accounts = spark.read.csv('data/raw/accounts.csv', header=True, inferSchema=True)
    df_transactions = spark.read.csv('data/raw/transactions.csv', header=True, inferSchema=True)

    print("Accounts sample")
    df_accounts.show(5)

    print("Transactions sample")
    df_transactions.show(5)
        
    spark.stop()

if __name__ =='__main__':
    load_data()