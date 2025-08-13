# batch_jobs/ingestion.py

from pyspark.sql import SparkSession
import pandas as pd
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType, TimestampType, StructField
from pyspark.sql.functions import *
from common.logging_config import get_logger

logger = get_logger("ingestion")



def get_first_friday():
    from datetime import date, timedelta

    today = date.today()
    first_day = today.replace(day=1)

    day_of_week = first_day.weekday()

    day_until_friday = (4-day_of_week) % 7
    first_friday = first_day + timedelta(days=day_until_friday)

    return first_friday.strftime('%Y-%m-%d')



def load_data():
    spark = SparkSession.builder \
            .appName('AML Batch Ingestion') \
            .getOrCreate()
    
    accounts_schema = StructType([
    StructField("ACCOUNT_ID", StringType(), True),
    StructField("CUSTOMER_ID", StringType(), True),
    StructField("BALANCE", DoubleType(), True),
    StructField("COUNTRY", StringType(), True),
    StructField("ACCOUNT_TYPE", StringType(), True),
    StructField("IS_FRAUD", IntegerType(), True),
    StructField("TX_BEHAVIOR_ID", TimestampType(), True)
    ])

    transactions_schema = StructType([
        StructField("TX_ID", StringType(), True),
        StructField("SENDER_ACCOUNT_ID", IntegerType(), True),
        StructField("RECEIVER_ACCOUNT_ID", IntegerType(), True),
        StructField("TX_TYPE", StringType(), True),
        StructField("TX_AMOUNT", DoubleType(), True),
        StructField("TX_DATETIME", IntegerType(), True),
        StructField("IS_FRAUD", IntegerType(), True),
        StructField("ALERT_ID", IntegerType(), True)
    ])
    
    df_accounts = spark.read.csv('data/raw/accounts.csv', header=True, schema=accounts_schema)
    df_transactions = spark.read.csv('data/raw/transactions.csv', header=True, schema=transactions_schema)

    df_accounts = df_accounts.withColumn('ACCOUNT_TYPE', trim(df_accounts.ACCOUNT_TYPE))
    df_transactions = df_transactions.fillna({"TX_TYPE":''})

    df_accounts = df_accounts.withColumn('prcsng_dt', lit(get_first_friday()).cast('date'))
    df_transactions = df_transactions.withColumn('prcsng_dt', lit(get_first_friday()).cast('date'))
    logger.info(f"Added prcsng_dt {get_first_friday()}")

    df_accounts.write.mode("overwrite").partitionBy('prcsng_dt').parquet('data/processed/accounts')
    df_transactions.write.mode("overwrite").partitionBy('prcsng_dt').parquet('data/processed/transaction')
    logger.info("processed data is created")

    spark.stop()

if __name__ =='__main__':
    logger.info("Starting ingestion job...")
    load_data()
    