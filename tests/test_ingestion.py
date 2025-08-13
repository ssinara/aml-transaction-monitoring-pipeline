import unittest
import os
from datetime import datetime
from pyspark.sql import SparkSession
from batch_jobs import ingestion


class IngestionTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
            .master("local[1]") \
            .appName("IngestionTest") \
            .getOrCreate()

        # Create test directories
        os.makedirs("data/raw", exist_ok=True)
        os.makedirs("data/processed", exist_ok=True)

        # Create sample accounts.csv
        accounts_data = [
            ("ACC001", "CUST001", 1000.0, "US", "SAVINGS", 0, datetime.now()),
            ("ACC002", "CUST002", 2000.0, "UK", "CHECKING", 1, datetime.now())
        ]
        accounts_df = cls.spark.createDataFrame(accounts_data, schema=ingestion.StructType([
            ingestion.StructField("ACCOUNT_ID", ingestion.StringType(), True),
            ingestion.StructField("CUSTOMER_ID", ingestion.StringType(), True),
            ingestion.StructField("BALANCE", ingestion.DoubleType(), True),
            ingestion.StructField("COUNTRY", ingestion.StringType(), True),
            ingestion.StructField("ACCOUNT_TYPE", ingestion.StringType(), True),
            ingestion.StructField("IS_FRAUD", ingestion.IntegerType(), True),
            ingestion.StructField("TX_BEHAVIOR_ID", ingestion.TimestampType(), True)
        ]))
        accounts_df.write.mode("overwrite").parquet("data/processed/accounts")

        # Create sample transactions.csv
        transactions_data = [
            ("TX001", 1, 2, "TRANSFER", 500.0, 20230101, 0, 101),
            ("TX002", 2, 3, "PAYMENT", 150.0, 20230102, 1, 102)
        ]
        transactions_df = cls.spark.createDataFrame(transactions_data, schema=ingestion.StructType([
            ingestion.StructField("TX_ID", ingestion.StringType(), True),
            ingestion.StructField("SENDER_ACCOUNT_ID", ingestion.IntegerType(), True),
            ingestion.StructField("RECEIVER_ACCOUNT_ID", ingestion.IntegerType(), True),
            ingestion.StructField("TX_TYPE", ingestion.StringType(), True),
            ingestion.StructField("TX_AMOUNT", ingestion.DoubleType(), True),
            ingestion.StructField("TX_DATETIME", ingestion.IntegerType(), True),
            ingestion.StructField("IS_FRAUD", ingestion.IntegerType(), True),
            ingestion.StructField("ALERT_ID", ingestion.IntegerType(), True)
        ]))
        transactions_df.write.mode("overwrite").parquet("data/processed/transactions")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_first_friday(self):
        date_str = ingestion.get_first_friday()
        # Ensure it's a valid date format
        datetime.strptime(date_str, "%Y-%m-%d")

    def test_load_data(self):
        ingestion.load_data()

        # Check processed data exists
        accounts_path = "data/processed/accounts"
        transactions_path = "data/processed/transaction"

        self.assertTrue(os.path.exists(accounts_path))
        self.assertTrue(os.path.exists(transactions_path))


if __name__ == "__main__":
    unittest.main()
