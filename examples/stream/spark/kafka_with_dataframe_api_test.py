from pyspark.sql.types import BinaryType, StringType, StructType, StructField, IntegerType, TimestampType
from pyspark.sql import SparkSession
import datetime
import unittest
import warnings
import time
from .kafka_with_dataframe_api import process_stream


class PySparkTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        warnings.filterwarnings("ignore", category=ResourceWarning)
        warnings.filterwarnings("ignore", category=DeprecationWarning)

        cls.spark = SparkSession.builder \
                                .appName("testing") \
                                .getOrCreate()

        cls.spark.sparkContext.setLogLevel("ERROR")


    @classmethod
    def tearDownClass(cls):
        cls.spark.sparkContext.stop()

class TestEtl(PySparkTestCase):

    def test_process_extract_and_transform(self):
        spark = self.spark

        schema = StructType([
            StructField("key", BinaryType(), False),
            StructField("value", BinaryType(), False),
            StructField("topic", StringType(), False),
            StructField("partition", IntegerType(), False),
            StructField("offset", IntegerType(), False),
            StructField("timestamp", TimestampType(), False),
            StructField("timestampType", IntegerType(), False),
        ])

        rows = []
        count = 0
        for i in range(60):
            count += 1
            rows.append(
                (bytearray("123456", encoding='utf8'), bytearray("some value", encoding='utf8'), "foobar", 0, 1, datetime.datetime.now(), 0)
            )

            if count == 10:
                count = 0
                time.sleep(2)

        df = self.spark.createDataFrame(rows, schema)

        formated_df, grouped_by_key_df, grouped_by_window_and_key_df = process_stream(df, once = True, windowTime="2 seconds")

        self.assertEqual(len(formated_df.collect()), 60)
        self.assertEqual(len(grouped_by_key_df.collect()), 1)
        self.assertEqual(len(grouped_by_window_and_key_df.collect()), 6)
