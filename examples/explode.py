from pyspark.sql import SparkSession

from pyspark.sql.functions import explode
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, ArrayType

spark = SparkSession.builder \
        .config("spark.eventLog.enabled", "true") \
        .config("spark.eventLog.dir", "/tmp/spark-events/") \
        .appName("explode-example") \
        .getOrCreate()

data = [
    (
        "asddfet-xasdf", 
        0, 
        []
    ),
    (
        "kghjkhgj-xasdf", 
        3, 
        [
            (123, "abc", 1.5),
            (122, "ttrtr", 0.5),
            (124, "gfrg", 1.1),
        ]
    ),
    (
        "asdfasd-xasdf",
        2, 
        [
            (12, "gfgf", 1.4),
            (1222, "gf", 1.1),
        ]
    )
]
schema = StructType([
    StructField('id', StringType(), True),
    StructField('products_count', IntegerType(), True),
    StructField('products', 
        ArrayType(
            StructType([
                StructField('id', IntegerType(), True),
                StructField('t', StringType(), True),
                StructField('cpc', FloatType(), True),
            ]), 
        )
    )
])

df = spark.sparkContext.parallelize(data).toDF(schema)

sample = df.where("products_count > 0").limit(2).collect()

df_products = df.where("products_count > 0").limit(2)

df_exploded = df.select(
                        explode(df_products.products).alias("product"), 
                        "*"
                       )

df_product_attrs = df_exploded.select(
                      df_exploded.product.id.alias("id"), 
                      df_exploded.product.t.alias("name"),
                      df_exploded.product.cpc.alias("cpc"),
                      df_exploded.id.alias("impression_id")
                    )
  
df_product_attrs.show()

df_product_attrs.printSchema()