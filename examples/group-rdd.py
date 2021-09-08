import sys
from awsglue.transforms import *
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import col
from functools import reduce

sc = SparkContext.getOrCreate()
gc = GlueContext(sc)
spark = gc.spark_session


attrs_data = [
    (53, 'Inverter', 'Sim'),
    (53, 'Material', 'PVC'),
    (62, 'Inverter', 'Sim'), 
    (62, 'Material', 'Borracha'), 
    (62, 'Material', 'PVC'), 
    (62, 'Material', 'Produto a base de misturas de solventes'),
    (10, 'Material', 'Borracha'), 
    (10, 'Material', 'PVC'), 
    (10, 'Material', 'Produto a base de misturas de solventes'), 
    (10, 'Inverter', 'Sim'), 
    (51, 'Inverter', 'Sim'),
    (51, 'Material', 'PVC'),
    (61, 'Inverter', 'Sim'), 
    (61, 'Material', 'Borracha'), 
    (61, 'Material', 'PVC'), 
    (61, 'Material', 'Produto a base de misturas de solventes'),
    (11, 'Material', 'Borracha'), 
    (11, 'Material', 'PVC'), 
    (11, 'Material', 'Produto a base de misturas de solventes'), 
    (11, 'Inverter', 'Sim'), 
    (12, 'Material', 'Borracha'), 
    (12, 'Material', 'PVC'), 
    (12, 'Material', 'Produto a base de misturas de solventes'), 
    (12, 'Inverter', 'Sim'), 
]

offer_data = [
    (53, 'prod 1', 1, 1),
    (62, 'prod 2', 2, 1), 
    (10, 'prod 3', 3, 1),
    (51, 'prod 4', 4, 1), 
    (61, 'prod 5', 5, 1),
    (11, 'prod 6', 6, 1),
    (12, 'prod 7', 7, 1),
]


attrs_df = sc.parallelize(attrs_data).toDF(["id","name", "value"])
offer_df = sc.parallelize(offer_data).toDF(["id","name", "prod_id", "cat_id"])


# group in attributes column
lst_of_dicts_df = attrs_df.rdd.map(
    lambda row: (row[0], row )
).groupByKey(
).mapValues(
    list
).toDF(['offer_id','list_attributes'])

lst_of_dicts_df.show()
lst_of_dicts_df.printSchema()


# group in attributes column
dict_of_lists_df = attrs_df.rdd.map(
    lambda row: (row[0], row )
).groupByKey(
).mapValues(
    lambda row: reduce(
        lambda m, c: {**m, **{ c.asDict()['name']:  m.get(c.asDict()['name'],[]) + [c.asDict()['value']] } }, 
        row,
        {}
    )
).toDF(['offer_id','dict_attributes'])

dict_of_lists_df.show()
dict_of_lists_df.printSchema()


offers_with_features = offer_df.join(
    lst_of_dicts_df,
    lst_of_dicts_df.offer_id == col("id"),
    'outer' ,
).join(
    dict_of_lists_df,
    dict_of_lists_df.offer_id == col("id"),
    'outer' ,
).drop(dict_of_lists_df['offer_id'])

offers_with_features.show()
offers_with_features.printSchema()


path = '/home/data4.json'

offers_with_features.write.format('json').mode("overwrite").save(path)

readed_from_file_df = spark.read.format('json').load(path)
