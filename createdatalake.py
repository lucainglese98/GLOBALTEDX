###### TEDx-Load-Aggregate-Model
######

import sys
import json
import pyspark
from pyspark.sql.functions import col, collect_list, array_join

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job




##### FROM FILES
tedx_dataset_path = "s3://unibg-luc-1/tedx_dataset.csv"

###### READ PARAMETERS
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

##### START JOB CONTEXT AND JOB
sc = SparkContext()


glueContext = GlueContext(sc)
spark = glueContext.spark_session


    
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


#### READ INPUT FILES TO CREATE AN INPUT DATASET
tedx_dataset = spark.read \
    .option("header","true") \
    .option("quote", "\"") \
    .csv(tedx_dataset_path)



    
tedx_dataset.printSchema()


#### FILTER ITEMS WITH NULL POSTING KEY
count_items = tedx_dataset.count()
count_items_null = tedx_dataset.filter("idx is not null").count()

print(f"Number of items from RAW DATA {count_items}")
print(f"Number of items from RAW DATA with NOT NULL KEY {count_items_null}")



## READ TAGS DATASET
tags_dataset_path = "s3://unibg-luc-1/tags_dataset.csv"
tags_dataset = spark.read.option("header","true").csv(tags_dataset_path)



# CREATE THE AGGREGATE MODEL, ADD TAGS TO TEDX_DATASET
tags_dataset_agg = tags_dataset.groupBy(col("idx").alias("idx_ref")).agg(collect_list("tag").alias("tags"))
tags_dataset_agg.printSchema()
tedx_dataset_agg = tedx_dataset.join(tags_dataset_agg, tedx_dataset.idx == tags_dataset_agg.idx_ref, "left") \
    .drop("idx_ref") \
    .select(col("idx").alias("_id"), col("*")) \
    .drop("idx") \
   

tedx_dataset_agg.printSchema()

## READ WATCH_NEXT DATASET ##
wn_dataset_path = "s3://unibg-luc-1/watch_next_dataset.csv"
wn_dataset = spark.read.option("header","true").csv(wn_dataset_path)
wn_dataset=wn_dataset.dropDuplicates().where('url != "https://www.ted.com/session/new?context=ted.www%2Fwatch-later"')

## CREATE THE AGGREGATE MODEL, ADD WATCH_NEXT TO TEDX_DATASET_AGG ##
wn_dataset_agg = wn_dataset.groupBy(col("idx").alias("idx_ref")).agg(collect_list("watch_next_idx").alias("wn_idx"))
wn_dataset_agg.printSchema()
tedx_dataset_agg_d = tedx_dataset_agg.join(wn_dataset_agg, tedx_dataset_agg._id == wn_dataset_agg.idx_ref, "left") \
    .drop("idx_ref") \
    .select(col("_id"), col("*")) 

tedx_dataset_agg_d=tedx_dataset_agg_d.dropDuplicates()
mongo_uri = "mongodb://cluster0-shard-00-00.6x1dc.mongodb.net:27017,cluster0-shard-00-01.6x1dc.mongodb.net:27017,cluster0-shard-00-02.6x1dc.mongodb.net:27017"

write_mongo_options = {
    "uri": mongo_uri,
    "database": "unibg_tedx_2021",
    "collection": "tedx_data",
    "username": "admin12",
    "password": "admin12",
    "ssl": "true",
    "ssl.domain_match": "false"}
from awsglue.dynamicframe import DynamicFrame
tedx_dataset_dynamic_frame = DynamicFrame.fromDF(tedx_dataset_agg_d, glueContext, "nested")

glueContext.write_dynamic_frame.from_options(tedx_dataset_dynamic_frame, connection_type="mongodb", connection_options=write_mongo_options)
