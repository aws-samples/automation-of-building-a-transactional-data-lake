import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.conf import SparkConf
from pyspark.sql.functions import (
    col,
    lit,
    max,
    to_timestamp
)
import ast

# @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME',
                                     'database',
                                     'initial_load_s3_path',
                                     'transactional_data_lake_s3_path',
                                     'target_tables_list'])

DATABASE = args.get('database')
INITIAL_LOAD_S3_PATH = args.get('initial_load_s3_path')
TRANSACTIONAL_DATA_LAKE_S3_PATH = args.get('transactional_data_lake_s3_path')
TARGET_TABLES = args.get('target_tables_list')


def set_spark_hudi_conf() -> SparkConf:
    conf = SparkConf()

    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.sql.hive.convertMetastoreParquet", "false")
    conf.set("spark.sql.catalog.spark_catalog",
             "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
    conf.set("spark.sql.extensions",
             "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")

    return conf


conf = set_spark_hudi_conf()
sparkContext = SparkContext(conf=conf)
glueContext = GlueContext(sparkContext)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


# Initial Load
def create_hudi_table(table_list, table_name, primary_key, precombine_key, partition_key):
    # 1. Create Iceberg Table
    if table_name not in table_list:
        initialDynamicFrame = glueContext.create_dynamic_frame_from_options(
            connection_type='s3',
            connection_options={
                'paths': [f'{INITIAL_LOAD_S3_PATH}{DATABASE}/{table_name}/'],
                'groupFiles': 'none',
                'recurse': True
            },
            format='parquet',
            transformation_ctx='initialDynamicFrame')
        # print(f"Number of Rows in {table_name} : {initialDynamicFrame.count()}")
        initialDF = initialDynamicFrame.toDF()

        # Create a Hudi table from a DataFrame and register the table to Glue Data Catalog
        if partition_key == "None" or partition_key == "none":
            additional_options = {
                "hoodie.table.name": table_name,
                "hoodie.datasource.write.storage.type": "COPY_ON_WRITE",
                "hoodie.datasource.write.operation": "bulk_insert",
                "hoodie.datasource.write.recordkey.field": primary_key,
                "hoodie.datasource.write.precombine.field": precombine_key,
                "hoodie.datasource.write.hive_style_partitioning": "true",
                "hoodie.datasource.hive_sync.enable": "true",
                "hoodie.datasource.hive_sync.database": DATABASE,
                "hoodie.datasource.hive_sync.table": table_name,
                "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor",
                "hoodie.datasource.hive_sync.use_jdbc": "false",
                "hoodie.datasource.hive_sync.mode": "hms"
            }
        else:
            additional_options = {
                "hoodie.table.name": table_name,
                "hoodie.datasource.write.storage.type": "COPY_ON_WRITE",
                "hoodie.datasource.write.operation": "bulk_insert",
                "hoodie.datasource.write.recordkey.field": primary_key,
                "hoodie.datasource.write.precombine.field": precombine_key,
                "hoodie.datasource.write.partitionpath.field": partition_key,
                "hoodie.datasource.write.hive_style_partitioning": "true",
                "hoodie.datasource.hive_sync.enable": "true",
                "hoodie.datasource.hive_sync.database": DATABASE,
                "hoodie.datasource.hive_sync.table": table_name,
                "hoodie.datasource.hive_sync.partition_fields": partition_key,
                "hoodie.datasource.hive_sync.partition_extractor_class": "org.apache.hudi.hive.MultiPartKeysValueExtractor",
                "hoodie.datasource.hive_sync.use_jdbc": "false",
                "hoodie.datasource.hive_sync.mode": "hms"
            }

        initialDF.write.format("hudi") \
            .options(**additional_options) \
            .mode("overwrite") \
            .save(f"{TRANSACTIONAL_DATA_LAKE_S3_PATH}{DATABASE}/{table_name}/")

        print(f"Succeed to create {table_name} table.")
        print(f"Succeed to insert records into {table_name} table.")

    else:
        print("Already Existed Table")


def main():
    table_list = [table.name for table in spark.catalog.listTables(DATABASE)]
    print(f"Tables in Glue Data Catalog Database named {DATABASE} : ")
    print(table_list)

    target_tables = ast.literal_eval(TARGET_TABLES)
    for target_table in target_tables:
        # Initial Load
        table_name = target_table["table_name"]
        primary_key = target_table["primary_key"]
        partition_key = target_table["partition_key"]
        precombine_key = target_table["precombine_key"]

        create_hudi_table(table_list, table_name, primary_key,
                          precombine_key, partition_key)


if __name__ == "__main__":
    main()

job.commit()
print("Glue Job is completed successfully.")
