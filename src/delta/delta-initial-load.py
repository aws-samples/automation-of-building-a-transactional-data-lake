import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.conf import SparkConf
import ast

# @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME',
                                     'database',
                                     'initial_load_s3_path',
                                     'transactional_data_lake_s3_path',
                                     'target_tables_list'])

DATABASE = args.get('database')  # game
# "s3://raw-zone-data-lake/initial-load/"
INITIAL_LOAD_S3_PATH = args.get('initial_load_s3_path')
# "s3://transactional-data-lake-us-east-1/"
TRANSACTIONAL_DATA_LAKE_S3_PATH = args.get('transactional_data_lake_s3_path')
TARGET_TABLES = args.get('target_tables_list')  # [{}, {}, {}]


def set_spark_delta_conf() -> SparkConf:
    conf = SparkConf()

    conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    conf.set("spark.sql.catalog.spark_catalog",
             "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    conf.set("spark.databricks.hive.metastore.glueCatalog.enabled", "true")
    conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    return conf


conf = set_spark_delta_conf()
sparkContext = SparkContext(conf=conf)
glueContext = GlueContext(sparkContext)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


def create_delta_table(table_list, table_name, partition_key):

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
        initialDF = initialDynamicFrame.toDF()

        additional_options = {
            "path": f"{TRANSACTIONAL_DATA_LAKE_S3_PATH}{DATABASE}/{table_name}/",
            "overwriteSchema": "true"
        }

        if partition_key == "None" or partition_key == "none":
            initialDF.write \
                .format("delta") \
                .options(**additional_options) \
                .mode("overwrite") \
                .save()
        else:
            initialDF.write \
                .format("delta") \
                .options(**additional_options) \
                .mode("overwrite") \
                .partitionBy(partition_key) \
                .save()

        """
        initialDF.write \
            .format("delta") \
            .options(**additional_options) \
            .mode("append") \
            #.partitionBy("<your_partitionkey_field>") \
            .saveAsTable(f"{DATABASE}.{table_name}")
        """

        print(f"Succeed to create {table_name} table.")
        print(f"Succeed to insert records into {table_name} table.")

    else:
        print("Already Existed Table.")


def main():
    # Check if the database already exists
    tablesDF = spark.sql(f"SHOW TABLES IN {DATABASE}")
    table_list = tablesDF.select(
        'tableName').rdd.flatMap(lambda x: x).collect()
    print(f"Tables in Glue Data Catalog Database named {DATABASE} : ")
    print(table_list)

    target_tables = ast.literal_eval(TARGET_TABLES)
    for target_table in target_tables:
        # Initial Load
        table_name = target_table["table_name"]
        primary_key = target_table["primary_key"]
        partition_key = target_table["partition_key"]

        create_delta_table(table_list, table_name, partition_key)


if __name__ == "__main__":
    main()


job.commit()
print("Glue Job is completed successfully.")
