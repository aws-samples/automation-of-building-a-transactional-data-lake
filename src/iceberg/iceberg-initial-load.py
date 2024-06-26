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

DATABASE = args.get('database')
INITIAL_LOAD_S3_PATH = args.get('initial_load_s3_path')
TRANSACTIONAL_DATA_LAKE_S3_PATH = args.get('transactional_data_lake_s3_path')
TARGET_TABLES = args.get('target_tables_list')


def set_spark_iceberg_conf() -> SparkConf:
    conf = SparkConf()

    conf.set("spark.sql.extensions",
             "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    conf.set(f"spark.sql.catalog.my_catalog",
             "org.apache.iceberg.spark.SparkCatalog")
    conf.set(f"spark.sql.catalog.my_catalog.warehouse",
             TRANSACTIONAL_DATA_LAKE_S3_PATH)
    conf.set(f"spark.sql.catalog.my_catalog.catalog-impl",
             "org.apache.iceberg.aws.glue.GlueCatalog")
    conf.set(f"spark.sql.catalog.my_catalog.io-impl",
             "org.apache.iceberg.aws.s3.S3FileIO")

    return conf


conf = set_spark_iceberg_conf()
sparkContext = SparkContext(conf=conf)
glueContext = GlueContext(sparkContext)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


# Initial Load
def create_iceberg_table(table_list, table_name, partition_key):

    if table_name not in table_list:
        # 1. Create Iceberg Table
        df = spark.read.format("parquet").option("header", True).load(
            f"{INITIAL_LOAD_S3_PATH}{DATABASE}/{table_name}/")
        ddl = spark.sparkContext._jvm.org.apache.spark.sql.types.DataType.fromJson(
            df.schema.json()).toDDL()

        if partition_key == "None" or partition_key == "none":
            create_query = f"""
            CREATE TABLE IF NOT EXISTS my_catalog.{DATABASE}.{table_name} ({ddl})
            USING iceberg
            location '{TRANSACTIONAL_DATA_LAKE_S3_PATH}{DATABASE}/{table_name}/'
            tblproperties ('format-version'='2')"""
        else:
            create_query = f"""
            CREATE TABLE IF NOT EXISTS my_catalog.{DATABASE}.{table_name} ({ddl})
            USING iceberg
            PARTITIONED BY ({partition_key})
            location '{TRANSACTIONAL_DATA_LAKE_S3_PATH}{DATABASE}/{table_name}/'
            tblproperties ('format-version'='2')"""

        spark.sql(create_query)
        print(f"Succeed to create {table_name} table.")

        # 2. Insert records into Iceberg table
        TEMP_TABLE = "temp_table"
        df.createOrReplaceTempView(TEMP_TABLE)

        if partition_key == "None" or partition_key == "none":
            insert_query = f"""
            INSERT INTO my_catalog.{DATABASE}.{table_name}
            SELECT * 
            FROM {TEMP_TABLE}  
            """
        else:
            insert_query = f"""
            INSERT INTO my_catalog.{DATABASE}.{table_name}
            SELECT * 
            FROM {TEMP_TABLE}
            ORDER BY {partition_key}
            """

        spark.sql(insert_query)
        print(f"Succeed to insert records into {table_name} table.")

    else:
        print("already existed table.")


def main():
    tablesDF = spark.sql(f"SHOW TABLES IN my_catalog.{DATABASE}")
    table_list = tablesDF.select(
        'tableName').rdd.flatMap(lambda x: x).collect()
    print(f"Tables in Glue Data Catalog Database named {DATABASE} : ")
    print(table_list)

    target_tables = ast.literal_eval(TARGET_TABLES)
    for target_table in target_tables:
        table_name = target_table["table_name"]
        primary_key = target_table["primary_key"]
        partition_key = target_table["partition_key"]

        print(table_name, primary_key, partition_key)
        create_iceberg_table(table_list, table_name, partition_key)


if __name__ == "__main__":
    main()

job.commit()
print("Glue Job is completed successfully.")
