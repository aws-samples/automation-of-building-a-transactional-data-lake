import sys
from datetime import datetime
import traceback

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.conf import SparkConf
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    concat,
    col,
    lit,
    max,
    rank,
    to_timestamp
)
import ast

# @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME',
                                     'database',
                                     'initial_load_s3_path',
                                     'cdc_load_s3_path',
                                     'transactional_data_lake_s3_path',
                                     'target_tables_list'])

DATABASE = args.get('database')  # mobile
# "s3://raw-zone-data-lake/initial-load/"
INITIAL_LOAD_S3_PATH = args.get('initial_load_s3_path')
# "s3://raw-zone-data-lake/cdc-load/"
CDC_LOAD_S3_PATH = args.get('cdc_load_s3_path')
# "s3://transactional-data-lake-us-east-1/"
TRANSACTIONAL_DATA_LAKE_S3_PATH = args.get('transactional_data_lake_s3_path')
TARGET_TABLES = args.get('target_tables_list')


def set_spark_delta_conf() -> SparkConf:
    conf = SparkConf()

    conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    conf.set("spark.sql.catalog.spark_catalog",
             "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    return conf


conf = set_spark_delta_conf()
sparkContext = SparkContext(conf=conf)
glueContext = GlueContext(sparkContext)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


# CDC Load
def cdc_delta_table(table_list, table_name, primary_key):
    cdcDynamicFrame = glueContext.create_dynamic_frame_from_options(
        connection_type='s3',
        connection_options={
            'paths': [f'{CDC_LOAD_S3_PATH}{DATABASE}/{table_name}/'],
            'groupFiles': 'none',
            'recurse': True
        },
        format='parquet',
        transformation_ctx='cdcDF')

    print(
        f"Count of CDC data after last job bookmark:{cdcDynamicFrame.count()}")

    if cdcDynamicFrame.count() == 0:
        print(f"No Data changed.")
    else:
        # toDF() : Glue DynamicFrame을 Spark DataFrame으로 변환한다.
        cdcDF = cdcDynamicFrame.toDF()
        cdcDF = cdcDF.withColumn('timestamp', to_timestamp(col('timestamp')))

        # Apply De-duplication logic on input data, to pickup latest record based on timestamp and operation
        IDWindowDF = Window.partitionBy(cdcDF[primary_key]).orderBy(
            cdcDF.timestamp).rangeBetween(-sys.maxsize, sys.maxsize)

        # Add new columns to capture first and last OP value and what is the latest timestamp
        inputDFWithTS = cdcDF.withColumn(
            "max_op_date", max(cdcDF.timestamp).over(IDWindowDF))

        # Filter out new records that are inserted, then select latest record from existing records and merge both to get deduplicated output
        newInsertedDF = inputDFWithTS.filter(
            "timestamp=max_op_date").filter("Op='I'")
        updatedOrDeletedDF = inputDFWithTS.filter(
            "timestamp=max_op_date").filter("Op IN ('U', 'D')")
        finalInputDF = newInsertedDF.unionAll(updatedOrDeletedDF)

        CURRENT_DATETIME = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        finalInputDF = finalInputDF.withColumn(
            'last_applied_date', to_timestamp(lit(CURRENT_DATETIME)))

        cdcInsertCount = finalInputDF.filter("Op = 'I'").count()
        cdcUpdateCount = finalInputDF.filter("Op = 'U'").count()
        cdcDeleteCount = finalInputDF.filter("Op = 'D'").count()
        totalCDCCount = finalInputDF.count()
        print(f"Inserted count:  {cdcInsertCount}")
        print(f"Updated count:   {cdcUpdateCount}")
        print(f"Deleted count:   {cdcDeleteCount}")
        print(f"Total CDC count: {totalCDCCount}")

        # Merge CDC data into Delta Lake table
        dropColumnList = ['Op', 'max_op_date']

        if table_name not in table_list:
            print(f"Table {table_name} doesn't exist in {DATABASE}.")
        else:
            # DataFrame for the inserted or updated data
            upsertedDF = finalInputDF.filter("Op != 'D'").drop(*dropColumnList)
            if upsertedDF.count() > 0:
                upsertedDF.createOrReplaceTempView(f"{table_name}_upsert")
                print(f"Table '{table_name}' is upserting...")
                try:
                    spark.sql(f"""MERGE INTO {DATABASE}.{table_name} t
                USING {table_name}_upsert s ON s.{primary_key} = t.{primary_key}
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
                """)
                except Exception as ex:
                    traceback.print_exc()
                    raise ex
            else:
                print("No data to insert or update.")

            # DataFrame for the deleted data
            deletedDF = finalInputDF.filter("Op = 'D'").drop(*dropColumnList)
            if deletedDF.count() > 0:
                deletedDF.createOrReplaceTempView(f"{table_name}_delete")
                print(f"Table '{table_name}' is deleting...")
                try:
                    spark.sql(f"""MERGE INTO {DATABASE}.{table_name} t
                USING {table_name}_delete s ON s.{primary_key} = t.{primary_key}
                WHEN MATCHED THEN DELETE
                """)
                except Exception as ex:
                    traceback.print_exc()
                    raise ex
            else:
                print("No data to delete.")

            # Read data from Delta Lake Table
            spark.sql(f"SELECT * FROM {DATABASE}.{table_name}").show()
            print(f"Total count of {table_name} Table Results:\n")
            countDF = spark.sql(
                f"SELECT count(*) FROM {DATABASE}.{table_name}")
            print(f"{countDF.show()}")


# 작업 시작
def main():
    tablesDF = spark.sql(f"SHOW TABLES IN {DATABASE}")
    table_list = tablesDF.select(
        'tableName').rdd.flatMap(lambda x: x).collect()

    target_tables = ast.literal_eval(TARGET_TABLES)
    for target_table in target_tables:
        table_name = target_table["table_name"]
        primary_key = target_table["primary_key"]
        partition_key = target_table["partition_key"]
        cdc_delta_table(table_list, table_name, primary_key)


if __name__ == "__main__":
    main()

job.commit()
print(f"Glue Job is completed successfully.")
