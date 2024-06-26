from aws_cdk import (
    Stack,
    RemovalPolicy,
    aws_s3 as s3,
    aws_s3_deployment as s3_deployment,
    aws_iam as iam,
)
from constructs import Construct

class S3Stack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        glue_assets_bucket_name = self.node.try_get_context("glue_assets_bucket_name")
        raw_data_lake_bucket_name = self.node.try_get_context("raw_data_lake_bucket_name")
        transactional_data_lake_bucket_name = self.node.try_get_context("transactional_data_lake_bucket_name")
        open_table_format = self.node.try_get_context("open_table_format")  # hudi | iceberg | delta

        # Creating S3 bucket for glue assets
        s3_bucket_for_glue_asssets = s3.Bucket(self, "glue-assets-bucket",
                                               bucket_name=glue_assets_bucket_name,
                                               block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
                                               removal_policy=RemovalPolicy.DESTROY,
                                               auto_delete_objects=True
                                               )
        

        # Creating S3 bucket for raw data lake
        s3_bucket_for_raw_data_lake = s3.Bucket(self, "raw-data-lake-bucket",
                                               bucket_name=raw_data_lake_bucket_name,
                                               block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
                                               removal_policy=RemovalPolicy.DESTROY,
                                               auto_delete_objects=True
                                               )
        
        # Creating S3 bucket for transactional data lake
        s3_bucket_for_transactional_data_lake = s3.Bucket(self, "transactional-data-lake-bucket",
                                               bucket_name=transactional_data_lake_bucket_name,
                                               block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
                                               removal_policy=RemovalPolicy.DESTROY,
                                               auto_delete_objects=True
                                               )
        
        if open_table_format == "hudi":
            script_pass = "./src/hudi/"
        elif open_table_format == "iceberg":
            script_pass = "./src/iceberg/"
        else:
            script_pass = "./src/delta/"
        
        # Uploading glue scripts to the bucekt
        s3_deployment.BucketDeployment(self, "scripts-files",
                                       sources=[s3_deployment.Source.asset(script_pass, exclude=[".*", "*/.*"])],
                                       destination_bucket=s3_bucket_for_glue_asssets,
                                       destination_key_prefix="scripts",
                                       retain_on_delete=False,
                                       )
        
        # Uploading demo data to the bucekt
        s3_deployment.BucketDeployment(self, "demo-data",
                                       sources=[s3_deployment.Source.asset("./demo_data/", exclude=[".*", "*/.*"])],
                                       destination_bucket=s3_bucket_for_raw_data_lake,
                                       retain_on_delete=False,
                                       )