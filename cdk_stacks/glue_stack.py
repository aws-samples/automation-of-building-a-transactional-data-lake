from aws_cdk import (
    # Duration,
    Stack,
    Aws,
    # aws_sqs as sqs,
    aws_ec2,
    aws_glue,
    aws_iam,
    aws_s3,
    aws_lakeformation
)
from constructs import Construct


class GlueStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        ######################################## IAM ROLE ########################################
        # VPC
        # vpc_name = self.node.try_get_context("vpc_name")
        # vpc = aws_ec2.Vpc.from_lookup(self, 'ExistingVPC', is_default=True, vpc_name=vpc_name)

        # IAM ROLE FOR GLUE JOB
        glue_job_role = aws_iam.Role(
            self,
            'glueJobRole',
            assumed_by=aws_iam.ServicePrincipal('glue.amazonaws.com'),
            managed_policies=[
                aws_iam.ManagedPolicy.from_aws_managed_policy_name(
                    'service-role/AWSGlueServiceRole'),
                aws_iam.ManagedPolicy.from_aws_managed_policy_name(
                    'AWSGlueConsoleFullAccess'),
                aws_iam.ManagedPolicy.from_aws_managed_policy_name(
                    'AmazonS3FullAccess')
            ]
        )

        # IAM ROLE FOR GLUE CRAWLER
        glue_crawler_role = aws_iam.Role(
            self,
            'glueCrawlerRole',
            assumed_by=aws_iam.ServicePrincipal('glue.amazonaws.com'),
            managed_policies=[
                aws_iam.ManagedPolicy.from_aws_managed_policy_name(
                    'service-role/AWSGlueServiceRole'),
                aws_iam.ManagedPolicy.from_aws_managed_policy_name(
                    'AWSGlueConsoleFullAccess'),
                aws_iam.ManagedPolicy.from_aws_managed_policy_name(
                    'AmazonS3FullAccess')
            ]
        )
        ######################################## IAM ROLE ########################################



        ######################################## INPUT PARAMETER FROM CDK.JSON ########################################
        # Glue Default Parameter
        initial_glue_job_name = self.node.try_get_context(
            'initial_glue_job_name')
        cdc_glue_job_name = self.node.try_get_context('cdc_glue_job_name')
        glue_assets_bucket_name = self.node.try_get_context(
            "glue_assets_bucket_name")

        # Glue Input Parameter
        raw_zone_bucket_name = self.node.try_get_context("raw_data_lake_bucket_name")
        initial_load_s3_path = f"s3://{raw_zone_bucket_name}/initial-load/"
        cdc_load_s3_path = f"s3://{raw_zone_bucket_name}/cdc-load/"

        transactional_data_lake_bucket_name = self.node.try_get_context("transactional_data_lake_bucket_name")
        transactional_data_lake_s3_path = f"s3://{transactional_data_lake_bucket_name}/"

        database = self.node.try_get_context("database")
        target_tables_list = self.node.try_get_context('target_tables_list')
        open_table_format = self.node.try_get_context("open_table_format")  # hudi | iceberg | delta

        # Glue Trigger Parameter
        glue_trigger_for_cdc_etl_name = self.node.try_get_context(
            "glue_trigger_for_cdc_etl_name")
        schedule_of_glue_trigger_for_cdc_etl = self.node.try_get_context(
            "schedule_of_glue_trigger_for_cdc_etl")  # cron(Minutes Hours Day-of-month Month Day-of-week Year) | None

        # Glue Parameter only when Delta Lake
        if open_table_format == "delta":
            delta_glue_database_for_manifest_name = self.node.try_get_context(
                "delta_glue_database_for_manifest_name")  # Symlink Manifest Files를 저장하는 글루 데이터베이스
            delta_native_table_glue_crawler_name = self.node.try_get_context(
                "delta_native_table_glue_crawler_name")  # 글루 데이터베이스에 Native Tables를 생성하는 글루 크롤러 이름
            delta_symlink_manifest_glue_crawler_name = self.node.try_get_context(
                "delta_symlink_manifest_glue_crawler_name")  # 글루 데이터베이스에 Symlink Manifest Files를 생성하는 글루 크롤러 이름
            delta_glue_trigger_for_initial_etl_name = self.node.try_get_context(
                "delta_glue_trigger_for_initial_etl_name")  # Glue Trigger makes Initial Load Glue Job run.
            # Glue Trigger makes Glue Crawler for creating native tables run under the condition of SUCCEEDED state from Initial Glue Job.
            delta_glue_trigger_for_native_table_crawler_name = self.node.try_get_context(
                "delta_glue_trigger_for_native_table_crawler_name")
            # Glue Trigger makes Glue Crawler for creating symlink manifest files run under the condition of SUCCEEDED state from Initial Glue Job.
            delta_glue_trigger_for_symlink_manifest_crawler_name = self.node.try_get_context(
                "delta_glue_trigger_for_symlink_manifest_crawler_name")

        glue_job_parameter = {
            "--TempDir": f"s3://{glue_assets_bucket_name}/scripts/",
            "--enable-continuous-cloudwatch-log": "true",
            "--enable-glue-datacatalog": "true",
            "--enable-job-insights": "true",
            "--enable-metrics": "true",
            "--enable-spark-ui": "true",
            "--job-bookmark-option": "job-bookmark-enable",
            "--job-language": "python",
            "--spark-event-logs-path": f"s3://{glue_assets_bucket_name}/sparkHistoryLogs/",
            "--initial_load_s3_path": initial_load_s3_path,
            "--cdc_load_s3_path": cdc_load_s3_path,
            "--transactional_data_lake_s3_path": transactional_data_lake_s3_path,
            "--database": database,
            "--target_tables_list": f"{target_tables_list}",
            "--datalake-formats": open_table_format
        }

        # Glue Job Script Name
        if open_table_format == "hudi":
            initial_glue_job_script_file_name = "hudi-initial-load.py"
            cdc_glue_job_script_file_name = "hudi-cdc-load.py"
        if open_table_format == "iceberg":
            initial_glue_job_script_file_name = "iceberg-initial-load.py"
            cdc_glue_job_script_file_name = "iceberg-cdc-load.py"
        if open_table_format == "delta":
            initial_glue_job_script_file_name = "delta-initial-load.py"
            cdc_glue_job_script_file_name = "delta-cdc-load.py"
        ######################################## INPUT PARAMETER FROM CDK.JSON ########################################



        # Glue Data Catalog Database storing a native open table format's table definition
        cfn_glue_database_for_native_table = aws_glue.CfnDatabase(self, 'glueDatabaseForNativeTable',
                                                                  catalog_id=Aws.ACCOUNT_ID,
                                                                  database_input=aws_glue.CfnDatabase.DatabaseInputProperty(
                                                                      name=database,
                                                                      description="Glue Data Catalog Database that stores a native open table format's table definition")
                                                                  )

        # Glue Job - Initial ETL
        cfn_glue_job_for_initial_etl = aws_glue.CfnJob(self, "GlueJobForInitialETL",
                                                       command=aws_glue.CfnJob.JobCommandProperty(
                                                           name="glueetl",
                                                           python_version="3",
                                                           script_location=f"s3://{glue_assets_bucket_name}/scripts/{initial_glue_job_script_file_name}"
                                                       ),
                                                       role=glue_job_role.role_arn,

                                                       # the properties below are optional
                                                       # XXX: Set only AllocatedCapacity or MaxCapacity
                                                       # Do not set Allocated Capacity if using Worker Type and Number of Workers
                                                       # allocated_capacity=2,
                                                       default_arguments=glue_job_parameter,
                                                       description=f"This job loads the data from S3-Raw Zone and creates {open_table_format} tables.",
                                                       execution_property=aws_glue.CfnJob.ExecutionPropertyProperty(
                                                           max_concurrent_runs=1
                                                       ),
                                                       # XXX: check AWS Glue Version in https://docs.aws.amazon.com/glue/latest/dg/add-job.html#create-job
                                                       glue_version="4.0",
                                                       # XXX: Do not set Max Capacity if using Worker Type and Number of Workers
                                                       # max_capacity=2,
                                                       max_retries=0,
                                                       name=initial_glue_job_name,
                                                       # notification_property=aws_glue.CfnJob.NotificationPropertyProperty(
                                                       #   notify_delay_after=10 # 10 minutes
                                                       # ),
                                                       number_of_workers=10,
                                                       timeout=2880,
                                                       # ['Standard' | 'G.1X' | 'G.2X' | 'G.025x']
                                                       worker_type="G.2X"
                                                       )

        # Glue Job - CDC ETL
        cfn_glue_job_for_cdc_etl = aws_glue.CfnJob(self, "GlueJobForCDCETL",
                                                   command=aws_glue.CfnJob.JobCommandProperty(
                                                       name="glueetl",
                                                       python_version="3",
                                                       script_location=f"s3://{glue_assets_bucket_name}/scripts/{cdc_glue_job_script_file_name}"
                                                   ),
                                                   role=glue_job_role.role_arn,

                                                   # the properties below are optional
                                                   # XXX: Set only AllocatedCapacity or MaxCapacity
                                                   # Do not set Allocated Capacity if using Worker Type and Number of Workers
                                                   # allocated_capacity=2,
                                                   default_arguments=glue_job_parameter,
                                                   description=f"This job loads CDC file from S3-Raw Zone and updates {open_table_format} table.",
                                                   execution_property=aws_glue.CfnJob.ExecutionPropertyProperty(
                                                       max_concurrent_runs=1
                                                   ),
                                                   # XXX: check AWS Glue Version in https://docs.aws.amazon.com/glue/latest/dg/add-job.html#create-job
                                                   glue_version="4.0",
                                                   # XXX: Do not set Max Capacity if using Worker Type and Number of Workers
                                                   # max_capacity=2,
                                                   max_retries=0,
                                                   name=cdc_glue_job_name,
                                                   # notification_property=aws_glue.CfnJob.NotificationPropertyProperty(
                                                   #   notify_delay_after=10 # 10 minutes
                                                   # ),
                                                   number_of_workers=10,
                                                   timeout=2880,
                                                   # ['Standard' | 'G.1X' | 'G.2X' | 'G.025x']
                                                   worker_type="G.2X"
                                                   )

        # Glue Scheduler - Triggers Glue Job for CDC ETL according to the schedule
        cfn_glue_scheduler_for_cdc_etl = aws_glue.CfnTrigger(self, "GlueSchedulerForCDCETL",
                                                             actions=[aws_glue.CfnTrigger.ActionProperty(
                                                                 job_name=cdc_glue_job_name,
                                                                 # notification_property=aws_glue.CfnTrigger.NotificationPropertyProperty(
                                                                 #    notify_delay_after=123
                                                                 # ),
                                                                 # security_configuration="securityConfiguration",
                                                                 # timeout=123
                                                             )],
                                                             type="SCHEDULED",
                                                             # the properties below are optional
                                                             description="A time-based scheduler for CDC ETL Glue Job.",
                                                             name=glue_trigger_for_cdc_etl_name,
                                                             schedule=schedule_of_glue_trigger_for_cdc_etl,
                                                             start_on_creation=True
                                                             )

        # Additional Resources when customer selects Delta Lake as an open table format
        if open_table_format == 'delta':
            delta_tables = []
            for target_table in target_tables_list:
                table_name = target_table["table_name"]
                table_path = f"{transactional_data_lake_s3_path}{database}/{table_name}/"
                delta_tables.append(table_path)

            # Glue Database storing a symlink-based manifest table definition
            cfn_glue_database_for_symlink_manifest = aws_glue.CfnDatabase(self, 'glueDatabaseForManifest',
                                                                          catalog_id=Aws.ACCOUNT_ID,
                                                                          database_input=aws_glue.CfnDatabase.DatabaseInputProperty(
                                                                              name=delta_glue_database_for_manifest_name,
                                                                              description="Glue Data Catalog Database that stores symlink-based manifest table definition")
                                                                          )

            # Glue Crawler that creates a native Delta Lake table definition on a Glue Data Catalog Database
            delta_lake_cfn_glue_crawler_for_native_table = aws_glue.CfnCrawler(self, "createTableCrawler",
                                                                               role=glue_crawler_role.role_arn,
                                                                               targets=aws_glue.CfnCrawler.TargetsProperty(
                                                                                   delta_targets=[aws_glue.CfnCrawler.DeltaTargetProperty(
                                                                                       create_native_delta_table=True,
                                                                                       delta_tables=delta_tables,
                                                                                       write_manifest=False
                                                                                   )]
                                                                               ),
                                                                               database_name=database,
                                                                               description="Glue Crawler for Delta Lake that creates a native Delta Lake table definition on a Glue Data Catalog Database",
                                                                               name=delta_native_table_glue_crawler_name,
                                                                               schema_change_policy=aws_glue.CfnCrawler.SchemaChangePolicyProperty(
                                                                                   # Allowed values: DELETE_FROM_DATABASE | DEPRECATE_IN_DATABASE | LOG
                                                                                   delete_behavior="DELETE_FROM_DATABASE",
                                                                                   update_behavior="UPDATE_IN_DATABASE"  # Allowed values: LOG | UPDATE_IN_DATABASE
                                                                               )
                                                                               )

            # Glue Crawler that creates a symlink-based manifest table definition on Glue Data Catalog Database, and generates its symlink files on S3-Transactional Data Lake
            delta_lake_cfn_glue_crawler_for_manifest = aws_glue.CfnCrawler(self, "createManifestCrawler",
                                                                           role=glue_crawler_role.role_arn,
                                                                           targets=aws_glue.CfnCrawler.TargetsProperty(
                                                                               delta_targets=[aws_glue.CfnCrawler.DeltaTargetProperty(
                                                                                   create_native_delta_table=False,
                                                                                   delta_tables=delta_tables,
                                                                                   write_manifest=True
                                                                               )]
                                                                           ),
                                                                           database_name=delta_glue_database_for_manifest_name,
                                                                           description="Glue Crawler for Delta Lake that creates a symlink-based manifest table definition on Glue Data Catalog Database, and generates its symlink files on S3-Transactional Data Lake",
                                                                           name=delta_symlink_manifest_glue_crawler_name,
                                                                           schema_change_policy=aws_glue.CfnCrawler.SchemaChangePolicyProperty(
                                                                               # Allowed values: DELETE_FROM_DATABASE | DEPRECATE_IN_DATABASE | LOG
                                                                               delete_behavior="DELETE_FROM_DATABASE",
                                                                               update_behavior="UPDATE_IN_DATABASE"  # Allowed values: LOG | UPDATE_IN_DATABASE
                                                                           )
                                                                           )

            # Glue Trigger that makes Initial ETL Glue Job run
            delta_lake_cfn_trigger_for_intial_etl = aws_glue.CfnTrigger(self, "jobTrigger",
                                                                        actions=[aws_glue.CfnTrigger.ActionProperty(
                                                                            job_name=initial_glue_job_name,
                                                                            # notification_property=glue.CfnTrigger.NotificationPropertyProperty(
                                                                            #    notify_delay_after=123
                                                                            # ),
                                                                            # security_configuration="securityConfiguration",
                                                                            # timeout=123
                                                                        )],
                                                                        type="ON_DEMAND",

                                                                        # the properties below are optional
                                                                        description="Trigger Initial Glue ETL Job",
                                                                        name=delta_glue_trigger_for_initial_etl_name,
                                                                        start_on_creation=False
                                                                        )

            # Glue Trigger that makes Glue Crawler for native table run
            delta_lake_cfn_trigger_for_native_table_crawler = aws_glue.CfnTrigger(self, "glueTriggerForNativeTableCrawler",
                                                                                  actions=[aws_glue.CfnTrigger.ActionProperty(
                                                                                      crawler_name=delta_native_table_glue_crawler_name,
                                                                                      # notification_property=glue.CfnTrigger.NotificationPropertyProperty(
                                                                                      #    notify_delay_after=123
                                                                                      # ),
                                                                                      # security_configuration="securityConfiguration",
                                                                                      # timeout=123
                                                                                  )],
                                                                                  type="CONDITIONAL",

                                                                                  # the properties below are optional
                                                                                  description="Trigger Glue Crawler for native table when Initial Glue Job is completed successfully",
                                                                                  name=delta_glue_trigger_for_native_table_crawler_name,
                                                                                  predicate=aws_glue.CfnTrigger.PredicateProperty(
                                                                                      conditions=[aws_glue.CfnTrigger.ConditionProperty(
                                                                                          job_name=initial_glue_job_name,
                                                                                          logical_operator="EQUALS",
                                                                                          state="SUCCEEDED"
                                                                                      )],
                                                                                      logical="ANY"
                                                                                  ),
                                                                                  start_on_creation=True
                                                                                  )

            # Glue Trigger
            delta_lake_cfn_trigger_for_symlink_manifest_crawler = aws_glue.CfnTrigger(self, "glueTriggerForSymlinkManifestCrawler",
                                                                                      actions=[aws_glue.CfnTrigger.ActionProperty(
                                                                                          crawler_name=delta_symlink_manifest_glue_crawler_name
                                                                                      )],
                                                                                      type="CONDITIONAL",

                                                                                      # the properties below are optional
                                                                                      description="Trigger Glue Crawler for symlink manifest when Initial Glue Job is completed successfully",
                                                                                      name=delta_glue_trigger_for_symlink_manifest_crawler_name,
                                                                                      predicate=aws_glue.CfnTrigger.PredicateProperty(
                                                                                          conditions=[aws_glue.CfnTrigger.ConditionProperty(
                                                                                              job_name=initial_glue_job_name,
                                                                                              logical_operator="EQUALS",
                                                                                              state="SUCCEEDED"
                                                                                          )],
                                                                                          logical="ANY"
                                                                                      ),
                                                                                      start_on_creation=True
                                                                                      )
