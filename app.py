#!/usr/bin/env python3
import os

import aws_cdk as cdk

from cdk_stacks.glue_stack import GlueStack
from cdk_stacks.s3_stack import S3Stack
from constructs import Construct

ACCOUNT_NUMBER = "891376935319"
REGION = "ap-northeast-2"

env_for_demo = cdk.Environment(account=ACCOUNT_NUMBER, region=REGION)

app = cdk.App()

s3_stack = S3Stack(app, "Transactional-Data-Lake-S3-Stack", env=env_for_demo)
glue_stack = GlueStack(app, "Transactional-Data-Lake-Glue-Stack", env=env_for_demo)

glue_stack.add_dependency(s3_stack)

app.synth()

### How to implement CDK code ###
# When you select open_table_format as hudi or iceberg
# 1. [Optional] $cdk bootstrap aws://ACCOUNT-NUMBER/REGION-NAME (e.g. cdk bootstrap aws://279067972884/us-west-2)
# 2. $cdk synth
# 3. $cdk deploy --require-approval never --all
### 4-1. $aws s3 cp demo_data/initial-load/game/ s3://raw-data-lake-891376935319/initial-load/game/ --recursive --exclude ".*" --exclude "*/.*"
### 4-2. $aws s3 cp demo_data/cdc-load/game/ s3://raw-data-lake-891376935319/cdc-load/game/ --recursive --exclude ".*" --exclude "*/.*"
### 5-1. $aws s3 cp src/iceberg/iceberg-initial-load.py s3://aws-glue-assets-891376935319/scripts/iceberg-initial-load.py | aws s3 cp src/hudi/hudi-initial-load.py s3://aws-glue-assets-demo-ap-northeast-2/scripts/hudi-initial-load.py
### 5-2. $aws s3 cp src/iceberg/iceberg-cdc-load.py s3://aws-glue-assets-891376935319/scripts/iceberg-cdc-load.py | aws s3 cp src/hudi/hudi-cdc-load.py s3://aws-glue-assets-demo-ap-northeast-2/scripts/hudi-cdc-load.py
# 6. [Optional] $aws glue start-job-run --job-name iceberg-initial-glue-etl-job # initial-glue-etl-job을 run 한다.


# When you select open_table_format as delta
# 1. $cdk bootstrap aws://ACCOUNT-NUMBER/REGION-NAME (e.g. cdk bootstrap aws://279067972884/us-west-2)
# 2. $cdk synth
# 3. $cdk deploy --require-approval never --all
# 4-1. $aws s3 cp demo_data/initial-load/game/ s3://raw-data-lake-891376935319/initial-load/game/ --recursive --exclude ".*" --exclude "*/.*"
# 4-2. $aws s3 cp demo_data/cdc-load/game/ s3://raw-data-lake-891376935319/cdc-load/game/ --recursive --exclude ".*" --exclude "*/.*"
# 5-1. $aws s3 cp src/delta/delta-initial-load.py s3://aws-glue-assets-891376935319/scripts/delta-initial-load.py
# 5-2. $aws s3 cp src/delta/delta-cdc-load.py s3://aws-glue-assets-891376935319/scripts/delta-cdc-load.py
# 6. $aws glue start-trigger --name delta_lake_glue_trigger_for_initial_load # initial-glue-etl-job을 run 한다. (Trigger를 통해 run 해야 initial-glue-etl-job의 status code 변화를 다른 Trigger가 감지한다.)
