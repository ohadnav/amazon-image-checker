"""An AWS Python Pulumi program"""

import pulumi_aws as aws

from data_pipeline import airflow_args

mwaa = aws.mwaa.Environment(
    "pulumi-mwaa-amazon-images-dag",
    airflow_configuration_options=airflow_args.default_args,
    dag_s3_path="data_pipeline/")
