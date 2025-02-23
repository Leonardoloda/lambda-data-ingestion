from boto3 import client

lambda_client = client("lambda")

lambda_client.create_function(
    FunctionName="ingestion-lambda",
    Runtime="python3.13"
)
