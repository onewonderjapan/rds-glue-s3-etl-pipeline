# AWS Secrets Manager for RDS credentials
resource "aws_secretsmanager_secret" "rds_secrets" {
  for_each = local.configs
  name     = "rds-secret-${each.key}"
}

resource "aws_secretsmanager_secret_version" "rds_secret_versions" {
  for_each = local.configs
  secret_id = aws_secretsmanager_secret.rds_secrets[each.key].id
  secret_string = jsonencode(each.value["SecretsManager"])
}

# 数据库连接
resource "aws_glue_connection" "mariadb_connection" {
  for_each = local.configs
  name     = "connection-${each.key}"
  
  connection_properties = {
    JDBC_CONNECTION_URL     = local.connection_urls[each.key].url
    JDBC_DRIVER_CLASS_NAME  = local.connection_settings.jdbc_driver.class_name
    JDBC_DRIVER_JAR_URI     = local.connection_settings.jdbc_driver.jar_uri
    SECRET_ID               = aws_secretsmanager_secret.rds_secrets[each.key].name
  }
  
  connection_type = "JDBC"
  
  physical_connection_requirements {
    availability_zone      = local.connection_settings.availability_zone
    security_group_id_list = [local.connection_settings.security_group_id]
    subnet_id             = local.connection_settings.subnet_id
  }
}

# Glue 作业定义
resource "aws_glue_job" "gule_test_job" {
  for_each          = local.configs
  name              = "job-${each.key}"
  role_arn          = "arn:aws:iam::566601428909:role/onewonder-glue-test"
  glue_version      = local.glue_settings.glue_version
  worker_type       = local.glue_settings.worker_type
  number_of_workers = local.glue_settings.number_of_workers
  timeout           = local.glue_settings.timeout_minutes

  command {
    name            = "glueetl"
    script_location = "s3://mariabd-old/scripts/gule_test_job.py"
    python_version  = "3"
  }
  
  default_arguments = {
    "--enable-glue-datacatalog" = "true"
    "--job-language"            = "python"
    "--TempDir"                 = "s3://maria-new/temp/"
    "--spark-event-logs-path"   = "s3://maria-new/sparkHistoryLogs/"
    # S3 configurations from configs.json
    "--source_bucket"           = local.s3_paths[each.key].source.bucket
    "--source_key"              = local.s3_paths[each.key].source.key
    "--destination_bucket"      = local.s3_paths[each.key].destination.bucket
    "--destination_file"        = local.s3_paths[each.key].destination.file
    # Add secret reference
    "--secret_name"             = aws_secretsmanager_secret.rds_secrets[each.key].name
    # connection_name
    "--connection_name"         = aws_glue_connection.mariadb_connection[each.key].name
  }
  
  connections = [aws_glue_connection.mariadb_connection[each.key].name]
  
  execution_property {
    max_concurrent_runs = 1
  }
}

# Lambda function for Slack messaging
resource "aws_lambda_function" "slack_message" {
  function_name = "slack-message"
  role          = "arn:aws:iam::566601428909:role/mariabd-uuid-role-v6kz8gw0"
  handler       = "index.handler"
  runtime       = "nodejs14.x"
  timeout       = 55
  
  # Replace with your actual code location
  filename      = "lambda_function.zip"
  
  vpc_config {
    subnet_ids         = ["subnet-070b343906d45de33", "subnet-0107c820184ab021a"]
    security_group_ids = ["sg-096831a3b35acbd60", "sg-0ebcc6d9929bb55e2"]
  }
}

# S3 bucket notification configuration
resource "aws_s3_bucket_notification" "bucket_notification" {
  bucket = "slack-api-message"
  
  lambda_function {
    lambda_function_arn = aws_lambda_function.slack_message.arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = ""
    filter_suffix       = "unmatched_records.json"
  }
}

# Lambda permission to allow S3 to invoke the function
resource "aws_lambda_permission" "allow_bucket" {
  statement_id  = "AllowExecutionFromS3Bucket"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.slack_message.function_name
  principal     = "s3.amazonaws.com"
  source_arn    = "arn:aws:s3:::slack-api-message"
}
