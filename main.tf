# 数据库连接
resource "aws_glue_connection" "mariadb_connection" {
  name = "terrfromConnectortest"
  
  connection_properties = {
    JDBC_CONNECTION_URL = "jdbc:mysql://onewonder-maria.cdnrnxfl6xnj.ap-northeast-1.rds.amazonaws.com:3306/onewonder"
    JDBC_DRIVER_CLASS_NAME = "org.mariadb.jdbc.Driver"
    JDBC_DRIVER_JAR_URI = "s3://mariabd-old/mariadb-java-client-2.7.2.jar"
    USERNAME = "maria"
    PASSWORD = "wangxingran1995"  # 在生产环境中应使用 AWS Secrets Manager
  }
  
  connection_type = "JDBC"
  
  physical_connection_requirements {
    availability_zone      = "ap-northeast-1a"  # 根据子网所在可用区调整
    security_group_id_list = ["sg-0e829cc882b945ce9"]
    subnet_id              = "subnet-070b343906d45de33"
  }
  
  tags = {
    VPC = "vpc-0ae395ea7178dd0e1"
  }
}

# Glue 作业定义
resource "aws_glue_job" "gule_test_job" {
  name              = "GuleterrfromConnectorjob"
  role_arn          = "arn:aws:iam::566601428909:role/onewonder-glue-test"
  glue_version      = "5.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 2880 # 默认超时时间，可根据需要调整

  command {
    name            = "glueetl"
    script_location = "s3://mariabd-old/scripts/gule_test_job.py"  # 脚本位置 - 会通过 YAML 工作流更新
    python_version  = "3"
  }
  
  default_arguments = {
    "--enable-glue-datacatalog" = "true"
    "--job-language"            = "python"
    "--TempDir"                 = "s3://maria-new/temp/"
    "--spark-event-logs-path"   = "s3://maria-new/sparkHistoryLogs/"
  }

  connections = [aws_glue_connection.mariadb_connection.name]
  
  execution_property {
    max_concurrent_runs = 1
  }
}
