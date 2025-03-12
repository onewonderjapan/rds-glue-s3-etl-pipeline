# 读取配置文件
locals {
  # 读取configs.json
  configs_raw = jsondecode(file("configs.json"))
  configs = local.configs_raw.configs

  # 读取common_settings.json
  common_settings = jsondecode(file("common_settings.json"))
  
  # Glue 设置
  glue_settings = local.common_settings.Glue
  
  # 连接设置
  connection_settings = local.common_settings.connection

  # 为每个配置生成连接URL
  connection_urls = {
    for key, value in local.configs : key => {
      url = "jdbc:mysql://onewonder-maria.cdnrnxfl6xnj.ap-northeast-1.rds.amazonaws.com:3306/{{resolve:secretsmanager:rds-secret-${key}:SecretString:db_name}}"
    }
  }

  # 为每个配置生成S3路径
  s3_paths = {
    for key, value in local.configs : key => {
      source = {
        bucket = value.S3.source_bucket
        key    = value.S3.source_key
      }
      destination = {
        bucket = value.S3.destination_bucket
        file   = value.S3.destination_file
      }
      temp = value.Glue.temp_output_path
    }
  }
}

# 输出配置以供检查
output "available_configs" {
  value = keys(local.configs)
  description = "Available configuration keys"
}

output "glue_settings" {
  value = local.glue_settings
  description = "Glue common settings"
}

output "connection_settings" {
  value = local.connection_settings
  description = "Connection common settings"
} 