import sys
import json
import boto3
import requests
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession, Row
# 修改这一行，确保包含udf
from pyspark.sql.functions import col, lit, when, isnan, isnull, expr, udf
from pyspark.sql.types import IntegerType

# 初始化 Spark 和 Glue 上下文
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# 定义数据库连接参数
rds_host = "onewonder-maria.cdnrnxfl6xnj.ap-northeast-1.rds.amazonaws.com"
username = "maria"
password = "wangxingran1995"
db_name = "onewonder"
table_name = "onewonder_data"

# 1. 从 S3 读取 JSON 数据
s3_client = boto3.client('s3')
response = s3_client.get_object(Bucket='mariabd-old', Key='test.json')
json_content = response['Body'].read().decode('utf-8')

# 2. 解析 JSON 并保留原始字段顺序
json_data = json.loads(json_content)
# 获取所有可能的字段集合
all_s3_fields = set()
# 跟踪原始数据顺序
s3_record_orders = {}

# 遍历所有JSON对象收集字段和记录顺序
for idx, item in enumerate(json_data):
    s3_record_orders[item.get("id")] = idx
    for field in item.keys():
        all_s3_fields.add(field)

# 从第一个对象获取基本字段顺序
base_field_order = list(json_data[0].keys() if json_data else [])
# 确保所有JSON对象的字段都被包括在内
json_field_order = base_field_order.copy()
for field in all_s3_fields:
    if field not in json_field_order:
        json_field_order.append(field)

print(f"JSON字段顺序: {json_field_order}")

# 3. 将JSON加载到Spark DataFrame
json_df = spark.read.json(
    sc.parallelize([json_content]),
    multiLine=True
)
print("JSON数据结构:")
json_df.printSchema()

# 将JSON数据中的列添加前缀
json_df_prefixed = json_df.select([col(c).alias(f"json_{c}") for c in json_df.columns])

# 4. 从RDS读取数据
maria_df = glueContext.create_dynamic_frame.from_options(
    connection_type="mysql",
    connection_options={
        "url": f"jdbc:mysql://{rds_host}:3306/{db_name}",
        "user": username,
        "password": password,
        "connectionName": "Mariadb connection",
        "dbtable": table_name
    }
).toDF()
print("MariaDB数据结构:")
maria_df.printSchema()

# 将MariaDB数据中的列添加前缀
maria_df_prefixed = maria_df.select([col(c).alias(f"maria_{c}") for c in maria_df.columns])

# 5. 获取所有可能的列（S3和RDS的并集）
s3_columns = set(json_df.columns)
rds_columns = set(maria_df.columns)
all_possible_columns = s3_columns.union(rds_columns)

# 6. 构建最终列顺序
# 首先添加JSON字段顺序中的列（按原始顺序）
final_prefixed_columns = []
for field in json_field_order:
    prefixed_field = f"json_{field}"
    if prefixed_field in json_df_prefixed.columns:
        final_prefixed_columns.append(prefixed_field)

# 然后添加在RDS中有但不在JSON字段顺序中的列
for field in rds_columns:
    prefixed_field = f"maria_{field}"
    original_field = field
    if original_field not in json_field_order and prefixed_field in maria_df_prefixed.columns:
        final_prefixed_columns.append(prefixed_field)

print(f"最终带前缀的列顺序: {final_prefixed_columns}")

# 7. 添加排序列以保持原始记录顺序
def get_order(id_val):
    return s3_record_orders.get(id_val, 999999)

# 这里使用udf，确保它已经被导入
order_udf = udf(get_order, IntegerType())
json_df_prefixed = json_df_prefixed.withColumn("original_order", order_udf(col("json_id")))

# 8. 合并数据 - 使用left join保留所有JSON记录
joined_df = json_df_prefixed.join(
    maria_df_prefixed,
    json_df_prefixed["json_id"] == maria_df_prefixed["maria_id"],
    "left"
)

# 9. 确保所有列都在结果中
result_columns = []
for col_name in final_prefixed_columns:
    if col_name in joined_df.columns:
        result_columns.append(col_name)
    else:
        # 如果列不存在，添加空列
        joined_df = joined_df.withColumn(col_name, lit(None))
        result_columns.append(col_name)

# 10. 选择最终列并排序
result_df = joined_df.select(result_columns + ["original_order"]).orderBy("original_order")

# 删除排序列
result_df = result_df.drop("original_order")

# 11. 将结果写入CSV
result_df.coalesce(1).write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("s3://maria-new/temp-output/")

# 12. 移动并重命名CSV文件
s3_resource = boto3.resource('s3')
bucket = s3_resource.Bucket('maria-new')
csv_file = None

for obj in bucket.objects.filter(Prefix='temp-output/'):
    if obj.key.endswith('.csv'):
        csv_file = obj.key
        break

if csv_file:
    s3_resource.Object('maria-new', 'test.csv').copy_from(
        CopySource={'Bucket': 'maria-new', 'Key': csv_file}
    )
    
    for obj in bucket.objects.filter(Prefix='temp-output/'):
        obj.delete()

# 13. 查找在RDS中存在但S3中不存在的ID，并发送Slack通知
json_ids = [row["json_id"] for row in json_df_prefixed.select("json_id").collect()]
maria_ids = [row["maria_id"] for row in maria_df_prefixed.select("maria_id").collect()]
missing_ids = set(maria_ids) - set(json_ids)

if missing_ids:
    message_lines = [f"数据库 {db_name} 中存在以下ID在S3中不存在:"]
    for missing_id in missing_ids:
        message_lines.append(f"TABLE：{table_name} UUID： {missing_id}")
    
    formatted_message = "\n".join(message_lines)
    
    webhook_url = "https://hooks.slack.com/services/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"
    slack_message = {
        "text": formatted_message
    }
    response = requests.post(webhook_url, json=slack_message)
    print(f"Slack通知已发送，状态码: {response.status_code}")

# 提交作业
job.commit()