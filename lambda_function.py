import json
import urllib.request
import boto3
import os

def lambda_handler(event, context):
    # Slack Webhook URL
    webhook_url = "https://hooks.slack.com/services/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"
    
    try:
        # 从S3事件中获取bucket和key信息
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        
        # 创建S3客户端
        s3_client = boto3.client('s3')
        
        # 从S3下载文件内容
        response = s3_client.get_object(Bucket=bucket, Key=key)
        json_content = response['Body'].read().decode('utf-8')
        
        # 解析JSON内容
        data = json.loads(json_content)
        
        # 准备发送到Slack的消息
        message = {
            "text": f"文件 `{key}` 已更新",
            "blocks": [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": f"S3文件更新通知: {key}"
                    }
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*未匹配记录内容:*\n```{json.dumps(data, indent=2, ensure_ascii=False)}```"
                    }
                }
            ]
        }
        
        # 发送到Slack
        req = urllib.request.Request(
            webhook_url,
            data=json.dumps(message).encode('utf-8'),
            headers={'Content-Type': 'application/json'}
        )
        
        response = urllib.request.urlopen(req)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Successfully sent to Slack',
                'file': key
            })
        }
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': f'Error: {str(e)}'
            })
        }