<img src="image/logo.jpg" alt="公司 Logo" style="width: 100%; display: block;">

- [OneWonder](https://www.onewonder.co.jp/) はクラウドコンピューティングに特化したサービスを提供しています
## 使用技術一覧

<img src="https://img.shields.io/badge/-python-5391FE.svg?logo=python&style=popout">
<img src="https://img.shields.io/badge/-Amazon%20aws-232F3E.svg?logo=amazon-aws&style=popout">

## プロジェクト名
rds-glue-s3-etl-pipeline

## プロジェクト概要
本プロジェクトはAWS Glue（PySpark）をデータ処理エンジンとして活用し、AWS Secrets Managerでデータベース認証情報を安全に管理しています。また、Webhook経由でSlack通知機能を統合し、完全な自動データ処理と監視フローを実現しています。このアーキテクチャは、JSONデータとリレーショナルデータベースのデータを定期的に統合し、特定の形式を維持する必要があるシナリオに最適です。

## アーキテクチャ図
- [glue.drawio](docs)
<img src="image/glue.drawio.png" alt="アーキテクチャ図" style="width: 100%;">

### 処理フロー
① **データセキュリティ**: 赤色の鍵アイコン（rds-secret-onewonder）からデータベース認証情報を取得し、中央のjob-wonder処理コンポーネントに渡します。
② **データベース接続**: 取得した認証情報を使用して上部のmegaRDSデータベースに接続し、データを読み取ります。
③ **入力データ取得**: 左側のS3バケットからtest.jsonファイルを入力データソースとして読み取ります。
④ **状態通知**: 下部に接続されているSlack APIを通じて、処理状態と結果通知をSlackに送信します。
⑤ **出力データ保存**: 処理完了後、結果データをtest.csv形式で右側のS3バケットに出力します。

全体のプロセスは黄色の枠（AWS GlueまたはLambda環境を表す）内で実行され、中心のjob-wonderコンポーネントがすべてのデータフローと変換操作を調整・処理します。
# GLue逻辑处理流程
- [gule_test_job.py](gule_test_job.py)
```mermaid
flowchart TD
    A[初始化 Spark/Glue 上下文] --> B[获取任务参数]
    B --> C[从 AWS Secrets Manager 获取数据库凭证]
    C --> D[从 S3 读取 JSON 数据]
    D -->|成功| F[从 RDS 读取数据]
    D -->|失败| E[发送错误通知到 Slack 并退出]
    F -->|成功| G[验证两个数据源都有 id 列]
    F -->|失败| E
    G -->|验证通过| H[获取所有可能的列]
    G -->|验证失败| E
    H --> I[收集 S3 数据中的所有 ID]
    I --> J[遍历处理 S3 数据]
    J --> K[找到匹配] & M[未找到匹配]
    K -->|找到匹配| L[合并数据: 优先使用 S3 值，缺失时使用 RDS 值]
    K -->|未找到匹配| M[只使用 S3 数据]
    L --> N[新加到结果集]
    M --> N
    N --> O[创建最终 DataFrame 并保持列顺序]
    O --> P[查找 RDS 中有但 S3 中没有的记录]
    P --> Q[有未匹配记录?]
    Q -->|是| R[发送未匹配记录信息到 Slack]
    Q -->|否| S[将结果写入 CSV]
    R --> S
    S -->|成功| T[发送成功通知到 Slack]
    S -->|失败| E
    T --> U[完成作业]