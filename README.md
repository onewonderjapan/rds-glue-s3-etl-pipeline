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
① **データセキュリティ**: シークレットマネージャー（rds-secret-onewonder）からデータベース認証情報を取得し、中央のjob-wonder処理コンポーネントに渡します。</br>
② **データベース接続**: 取得した認証情報を使用して上部のmegaRDSデータベースに接続し、データを読み取ります。</br>
③ **入力データ取得**: 左側のS3バケットからtest.jsonファイルを入力データソースとして読み取ります。</br>
④ **状態通知**: 下部に接続されているSlack APIを通じて、処理状態と結果通知をSlackに送信します。</br>
⑤ **出力データ保存**: 処理完了後、結果データをtest.csv形式で右側のS3バケットに出力します。</br>

全体のプロセスは黄色の枠（Terraform化にする）内で実行され、中心のGlue(job-wonder)がすべてのデータフローと変換操作を調整・処理します。
# GLue逻辑处理流程
- [gule_test_job.py](gule_test_job.py)

%%{init: {'flowchart': {'nodeSpacing': 30, 'rankSpacing': 80, 'width': 700}}}%%
flowchart LR
    subgraph 初始化
    A[初始化] --> B[获取参数]
    B --> C[获取凭证]
    end
    
    subgraph 数据读取
    C --> D[读取S3]
    D -->|成功| F[读取RDS]
    D -->|失败| E[错误通知]
    end
    
    subgraph 数据处理
    F -->|成功| G[验证ID列]
    F -->|失败| E
    G -->|通过| H[获取所有列]
    G -->|失败| E
    H --> I[收集S3 ID]
    I --> J[处理S3数据]
    end
    
    subgraph 数据合并
    J --> K[匹配处理]
    K --> N[创建结果集]
    N --> O[最终DataFrame]
    end
    
    subgraph 输出处理
    O --> P[查找未匹配记录]
    P --> Q{有未匹配?}
    Q -->|是| R[发送通知]
    Q -->|否| S[写入CSV]
    R --> S
    S -->|成功| T[成功通知]
    S -->|失败| E
    T --> U[完成]
    end