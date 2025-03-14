<img src="image/logo.jpg" alt="公司 Logo" style="width: 100%; display: block;">

- [OneWonder](https://next.onewonder.co.jp/) はクラウドコンピューティングに特化したサービスを提供しています

## 使用技術一覧

<img src="https://img.shields.io/badge/-python-5391FE.svg?logo=python&style=popout">
<img src="https://img.shields.io/badge/-Amazon%20aws-232F3E.svg?logo=amazon-aws&style=popout">

## プロジェクト名

rds-glue-s3-etl-pipeline

## プロジェクト概要

本プロジェクトは AWS Glue（PySpark）をデータ処理エンジンとして活用し、AWS Secrets Manager でデータベース認証情報を安全に管理しています。また、Webhook 経由で Slack 通知機能を統合し、完全な自動データ処理と監視フローを実現しています。このアーキテクチャは、JSON データとリレーショナルデータベースのデータを定期的に統合し、特定の形式を維持する必要があるシナリオに最適です。

## アーキテクチャ図

- [glue.drawio](docs)

<img src="image/glue.drawio.png" alt="アーキテクチャ図" style="width: 100%;">

### 処理フロー

① **データセキュリティ**: シークレットマネージャー（rds-secret-onewonder）からデータベース認証情報を取得し、中央の job-wonder 処理コンポーネントに渡します。</br>
② **データベース接続**: 取得した認証情報を使用して上部の megaRDS データベースに接続し、データを読み取ります。</br>
③ **入力データ取得**: 左側の S3 バケットから test.json ファイルを入力データソースとして読み取ります。</br>
④ **状態通知**: 下部に接続されている Slack API を通じて、処理状態と結果通知を Slack に送信します。</br>
⑤ **出力データ保存**: 処理完了後、結果データを test.csv 形式で右側の S3 バケットに出力します。</br>

全体のプロセスは黄色の枠（Terraform 化にする）内で実行され、中心の Glue(job-wonder)がすべてのデータフローと変換操作を調整・処理します。

# GLue 逻辑处理流程

- [gule_test_job.py](gule_test_job.py)

```mermaid
flowchart TD

    A[Spark/Glue コンテキストの初期化] --> B[タスクパラメータの取得]
    B --> C[AWS Secrets Manager からデータベース認証情報を取得]
    C --> D[S3 から JSON データを読み込み]
    D -->|成功| F[RDS からデータを読み込み]
    D -->|失敗| E[Slack にエラー通知を送信して終了]
    F -->|成功| G[両方のデータソースに id 列があることを確認]
    F -->|失敗| E
    G -->|検証通過| H[すべての可能な列を取得]
    G -->|検証失敗| E
    H --> I[S3 データからすべての ID を収集]
    I --> J[S3 データを反復処理]
    J --> K[一致を見つける] & M[一致が見つからない]
    K -->|一致を見つけた| L[データ結合: S3 値を優先し、不足時は RDS 値を使用]
    K -->|一致が見つからない| M[S3 データのみを使用]
    L --> N[結果セットに追加]
    M --> N
    N --> O[最終 DataFrame を作成し列順序を維持]
    O --> P[RDS にあるが S3 にない記録を検索]
    P --> Q[未一致レコードがある?]
    Q -->|はい| R[未一致レコード情報を Slack に送信]
    Q -->|いいえ| S[結果を CSV に書き込み]
    R --> S
    S -->|成功| T[成功通知を Slack に送信]
    S -->|失敗| E
    T --> U[ジョブ完了]
```
#### 連絡先
蔡：inje.sai@onewonder.co.jp</br>

王：xingran.wang@onewonder.co.jp

#### SlackApiについて
もしslackapiがパブリックリポジトリにコミットしたら無効になれます。