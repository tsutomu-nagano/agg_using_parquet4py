# agg_using_parquet4py

Pythonでparquetを使用した集計処理のサンプル

## requirement

- python

  ``` shell
  $ python --version
  Python 3.10.13
  ```

- requirements.txt
  
  ``` shell
  numpy==1.26.1
  pandas==2.1.2
  pyarrow==13.0.0
  PyYaml==6.0.1
  ```

## description

- 集計処理の指示はsetting.yamlでおこないます

    ``` yaml
    - id: なんでもOK
      description: この集計指示の概要を記載
      src: data/src/inp_100000_col4.csv
      dest: data/dest/out_100000_col4_csv_Ken_Sei_Age.csv
      dimension:
        - name: 分類事項として使用する列名１
          position: データ中の位置
        - name: 分類事項として使用する列名２
          position: データ中の位置
      measure:
        - name: 集計事項として使用する列名
          position: データ中の位置
    ```

- setting.yamlはdataディレクトリ直下に置いてください
  - 直下でなくてもよいのですが、Program.csのMainで引数として受けてますが、quickstartで使用するdokcer-compose内でdata直下にある想定で実行しているので、dockerで実行する場合はdataディレクトリ直下に置いてください
- 集計結果はログとして出力されます
- 各種ファイルの配置場所は以下で

  ``` shell
  data
  ├── log_yyyyMMdd_HHmmss.csv
  └── setting.yaml
  ```

## quickstart

- 入出力、使用する事項の情報をsetting.yamlに記載
- docker-compose up で実行

``` sh
cd agg_using_parquet4py
docker-compose up 
```

## memo

- parquetをinputにする処理はpandasのchunkの処理が実装されていません。そのため、メモリに乗り切らない場合に処理できない可能性があります。
  - chunkの処理の実装が必要
- pysparkを使えば並列処理も簡単に実装できそう
