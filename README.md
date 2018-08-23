# A Scala Application for Generating Fake Datasets

**The tool can generate any format given a provided schema, for example generate cards, transaction, and suppression data.**

The application requires a **yaml file** specifying the schema of tables to be generated.

## Usage

Submit the jar artifact to a spark cluster with hive enabled, with the following arguments:

1. `--database` - Name of the **Hive database** to write the tables to.
2. `--file` - Path to the yaml file.

### Example Yaml File Tables

``` yaml
tables:
- name: card_dim_c
  rows: 10
  columns:
  - name: card_id
    data_type: Int
    column_type: Sequential
    start: 0
    step: 1
  - name: card_code
    column_type: Expression
    expression: concat('0000000000', card_id)
  - name: hshd_id
    data_type: Int
    column_type: Sequential
    start: 0
    step: 1
  - name: hshd_code
    column_type: Expression
    expression: concat('0000000000', hshd_id)
  - name: prsn_id
    data_type: Int
    column_type: Sequential
    start: 0
    step: 1
  - name: prsn_code
    column_type: Expression
    expression: concat('0000000000', prsn_id)
  - name: hshd_isba_market_code
    column_type: Expression
    expression: concat('isba', hshd_code)

- name: transaction_item_fct_data
  rows: 100
  columns:
  - name: card_id
    data_type: Int
    column_type: Random
    min: 0
    max: 10 # number of cards generated
  - name: prod_id
    data_type: Int
    column_type: Random
    min: 0
    max: 1000
  - name: store_id
    data_type: Int
    column_type: Random
    min: 0
    max: 10
  - name: item_qty
    data_type: Int
    column_type: Random
    min: 0
    max: 10
  - name: item_cost
    data_type: Float
    column_type: Random
    min: 1
    max: 5
    decimal_places: 2
  - name: item_discount
    data_type: Float
    column_type: Random
    min: 1
    max: 2
    decimal_places: 2
  - name: spend_amt
    column_type: Expression
    expression: round((item_cost * item_discount) * item_qty, 2)
  - name: date_id
    data_type: Date
    column_type: Random
    min: 2017-01-01
    max: 2018-01-01
  partitions:
    - date_id

- name: card_dim_c_suppressions
  rows: 10
  columns:
  - name: identifier
    data_type: Int
    column_type: Random
    min: 0
    max: 10 # number of cards generated
  - name: identifier_type
    data_type: String
    column_type: Fixed
    value: card_id
```

#### card_dim_c

| card_id | card_code  | hshd_id | hshd_code  | prsn_id | prsn_code  | hshd_isba_market_code |
|---------|------------|---------|------------|---------|------------|-----------------------|
| 0       | 0000000000 | 9       | 0000000009 | 3       | 0000000003 | isba0000000009        |
| 1       | 0000000001 | 8       | 0000000008 | 8       | 0000000008 | isba0000000008        |
| 2       | 0000000002 | 4       | 0000000004 | 0       | 0000000000 | isba0000000004        |

#### transaction_item_fct_data


| card_id | prod_id |   store_id | item_qty | item_distcount_amt | spend_amt | date_id    | net_spend_amt |
|---------|---------|------------|----------|--------------------|-----------|------------|---------------|
| 0       | 25      | 4          | 1        | 2                  | 60        | 2018-06-03 | 58            |
| 1       | 337     | 8          | 3        | 8                  | 47        | 2018-04-12 | 117           |
| 2       | 550     | 2          | 6        | 0                  | 23        | 2018-07-09 | 138           |

#### card_dim_c_suppressions

| identifier | identifier_type |
|------------|-----------------|
| 0          | card_id         |
| 10         | card_id         |
| 34         | card_id         |

### Example:

Call **datafaker** with **example.yaml**

#### Execute against GCP:

```
gcloud dataproc jobs submit spark --cluster <dataproc clustername> --region <region> \
  --jar <datafaker-jar> --files <data-spec-yaml> -- --database <database> --file <data-spec-yaml>
```
Example, submit the datafaker jar to GCP with a spec file `example.yaml`, both in the current directory:
```
gcloud dataproc jobs submit spark --cluster dh-data-dev --region europe-west1 \
  --jar datafaker-assembly-0.1-SNAPSHOT.jar --files example.yaml -- --database dev_db --file example.yaml
  ```
#### Execute with docker:

This can be deployed to with our [docker spark cluster](https://dhgitlab.dunnhumby.co.uk/core-data-engineering/docker-hadoop-spark-workbench).

- Checkout Project
- Deploy cluster with `docker compose -f compose-spark.yml up -d`
- Submit Spark job
  - with both the datafaker jar and `example.yaml` in the current directory, along with the `hadoop-hive.xml`


```bash
docker run --net docker-hadoop-spark-workbench_spark-net --name submit --rm \
  -v $PWD:/app --env-file hadoop-hive.env bde2020/spark-worker:2.1.0-hadoop2.8-hive-java8 /spark/bin/spark-submit \
  --files /app/example.yaml --master spark://spark-master:7077 /app/datafaker-assembly-0.1-SNAPSHOT.jar \
  --database test --file /app/example.yaml
```
Note: 
- **hadoop-hive.env** is located in the **docker-hadoop-spark-workbench** directory
- Mount the directory containing the **example.yaml** and **datafaker.jar** to app


#### Execute locally:

```
spark-submit --master local datafaker-assembly-0.1-SNAPSHOT.jar --database test --file example.yaml 
```

###  Column Types

#### - Fixed

Supported Data Types: `Int`, `Long`, `Float`, `Double`, `Date`, `Timestamp`, `String`, `Boolean`

`value` - column value

#### - Random

Supported Data Types: `Int`, `Long`, `Float`, `Double`, `Date`, `Timestamp`, `Boolean`

`min` - minimum bound of random data (inclusive)

`max` - maximum bound of random data (inclusive) 

#### - Selection

Supported Data Types: `Int`, `Long`, `Float`, `Double`, `Date`, `Timestamp`, `String`

`values` - set of values to be chosen from

#### - Sequential

Supported Data Types: `Int`, `Long`, `Float`, `Double`, `Date`, `Timestamp`

`start` - start value

`step` - increment between each row

#### - Expression

`expression` - a spark sql expression



### Build Artifact

This project is written in Scala.

We compile a **fat jar** of the application, including all dependencies.

Build the jar with `sbt assembly` from the project's base directory, the artifact is written to `target/scala-2.11/`