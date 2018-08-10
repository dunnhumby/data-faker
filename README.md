# A Scala Application for Generating Fake Datasets

The tool can generate any format given a provided schema, for example generate card, card transaction, and suppression data.

The application requires a **yaml file** specifying the schema of tables to be generated.

## Usage

1. `--database` - Name of the Hive database to write the tables to.
2. `--file` - Path to the yaml file.

### Example Yaml File Tables

#### Card Table Structure

| card_id | card_code  | hshd_id | hshd_code  | prsn_id | prsn_code  | hshd_isba_market_code |
|---------|------------|---------|------------|---------|------------|-----------------------|
| 0       | 0000000000 | 9       | 0000000009 | 3       | 0000000003 | isba0000000009        |
| 1       | 0000000001 | 8       | 0000000008 | 8       | 0000000008 | isba0000000008        |
| 2       | 0000000002 | 4       | 0000000004 | 0       | 0000000000 | isba0000000004        |

#### Item Table Structure


| card_id | prod_id |   store_id | item_qty | item_distcount_amt | spend_amt | date_id    | net_spend_amt |
|---------|---------|------------|----------|--------------------|-----------|------------|---------------|
| 0       | 25      | 4          | 1        | 2                  | 60        | 2018-06-03 | 58            |
| 1       | 337     | 8          | 3        | 8                  | 47        | 2018-04-12 | 117           |
| 2       | 550     | 2          | 6        | 0                  | 23        | 2018-07-09 | 138           |

#### Card Suppression Table Structure

| identifier | identifier_type |
|------------|-----------------|
| 0          | card_id         |
| 10         | card_id         |
| 34         | card_id         |

### Example:

Call **datafaker** with **example.yaml**

#### Execute against GCP:

```
gcloud dataproc jobs submit spark --cluster <cluster-name> --region europe-west1 \
--jar /path/to/jar/datafaker-assembly-0.1-SNAPSHOT.jar --files example.yaml -- --database dev_db --file example.yaml
```

#### Execute with docker:

This can be deployed to with our [docker spark cluster](https://dhgitlab.dunnhumby.co.uk/core-data-engineering/docker-hadoop-spark-workbench).

- Checkout project
- Deploy cluster with `docker compose -f compose-spark.yml up`
- Submit spark job e.g.

```
docker run --net docker-hadoop-spark-workbench_spark-net --name submit --rm -v /absolute/path/to/directory:/app \
--env-file /relative/path/to/hadoop-hive.env bde2020/spark-worker:2.1.0-hadoop2.8-hive-java8 /spark/bin/spark-submit \
--files app/relative/path/to/yaml-config.yaml --master spark://spark-master:7077 app/relative/path/to/jar/datafaker.jar 
--database dev_db 
--file app/relative/path/to/jar/yaml-config.yaml
```

Note: 
- **hadoop-hive.env** should be located in the **docker-hadoop-spark-workbench** directory.
- `-v /absolute/path/to/directory:/app` should mount the directory containing the **yaml-config.yaml** and **datafaker.jar** to app.

###  Column Types

#### - Fixed

Supported Data Types: Int, Long, Float, Double, Date, Timestamp, String, Boolean

`value` - column value

#### - Random

Supported Data Types: Int, Long, Float, Double, Date, Timestamp, Boolean

`min` - minimum bound of random data (inclusive)

`max` - maximum bound of random data (inclusive) 

#### - Selection

Supported Data Types: Int, Long, Float, Double, Date, Timestamp, String

`values` - set of values to be chosen from

#### - Sequential

Supported Data Types: Int, Long, Float, Double, Date, Timestamp

`start` - start value

`step` - increment between each row

#### - Expression

`expression` - a spark sql expression



### Build Artifact
This project is written in Scala.

We compile a **fat jar** of the application, including all dependencies.

Build the jar with `sbt assembly` from the project's base directory, the artifact is written to `target/scala-2.11/`