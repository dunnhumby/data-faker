# Fake Data Generator in Scala

A command-line tool to generate card, card transaction, and supression data.

The tool requires the **number of cards** and can also generate the **number of records per card**.

## Usage

1. `--num-cards` - The number of cards to generate.
2. `--records-per-card` - The number of records per card to generate.

### Card Table Structure

| card_id | card_code  | hshd_id | hshd_code  | prsn_id | prsn_code  | hshd_isba_market_code |
|---------|------------|---------|------------|---------|------------|-----------------------|
| 0       | 0000000000 | 9       | 0000000009 | 3       | 0000000003 | isba0000000009        |
| 1       | 0000000001 | 8       | 0000000008 | 8       | 0000000008 | isba0000000008        |
| 2       | 0000000002 | 4       | 0000000004 | 0       | 0000000000 | isba0000000004        |

### Item Table Structure


| card_id | prod_id |   store_id | item_qty | item_distcount_amt | spend_amt | date_id    | net_spend_amt |
|---------|---------|------------|----------|--------------------|-----------|------------|---------------|
| 0       | 25      | 4          | 1        | 2                  | 60        | 2018-06-03 | 58            |
| 1       | 337     | 8          | 3        | 8                  | 47        | 2018-04-12 | 117           |
| 2       | 550     | 2          | 6        | 0                  | 23        | 2018-07-09 | 138           |

### Example:

Call **datafaker** with 100 cards and 100 transaction records for each card.

#### Execute against GCP:

```
gcloud dataproc jobs submit spark --cluster data-dev-dataplatform-dataproc \
    --region europe-west1 \
    --jar datafaker-assembly-0.1-SNAPSHOT.jar \
    -- --num-cards 100 --records-per-card 100
```

#### Execute with docker:

```
docker run --net docker-hadoop-spark-workbench_spark-net \
--name submit \
--rm -it \
-v $PWD:/app \
--env-file $HOME/core-data-engineering/docker-hadoop-spark-workbench/hadoop-hive.env \
bde2020/spark-worker:2.1.0-hadoop2.8-hive-java8 \
/spark/bin/spark-submit --master spark://spark-master:7077 /app/datafaker-assembly-0.1-SNAPSHOT.jar --num-cards 10 --records-per-card 10
```

### Build Artifact
This project is written in Scala.

We compile a **fat jar** of the application, including all dependencies.

Build the jar with `sbt assembly` from the project's base directory, the artifact is written to `target/scala-2.11/`