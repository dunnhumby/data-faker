# data-faker

[![Build Status](https://travis-ci.org/dunnhumby/data-faker.svg?branch=master)](https://travis-ci.org/dunnhumby/data-faker)

**A Scala Application for Generating Fake Datasets with Spark**

**The tool can generate any format given a provided schema, for example generate customers, transactions, and products.**

The application requires a **yaml file** specifying the schema of tables to be generated.

## Usage

Submit the jar artifact to a spark cluster with hive enabled, with the following arguments:

1. `--database` - Name of the **Hive database** to write the tables to.
2. `--file` - Path to the yaml file.

### Example Yaml File Tables

``` yaml
tables:
- name: customers
  rows: 10
  columns:
  - name: customer_id
    data_type: Int
    column_type: Sequential
    start: 0
    step: 1
  - name: customer_code
    column_type: Expression
    expression: concat('0000000000', customer_id)

- name: products
  rows: 200
  columns:
  - name: product_id
    data_type: Int
    column_type: Sequential
    start: 0
    step: 1
  - name: product_code
    column_type: Expression
    expression: concat('0000000000', product_id)

- name: transactions
  rows: 100
  columns:
  - name: customer_id
    data_type: Int
    column_type: Random
    min: 0
    max: 10 # number of customers generated
  - name: product_id
    data_type: Int
    column_type: Random
    min: 0
    max: 200
  - name: quantity
    data_type: Int
    column_type: Random
    min: 0
    max: 10
  - name: cost
    data_type: Float
    column_type: Random
    min: 1
    max: 5
    decimal_places: 2
  - name: discount
    data_type: Float
    column_type: Random
    min: 1
    max: 2
    decimal_places: 2
  - name: spend
    column_type: Expression
    expression: round((cost * discount) * quantity, 2)
  - name: date
    data_type: Date
    column_type: Random
    min: 2017-01-01
    max: 2018-01-01
  partitions:
    - date
```

#### customers

| customer_id | customer_code  |
|-------------|----------------|
| 0           | 0000000000     |
| 1           | 0000000001     |
| 2           | 0000000002     |

#### products

| product_id  | product_code   |
|-------------|----------------|
| 0           | 0000000000     |
| 1           | 0000000001     |
| 2           | 0000000002     |

#### transactions 

| customer_id | product_id | quantity | cost     | discount | spend | date       |
|-------------|------------|----------|----------|----------|-------|------------|
| 0           | 25         | 1        | 1.53     | 1.2      | 1.83  | 2018-06-03 |
| 1           | 337        | 3        | 0.34     | 1.64     | 1.22  | 2018-04-12 |
| 2           | 550        | 6        | 4.84     | 1.03     | 29.91 | 2018-07-09 |

### Example:

Call **datafaker** with **example.yaml**



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