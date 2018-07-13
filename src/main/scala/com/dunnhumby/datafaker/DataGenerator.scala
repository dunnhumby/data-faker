package com.dunnhumby.datafaker

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DataGenerator {
  def apply(spark: SparkSession, database: String): DataGenerator = {
    new DataGenerator(spark, database)
  }
}

class DataGenerator(spark: SparkSession, database: String) extends Serializable {

  import spark.implicits._

  def generateAndWriteCardDimCData(numCards: Int) {
    val dataFrame = spark.sparkContext.range(0, numCards).map(f => CardDim.random(numCards, f)).toDF
    dataFrame.write.mode("overwrite").saveAsTable(s"$database.${Table.cards}")
  }

  def generateAndWriteTransactionItemFctData(numCards: Int, numRecordsPerCard: Int, partitionByCol: String) {
    val dataFrame = spark.sparkContext.range(0, numCards * numRecordsPerCard).map(f => Item.random((f / numRecordsPerCard).toInt)).toDF
    dataFrame.write.mode("overwrite").partitionBy("date_id").saveAsTable(s"$database.${Table.transactions}")
  }

  def generateAndWriteSuppressions(identifiers: Array[(String, String, Double)], tableName: String) {
    val table = spark.table(s"$database.$tableName")
    val dataFrame = identifiers.map {
      case (identifierType, column, fraction) => table.select(table.col(column).alias("identifier")).sample(withReplacement = false, fraction).withColumn("identifier_type", lit(identifierType))
    }.reduce((d1, d2) => d1.union(d2))

    dataFrame.write.mode("overwrite").saveAsTable(s"$database.${tableName}_suppressions")
  }
}

object Table {
  val transactions = "transaction_item_fct"
  val cards = "card_dim_c"
  val transaction_suppressions = s"${transactions}_suppressions"
  val card_suppressions = s"${cards}_suppressions"
}