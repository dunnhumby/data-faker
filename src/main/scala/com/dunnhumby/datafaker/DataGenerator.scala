package com.dunnhumby.datafaker

import com.dunnhumby.datafaker.schema.Schema
import org.apache.log4j._
import org.apache.spark.sql.{SaveMode, SparkSession}

class DataGenerator(spark: SparkSession, database: String) extends Serializable {

  private val logger = Logger.getLogger(getClass)
  logger.setLevel(Level.INFO)

  def generateAndWriteDataFromSchema(schema: Schema) {
    import spark.sqlContext.implicits._

    for (table <- schema.tables) {
      logger.info(s"generating ${table.rows} rows for ${table.name}")

      val dataFrame = table.columns.foldLeft(spark.sparkContext.range(0, table.rows).toDF("temp"))((a, b) => {
        a.withColumn(b.name, b.column)
      }).drop("temp")

      logger.info(s"writing ${table.rows} rows for ${table.name}")

      val partitions = table.partitions.getOrElse(List.empty[String])
      dataFrame.write.mode(SaveMode.Overwrite).partitionBy(partitions: _*).saveAsTable(s"$database.${table.name}")

      logger.info(s"${table.name} - complete")
    }
  }

}
