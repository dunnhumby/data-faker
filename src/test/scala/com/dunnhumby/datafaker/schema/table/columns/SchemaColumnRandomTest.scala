
package com.dunnhumby.datafaker.schema.table.columns

import java.sql.{Date, Timestamp}
import com.dunnhumby.datafaker.{SharedSparkSession}
import org.scalatest.{Inspectors, MustMatchers, WordSpec}

class SchemaColumnRandomTest extends WordSpec with SharedSparkSession with MustMatchers  with Inspectors {

  import com.dunnhumby.datafaker.schema.table.columns.SchemaColumnRandomProtocol._
  import net.jcazevedo.moultingyaml._

  val name = "test"
  val column_type = "Random"

  val baseString =
    s"""name: $name
       |column_type: $column_type
    """.stripMargin

  "SchemaColumnRandom" must {
    "read an Int column" in {
      val string =
        s"""$baseString
           |data_type: ${SchemaColumnDataType.Int}
           |min: 1
           |max: 10
         """.stripMargin

      string.parseYaml.convertTo[SchemaColumnRandom[_]] mustBe SchemaColumnRandom(name, 1, 10)
    }

    "read a Long column" in {
      val string =
        s"""$baseString
           |data_type: ${SchemaColumnDataType.Long}
           |min: 1
           |max: 10
         """.stripMargin

      string.parseYaml.convertTo[SchemaColumnRandom[_]] mustBe SchemaColumnRandom(name, 1l, 10l)
    }

    "read a Float column" in {
      val string =
        s"""$baseString
           |data_type: ${SchemaColumnDataType.Float}
           |min: 1.0
           |max: 10.0
         """.stripMargin

      string.parseYaml.convertTo[SchemaColumnRandom[_]] mustBe SchemaColumnRandom(name, 1f, 10f)
    }

    "read a Double column" in {
      val string =
        s"""$baseString
           |data_type: ${SchemaColumnDataType.Double}
           |min: 1.0
           |max: 10.0
         """.stripMargin

      string.parseYaml.convertTo[SchemaColumnRandom[_]] mustBe SchemaColumnRandom(name, 1d, 10d)
    }

    "generate numeric values between a min and a max" in {
      val dataFrame = spark.range(1000).toDF("temp")
        .withColumn("Numeric", SchemaColumnRandom("", 1, 10).column())
        .drop("temp")

      val testData = dataFrame.select("Numeric").collect().map(_.getInt(0))
      testData must contain only (1 to 10: _*)
    }

    "read a Date column" in {
      val minDate = Date.valueOf("2000-01-01")
      val maxDate = Date.valueOf("2000-01-07")

      val string =
        s"""$baseString
           |data_type: ${SchemaColumnDataType.Date}
           |min: ${minDate.toString}
           |max: ${maxDate.toString}
         """.stripMargin

      string.parseYaml.convertTo[SchemaColumnRandom[_]] mustBe SchemaColumnRandom(name, minDate, maxDate)
    }

    "generate a Date between a min and a max date" in {
      val minDate = Date.valueOf("2000-01-01")
      val maxDate = Date.valueOf("2000-01-07")

      val dataFrame = spark.range(1000).toDF("temp")
        .withColumn("Date", SchemaColumnRandom("", minDate, maxDate).column())
        .drop("temp")

      val testData = dataFrame.select("Date").collect().map(_.getDate(0))
      forAll(testData)(f => f.after(minDate) && f.before(maxDate))
    }

    "read a Timestamp column" in {
      val minTimestamp = Timestamp.valueOf("2000-01-01 00:00:00")
      val maxTimestamp = Timestamp.valueOf("2000-01-07 23:59:59")

      val string =
        s"""$baseString
           |data_type: ${SchemaColumnDataType.Timestamp}
           |min: ${minTimestamp.toString}
           |max: ${maxTimestamp.toString}
         """.stripMargin

      string.parseYaml.convertTo[SchemaColumnRandom[_]] mustBe SchemaColumnRandom(name, minTimestamp, maxTimestamp)
    }

    "generate a Timestamp between a min and a max Timestamp" in {
      val minTimestamp = Timestamp.valueOf("2000-01-01 00:00:00")
      val maxTimestamp = Timestamp.valueOf("2000-01-01 23:59:59")

      val dataFrame = spark.range(1000).toDF("temp")
        .withColumn("Timestamp", SchemaColumnRandom("", minTimestamp, maxTimestamp).column())
        .drop("temp")

      val testData = dataFrame.select("Timestamp").collect().map(_.getTimestamp(0))
      forAll(testData)(f => f.after(minTimestamp) && f.before(maxTimestamp))
    }

    "read a Boolean column" in {
      val string =
        s"""$baseString
           |data_type: ${SchemaColumnDataType.Boolean}
         """.stripMargin

      string.parseYaml.convertTo[SchemaColumnRandom[_]] mustBe SchemaColumnRandom(name)
    }
  }
}
