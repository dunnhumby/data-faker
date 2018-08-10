
package com.dunnhumby.datafaker.schema.table.columns

import java.sql.{Date, Timestamp}
import com.dunnhumby.datafaker.{SharedSparkSession}
import org.scalatest.{Inspectors, MustMatchers, WordSpec}

class SchemaColumnSelectionTest extends WordSpec with SharedSparkSession with MustMatchers with Inspectors {

  import com.dunnhumby.datafaker.schema.table.columns.SchemaColumnSelectionProtocol._
  import net.jcazevedo.moultingyaml._

  val name = "test"
  val column_type = "Selection"

  val baseString =
    s"""name: $name
       |column_type: $column_type
    """.stripMargin

  "SchemaColumnSelection" must {
    "read an Int column" in {
      val string =
        s"""$baseString
           |data_type: ${SchemaColumnDataType.Int}
           |values:
           |- 1
           |- 2
           |- 3
         """.stripMargin

      string.parseYaml.convertTo[SchemaColumnSelection[_]] mustBe SchemaColumnSelection(name, List(1, 2, 3))
    }

    "read a Long column" in {
      val string =
        s"""$baseString
           |data_type: ${SchemaColumnDataType.Long}
           |values:
           |- 1
           |- 2
           |- 3
         """.stripMargin

      string.parseYaml.convertTo[SchemaColumnSelection[_]] mustBe SchemaColumnSelection(name, List(1l, 2l, 3l))
    }

    "read a Float column" in {
      val string =
        s"""$baseString
           |data_type: ${SchemaColumnDataType.Float}
           |values:
           |- 1.0
           |- 2.0
           |- 3.0
         """.stripMargin

      string.parseYaml.convertTo[SchemaColumnSelection[_]] mustBe SchemaColumnSelection(name, List(1f, 2f, 3f))
    }

    "read a Double column" in {
      val string =
        s"""$baseString
           |data_type: ${SchemaColumnDataType.Double}
           |values:
           |- 1.0
           |- 2.0
           |- 3.0
         """.stripMargin

      string.parseYaml.convertTo[SchemaColumnSelection[_]] mustBe SchemaColumnSelection(name, List(1d, 2d, 3d))
    }

    "read a Date column" in {
      val string =
        s"""$baseString
           |data_type: ${SchemaColumnDataType.Date}
           |values:
           |- 1998-06-03
           |- 1998-06-04
           |- 1998-06-05
         """.stripMargin

      string.parseYaml.convertTo[SchemaColumnSelection[_]] mustBe SchemaColumnSelection(name, List(Date.valueOf("1998-06-03"), Date.valueOf("1998-06-04"), Date.valueOf("1998-06-05")))
    }

    "read a Timestamp column" in {
      val string =
        s"""$baseString
           |data_type: ${SchemaColumnDataType.Timestamp}
           |values:
           |- 1998-06-03 01:23:45
           |- 1998-06-04 02:34:56
           |- 1998-06-05 03:45:67
         """.stripMargin

      string.parseYaml.convertTo[SchemaColumnSelection[_]] mustBe SchemaColumnSelection(name, List(Timestamp.valueOf("1998-06-03 01:23:45"), Timestamp.valueOf("1998-06-04 02:34:56"), Timestamp.valueOf("1998-06-05 03:45:67")))
    }

    "read a String column" in {
      val string =
        s"""$baseString
           |data_type: ${SchemaColumnDataType.String}
           |values:
           |- test
           |- test1
           |- test2
         """.stripMargin

      string.parseYaml.convertTo[SchemaColumnSelection[_]] mustBe SchemaColumnSelection(name, List("test", "test1", "test2"))
    }

    "select a random value from the given list of values" in {
      val list = List(1, 2, 3, 4, 5, 6)

      val dataFrame = spark.range(1000).toDF("temp")
        .withColumn("Selection", SchemaColumnSelection("", list).column())
        .drop("temp")

      val testData = dataFrame.select("Selection").collect().map(_.getInt(0))
      testData must contain only (list:_*)
    }
  }
}
