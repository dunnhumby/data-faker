
package com.dunnhumby.datafaker.schema.table.columns

import java.sql.{Date, Timestamp}
import org.scalatest.{MustMatchers, WordSpec}

class SchemaColumnSequentialTest extends WordSpec with MustMatchers {

  import com.dunnhumby.datafaker.schema.table.columns.SchemaColumnSequentialProtocol._
  import net.jcazevedo.moultingyaml._

  val name = "test"
  val column_type = "Sequential"

  val baseString =
    s"""name: $name
       |column_type: $column_type
    """.stripMargin

  "SchemaColumnSequential" must {
    "read an Int column" in {
      val string =
        s"""$baseString
           |data_type: ${SchemaColumnDataType.Int}
           |start: 1
           |step: 1
         """.stripMargin

      string.parseYaml.convertTo[SchemaColumnSequential[_]] mustBe SchemaColumnSequential(name, 1, 1)
    }

    "read a Long column" in {
      val string =
        s"""$baseString
           |data_type: ${SchemaColumnDataType.Long}
           |start: 1
           |step: 1
         """.stripMargin

      string.parseYaml.convertTo[SchemaColumnSequential[_]] mustBe SchemaColumnSequential(name, 1l, 1l)
    }

    "read a Float column" in {
      val string =
        s"""$baseString
           |data_type: ${SchemaColumnDataType.Float}
           |start: 1.0
           |step: 1.0
         """.stripMargin

      string.parseYaml.convertTo[SchemaColumnSequential[_]] mustBe SchemaColumnSequential(name, 1f, 1f)
    }

    "read a Double column" in {
      val string =
        s"""$baseString
           |data_type: ${SchemaColumnDataType.Double}
           |start: 1.0
           |step: 1.0
         """.stripMargin

      string.parseYaml.convertTo[SchemaColumnSequential[_]] mustBe SchemaColumnSequential(name, 1d, 1d)
    }

    "read a Date column" in {
      val string =
        s"""$baseString
           |data_type: ${SchemaColumnDataType.Date}
           |start: 1998-06-03
           |step: 1
         """.stripMargin

      string.parseYaml.convertTo[SchemaColumnSequential[_]] mustBe SchemaColumnSequential(name, Date.valueOf("1998-06-03"), 1)
    }

    "read a Timestamp column" in {
      val string =
        s"""$baseString
           |data_type: ${SchemaColumnDataType.Timestamp}
           |start: 1998-06-03 01:23:45
           |step: 1
         """.stripMargin

      string.parseYaml.convertTo[SchemaColumnSequential[_]] mustBe SchemaColumnSequential(name, Timestamp.valueOf("1998-06-03 01:23:45"), 1)
    }
  }
}
