

package com.dunnhumby.datafaker.schema.table.columns

import java.sql.{Date, Timestamp}
import com.dunnhumby.datafaker.YamlParser.YamlParserProtocol
import net.jcazevedo.moultingyaml.{deserializationError, YamlFormat, YamlString, YamlValue}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType

trait SchemaColumnRandom[T] extends SchemaColumn

case class SchemaColumnRandomNumeric[T: Numeric](override val name: String, min: T, max: T, decimalPlaces: Int) extends SchemaColumnRandom[T] {
  override def column: Column = {
    import Numeric.Implicits._
    if (decimalPlaces == 0) {
      round(rand() * (max - min) + min, decimalPlaces).cast(LongType)
    }
    else {
      round(rand() * (max - min) + min, decimalPlaces)
    }
  }
}

case class SchemaColumnRandomTimestamp(override val name: String, min: Timestamp, max: Timestamp) extends SchemaColumnRandom[Timestamp] {
  override def column: Column = {
    val minTime = min.getTime / 1000
    val maxTime = max.getTime / 1000
    to_utc_timestamp(from_unixtime(rand() * (maxTime - minTime) + minTime), "UTC")
  }
}

case class SchemaColumnRandomDate(override val name: String, min: Date, max: Date) extends SchemaColumnRandom[Date] {
  val timestamp = SchemaColumnRandomTimestamp(name, new Timestamp(min.getTime), new Timestamp(max.getTime))

  override def column: Column = to_date(timestamp.column)
}

case class SchemaColumnRandomBoolean(override val name: String) extends SchemaColumnRandom[Boolean] {
  override def column: Column = rand() < 0.5f
}

trait SchemaColumnRandomProtocol extends YamlParserProtocol {

  implicit object SchemaColumnRandomFormat extends YamlFormat[SchemaColumnRandom[_]] {

    override def read(yaml: YamlValue): SchemaColumnRandom[_] = {
      val fields = yaml.asYamlObject.fields
      val YamlString(name) = fields.getOrElse(YamlString("name"), deserializationError("name not set"))
      val YamlString(dataType) = fields.getOrElse(YamlString("data_type"), deserializationError(s"data_type not set for $name"))

      if (dataType == SchemaColumnDataType.Boolean) {
        SchemaColumnRandomBoolean(name)
      }
      else {
        val min = fields.getOrElse(YamlString("min"), deserializationError(s"min not set for $name"))
        val max = fields.getOrElse(YamlString("max"), deserializationError(s"max not set for $name"))

        dataType match {
          case SchemaColumnDataType.Int => SchemaColumnRandomNumeric(name, min.convertTo[Int], max.convertTo[Int], 0)
          case SchemaColumnDataType.Long => SchemaColumnRandomNumeric(name, min.convertTo[Long], max.convertTo[Long], 0)
          case SchemaColumnDataType.Float => SchemaColumnRandomNumeric(name, min.convertTo[Float], max.convertTo[Float], 3)
          case SchemaColumnDataType.Double => SchemaColumnRandomNumeric(name, min.convertTo[Double], max.convertTo[Double], 3)
          case SchemaColumnDataType.Date => SchemaColumnRandomDate(name, min.convertTo[Date], max.convertTo[Date])
          case SchemaColumnDataType.Timestamp => SchemaColumnRandomTimestamp(name, min.convertTo[Timestamp], max.convertTo[Timestamp])
          case _ => deserializationError(s"unsupported data_type: $dataType for ${SchemaColumnType.Random}")
        }
      }

    }

    override def write(obj: SchemaColumnRandom[_]): YamlValue = ???

  }

}
