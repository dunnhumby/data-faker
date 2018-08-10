package com.dunnhumby.datafaker.schema.table.columns

import java.sql.{Date, Timestamp}
import com.dunnhumby.datafaker.YamlParser.YamlParserProtocol
import net.jcazevedo.moultingyaml.{deserializationError, YamlFormat, YamlString, YamlValue}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

trait SchemaColumnSequential[T] extends SchemaColumn

case class SchemaColumnSequentialNumeric[T: Numeric](override val name: String, start: T, step: T) extends SchemaColumnSequential[T] {
  override def column: Column = (monotonically_increasing_id * step) + start
}

case class SchemaColumnSequentialTimestamp(override val name: String, start: Timestamp, stepSeconds: Int) extends SchemaColumnSequential[Timestamp] {
  override def column: Column = {
    val startTime = start.getTime / 1000
    to_utc_timestamp(from_unixtime(monotonically_increasing_id() * stepSeconds + startTime), "UTC")
  }
}

case class SchemaColumnSequentialDate(override val name: String, start: Date, stepDays: Int) extends SchemaColumnSequential[Date] {
  val timestamp = SchemaColumnSequentialTimestamp(name, new Timestamp(start.getTime), stepDays * 86400)

  override def column: Column = timestamp.column
}

trait SchemaColumnSequentialProtocol extends YamlParserProtocol {

  implicit object SchemaColumnSequentialFormat extends YamlFormat[SchemaColumnSequential[_]] {

    override def read(yaml: YamlValue): SchemaColumnSequential[_] = {
      val fields = yaml.asYamlObject.fields
      val YamlString(dataType) = fields.getOrElse(YamlString("data_type"), deserializationError("data_type not set"))
      val YamlString(name) = fields.getOrElse(YamlString("name"), deserializationError("name not set"))
      val start = fields.getOrElse(YamlString("start"), deserializationError("start not set"))
      val step = fields.getOrElse(YamlString("step"), deserializationError("step not set"))

      dataType match {
        case "Int" => SchemaColumnSequentialNumeric(name, start.convertTo[Int], step.convertTo[Int])
        case "Long" => SchemaColumnSequentialNumeric(name, start.convertTo[Long], step.convertTo[Long])
        case "Float" => SchemaColumnSequentialNumeric(name, start.convertTo[Float], step.convertTo[Float])
        case "Double" => SchemaColumnSequentialNumeric(name, start.convertTo[Double], step.convertTo[Double])
        case "Date" => SchemaColumnSequentialDate(name, start.convertTo[Date], step.convertTo[Int])
        case "Timestamp" => SchemaColumnSequentialTimestamp(name, start.convertTo[Timestamp], step.convertTo[Int])
        case _ => deserializationError(s"unsupported data_type: $dataType for ${SchemaColumnType.Sequential}")
      }

    }

    override def write(obj: SchemaColumnSequential[_]): YamlValue = ???

  }

}