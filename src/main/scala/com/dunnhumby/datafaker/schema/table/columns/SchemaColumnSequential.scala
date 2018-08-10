
package com.dunnhumby.datafaker.schema.table.columns

import java.sql.{Date, Timestamp}
import com.dunnhumby.datafaker.YamlParser.YamlParserProtocol
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{to_utc_timestamp, from_unixtime, monotonically_increasing_id, to_date}

trait SchemaColumnSequential[T] extends SchemaColumn

object SchemaColumnSequential {
  def apply(name: String, start: Int, step: Int): SchemaColumn = SchemaColumnSequentialNumeric(name, start, step)
  def apply(name: String, start: Long, step: Long): SchemaColumn = SchemaColumnSequentialNumeric(name, start, step)
  def apply(name: String, start: Float, step: Float): SchemaColumn = SchemaColumnSequentialNumeric(name, start, step)
  def apply(name: String, start: Double, step: Double): SchemaColumn = SchemaColumnSequentialNumeric(name, start, step)
  def apply(name: String, start: Date, step: Int): SchemaColumn = SchemaColumnSequentialDate(name, start, step)
  def apply(name: String, start: Timestamp, step: Int): SchemaColumn = SchemaColumnSequentialTimestamp(name, start, step)
}

private case class SchemaColumnSequentialNumeric[T: Numeric](override val name: String, start: T, step: T) extends SchemaColumnSequential[T] {
  override def column(rowID: Option[Column] = Some(monotonically_increasing_id)): Column = (rowID.get * step) + start
}

private case class SchemaColumnSequentialTimestamp(override val name: String, start: Timestamp, stepSeconds: Int) extends SchemaColumnSequential[Timestamp] {
  override def column(rowID: Option[Column] = Some(monotonically_increasing_id)): Column = {
    val startTime = start.getTime / 1000
    to_utc_timestamp(from_unixtime(rowID.get * stepSeconds + startTime), "UTC")
  }
}

private case class SchemaColumnSequentialDate(override val name: String, start: Date, stepDays: Int) extends SchemaColumnSequential[Date] {
  val timestamp = SchemaColumnSequentialTimestamp(name, new Timestamp(start.getTime), stepDays * 86400)

  override def column(rowID: Option[Column]): Column = to_date(timestamp.column())
}

object SchemaColumnSequentialProtocol extends SchemaColumnSequentialProtocol
trait SchemaColumnSequentialProtocol extends YamlParserProtocol {

  import net.jcazevedo.moultingyaml._

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