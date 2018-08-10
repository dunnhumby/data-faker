
package com.dunnhumby.datafaker.schema.table.columns

import java.sql.{Date, Timestamp}
import com.dunnhumby.datafaker.YamlParser.YamlParserProtocol
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{to_utc_timestamp, round, rand, from_unixtime, to_date}
import org.apache.spark.sql.types.{IntegerType, LongType}

trait SchemaColumnRandom[T] extends SchemaColumn

object SchemaColumnRandom {
  val FloatDP = 3
  val DoubleDP = 3

  def apply(name: String, min: Int, max: Int): SchemaColumn = SchemaColumnRandomNumeric(name, min, max)
  def apply(name: String, min: Long, max: Long): SchemaColumn = SchemaColumnRandomNumeric(name, min, max)
  def apply(name: String, min: Float, max: Float): SchemaColumn = SchemaColumnRandomNumeric(name, min, max)
  def apply(name: String, min: Double, max: Double): SchemaColumn = SchemaColumnRandomNumeric(name, min, max)
  def apply(name: String, min: Date, max: Date): SchemaColumn = SchemaColumnRandomDate(name, min, max)
  def apply(name: String, min: Timestamp, max: Timestamp): SchemaColumn = SchemaColumnRandomTimestamp(name, min, max)
  def apply(name: String): SchemaColumn = SchemaColumnRandomBoolean(name)
}

private case class SchemaColumnRandomNumeric[T: Numeric](override val name: String, min: T, max: T) extends SchemaColumnRandom[T] {
  override def column(rowID: Option[Column] = None): Column = {
    import Numeric.Implicits._

    (min, max) match {
      case (_: Int, _: Int) => round(rand() * (max - min) + min, 0).cast(IntegerType)
      case (_: Long, _: Long) => round(rand() * (max - min) + min, 0).cast(LongType)
      case (_: Float, _: Float) => round(rand() * (max - min) + min, SchemaColumnRandom.FloatDP)
      case (_: Double, _: Double) => round(rand() * (max - min) + min, SchemaColumnRandom.DoubleDP)
    }
  }
}

private case class SchemaColumnRandomTimestamp(override val name: String, min: Timestamp, max: Timestamp) extends SchemaColumnRandom[Timestamp] {
  override def column(rowID: Option[Column] = None): Column = {
    val minTime = min.getTime / 1000
    val maxTime = max.getTime / 1000
    to_utc_timestamp(from_unixtime(rand() * (maxTime - minTime) + minTime), "UTC")
  }
}

private case class SchemaColumnRandomDate(override val name: String, min: Date, max: Date) extends SchemaColumnRandom[Date] {
  val timestamp = SchemaColumnRandomTimestamp(name, new Timestamp(min.getTime), new Timestamp(max.getTime + 86400000))

  override def column(rowID: Option[Column] = None): Column = to_date(timestamp.column())
}

private case class SchemaColumnRandomBoolean(override val name: String) extends SchemaColumnRandom[Boolean] {
  override def column(rowID: Option[Column] = None): Column = rand() < 0.5f
}

object SchemaColumnRandomProtocol extends SchemaColumnRandomProtocol
trait SchemaColumnRandomProtocol extends YamlParserProtocol {

  import net.jcazevedo.moultingyaml._

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
          case SchemaColumnDataType.Int => SchemaColumnRandomNumeric(name, min.convertTo[Int], max.convertTo[Int])
          case SchemaColumnDataType.Long => SchemaColumnRandomNumeric(name, min.convertTo[Long], max.convertTo[Long])
          case SchemaColumnDataType.Float => SchemaColumnRandomNumeric(name, min.convertTo[Float], max.convertTo[Float])
          case SchemaColumnDataType.Double => SchemaColumnRandomNumeric(name, min.convertTo[Double], max.convertTo[Double])
          case SchemaColumnDataType.Date => SchemaColumnRandomDate(name, min.convertTo[Date], max.convertTo[Date])
          case SchemaColumnDataType.Timestamp => SchemaColumnRandomTimestamp(name, min.convertTo[Timestamp], max.convertTo[Timestamp])
          case _ => deserializationError(s"unsupported data_type: $dataType for ${SchemaColumnType.Random}")
        }
      }

    }

    override def write(obj: SchemaColumnRandom[_]): YamlValue = ???

  }

}
