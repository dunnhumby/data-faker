package com.dunnhumby.datafaker.schema.table.columns

import scala.reflect.runtime.universe._
import java.sql.{Date, Timestamp}
import com.dunnhumby.datafaker.YamlParser.YamlParserProtocol
import net.jcazevedo.moultingyaml.{deserializationError, YamlFormat, YamlString, YamlValue}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{rand, udf}

case class SchemaColumnSelection[T](override val name: String, values: List[T])(implicit tag: TypeTag[T]) extends SchemaColumn {
  override def column: Column = {
    val intToSelectionUDF = udf((index: Int) => {
      values(index)
    })

    intToSelectionUDF(rand() * values.length % values.length)
  }
}

trait SchemaColumnSelectionProtocol extends YamlParserProtocol {

  implicit object SchemaColumnSelectionFormat extends YamlFormat[SchemaColumnSelection[_]] {

    override def read(yaml: YamlValue): SchemaColumnSelection[_] = {
      val fields = yaml.asYamlObject.fields
      val YamlString(dataType) = fields.getOrElse(YamlString("data_type"), deserializationError("data_type not set"))
      val YamlString(name) = fields.getOrElse(YamlString("name"), deserializationError("name not set"))
      val values = fields.getOrElse(YamlString("values"), deserializationError("selection values not set"))

      dataType match {
        case SchemaColumnDataType.Int => SchemaColumnSelection(name, values.convertTo[List[Int]])
        case SchemaColumnDataType.Long => SchemaColumnSelection(name, values.convertTo[List[Long]])
        case SchemaColumnDataType.Float => SchemaColumnSelection(name, values.convertTo[List[Float]])
        case SchemaColumnDataType.Double => SchemaColumnSelection(name, values.convertTo[List[Double]])
        case SchemaColumnDataType.Date => SchemaColumnSelection(name, values.convertTo[List[Date]])
        case SchemaColumnDataType.Timestamp => SchemaColumnSelection(name, values.convertTo[List[Timestamp]])
        case SchemaColumnDataType.String => SchemaColumnSelection(name, values.convertTo[List[String]])
        case SchemaColumnDataType.Boolean => SchemaColumnSelection(name, values.convertTo[List[Boolean]])
        case _ => deserializationError(s"unsupported data_type: $dataType for ${SchemaColumnType.Selection}")
      }

    }

    override def write(obj: SchemaColumnSelection[_]): YamlValue = ???

  }

}
