package com.dunnhumby.datafaker

import scala.io.Source
import java.sql.{Date, Timestamp}
import com.dunnhumby.datafaker.schema.Schema
import net.jcazevedo.moultingyaml.{deserializationError, DefaultYamlProtocol, YamlDate, YamlFormat, YamlValue}
import org.joda.time.DateTimeZone

object YamlParser {

  def parseSchemaFromFile(fileName: String): Schema = {
    import com.dunnhumby.datafaker.schema.SchemaProtocol._
    import net.jcazevedo.moultingyaml._

    val yaml = Source.fromFile(fileName).mkString.stripMargin.parseYaml

    yaml.convertTo[Schema]
  }

  /**
    * Provides additional formats for unsupported types in DefaultYamlProtocol
    */
  trait YamlParserProtocol extends DefaultYamlProtocol {

    implicit object TimestampFormat extends YamlFormat[Timestamp] {
      override def read(yaml: YamlValue): Timestamp = {
        yaml match {
          case YamlDate(d) => Timestamp.valueOf(d.toDateTime(DateTimeZone.UTC).toString("yyyy-MM-dd HH:mm:ss"))
          case _ => deserializationError("error parsing Timestamp")
        }
      }

      override def write(obj: Timestamp): YamlValue = ???
    }

    implicit object DateFormat extends YamlFormat[Date] {

      override def read(yaml: YamlValue): Date = {
        yaml match {
          case YamlDate(d) => Date.valueOf(d.toString("yyyy-MM-dd"))
          case _ => deserializationError("error parsing Date")
        }
      }

      override def write(obj: Date): YamlValue = ???

    }

  }

}
