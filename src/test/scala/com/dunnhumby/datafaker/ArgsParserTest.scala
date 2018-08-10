
package com.dunnhumby.datafaker

import java.sql.{Date, Timestamp}
import org.scalatest.{MustMatchers, WordSpec}

class ArgsParserTest extends WordSpec with MustMatchers {

  import com.dunnhumby.datafaker.YamlParser.YamlParserProtocol._
  import net.jcazevedo.moultingyaml._

  "ArgsParser" must {
    "accepts --file arg" in {
      ArgsParser.parseArgs(List("--file", "test")) mustBe Map("file" -> "test")
    }

    "accepts --database arg" in {
      ArgsParser.parseArgs(List("--database", "test")) mustBe Map("database" -> "test")
    }
  }
}
