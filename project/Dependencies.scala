import sbt._

object Dependencies {

  object Version {
    val spark = "2.1.0"
    val scalaTest = "3.0.4"
  }

  val deps = Seq(
    "org.apache.spark" %% "spark-core" % Version.spark % Provided,
    "org.apache.spark" %% "spark-hive" % Version.spark % Provided,
    "org.apache.spark" %% "spark-mllib" % Version.spark % Provided,
    "org.apache.spark" %% "spark-sql" % Version.spark % Provided,
    "org.scalatest" %% "scalatest" % Version.scalaTest % Test,
    "org.scalamock" %% "scalamock" % "4.1.0" % Test,
    "com.holdenkarau" %% "spark-testing-base" % s"${Version.spark}_0.8.0" % Test,
    "ch.qos.logback" % "logback-core" % "1.2.3",
    "ch.qos.logback" % "logback-classic" % "1.2.3",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.8.0",
    "com.typesafe" % "config" % "1.3.2",
    "org.neo4j.driver" % "neo4j-java-driver" % "1.0.4",
    "net.jcazevedo" %% "moultingyaml" % "0.4.0"
  )

  val exc = Seq(
    ExclusionRule("org.slf4j", "slf4j-log4j12")
  )

}
