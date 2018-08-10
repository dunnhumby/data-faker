import Dependencies._

name := "datafaker"

organization := "com.dunnhumby"

scalaVersion := "2.11.12"


//publishTo := Some("Artifactory Realm" at "https://artifactory.dunnhumby.com/artifactory/libs-release-local")

publishTo := {
  val artif = "https://artifactory.dunnhumby.com/artifactory/"
  if (isSnapshot.value)
    Some("Artifactory Realm" at artif + "libs-snapshot-local")
  else
    Some("Artifactory Realm"  at artif + "libs-release-local")
}

credentials += Credentials("Artifactory Realm", "artifactory.dunnhumby.com", "cs-anonymous", "Welcome123")

//Uncomment to pull from artifactory.
//resolvers += "Artifactory" at "https://artifactory.dunnhumby.com/artifactory/libs-snapshot-local/"

//Add assembly to the lifecycle.
artifact in (Compile, assembly) := {
  val art = (artifact in (Compile, assembly)).value
  art.withClassifier(Some("assembly"))
}
addArtifact(artifact in (Compile, assembly), assembly)

//Only run tests single threaded.
concurrentRestrictions in Global += Tags.limit(Tags.Test, 1)

//Skip tests in assembly as they are run elsewhere.
test in assembly := {}

//Dependencies - managed in project/Dependencies.scala
libraryDependencies ++= deps
excludeDependencies ++= exc

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

fork in run := true

mainClass in assembly := Some("com.dunnhumby.datafaker.Application")

