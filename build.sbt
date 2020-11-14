name := "mloop"

version := "0.1.0-SNAPSHOT"

organization := "io.github.manuzhang"

scalaVersion := "2.11.8"

val sparkVersion = "2.3.1"

val scalaPbDependencies = Seq(
  //  "io.grpc" % "grpc-netty" % scalapb.compiler.Version.grpcJavaVersion,
  //  "com.thesamet.scalapb" %% "scalapb-runtime-grpc" % scalapb.compiler.Version.scalapbVersion,
  "com.thesamet.scalapb" %% "scalapb-json4s" % "0.7.1"
    exclude("org.json4s", "json4s-jackson_2.11")
    exclude("com.fasterxml.jackson.core", "jackson-databind"),
  "org.json4s" %% "json4s-jackson" % "3.2.11" % "provided",
  "org.json4s" %% "json4s-ast" % "3.4.0",
  "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion,
  "com.thesamet.scalapb" %% "scalapb-runtime" % scalapb.compiler.Version.scalapbVersion % "protobuf"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
  "com.typesafe.akka" %% "akka-http" % "10.0.11",
  "org.rogach" %% "scallop" % "3.1.1",
  "org.antlr" % "ST4" % "4.0.8" % "provided",
  "redis.clients" % "jedis" % "2.7.2",
  "org.tensorflow" %% "spark-tensorflow-connector" % "1.15.0",
  "ml.combust.mleap" %% "mleap-spark" % "0.16.0"
    exclude("org.apache.spark", "spark-mllib-local_2.11")
    exclude("com.trueaccord.scalapb", "scalapb-runtime_2.11")
    exclude("com.github.rwl", "jtransforms"),
  // "ml.combust.mleap" %% "mleap-xgboost-spark" % "0.16.0",
  "org.scalatest" %% "scalatest" % "3.0.4" % "test",
  "org.scalacheck" %% "scalacheck" % "1.14.0" % "test",
  "org.mockito" % "mockito-core" % "2.5.5" % "test"
) ++ scalaPbDependencies

PB.targets in Compile := Seq(
  PB.gens.java -> (sourceManaged in Compile).value,
  scalapb.gen(javaConversions=true, flatPackage = true, grpc = false) -> (sourceManaged in Compile).value
)
PB.protoSources in Compile := Seq(sourceDirectory.value / "main/resources")
javacOptions ++= Seq("-encoding", "UTF-8")

assemblyMergeStrategy in assembly := {
  x =>
    if (x.endsWith(".proto")) {
      MergeStrategy.discard
    } else {
      val oldStrategy = (assemblyMergeStrategy in assembly).value
      oldStrategy(x)
    }
}

assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("com.google.protobuf.**" -> "shadeproto.@1").inAll
)

assemblyJarName in assembly := {
  val ver =
    if (version.value.endsWith("SNAPSHOT")) {
      import sys.process._
      import scala.languageFeature.postfixOps

      val hash = ("git log -n 1 --pretty=format:'%h'" !!).trim().replace("'", "")
      s"${version.value}-$hash"
    } else {
      version.value
    }

  s"${name.value}_${scalaBinaryVersion.value}-$ver.jar"
}

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

test in assembly := {}

publishArtifact in Test := false

publishMavenStyle := true

