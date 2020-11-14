addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.6"
  exclude("org.apache.maven", "maven-plugin-api"))

addSbtPlugin("com.thesamet" % "sbt-protoc" % "0.99.18")
libraryDependencies += "com.thesamet.scalapb" %% "compilerplugin" % "0.7.4"
