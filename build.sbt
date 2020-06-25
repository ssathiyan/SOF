name := "SOF"

version := "0.1"

scalaVersion := "2.11.12"

val excludeJpounts = ExclusionRule(organization = "net.jpountz.lz4", name = "lz4")

libraryDependencies ++= Seq("org.apache.spark" %% "spark-core" % "2.4.0",
"org.apache.spark" %% "spark-sql" % "2.4.0",
"com.typesafe.play" %% "play-json" % "2.4.0-M1",
"org.apache.kafka" % "kafka-clients" % "0.10.0.0" excludeAll(excludeJpounts),
"io.delta" %% "delta-core" % "0.4.0",
"com.github.pureconfig" %% "pureconfig" % "0.10.1")

libraryDependencies += "org.json" % "json" % "20200518"
