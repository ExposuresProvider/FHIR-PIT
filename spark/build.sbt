name := "Preproc"

version := "1.0"

scalaVersion := "2.12.14"

resolvers ++= Seq(
  "Open Source Geospatial Foundation Repository" at "https://repo.osgeo.org/repository/release/",
  "Java.net repository" at "https://download.java.net/maven/2"
)

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.5"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.5"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.5" % "provided"
libraryDependencies += "org.deeplearning4j" % "deeplearning4j-core" % "1.0.0-beta7"
libraryDependencies += "org.nd4j" % "nd4j-native-platform" % "1.0.0-beta7"
libraryDependencies += "org.nd4j" %% "nd4j-parameter-server-node" % "1.0.0-beta7"
libraryDependencies += "org.deeplearning4j" %% "dl4j-spark-parameterserver" % "1.0.0-beta7"
libraryDependencies += "org.datavec" %% "datavec-spark" % "1.0.0-beta7"
libraryDependencies += "org.apache.commons" % "commons-text" % "1.3"
libraryDependencies += "com.github.scopt" %% "scopt" % "3.7.1"
libraryDependencies += "org.locationtech.geotrellis" %% "geotrellis-proj4" % "3.2.0"
libraryDependencies += "org.geotools" % "gt-shapefile" % "19.0"
libraryDependencies += "org.geotools" % "gt-epsg-hsql" % "19.0"
libraryDependencies += "org.geotools" % "gt-epsg-hsql" % "19.0" % Test
libraryDependencies += "com.vividsolutions" % "jts" % "1.13"
libraryDependencies += "junit" % "junit" % "4.11" % Test
libraryDependencies += "javax.media" % "jai_core" % "1.1.3" from "https://repo.osgeo.org/repository/release/javax/media/jai_core/1.1.3/jai_core-1.1.3.jar"
libraryDependencies += "org.apache.commons" % "commons-csv" % "1.8"
libraryDependencies += "org.typelevel" %% "squants" % "1.6.0"
libraryDependencies += "io.suzaku" %% "boopickle" % "1.3.1"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-core" % "2.8.9"
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.9"
libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.8.9"
libraryDependencies ++= Seq(
  "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-core"   % "0.55.2" % Compile,
  "com.github.plokhotnyuk.jsoniter-scala" %% "jsoniter-scala-macros" % "0.55.2" % Provided // required only in compile-time
)
// https://mvnrepository.com/artifact/com.jsoniter/jsoniter
libraryDependencies += "com.jsoniter" % "jsoniter" % "0.9.23"
libraryDependencies += "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "2.7.7"
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.7"

libraryDependencies += "org.scalactic" %% "scalactic" % "3.1.0" % Test
libraryDependencies += "org.scalatest" %% "scalatest" % "3.1.0" % Test

libraryDependencies ++= Seq(
  "io.circe" %% "circe-core" % "0.11.2",
  "io.circe" %% "circe-generic" % "0.11.2",
  "io.circe" %% "circe-parser" % "0.11.2",
  "io.circe" %% "circe-optics" % "0.11.0",
  "io.circe" %% "circe-yaml" % "0.10.1"
)

libraryDependencies += "org.gnieh" %% "diffson-circe" % "4.0.0-M3" % Test

testOptions += Tests.Argument("-oF")

assembly / logLevel := Level.Error

assembly / assemblyMergeStrategy := {
  case PathList("com", "vividsolutions", _*) => MergeStrategy.last
  case PathList("javax", "inject", _*) => MergeStrategy.last
  case PathList("javax", "xml", _*) => MergeStrategy.last
  case PathList("javax", "ws", _*) => MergeStrategy.last
  case PathList("javax", "servlet", _*) => MergeStrategy.last
  case PathList("com", "sun", _*) => MergeStrategy.last
  case PathList("org", "aopalliance", _*) => MergeStrategy.last
  case PathList("org", "apache", _*) => MergeStrategy.last
  case PathList("com", "google", _*) => MergeStrategy.last
  case PathList("net", "jpountz", _*) => MergeStrategy.last
  case PathList("io", "netty", _*) => MergeStrategy.last
  case PathList("javax", "annotation", _*) => MergeStrategy.last
  case PathList("javax", "activation", _*) => MergeStrategy.last
  case PathList("git.properties") => MergeStrategy.discard
  case PathList("module-info.class") => MergeStrategy.discard
  case PathList("META-INF", xs @ _*) =>
    xs map {_.toLowerCase} match {
      case ("manifest.mf" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) =>
        MergeStrategy.discard
      case ps @ (_ :: _) if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") || ps.last.endsWith(".rsa") =>
        MergeStrategy.discard
      case "plexus" :: _ =>
        MergeStrategy.discard
      case ("services" :: _ :: Nil) =>
        MergeStrategy.concat
      case ("javax.media.jai.registryfile.jai" :: Nil) | ("registryfile.jai" :: Nil) | ("registryfile.jaiext" :: Nil) =>
        MergeStrategy.concat
      case _ => MergeStrategy.discard
    }
  case x =>
    val oldStrategy = (assembly / assemblyMergeStrategy).value
    oldStrategy(x)
}

maxErrors := 1

assembly / test := {}