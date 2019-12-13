name := "Preproc"

version := "1.0"

scalaVersion := "2.11.12"

resolvers ++= Seq(
  "Open Source Geospatial Foundation Repository" at "http://download.osgeo.org/webdav/geotools/",
  "Boundless Maven Repository" at "http://repo.boundlessgeo.com/main",
  "Java.net repository" at "http://download.java.net/maven/2"
)

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.4.4"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.4.4"
libraryDependencies += "org.apache.commons" % "commons-text" % "1.3"
libraryDependencies += "com.github.scopt" %% "scopt" % "3.7.1"
libraryDependencies += "org.locationtech.geotrellis" %% "geotrellis-proj4" % "3.2.0"
libraryDependencies += "org.geotools" % "gt-shapefile" % "19.0"
libraryDependencies += "org.geotools" % "gt-epsg-hsql" % "19.0"
libraryDependencies += "com.vividsolutions" % "jts" % "1.13"
libraryDependencies += "junit" % "junit" % "4.11" % Test
libraryDependencies += "javax.media" % "jai_core" % "1.1.3" from "http://download.osgeo.org/webdav/geotools/javax/media/jai_core/1.1.3/jai_core-1.1.3.jar"
libraryDependencies += "org.apache.commons" % "commons-csv" % "1.6"
libraryDependencies += "org.typelevel" %% "squants" % "1.6.0"
libraryDependencies += "net.jcazevedo" %% "moultingyaml" % "0.4.1"
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

libraryDependencies += "org.scalactic" %% "scalactic" % "3.1.0" % Test
libraryDependencies += "org.scalatest" %% "scalatest" % "3.1.0" % Test
val circeVersion = "0.11.2"

libraryDependencies ++= Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser"
).map(_ % circeVersion)

libraryDependencies += "org.gnieh" %% "diffson-circe" % "3.1.1" % Test

testOptions += Tests.Argument("-oF")

assemblyMergeStrategy in assembly := {
  case PathList("com", "vividsolutions", _*) => MergeStrategy.last
  case PathList("javax", "inject", _*) => MergeStrategy.last
  case PathList("org", "aopalliance", _*) => MergeStrategy.last
  case PathList("org", "apache", _*) => MergeStrategy.last
  case PathList("git.properties") => MergeStrategy.discard
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
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

maxErrors := 1

