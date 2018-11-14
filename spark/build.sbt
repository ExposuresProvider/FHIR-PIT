name := "Preproc"

version := "1.0"

scalaVersion := "2.11.8"

resolvers ++= Seq(
  "Open Source Geospatial Foundation Repository" at "http://download.osgeo.org/webdav/geotools/",
  "Boundless Maven Repository" at "http://repo.boundlessgeo.com/main",
  "Java.net repository" at "http://download.java.net/maven/2"
)

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.2.1"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.2.1"
libraryDependencies += "org.apache.commons" % "commons-text" % "1.3"
libraryDependencies += "com.typesafe.play" %% "play-json" % "2.6.7"
libraryDependencies += "com.github.scopt" %% "scopt" % "3.7.0"
libraryDependencies += "org.locationtech.geotrellis" %% "geotrellis-proj4" % "1.1.0"
libraryDependencies += "org.geotools" % "gt-shapefile" % "19.0"
libraryDependencies += "org.geotools" % "gt-epsg-hsql" % "19.0"
libraryDependencies += "com.vividsolutions" % "jts" % "1.13"
libraryDependencies += "junit" % "junit" % "4.11" % Test
libraryDependencies += "javax.media" % "jai_core" % "1.1.3" from "http://download.osgeo.org/webdav/geotools/javax/media/jai_core/1.1.3/jai_core-1.1.3.jar"
libraryDependencies += "org.apache.commons" % "commons-csv" % "1.6"
libraryDependencies += "org.typelevel" %% "squants" % "1.4.0"

maxErrors := 1

