package datatrans.step

import org.scalatest.FlatSpec
import org.apache.spark.sql._
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import java.io.File
import java.nio.file.{Files, Paths, Path}
import org.scalatest.Assertions._
import java.nio.file.Files
import diffson.circe._
import io.circe.parser._
import TestUtils._
import datatrans.Utils.{stringToDateTime}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import datatrans.Mapper

class EnvDataFIPSSpec extends FlatSpec {
  
  lazy val spark = SparkSession.builder().master("local").appName("datatrans preproc").getOrCreate()

  val toDouble = (x : String) => if (x == "") null else x.toDouble

  val m = Map(
    "ozone_daily_8hour_maximum" -> toDouble,
    "pm25_daily_average" -> toDouble,
    "CO_ppbv" -> toDouble,
    "NO_ppbv" -> toDouble,
    "NO2_ppbv" -> toDouble,
    "NOX_ppbv" -> toDouble,
    "SO2_ppbv" -> toDouble,
    "ALD2_ppbv" -> toDouble,
    "FORM_ppbv" -> toDouble,
    "BENZ_ppbv" -> toDouble,
    "ozone_daily_8hour_maximum_max" -> toDouble,
    "pm25_daily_average_max" -> toDouble,
    "CO_ppbv_max" -> toDouble,
    "NO_ppbv_max" -> toDouble,
    "NO2_ppbv_max" -> toDouble,
    "NOX_ppbv_max" -> toDouble,
    "SO2_ppbv_max" -> toDouble,
    "ALD2_ppbv_max" -> toDouble,
    "FORM_ppbv_max" -> toDouble,
    "BENZ_ppbv_max" -> toDouble,
    "ozone_daily_8hour_maximum_min" -> toDouble,
    "pm25_daily_average_min" -> toDouble,
    "CO_ppbv_min" -> toDouble,
    "NO_ppbv_min" -> toDouble,
    "NO2_ppbv_min" -> toDouble,
    "NOX_ppbv_min" -> toDouble,
    "SO2_ppbv_min" -> toDouble,
    "ALD2_ppbv_min" -> toDouble,
    "FORM_ppbv_min" -> toDouble,
    "BENZ_ppbv_min" -> toDouble,
    "ozone_daily_8hour_maximum_avg" -> toDouble,
    "pm25_daily_average_avg" -> toDouble,
    "CO_ppbv_avg" -> toDouble,
    "NO_ppbv_avg" -> toDouble,
    "NO2_ppbv_avg" -> toDouble,
    "NOX_ppbv_avg" -> toDouble,
    "SO2_ppbv_avg" -> toDouble,
    "ALD2_ppbv_avg" -> toDouble,
    "FORM_ppbv_avg" -> toDouble,
    "BENZ_ppbv_avg" -> toDouble,
    "ozone_daily_8hour_maximum_stddev" -> toDouble,
    "pm25_daily_average_stddev" -> toDouble,
    "CO_ppbv_stddev" -> toDouble,
    "NO_ppbv_stddev" -> toDouble,
    "NO2_ppbv_stddev" -> toDouble,
    "NOX_ppbv_stddev" -> toDouble,
    "SO2_ppbv_stddev" -> toDouble,
    "ALD2_ppbv_stddev" -> toDouble,
    "FORM_ppbv_stddev" -> toDouble,
    "BENZ_ppbv_stddev" -> toDouble,
    "ozone_daily_8hour_maximum_prev_date" -> toDouble,
    "pm25_daily_average_prev_date" -> toDouble,
    "CO_ppbv_prev_date" -> toDouble,
    "NO_ppbv_prev_date" -> toDouble,
    "NO2_ppbv_prev_date" -> toDouble,
    "NOX_ppbv_prev_date" -> toDouble,
    "SO2_ppbv_prev_date" -> toDouble,
    "ALD2_ppbv_prev_date" -> toDouble,
    "FORM_ppbv_prev_date" -> toDouble,
    "BENZ_ppbv_prev_date" -> toDouble,
    "lat" -> toDouble,
    "lon" -> toDouble,
    "Latitude" -> toDouble,
    "Longitude" -> toDouble
  )

  def test(inp: String, outp: String) : Unit = {
    val tempDir = Files.createTempDirectory("env")
    val tempDir2 = Files.createTempDirectory("env2")
    val tempDir3 = Files.createTempDirectory("env3")
    val tempDirExpected = Files.createTempDirectory("envExpected")
    val tempDir2Exepcted = Files.createTempDirectory("env2Expected")
    val tempDir3Expected = Files.createTempDirectory("env3Expected")

    val cols = Seq(
      "ozone_daily_8hour_maximum" ,
      "pm25_daily_average" ,
      "CO_ppbv" ,
      "NO_ppbv" ,
      "NO2_ppbv" ,
      "NOX_ppbv" ,
      "SO2_ppbv" ,
      "ALD2_ppbv" ,
      "FORM_ppbv" ,
      "BENZ_ppbv"
    )

    val (cols2009, raw2009) = readCSV(f"src/test/data/other/$inp/merged_cmaq_2009.csv", m)
    val (cols2010, raw2010) = readCSV(f"src/test/data/other/$inp/merged_cmaq_2010.csv", m)

    val rawCols = (cols2009.toSet | cols2010.toSet).toSeq

    val raw = (raw2009 ++ raw2010).filter(_("FIPS") == "0")

    val dateTimeFormatter = DateTimeFormat.forPattern("yyyy/MM/dd")
    val dateTimeFormatterOutput = DateTimeFormat.forPattern("yyyy-MM-dd")

    val patient_num = "0"
    val lat = 0
    val lon = 0
    val FIPS = "0"

    def parseDate(r: Map[String, Any]): DateTime = dateTimeFormatter.parseDateTime(r("Date").asInstanceOf[String])

    def aggRaw(table: Seq[Map[String, Any]]): Seq[Seq[Option[Any]]] = {

      val tableSorted = table.sortWith((a, b) => parseDate(a).isBefore(parseDate(b)))
      val daily = tableSorted.map(r => Some(dateTimeFormatterOutput.print(parseDate(r))) +: cols.map(col => r.get(col)))

      val groupsByYear = tableSorted.groupBy(r => parseDate(r).year)

      val yearly = groupsByYear.map({
        case (year, tableSortedYear) =>
          cols.map(col => {
            val colVals = tableSortedYear.map(_.get(col).flatMap {
              case s : String =>
                if(s == "")
                  None
                else
                  Some(s.toDouble)
              case s : Double =>
                Some(s)
              case s => throw new RuntimeException(f"unsupported type $s")
            })
            val colValsSome = colVals.flatten
            val colMax = Seq.fill(tableSortedYear.length)(if(colValsSome.isEmpty) None else Some(colValsSome.max))
            val colAvg = Seq.fill(tableSortedYear.length)(if(colValsSome.isEmpty) None else Some(colValsSome.sum / colValsSome.length))
            Seq(colMax, colAvg)
          }).transpose.flatten.transpose
      }).flatten

      val colPrev = Seq.fill(cols.length)(None) +: tableSorted.map(r => cols.map(col => r.get(col))).take(tableSorted.length - 1)

      (daily, yearly, colPrev).zipped.map(_ ++ _ ++ _)
    }

    writeCSV(tempDirExpected.resolve("geoids.csv").toString(), Seq("patient_num","lat","lon","FIPS"), Seq(Seq(patient_num,lat,lon,FIPS)))

    val headers = Seq("patient_num", "start_date") ++ rawCols

    val preagg = raw.map(r => Seq(patient_num, dateTimeFormatterOutput.print(parseDate(r))) ++ rawCols.map(col => r.get(col)))
    
    writeCSV(tempDirExpected.resolve("preagg").toString(), headers, preagg)

    writeCSV(tempDir2Exepcted.resolve("0").toString(), headers, preagg)

    val headers3 = Seq("start_date") ++ cols ++ cols.map(_ + "_max") ++ cols.map(_ + "_avg") ++ cols.map(_ + "_prev_date")

    val agg = aggRaw(raw2009 ++ raw2010)
    writeCSV(tempDir3Expected.resolve("0").toString(), headers3, agg)

    val config0 = LatLonToGeoidConfig(
      patgeo_data = "src/test/data/fhir_processed/2010/geo.csv",
      output_file = s"${tempDir.toString()}/geoids.csv",
      fips_data = "src/test/data/other/spatial/env/env.shp"
    )

    PreprocLatLonToGeoid.step(spark, config0)

    val config = EnvDataFIPSConfig(
      environmental_data = f"src/test/data/other/$inp",
      output_file = s"${tempDir.toString()}/preagg",
      start_date = stringToDateTime("2009-01-01T00:00:00Z"),
      end_date = stringToDateTime("2011-01-01T00:00:00Z"),
      fips_data = s"${tempDir.toString()}/geoids.csv",
      offset_hours = 0
    )

    PreprocEnvDataFIPS.step(spark, config)

    val config3 = SplitConfig(
      input_file = s"${tempDir.toString()}/preagg",
      split_index = "patient_num",
      output_dir = s"${tempDir2.toString()}"
    )

    PreprocSplit.step(spark, config3)

    val config2 = EnvDataAggregateConfig(
      input_dir=s"${tempDir2.toString()}",
      output_dir = s"${tempDir3.toString()}",
      indices = Mapper.envInputColumns2,
      statistics = Mapper.studyPeriodMappedEnvStats
    )

    PreprocEnvDataAggregate.step(spark, config2)


    compareFileTree(tempDir.toString(), tempDirExpected.toString(), true, m)
    compareFileTree(tempDir2.toString(), tempDir2Exepcted.toString(), true, m)
    compareFileTree(tempDir3.toString(), tempDir3Expected.toString(), true, m)

    deleteRecursively(tempDir)
    deleteRecursively(tempDir2)
    deleteRecursively(tempDir3)
    deleteRecursively(tempDirExpected)
    deleteRecursively(tempDir2Exepcted)
    deleteRecursively(tempDir3Expected)
  }

  "EnvDataFIPS" should "handle all columns" in {
    test("env", "env2")
  }

  "EnvDataFIPS" should "handle union columns inside of schema" in {
    test("envfips2", "envfips2")
  }

  "EnvDataFIPS" should "handle union columns outside of schema" in {
    test("envfips3", "envfips3")
  }

}
