package datatrans.step

import sys.process.{Process, ProcessLogger}
import sys.process._
import java.io.PrintWriter
import java.nio.file.Files
import scala.reflect.io.Directory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.{StructType, StructField, DateType, DoubleType}
import org.apache.spark.sql.{Column, Row, SparkSession, DataFrame}
import org.joda.time.format.{ISODateTimeFormat, DateTimeFormat}
import org.joda.time.{DateTime, DateTimeZone}
import org.apache.spark.sql.functions.udf
import io.circe.{Decoder, Encoder}
import io.circe.syntax._
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import datatrans.Utils.{fileExists, readCSV2, time, withCounter, HDFSCollection, writeDataframe}
import datatrans.StepImpl
import datatrans.Mapper

object PreprocPerPatSeriesCSVTableLocal extends StepImpl {
  
  type ConfigType = PreprocPerPatSeriesCSVTableConfig

  import datatrans.SharedImplicits._

  val configDecoder : Decoder[ConfigType] = deriveDecoder
  implicit val configEncoder : Encoder[ConfigType] = deriveEncoder

  def step(spark: SparkSession, config:PreprocPerPatSeriesCSVTableConfig) : Unit = {

    time {
      val tempFi = Files.createTempFile(null, null)
      val tempDir = Files.createTempDirectory(null)
      try {
        val w = new PrintWriter(tempFi.toFile)

        try {
          w.write(config.asJson.toString)
        } finally {
          w.close()
        }

        val exitValue = Process(Seq("virtualenv", "-p", "/usr/bin/python3", f"${tempDir.toString}/venv")).!
        if (exitValue < 0) {
          throw new RuntimeException(f"error: virtualenv {exitValue}")
        }
        val exitValue2 = Process(Seq(f"${tempDir.toString}/venv/bin/pip", "install", "--no-cache-dir", "-r", "requirements.txt")).!
        if (exitValue2 < 0) {
          throw new RuntimeException(f"error: pip install {exitValue2}")
        }
        val exitValue3 = Process(f"${tempDir.toString}/venv/bin/python src/main/python/perPatSeriesCSVTable.py --n_jobs 32 --config ${tempFi.toString}").!(ProcessLogger(println, println))
        if (exitValue3 < 0) {
          throw new RuntimeException(f"error: {exitValue3}")
        } 
      } finally {
        Files.delete(tempFi)
        new Directory(tempDir.toFile).deleteRecursively()
      }
    }
  }

}
