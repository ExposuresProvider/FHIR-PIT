package datatrans.step

import java.util.concurrent.atomic.AtomicInteger
import java.io._
import datatrans.Utils._

import org.apache.spark.sql._
import org.apache.hadoop.fs._
import org.apache.log4j.{Logger, Level}

import io.circe._
import io.circe.generic.semiauto._
import org.apache.commons.csv._

import datatrans.Config._
import datatrans.Implicits._
import datatrans._
import scala.collection.JavaConverters._


case class SplitConfig(
  input_file : String,
  output_dir : String,
  split_index: String
)

/**
  *  split preagg into individual files for patients 
  */
object PreprocSplit extends StepImpl {

  type ConfigType = SplitConfig

  val configDecoder : Decoder[ConfigType] = deriveDecoder

  val log = Logger.getLogger(getClass.getName)

  log.setLevel(Level.INFO)

  def step(spark: SparkSession, config: SplitConfig) = {
    time {

      val hc = spark.sparkContext.hadoopConfiguration

      val patient_dimension = config.input_file
      log.info("loading patient_dimension from " + patient_dimension)
      val input_file_path = new Path(config.input_file)
      val input_file_file_system = input_file_path.getFileSystem(hc)
      val output_dir = config.output_dir
      val output_dir_path = new Path(output_dir)
      val output_dir_file_system = output_dir_path.getFileSystem(hc)


      val csvParser = new CSVParser(new InputStreamReader(input_file_file_system.open(input_file_path), "UTF-8"), CSVFormat.DEFAULT
        .withFirstRecordAsHeader()
        .withIgnoreHeaderCase()
        .withTrim())

      var fileMap = Map[String, (Path, CSVPrinter)]()
      val headers = csvParser.getHeaderNames()
      val index = headers.indexOf(config.split_index)
      val rows = csvParser.iterator()
      var i = 0

      if (!output_dir_file_system.exists(output_dir_path)) {
        output_dir_file_system.mkdirs(output_dir_path)
      }

      while(rows.hasNext()) {
        log.info(f"processing row $i")
        i += 1
        val row = rows.next()
        val filename = row.get(index)
        val writer = fileMap.get(filename) match {
          case Some((_,w)) =>
            w
          case None =>
            val output_file = new Path(s"$output_dir/$filename")
            val output_file_csv_writer = new CSVPrinter(new OutputStreamWriter(output_dir_file_system.create(output_file), "UTF-8"), CSVFormat.DEFAULT.withHeader(headers.asScala:_*))
            fileMap += filename -> (output_file, output_file_csv_writer)
            output_file_csv_writer
        }
        writer.printRecord(row)
      }

      for ((path, output_file_csv_writer) <- fileMap.values) {
        output_file_csv_writer.close()
        deleteCRCFile(output_dir_file_system, path)
      }
    }
  }

}
