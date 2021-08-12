package datatrans.step

import datatrans.GeoidFinder
import java.util.concurrent.atomic.AtomicInteger
import datatrans.Utils.{time, withCounter, writeDataframe}
import org.apache.spark.sql.{DataFrame, SparkSession, Column}

import org.apache.hadoop.fs.Path
import org.apache.log4j.{Logger, Level}

import org.joda.time._

import io.circe.Decoder
import io.circe.generic.semiauto.deriveDecoder

import datatrans.environmentaldata.Utils.aggregateByStudyPeriod
import datatrans.{StepImpl, Mapper}


case class EnvDataAggregateConfig(
  input_dir : String,
  output_dir : String,
  indices: Seq[String],
  statistics: Seq[String],
  study_period_bounds: Seq[DateTime],
  study_periods: Seq[String]
)

object PreprocEnvDataAggregate extends StepImpl {

  type ConfigType = EnvDataAggregateConfig

  import datatrans.SharedImplicits._

  val configDecoder : Decoder[ConfigType] = deriveDecoder

  val log = Logger.getLogger(getClass.getName)

  log.setLevel(Level.INFO)

  def step(spark: SparkSession, config: EnvDataAggregateConfig) = {
    time {
      import spark.implicits._

      val hc = spark.sparkContext.hadoopConfiguration

      val input_dir_path = new Path(config.input_dir)
      val input_dir_file_system = input_dir_path.getFileSystem(hc)
      val itr = input_dir_file_system.listFiles(input_dir_path, false)
      val output_dir = config.output_dir

      val indices = config.indices

      val statistics = config.statistics

      withCounter(count =>
        while(itr.hasNext) {
          val input_file_path = itr.next().getPath()

          val p = input_file_path.getName()

          val patient_dimension = input_file_path.toString()
          log.info("processing patient " + count.incrementAndGet() + " " + p + " from " + patient_dimension)
          val df3year_pat = spark.read.format("csv").option("header", value = true).load(patient_dimension)

          log.info(f"aggregating $indices")
          val df3year_pat_aggbyyear = aggregateByStudyPeriod(spark, df3year_pat, config.study_period_bounds, config.study_periods, indices, statistics, Seq())
          //        df3year_pat_aggbyyear.cache()
          // log.info(f"columns4 = ${df3year_pat_aggbyyear.columns.toSeq}, nrows1 = ${df3year_pat_aggbyyear.count()}")

          val names3 = for (i <- statistics; j <- indices) yield f"${j}_$i"
          val df4 = df3year_pat_aggbyyear.select("start_date", indices ++ names3 ++ indices.map((s: String) => f"${s}_prev_date"): _*)
          // log.info(f"columns5 = ${df4.columns.toSeq}, nrows1 = ${df4.count()}")

          writeDataframe(hc, f"$output_dir/$p", df4)
        }
      )
    }
  }

}












