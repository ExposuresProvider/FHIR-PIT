package datatrans.step

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.{DateType, DoubleType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.joda.time.format.{DateTimeFormat, ISODateTimeFormat}
import org.joda.time.{DateTime, DateTimeZone}
import org.apache.spark.sql.functions.udf
import io.circe.Decoder
import io.circe.generic.semiauto.deriveDecoder
import datatrans.Utils.{HDFSCollection, fileExists, readCSV2, time, withCounter, writeDataframe}
import datatrans.StepImpl
import datatrans.Mapper
import datatrans.environmentaldata.Utils

case class PreprocPerPatSeriesCSVTableConfig(
  patient_file : String = "",
  environment_file : Option[String] = None,
  environment2_file : Option[String] = None,
  input_files : Seq[String] = Seq(),
  output_dir : String = "",
  study_period_bounds : Seq[DateTime] = Seq(),
  study_periods : Seq[String] = Seq(),
  offset_hours : Int = 0
)

object PreprocPerPatSeriesCSVTable extends StepImpl {
  
  type ConfigType = PreprocPerPatSeriesCSVTableConfig

  import datatrans.SharedImplicits._

  val configDecoder : Decoder[ConfigType] = deriveDecoder

  def step(spark: SparkSession, config:PreprocPerPatSeriesCSVTableConfig) : Unit = {
    import spark.implicits._

    def make_env_schema(names: Seq[String]) = StructType(
      StructField("start_date", DateType, true) +:
        (for(
          name <- names
        ) yield StructField(name, DoubleType, false)).toSeq
    )

    val env_schema = make_env_schema(Mapper.mappedEnvOutputColumns)

    val env2_schema = make_env_schema(Mapper.mappedEnvOutputColumns2)

    def join_env(hc: Configuration, pat_df: DataFrame, schema: StructType, menv_file: Option[String], cols : String) : DataFrame = {
      menv_file match {
        case Some(env_file) =>
          if(fileExists(hc, env_file)) {
            val env_df = readCSV2(spark, env_file, schema, _ => DoubleType)

            pat_df.join(env_df, Seq(cols), "left")
          } else {
            println(f"file not exists $env_file")
            pat_df // expandDataFrame(pat_df, env_schema)
          }
        case None =>
          println(f"no file name provided")
          pat_df // expandDataFrame(pat_df, env_schema)
      }
    }

    time {
      val hc = spark.sparkContext.hadoopConfiguration

      val dfs = config.input_files.map(input_file => {
        spark.read.format("csv").option("header", value = true).load(input_file)
      })

      val df = if (config.input_files.isEmpty) None else Some(dfs.reduce(_.join(_, Seq("patient_num"), "left")))

      withCounter(count =>
        new HDFSCollection(hc, new Path(config.patient_file)).foreach(f => {
          val p = f.getName().stripSuffix(".csv")
          println("processing patient " + count.incrementAndGet() + " " + p)
            val pat_df = spark.read.format("csv").option("header", value = true).load(f.toString())

            if(!pat_df.head(1).isEmpty) {
              val patenv_df0 = join_env(hc, pat_df, env_schema, config.environment_file.map(env => s"${env}/$p"), "start_date")

              val patenv_df1 = join_env(hc, pat_df, env2_schema, config.environment2_file.map(env2 => s"${env2}/$p"), "start_date")

              val patenv_df2 = df match {
                case Some(df) => patenv_df1.join(df, Seq("patient_num"), "left")
                case _ => patenv_df1
              }

              val patenv_df2_with_study_period = Utils.bucketize(spark, patenv_df2, config.study_period_bounds, config.study_periods)
              for (year <- config.study_periods) {
                val per_pat_output_dir = f"${config.output_dir}/$year"
                val output_file = f"$per_pat_output_dir/$p"
                writeDataframe(hc, output_file, patenv_df2.filter($"study_period" === year))
              }

            }


        })

      )

    }

  }

}
