package datatrans.environmentaldata

import datatrans.Utils._
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.joda.time._
import org.joda.time.format.{DateTimeFormat, ISODateTimeFormat}
import org.apache.spark.sql.functions._
import org.apache.hadoop.fs._
import org.apache.log4j.{Level, Logger}
import datatrans.Mapper
import org.apache.spark.ml.feature.Bucketizer

object Utils {
  val log = Logger.getLogger(getClass.getName)

  log.setLevel(Level.INFO)

  val plusOneDayDate = udf((x : String) =>
    if (x == null)
      null
    else 
      DateTime.parse(x, ISODateTimeFormat.dateParser()).plusDays(1).toString("yyyy-MM-dd")
  )

  val aggFuncs = Map[String, Column => Column](
    "avg" -> avg,
    "max" -> max,
    "min" -> min,
    "stddev" -> stddev
  )

  val studyPeriodStatsToCompute : Seq[(Column => Column, String)] = Mapper.studyPeriodMappedEnvStats.map(stat => (aggFuncs(stat), stat))

  def aggregateByStudyPeriod(spark: SparkSession, df: DataFrame, study_period_bounds: Seq[DateTime], study_periods: Seq[String],  names: Seq[String], statistics:Seq[String], additional_primary_keys : Seq[String]) = {
    import spark.implicits._
    val study_period_schema = StructType(Seq(
      StructField("bucket", IntegerType, false),
      StructField("study_period", StringType, false)
    ))
    val study_period_df = spark.createDataFrame(spark.sparkContext.parallelize(study_periods.zipWithIndex.map {
      case (study_period, bucket) => Row(study_period, bucket)
    }), study_period_schema)
    val bucketizer = new Bucketizer().setInputCol("start_date").setOutputCol("bucket").setSplits(study_period_bounds.map(_.getMillis().toDouble / 1000).toArray)
    val df_with_study_period = bucketizer.transform(df.withColumn("start_date_milliseconds", unix_timestamp($"start_date"))).drop("start_date_millis").join(broadcast(study_period_df), "bucket").withColumn("year", year($"start_date"))
    val stats = statistics.flatMap((stat: String) => studyPeriodStatsToCompute.find{
      case (_, suffix) => suffix == stat
    })
    val exprs = for(name <- names; (func, suffix) <- stats) yield func(col(name).cast(DoubleType)).alias(name + "_" + suffix)
    
    val df_with_agg = if (!exprs.isEmpty) {
      val aggregate = df_with_study_period.groupBy("study_period", additional_primary_keys : _*).agg(
        exprs.head, exprs.tail : _*
      )
      df_with_study_period.join(aggregate, "study_period" +: additional_primary_keys, "left")
    } else {
      df_with_study_period
    }
    val df_with_next_date = df.withColumn("next_date", plusOneDayDate($"start_date"))
    val df_prev_date = df_with_next_date.select(($"next_date".as("start_date") +: additional_primary_keys.map((a:String) => df_with_next_date.col(a))) ++ names.map((s:String) => df_with_next_date.col(s).as(f"${s}_prev_date")) : _*)
    df_with_agg.join(df_prev_date, "start_date" +: additional_primary_keys, "left")
  }

}
