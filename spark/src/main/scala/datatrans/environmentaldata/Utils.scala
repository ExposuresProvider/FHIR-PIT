package datatrans.environmentaldata

import datatrans.Utils._
import org.apache.spark.sql.{DataFrame, SparkSession, Column}
import org.apache.spark.sql.types._
import org.joda.time._
import org.joda.time.format.{ISODateTimeFormat, DateTimeFormat}

import org.apache.spark.sql.functions._
import org.apache.hadoop.fs._
import org.apache.log4j.{Logger, Level}

object Utils {
  val log = Logger.getLogger(getClass.getName)

  log.setLevel(Level.INFO)

  val plusOneDayDate = udf((x : String) =>
    if (x == null)
      null
    else 
      DateTime.parse(x, ISODateTimeFormat.dateParser()).plusDays(1).toString("yyyy-MM-dd")
  )

  val yearlyStatsToCompute : Seq[(String => Column, String)] = Seq((avg, "avg"), (max, "max"), (min, "min"), (stddev, "stddev"))

  def aggregateByYear(spark: SparkSession, df: DataFrame, names: Seq[String], statistics:Seq[String], additional_primary_keys : Seq[String]) = {
    import spark.implicits._
    val df_with_year = df.withColumn("year", year($"start_date"))
    val stats = statistics.flatMap((stat: String) => yearlyStatsToCompute.find{
      case (_, suffix) => suffix == stat
    })
    val exprs = for(name <- names; (func, suffix) <- stats) yield func(name).alias(name + "_" + suffix)
    
    val df_with_agg = if (!exprs.isEmpty) {
      val aggregate = df_with_year.groupBy("year", additional_primary_keys : _*).agg(
        exprs.head, exprs.tail : _*
      )
      df_with_year.join(aggregate, "year" +: additional_primary_keys, "left")
    } else {
      df_with_year
    }
    val df_with_next_date = df.withColumn("next_date", plusOneDayDate($"start_date"))
    val df_prev_date = df_with_next_date.select(($"next_date".as("start_date") +: additional_primary_keys.map((a:String) => df_with_next_date.col(a))) ++ names.map((s:String) => df_with_next_date.col(s).as(f"${s}_prev_date")) : _*)
    df_with_agg.join(df_prev_date, "start_date" +: additional_primary_keys, "left")
  }

}
