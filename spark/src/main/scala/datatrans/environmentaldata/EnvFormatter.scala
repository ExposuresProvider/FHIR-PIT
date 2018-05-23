package datatrans.environmentaldata

import datatrans.Utils.{CSV, Format, JSON}
import datatrans.components.Formatter
import org.apache.spark.sql.SparkSession
import play.api.libs.json.{JsDefined, JsObject, Json}

class EnvFormatter(output_format : Format) extends Formatter[SparkSession, String, Seq[JsObject]] {
  override def format(spark: SparkSession, identifier: String, value: () => Seq[JsObject]): (String, () => String) =
    (identifier, () => {
      val data = value()
      output_format match {
        case JSON =>
          data.map(obj => Json.stringify (obj)+"\n").mkString("").mkString("\n")
        case CSV(sep) =>
          val headers = data.map(obj => obj.keys).fold(Set.empty[String])((keys1, keys2) => keys1.union(keys2)).toSeq
          val rows = data.map(obj => headers.map(col => obj \ col match {
            case JsDefined(a) =>
              a.toString
            case _ =>
              ""
          }).mkString(sep)).mkString("\n")
          headers.mkString(sep) + "\n" + rows
        case _ =>
          throw new UnsupportedOperationException("unsupported output format " + output_format)
      }
    })
}
