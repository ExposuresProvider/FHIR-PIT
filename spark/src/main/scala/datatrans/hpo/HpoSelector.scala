package datatrans.hpo

import datatrans.Utils.insertOrUpdate
import datatrans.components.Selector
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import play.api.libs.json.{JsDefined, JsObject, JsUndefined, JsValue}

class HpoSelector extends Selector[(String, JsValue), String, DateTime] {
  def getIdentifierAndKey(r: (String, JsValue)): Seq[(String, DateTime)] = r match {
    case (p, jsvalue) =>
      val observation = jsvalue("observation").as[JsObject]
      val encounters = observation.fields

      encounters.foreach {
        case (_, encounter) =>
          encounter.as[JsObject].fields.foreach {
            case (concept_cd, instances) =>
              instances.as[JsObject].fields.foreach {
                case (_, modifiers) =>
                  val start_date = DateTime.parse (modifiers.as[JsObject].values.toSeq.head ("start_date").as[String], DateTimeFormat.forPattern("Y-M-d H:m:s") )
                  val (start_date_filtered, cols) = col_filter_observation(concept_cd, start_date)
                  if(start_date_filtered) {
                    start_date_set.add(start_date)
                  }
                  cols.foreach {
                    case (col, value) =>
                      insertOrUpdate(listBuf, start_date, col, value)
                  }

              }
          }
      }

  }
}
