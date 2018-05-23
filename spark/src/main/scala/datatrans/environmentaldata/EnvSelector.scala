package datatrans.environmentaldata

import datatrans.components.Selector
import play.api.libs.json.{JsDefined, JsUndefined, JsValue}

class EnvSelector extends Selector[(String, JsValue), String, LatLon] {
  def getIdentifierAndKey(r: (String, JsValue)): Seq[(String, LatLon)] = r match {
    case (p, jsvalue) =>
      jsvalue \ "lat" match {
        case JsUndefined() =>
          println("lat doesn't exists")
          Seq()
        case JsDefined(latitutestr) =>
          val lat = latitutestr.as[Double]
          jsvalue \ "lon" match {
            case JsUndefined() =>
              println("lon doesn't exists")
              Seq()
            case JsDefined(lons) =>
              val lon = lons.as[Double]
              Seq((p, LatLon(lat, lon)))
          }

      }

  }
}
