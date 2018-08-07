package datatrans

import datatrans.Utils.latlon2rowcol

object LatLon2RowCol {
  def main(args: Array[String]): Unit = {
    println(latlon2rowcol(args(0).toDouble, args(1).toDouble, args(2).toInt))
  }
}
