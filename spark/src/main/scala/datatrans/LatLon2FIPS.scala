package datatrans
import java.io.File
import java.io.PrintWriter
import scala.io.Source

object LatLon2FIPS {
  def main(args: Array[String]): Unit = {
    val shpfilepath = args(0)
    val inputfilepath = args(1)
    val outputfilepath = args(2)
    val geoidfinder = new GeoidFinder(shpfilepath, "")

    val bufferedSource = Source.fromFile(inputfilepath)
    val writer = new PrintWriter(new File(outputfilepath))


    for (line <- bufferedSource.getLines) {
      val cols = line.split(",")
      val lat = cols(0).toDouble
      val lon = cols(1).toDouble
      writer.println(geoidfinder.getGeoidForLatLon(lat, lon))
    }
    writer.close()
    bufferedSource.close
  }
}
